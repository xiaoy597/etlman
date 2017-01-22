package io.jacob.etlman.processor.hive

import io.jacob.etlman.metadata.DWColumn
import io.jacob.etlman.processor.ZipperConfig
import io.jacob.etlman.utils.HiveUtils
import org.apache.spark.SparkContext
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

import scala.collection.mutable.ListBuffer

/**
  * Created by xiaoy on 1/16/2017.
  */
class HiveZipper(val sparkContext : SparkContext,
                 val tableName : String,
                 val columnList : List[DWColumn]
                ) {

  val pkColumnList : List[DWColumn] = columnList.filter(_.isPK == true)
  val compareColumnList : List[DWColumn] = columnList.filter(_.chainCompare == true)
  val partitionColumnList : List[DWColumn] = columnList.filter(_.isPartitionKey == true)

  def zip() : Unit = {
    val querySql = "select * from %s where %s >= '%s' and %s <= '%s'".format(
      tableName, ZipperConfig.loadDateColName, ZipperConfig.zipStartDt, ZipperConfig.loadDateColName, ZipperConfig.zipEndDt)

    val tableDF = HiveUtils.getDataFromHive(querySql , sparkContext, ZipperConfig.partitionNum)

    val compareColumnListBroadcast = sparkContext.broadcast(compareColumnList)
    val columnListBroadcast = sparkContext.broadcast(columnList)
    val pkColumnListBroadcast = sparkContext.broadcast(pkColumnList)

    val loadDateColName = ZipperConfig.loadDateColName
    val defaultEndDate = ZipperConfig.defaultEndDate

    val keyWithRows = tableDF.map(
      row => ((for (c <- pkColumnListBroadcast.value) yield row.getAs[String](c.phyName)).reduce(_ + "," + _), row)
    ).groupByKey().mapValues(rowSet => rowSet.toList.sortBy(_.getAs[String](loadDateColName)))

    val keyWithZippedRows = keyWithRows.mapValues(rows => {
      var zippedRows = List[ZippedRow]()
      for (row <- rows){
        if (zippedRows.isEmpty){
          val zippedRow = ZippedRow(row, row.getAs[String](loadDateColName), defaultEndDate)
          zippedRows = zippedRow :: zippedRows
        }else{
          if (zippedRows.head.canZip(row, compareColumnListBroadcast.value)){
            zippedRows.head.end_dt = row.getAs[String](loadDateColName)
          }else{
            val zippedRow = ZippedRow(row, row.getAs[String](loadDateColName), defaultEndDate)
            zippedRows = zippedRow :: zippedRows
          }
        }
      }
      zippedRows
    })

    keyWithZippedRows.take(100).foreach(println)

    // Here we assume the the start_dt and end_dt column be placed at the top
    // of the zipped table column list.
    // And the dynamic paritioning column should be at the tail of the column list.
    val structFieldList = ListBuffer[StructField](StructField("start_dt", StringType, true),
      StructField("end_dt", StringType, true))
    structFieldList ++= columnList.map(
      c => StructField(
        c.phyName,
        c.dataType.toLowerCase() match {
          case "string" => StringType
          case "int" => IntegerType
          case _ => StringType
        }, true))

    val schema = StructType(structFieldList)

    println(schema)

    val zRDD = keyWithZippedRows.flatMap(zr => zr._2).map(r => {
      val valueList = ListBuffer[Any](r.start_dt, r.end_dt)
      for (c <- columnListBroadcast.value){
        c.dataType.toLowerCase() match {
          case "string" => valueList += r.content.getAs[String](c.phyName)
          case "int" => valueList += r.content.getAs[Int](c.phyName)
          case _ => valueList += r.content.getAs[String](c.phyName)
        }
      }

      Row(valueList.toArray:_*)
    })

    zRDD.take(100).foreach(println)

    val zDF = HiveUtils.createDataFrame(sparkContext, zRDD, schema)

    val tempTableName = "temp_table_for_" + tableName

    zDF.registerTempTable(tempTableName)

    HiveUtils.saveDatatoHive(sparkContext, tempTableName, tableName + "_his",
      ZipperConfig.loadMonColName, ZipperConfig.loadMonth, partitionColumnList)
  }
}
