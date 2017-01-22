package io.jacob.etlman.utils

import io.jacob.etlman.metadata.DWColumn
import io.jacob.etlman.processor.ZipperConfig
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types.StructType

/**
  * Created by Yu on 16/7/6.
  */
object HiveUtils {

  var sqlContext: HiveContext = null

  def getDataFromHive(sqlStmt: String, sc: SparkContext, numPartitions: Int): DataFrame = {

    if (sqlContext == null) {
      sqlContext = new HiveContext(sc)
    }

    sqlContext.sql("use pdata")

    val dataFrame = sqlContext.sql(sqlStmt).coalesce(numPartitions).cache()

    if (ZipperConfig.isDebug) {
      println("Sample result of the SQL [%s]:".format(sqlStmt))
      dataFrame.show(100)

    }

    dataFrame
  }

  def saveDatatoHive(sc: SparkContext, srcTableName: String, destTableName: String,
                     monPartColName: String, monPartValue: String, otherPartColumns: List[DWColumn]): Unit = {

    if (sqlContext == null) {
      sqlContext = new HiveContext(sc)
    }

    // Here we assume that the partitioning columns other than zip month should be at the end of the
    // snapshot table column list.
    val sql = "insert overwrite table " + destTableName +
      " partition(" + monPartColName + " = '" + monPartValue + "' " +
      (otherPartColumns.map(_.phyName).reduce(_ + ", " + _) match {
        case "" => ""
        case x => ", " + x
      }) + ") " +
      "select * from " + srcTableName

    println(sql)

    sqlContext.sql(sql)
  }

  def createDataFrame(sc: SparkContext, rdd: RDD[Row], schema: StructType): DataFrame = {

    if (sqlContext == null) {
      sqlContext = new HiveContext(sc)
    }

    sqlContext.createDataFrame(rdd, schema)

  }
}
