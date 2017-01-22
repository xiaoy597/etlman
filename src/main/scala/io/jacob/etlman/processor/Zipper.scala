package io.jacob.etlman.processor

import java.sql.Connection
import java.util.Properties

import io.jacob.etlman.metadata.{DWColumn, DWTable}
import io.jacob.etlman.processor.hive.HiveZipper
import io.jacob.etlman.utils.JDBCUtils
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ListBuffer

/**
  * Created by xiaoy on 1/16/2017.
  */
object Zipper {

  def main(args : Array[String]): Unit ={

    println("Hello, this is Zipper!")

    val metaDBConnParameters = new Properties()

    metaDBConnParameters.setProperty("dbHost", System.getenv("ETL_METADB_SERVER"))
    metaDBConnParameters.setProperty("dbName", System.getenv("ETL_METADB_DBNAME"))
    metaDBConnParameters.setProperty("userName", System.getenv("ETL_METADB_USER"))
    metaDBConnParameters.setProperty("password", System.getenv("ETL_METADB_PASSWORD"))

    ZipperConfig.partitionNum = System.getenv("ETL_ZIPPER_PARTITION").toInt
    ZipperConfig.isDebug = System.getenv("ETL_ZIPPER_DEBUG").toBoolean
    ZipperConfig.schemaName = args(1)
    ZipperConfig.tableName = args(2)
    ZipperConfig.loadMonth = args(3)
    ZipperConfig.zipStartDt = args(4)
    ZipperConfig.zipEndDt = args(5)
    ZipperConfig.defaultEndDate = args(6)

    println(ZipperConfig)

    val connection = JDBCUtils.getConn(metaDBConnParameters)

    println("Connection to etl metadata database is established.")

    val tableList = getTableInfo(connection, ZipperConfig.schemaName, ZipperConfig.tableName)
    tableList.foreach(println)
    val columnList = getColumnInfo(connection, ZipperConfig.schemaName, ZipperConfig.tableName)
    columnList.foreach(println)

    JDBCUtils.closeConn(null, null, connection)

    var sparkConf : SparkConf = null
    if (args(0).equals("local")) {
      sparkConf = new SparkConf().setAppName("Zipper").setMaster("local[4]")
    }else {
      sparkConf = new SparkConf().setAppName("Zipper")
    }

    val sparkContext = new SparkContext(sparkConf)

    ZipperConfig.zipPlatform match {
      case "hive" => new HiveZipper(sparkContext, tableList.head.phyName, columnList).zip()
      case _ => throw new Exception("Unsupported zipper platform of " + ZipperConfig.zipPlatform)
    }

    println("Exiting Zipper ...")
    sparkContext.stop()
  }


  def getColumnInfo(connection : Connection, schemaName : String, tableName : String) : List[DWColumn] = {

    val columnList = ListBuffer[DWColumn]()

    val sql = "select column_name, column_id, data_type, phy_name, " +
      "agg_period, is_pk, chain_compare, comments, is_partition_key " +
      "from dw_columns " +
      "where schema_name = '" + schemaName + "' " +
      "and table_name = '" + tableName + "' " +
      "order by column_id"

    println(sql)

    try {
      val rs = connection.prepareStatement(sql).executeQuery()

      while (rs.next()){
        val column = DWColumn(
          schemaName,
          tableName,
          rs.getInt("column_id"),
          rs.getString("column_name"),
          rs.getString("data_type"),
          rs.getString("phy_name"),
          rs.getString("agg_period"),
          rs.getBoolean("is_pk"),
          rs.getBoolean("chain_compare"),
          rs.getBoolean("is_partition_key"),
          rs.getString("comments")
        )

        columnList += column
      }

      rs.close()

      columnList.toList

    }catch{
      case e : Exception =>
        throw e
    }
  }

  def getTableInfo(connection : Connection, schemaName : String, tableName : String) : List[DWTable] = {

    val sql = "select * " +
      "from dw_table " +
      "where schema_name = '" + schemaName + "' " +
      "and table_name = '" + tableName + "' "

    println(sql)

    val tableList = ListBuffer[DWTable]()

    try {
      val rs = connection.prepareStatement(sql).executeQuery()

      while (rs.next()){
        val table = DWTable(
          rs.getString("schema_name"),
          rs.getString("table_name"),
          rs.getString("sys_name"),
          rs.getString("phy_name"),
          rs.getString("load_mode"),
          rs.getString("clear_mode"),
          rs.getBoolean("keep_load_dt"),
          rs.getBoolean("do_aggregate"),
          rs.getString("subject_name"),
          rs.getBoolean("is_fact"),
          rs.getString("comments")
        )

        tableList += table
      }

      rs.close()

      tableList.toList
    }catch{
      case e : Exception =>
        throw e
    }
  }

}

