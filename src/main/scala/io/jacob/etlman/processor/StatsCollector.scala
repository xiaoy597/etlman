package io.jacob.etlman.processor

import java.util.Properties

import io.jacob.etlman.processor.hive.HiveStatsCollector
import io.jacob.etlman.utils.{HiveUtils, JDBCUtils}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by xiaoy on 2017/5/15.
  */
object StatsCollector {

  def main(args: Array[String]): Unit = {

    var sparkConf: SparkConf = null
    if (args(0).equals("local")) {
      sparkConf = new SparkConf().setAppName("StatsCollector").setMaster("local[4]")
    } else {
      sparkConf = new SparkConf().setAppName("StatsCollector")
    }

    val schemaName = args(1)
    val tableName = if (args.length > 2) args(2) else null
    val loadDate = if (args.length > 3) args(3) else null

    val props = new Properties()

    props.setProperty("dbHost", System.getenv("ETL_METADB_SERVER"))
    props.setProperty("dbName", System.getenv("ETL_METADB_DBNAME"))
    props.setProperty("userName", System.getenv("ETL_METADB_USER"))
    props.setProperty("password", System.getenv("ETL_METADB_PASSWORD"))

    val metaDBConnection = JDBCUtils.getConn(props)

    val sparkContext = new SparkContext(sparkConf)

    if (tableName == null || tableName == "-") {
      val tableList = HiveUtils.getDataFromHive("show tables in sdata", sparkContext).collect()

      tableList.foreach(t => {
        new HiveStatsCollector(sparkContext, "src_sys", schemaName, t.getString(0), loadDate, metaDBConnection).collect()
      })
    }else
      new HiveStatsCollector(sparkContext, "src_sys", schemaName, tableName, loadDate, metaDBConnection).collect()

    sparkContext.stop()

    metaDBConnection.close()
  }
}
