package io.jacob.etlman.processor.hive

import java.sql.Connection
import java.text.SimpleDateFormat
import java.util.Date

import io.jacob.etlman.utils.HiveUtils
import org.apache.spark.SparkContext
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

/**
  * Created by xiaoy on 2017/5/15.
  */
class HiveStatsCollector(val sparkContext: SparkContext,
                         val sysName: String,
                         val schemaName: String,
                         val tableName: String,
                         val loadDate: String,
                         val metaDBConnection: Connection
                        ) {

  def collect(): Unit = {
    val timeStamp = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date())

    val defaultParallelism = System.getProperty("spark.default.parallelism").toInt

    val queryStmt = "select * from %s.%s %s"
      .format(schemaName, tableName, if (loadDate == null) "" else " where data_dt_iso = '" + loadDate + "'")

    val tableDF = HiveUtils.getDataFromHive(queryStmt, sparkContext, defaultParallelism)

    val rowCount = tableDF.count()
    println("Total number of row is : %d".format(rowCount))

    saveTableStats(rowCount, timeStamp)

    tableDF.schema.fields.filter(!_.name.equalsIgnoreCase("data_dt_iso")).foreach(c => {
      println("Collecting stats for %s ...".format(c.name))

      val aggDF = tableDF.agg(count(c.name), min(c.name), max(c.name))
      println(aggDF.schema)

      val colRowCount = aggDF.head().getLong(0)
      val minVal = aggDF.head().get(1)
      val maxVal = aggDF.head().get(2)

      println("The row number is %d, min value is %s, the max value is %s".format(
        colRowCount,
        if (minVal != null) minVal.toString else "null",
        if (maxVal != null) maxVal.toString else "null"))

      val numNull = tableDF.where(tableDF.col(c.name).isNull).count()
      println("Number of null is %s".format(numNull))

      val stats = tableDF.where(c.name + " is not null").groupBy(c.name).count()
      println(stats.schema)

      val numValue = stats.count()
      println("The total number of values is : %d".format(numValue))

      println("The top 200 number of values are:")

      saveColumnStats(c.name, timeStamp,
        if (minVal != null) minVal.toString else "null",
        if (maxVal != null) maxVal.toString else "null",
        numValue, numNull, stats)
    })
  }

  def saveTableStats(rowCount: Long, timeStamp: String): Unit = {

    val sqlInsertTableStats = "insert into table_stats values ('%s', '%s', '%s', '%s', %d)".format(
      sysName, schemaName, tableName, timeStamp, rowCount
    )

    val ps = metaDBConnection.prepareStatement(sqlInsertTableStats)
    ps.executeUpdate()
  }

  def saveColumnStats(columnName: String,
                      timeStamp: String,
                      minVal: String,
                      maxVal: String,
                      numValue: Long,
                      numNull: Long,
                      stats: DataFrame): Unit = {

    val histogramId = "%s.%s.%s".format(tableName, columnName, timeStamp).replaceAll(" ", "T")

    val sqlInsertColumnStats = "insert into column_stats values ('%s', '%s', '%s', '%s', '%s', '%s', '%s', %d, %d, '%s')".format(
      sysName, schemaName, tableName, columnName, timeStamp, maxVal, minVal, numValue, numNull, histogramId
    )

    var ps = metaDBConnection.prepareStatement(sqlInsertColumnStats)
    ps.executeUpdate()

    val sqlInsertHistogram = "insert into col_value_histogram values ('%s', ?, ?)".format(histogramId)
    ps = metaDBConnection.prepareStatement(sqlInsertHistogram)

    stats.sort(desc("count")).take(200).foreach(x => {
      println(x)
      ps.setString(1, x.get(0).toString)
      ps.setLong(2, x.getLong(1))
      ps.executeUpdate()
    })
  }
}
