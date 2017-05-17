package io.jacob.etlman.processor.hive

import io.jacob.etlman.metadata.DWColumn
import io.jacob.etlman.utils.HiveUtils
import org.apache.spark.SparkContext
import org.apache.spark.sql.functions._

/**
  * Created by xiaoy on 2017/5/15.
  */
class HiveStatsCollector(val sparkContext: SparkContext,
                         val tableName: String
                        ) {

  def collect(): Unit = {
    val queryStmt = "select * from %s".format(tableName)

    val tableDF = HiveUtils.getDataFromHive(queryStmt, sparkContext, 10)

    println("Total number of row is : %d".format(tableDF.count()))

    tableDF.schema.fields.foreach(c => {
      println("Collecting stats for %s ...".format(c.name))

      val aggDF = tableDF.agg(count(c.name), min(c.name), max(c.name))
      println(aggDF.schema)

      println("The row number is %s, min value is %s, the max value is %s".format(
        aggDF.head().getAs[String](0), aggDF.head().getAs[String](1), aggDF.head().getAs[String](2)))

      println("Number of null is %s".format(tableDF.where(tableDF.col(c.name).isNull).count()))

      val stats = tableDF.groupBy(c.name).count()
      println(stats.schema)

      println("The total number of values is : %d".format(stats.count()))

      println("The top 200 number of values are:")
      stats.sort("count").take(200).foreach(println)
    })
  }
}
