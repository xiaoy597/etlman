package io.jacob.etlman.processor

import io.jacob.etlman.processor.hive.HiveStatsCollector
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by xiaoy on 2017/5/15.
  */
object StatsCollector {

  def main(args : Array[String]): Unit ={

    var sparkConf : SparkConf = null
    if (args(0).equals("local")) {
      sparkConf = new SparkConf().setAppName("StatsCollector").setMaster("local[4]")
    }else {
      sparkConf = new SparkConf().setAppName("StatsCollector")
    }

    val sparkContext = new SparkContext(sparkConf)

    new HiveStatsCollector(sparkContext, "t1").collect()

    sparkContext.stop()
  }
}
