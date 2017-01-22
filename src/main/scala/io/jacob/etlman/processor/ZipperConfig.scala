package io.jacob.etlman.processor

/**
  * Created by Yu on 16/5/24.
  */
object ZipperConfig {
  var isDebug = true
  var defaultEndDate = "20991231"
  var loadMonth = "200001"
  var loadDateColName = "data_dt"
  var loadMonColName = "data_mon"
  var zipPlatform = "hive"
  var zipStartDt = "00000000"
  var zipEndDt = "00000000"
  var schemaName = "pdata"
  var tableName = ""
  var partitionNum = 4

  override def toString: String = {
    s"isDebug: ${isDebug}, partitionNum: ${partitionNum}, zipPlatform: ${zipPlatform}, schemaName: ${schemaName}, tableName: ${tableName}" +
    "\n" +
    s"defaultEndDate: ${defaultEndDate}, loadMonth: ${loadMonth}, zipStartDt: ${zipStartDt}, zipEndDt: ${zipEndDt}" +
    "\n" +
    s"loadDateColName: ${loadDateColName}, loadMonColName: ${loadMonColName}"
  }
}
