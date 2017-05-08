package io.jacob.etlman.processor

/**
  * Created by Yu on 16/5/24.
  */
object ZipperConfig {
  var isDebug = true
  var defaultEndDate = "2099-12-31"
  var loadMonth = "2000-01"
  var loadDateColName = "data_dt_iso"
  var loadMonColName = "data_month"
  var zipPlatform = "hive"
  var zipStartDt = "0000-00-00"
  var zipEndDt = "0000-00-00"
  var schemaName = "pdata"
  var tableName = ""
  var partitionNum = 4
  var zipDBName = "dblake"

  override def toString: String = {
    s"isDebug: ${isDebug}, partitionNum: ${partitionNum}, zipPlatform: ${zipPlatform}, schemaName: ${schemaName}, tableName: ${tableName}" +
    "\n" +
    s"defaultEndDate: ${defaultEndDate}, loadMonth: ${loadMonth}, zipStartDt: ${zipStartDt}, zipEndDt: ${zipEndDt}" +
    "\n" +
    s"loadDateColName: ${loadDateColName}, loadMonColName: ${loadMonColName}"
  }
}
