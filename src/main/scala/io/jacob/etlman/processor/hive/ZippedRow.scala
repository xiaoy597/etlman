package io.jacob.etlman.processor.hive

import io.jacob.etlman.metadata.DWColumn
import org.apache.spark.sql.Row

/**
  * Created by xiaoy on 1/17/2017.
  */
case class ZippedRow(content : Row, start_dt : String, var end_dt : String) {
  def canZip(newRow : Row, compareColumns : List[DWColumn]) : Boolean = {
    for (c <- compareColumns){
      if (content.getAs[String](c.phyName) != newRow.getAs[String](c.phyName))
        return false
    }
    true
  }
}
