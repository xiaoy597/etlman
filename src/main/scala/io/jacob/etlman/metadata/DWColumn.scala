package io.jacob.etlman.metadata

/**
  * Created by xiaoy on 1/16/2017.
  */
case class DWColumn (
                    schemaName : String,
                    tableName : String,
                    columnId : Int,
                    columnName : String,
                    dataType : String,
                    phyName : String,
                    aggPeriod : String,
                    isPK : Boolean,
                    chainCompare : Boolean,
                    isPartitionKey : Boolean,
                    comments: String
                    )
