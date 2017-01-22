package io.jacob.etlman.metadata

/**
  * Created by xiaoy on 1/17/2017.
  */
case class DWTable (
                   schemaName : String,
                   tableName : String,
                   sysName : String,
                   phyName : String,
                   loadMode : String,
                   clearMode : String,
                   keepLoadDate : Boolean,
                   doAggregate : Boolean,
                   subjectName : String,
                   isFact : Boolean,
                   comments : String
                   )
