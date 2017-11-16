@echo off

if "%1" == "" goto NOPARAM
if "%2" == "" goto NOPARAM

set ETL_METADB_SERVER=127.0.0.1:3306
set ETL_METADB_DBNAME=etl_metadata_jx
set ETL_METADB_USER=root
set ETL_METADB_PASSWORD=root

java -classpath "etlman-1.1-SNAPSHOT.jar;lib\log4j-1.2.17.jar;lib\mysql-connector-java-5.1.18.jar;lib\slf4j-api-1.7.12.jar;lib\slf4j-log4j12-1.7.10.jar;" io.jacob.etlman.ETLMan %1 %2

goto END

:NOPARAM
echo "Usage: job-script.bat <dw-table-name> <script-path>"

:END