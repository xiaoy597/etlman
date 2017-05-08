#!/bin/sh


sqoop export --connect jdbc:mysql://datamining:3306/dg_dqc --table m07_checkresult --username root --password 123456 --export-dir /apps/hive/warehouse/dg_dqc.db/m07_checkresult --fields-terminated-by '\t'
sqoop export --connect jdbc:mysql://datamining:3306/dg_dqc --table m07_checkresult_log --username root --password 123456 --export-dir /apps/hive/warehouse/dg_dqc.db/m07_checkresult_log --fields-terminated-by '\t'
