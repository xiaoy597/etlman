#!/bin/sh


sqoop import --connect jdbc:mysql://datamining:3306/dg_dqc --username root --password 123456 --table m07_checklist --hive-import --hive-table dg_dqc.m07_checklist --hive-import -hive-overwrite --hive-drop-import-delims -m 1 --fields-terminated-by '\t'

