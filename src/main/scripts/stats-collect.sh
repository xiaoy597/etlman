#!/bin/sh

DB_NAME=$1
THREAD_NUM=$2

TIME=`date +%Y%m%d%H%M`

echo "select TBL_NAME from metastore.TBLS, metastore.DBS where TBLS.DB_ID = DBS.DB_ID and DBS.NAME='sdata'"| \
mysql -uroot -proot 2>/dev/null | python stats-collect.py $DB_NAME $THREAD_NUM 1>stats-collect.${TIME}.log 2>&1

