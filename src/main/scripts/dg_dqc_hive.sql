
CREATE DATABASE dg_dqc;

USE dg_dqc;

CREATE TABLE `m07_checkerror`(
  `dqid` string, 
  `tx_date` string, 
  `taskstarttime` string, 
  `columnval` string, 
  `value1` string, 
  `value2` string)
ROW FORMAT DELIMITED 
  FIELDS TERMINATED BY '\t' 
  LINES TERMINATED BY '\n' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
;

CREATE TABLE `m07_checklist`(
  `dqid` string COMMENT 'id', 
  `start_dt` string COMMENT '', 
  `task_name` string COMMENT '', 
  `task_desc` string COMMENT '', 
  `dqtype_id` string COMMENT 'id', 
  `setting1` string COMMENT 'sql1', 
  `setting2` string COMMENT 'sql2', 
  `setting3` string COMMENT 'sql3', 
  `setting4` string COMMENT 'sql4', 
  `setting5` string COMMENT 'sql5', 
  `setting6` string COMMENT 'sql6', 
  `setting7` string COMMENT 'sql7', 
  `setting8` string COMMENT 'sql8', 
  `error_level_cd` string COMMENT '', 
  `etl_job_id` string COMMENT 'etl', 
  `dqtask_status_cd` string COMMENT '', 
  `target_table` string COMMENT '', 
  `saveerrors` string COMMENT '', 
  `targettablesavedays` string COMMENT '', 
  `owner` string COMMENT '', 
  `end_dt` string COMMENT '', 
  `owner_app` string COMMENT '', 
  `stage_cd` string COMMENT '', 
  `dq_class_cd` string, 
  `dq_subclass_id` string COMMENT '', 
  `etl_type_cd` string COMMENT '', 
  `handle_mode_cd` string COMMENT '', 
  `is_impl_busi_system` string COMMENT '', 
  `reviewer` string COMMENT '', 
  `updater` string COMMENT '', 
  `interval_cd` string COMMENT '', 
  `interval_day` string COMMENT '', 
  `interval_param_cd` string COMMENT '', 
  `task_comment` string COMMENT '')
ROW FORMAT DELIMITED 
  FIELDS TERMINATED BY '\t' 
  LINES TERMINATED BY '\n' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
;

CREATE TABLE `m07_checkresult`(
  `dqid` string COMMENT '????ID', 
  `tx_date` string COMMENT '????', 
  `taskruntime` string COMMENT '??????', 
  `error_level_cd` string COMMENT '??????????', 
  `dqresult_reclog_cd` string COMMENT '?????????', 
  `dqtask_run_type_cd` string COMMENT '????????????', 
  `result1` string, 
  `result2` string, 
  `result3` string, 
  `result4` string, 
  `result5` string, 
  `result6` string, 
  `result7` string, 
  `result8` string, 
  `result9` string, 
  `result10` string, 
  `handle_cd` string COMMENT '????', 
  `taskstarttime` string COMMENT '??????', 
  `target_table_row_count` string, 
  `handle_session_id` string COMMENT '????ID')
ROW FORMAT DELIMITED 
  FIELDS TERMINATED BY '\t' 
  LINES TERMINATED BY '\n' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
;

CREATE TABLE `m07_checkresult_log`(
  `dqid` string COMMENT 'DQID', 
  `tx_date` string COMMENT '????', 
  `taskruntime` string COMMENT '??????', 
  `log_id` string COMMENT '??????????', 
  `log_detail` string COMMENT '?????????')
ROW FORMAT DELIMITED 
  FIELDS TERMINATED BY '\t' 
  LINES TERMINATED BY '\n' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
;
