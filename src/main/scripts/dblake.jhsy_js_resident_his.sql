CREATE TABLE `dblake.jhsy_js_resident_his`(
  `start_dt` string comment '拉链开始日期',
  `end_dt` string comment '拉链结束日期',
  `uuid` string COMMENT '', 
  `resident_name` string COMMENT '', 
  `certificate_type_code` string COMMENT '', 
  `certificate_number` string COMMENT '', 
  `gender_code` string COMMENT '', 
  `gender_name` string COMMENT '', 
  `nation_code` string COMMENT '', 
  `nation_name` string COMMENT '', 
  `nationality_code` string COMMENT '', 
  `nationality_name` string COMMENT '', 
  `birth_date` string COMMENT '', 
  `death_date` string COMMENT '', 
  `residence_region_code` string COMMENT '', 
  `resident_address_info` string COMMENT '', 
  `household_region_code` string COMMENT '', 
  `household_address_info` string COMMENT '', 
  `household_nature_code` string COMMENT '', 
  `household_nature_name` string COMMENT '', 
  `education_code` string COMMENT '', 
  `education_name` string COMMENT '', 
  `health_code` string COMMENT '', 
  `health_name` string COMMENT '', 
  `dataflag` string COMMENT '', 
  `synflag` string COMMENT '', 
  `syndate` string COMMENT '', 
  `merriage_type_code` string COMMENT '', 
  `flow_flag` string COMMENT '',
  `insert_date` string COMMENT '')
COMMENT ''
PARTITIONED BY ( 
  `data_month` string)
ROW FORMAT DELIMITED 
  FIELDS TERMINATED BY '\u0001' 
  LINES TERMINATED BY '\n' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
;
