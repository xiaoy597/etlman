DROP TABLE IF EXISTS col_value_histogram;
CREATE TABLE col_value_histogram
(
	value_histogram_id   VARCHAR(20) NOT NULL COMMENT '值域分布ID',
	mode_value           VARCHAR(128) NOT NULL COMMENT '特征值',
	mode_value_freq     INTEGER NOT NULL COMMENT '特征值出现频率'
) COMMENT '字段值域分布表';

ALTER TABLE col_value_histogram
ADD PRIMARY KEY (value_histogram_id,mode_value);

DROP TABLE IF EXISTS column_stats;
CREATE TABLE column_stats
(
	sys_name             VARCHAR(64) NOT NULL COMMENT '系统名称',
	schema_name          VARCHAR(64) NOT NULL COMMENT '数据库名/模式名',
	table_name           VARCHAR(64) NOT NULL COMMENT '表名',
	column_name          VARCHAR(64) NOT NULL COMMENT '字段名',
	collect_time         DATETIME NOT NULL COMMENT '统计时间',
	max_value            VARCHAR(128) NULL COMMENT '最大值',
	min_value            VARCHAR(128) NULL COMMENT '最小值',
	value_count          INTEGER NULL COMMENT '唯一值数目',
	null_count           INTEGER NULL COMMENT '空值数目',
	value_histogram_id   VARCHAR(20) NULL COMMENT '值域分布ID'
) COMMENT '字段统计信息表';

ALTER TABLE column_stats
ADD PRIMARY KEY (sys_name,schema_name,table_name,column_name,collect_time);

DROP TABLE IF EXISTS table_stats;
CREATE TABLE table_stats
(
	sys_name             VARCHAR(64) NOT NULL COMMENT '系统名称',
	schema_name          VARCHAR(64) NOT NULL COMMENT '数据库名/模式名',
	table_name           VARCHAR(64) NOT NULL COMMENT '表名',
	collect_time         DATETIME NOT NULL COMMENT '统计时间',
	row_count            INTEGER NULL COMMENT '记录数目'
) COMMENT '表统计信息表';

ALTER TABLE table_stats
ADD PRIMARY KEY (sys_name,schema_name,table_name,collect_time);
