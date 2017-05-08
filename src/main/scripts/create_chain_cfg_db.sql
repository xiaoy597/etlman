CREATE DATABASE chain_cfg CHARACTER SET UTF8;
USE chain_cfg;

CREATE TABLE analysis_question
(
	column_name          VARCHAR(64) NOT NULL,
	serial_no            INTEGER NULL,
	question             VARCHAR(512) NULL,
	reply                VARCHAR(512) NULL,
	submit_date          DATE NULL,
	reply_date           DATE NULL,
	reply_prsn           VARCHAR(64) NULL,
	comments             VARCHAR(512) NULL,
	sys_name             VARCHAR(64) NOT NULL,
	schema_name          VARCHAR(64) NOT NULL,
	table_name           VARCHAR(64) NOT NULL
);

ALTER TABLE analysis_question
ADD PRIMARY KEY (sys_name,schema_name,table_name,column_name);

CREATE TABLE dw_column_mapping
(
	column_name          VARCHAR(64) NOT NULL,
	src_column_name      VARCHAR(64) NOT NULL,
	column_expr          TEXT NULL,
	comments             VARCHAR(512) NULL,
	load_batch           INTEGER NOT NULL,
	schema_name          VARCHAR(64) NOT NULL,
	src_schema           VARCHAR(64) NOT NULL,
	table_name           VARCHAR(64) NOT NULL,
	src_table_name       VARCHAR(64) NOT NULL,
	sys_name             VARCHAR(64) NOT NULL,
	src_sys_name         VARCHAR(64) NOT NULL,
	load_group           INTEGER NOT NULL
);

ALTER TABLE dw_column_mapping
ADD PRIMARY KEY (column_name,schema_name,table_name,sys_name,load_group,src_sys_name,src_schema,src_table_name,src_column_name,load_batch);

CREATE TABLE dw_column_mapping_his
(
	column_name          VARCHAR(18) NOT NULL,
	src_column_name      VARCHAR(18) NULL,
	column_expr          TEXT NULL,
	comments             VARCHAR(512) NULL,
	load_batch           INTEGER NOT NULL,
	schema_name          VARCHAR(18) NOT NULL,
	src_schema           VARCHAR(18) NULL,
	table_name           VARCHAR(18) NOT NULL,
	src_table_name       VARCHAR(18) NULL,
	sys_name             VARCHAR(18) NOT NULL,
	src_sys_name         VARCHAR(18) NULL,
	version              INTEGER NOT NULL
);

ALTER TABLE dw_column_mapping_his
ADD PRIMARY KEY (column_name,load_batch,schema_name,table_name,sys_name,version);

CREATE TABLE dw_columns
(
	column_name          VARCHAR(64) NOT NULL,
	data_type            VARCHAR(64) NULL,
	phy_name             VARCHAR(64) NULL,
	agg_period           VARCHAR(20) NULL,
	is_pk                boolean NULL,
	chain_compare        boolean NULL,
	comments             VARCHAR(512) NULL,
	column_id            INTEGER NULL,
	schema_name          VARCHAR(64) NOT NULL,
	table_name           VARCHAR(64) NOT NULL,
	sys_name             VARCHAR(64) NOT NULL,
	is_partition_key     boolean NULL
);

ALTER TABLE dw_columns
ADD PRIMARY KEY (column_name,schema_name,table_name,sys_name);

CREATE TABLE dw_subject
(
	subject_name         VARCHAR(64) NOT NULL,
	comments             VARCHAR(512) NULL,
	cn_name              VARCHAR(64) NULL,
	phy_name             VARCHAR(64) NULL
);

ALTER TABLE dw_subject
ADD PRIMARY KEY (subject_name);

CREATE TABLE dw_table
(
	phy_name             VARCHAR(64) NULL,
	load_mode            VARCHAR(20) NULL,
	clear_mode           VARCHAR(20) NULL,
	keep_load_dt         boolean NULL,
	do_aggregate         boolean NULL,
	comments             VARCHAR(512) NULL,
	schema_name          VARCHAR(64) NOT NULL,
	table_name           VARCHAR(64) NOT NULL,
	sys_name             VARCHAR(64) NOT NULL,
	subject_name         VARCHAR(64) NULL,
	is_fact              boolean NULL
);

ALTER TABLE dw_table
ADD PRIMARY KEY (schema_name,table_name,sys_name);

CREATE TABLE dw_table_mapping
(
	table_alias          VARCHAR(64) NULL,
	join_order           INTEGER NOT NULL,
	join_type            VARCHAR(20) NULL,
	join_condition       TEXT NULL,
	filter_condition     TEXT NULL,
	comments             VARCHAR(512) NULL,
	load_batch           INTEGER NOT NULL,
	schema_name          VARCHAR(64) NOT NULL,
	src_schema           VARCHAR(64) NOT NULL,
	table_name           VARCHAR(64) NOT NULL,
	src_table_name       VARCHAR(64) NOT NULL,
	sys_name             VARCHAR(64) NOT NULL,
	src_sys_name         VARCHAR(64) NOT NULL
);

ALTER TABLE dw_table_mapping
ADD PRIMARY KEY (load_batch,schema_name,src_schema,table_name,src_table_name,sys_name,src_sys_name,join_order);

CREATE TABLE dw_table_mapping_his
(
	table_alias          VARCHAR(64) NULL,
	join_order           INTEGER NULL,
	join_type            VARCHAR(20) NULL,
	join_condition       TEXT NULL,
	filter_condition     TEXT NULL,
	comments             VARCHAR(512) NULL,
	load_batch           INTEGER NOT NULL,
	schema_name          VARCHAR(18) NOT NULL,
	src_schema           VARCHAR(18) NOT NULL,
	table_name           VARCHAR(18) NOT NULL,
	src_table_name       VARCHAR(18) NOT NULL,
	sys_name             VARCHAR(18) NOT NULL,
	src_sys_name         VARCHAR(18) NOT NULL,
	version              INTEGER NOT NULL
);

ALTER TABLE dw_table_mapping_his
ADD PRIMARY KEY (load_batch,schema_name,src_schema,table_name,src_table_name,sys_name,src_sys_name,version);

CREATE TABLE ent_system
(
	sys_name             VARCHAR(64) NOT NULL,
	comments             VARCHAR(512) NULL,
	cn_name              VARCHAR(64) NULL
);

ALTER TABLE ent_system
ADD PRIMARY KEY (sys_name);

CREATE TABLE etl_developer
(
	etl_dvlpr_name       VARCHAR(64) NOT NULL,
	comments             VARCHAR(512) NULL
);

ALTER TABLE etl_developer
ADD PRIMARY KEY (etl_dvlpr_name);

CREATE TABLE etl_tables
(
	schema_name          VARCHAR(64) NOT NULL,
	comments             VARCHAR(512) NULL,
	table_name           VARCHAR(64) NOT NULL,
	sys_name             VARCHAR(64) NOT NULL,
	cn_name              VARCHAR(64) NULL
);

ALTER TABLE etl_tables
ADD PRIMARY KEY (schema_name,table_name,sys_name);

CREATE TABLE etl_tasks
(
	task_name            VARCHAR(128) NOT NULL,
	comments             VARCHAR(512) NULL,
	schema_name          VARCHAR(64) NULL,
	etl_dvlpr_name       VARCHAR(64) NULL,
	table_name           VARCHAR(64) NULL,
	sys_name             VARCHAR(64) NULL,
	plan_start_dt        DATE NULL,
	plan_finish_dt       DATE NULL,
	actual_finish_dt     DATE NULL,
	task_status          VARCHAR(20) NULL,
	actual_start_dt      DATE NULL,
	serial_no            INTEGER NULL
);

ALTER TABLE etl_tasks
ADD PRIMARY KEY (task_name);

CREATE TABLE etl_tasks_his
(
	etl_dvlpr_name       VARCHAR(18) NULL,
	src_table_name       VARCHAR(20) NULL,
	version              INTEGER NOT NULL,
	schema_name          VARCHAR(18) NULL,
	comments             VARCHAR(512) NULL,
	timestamp            VARCHAR(20) NULL,
	task_name            VARCHAR(128) NOT NULL,
	sys_name             VARCHAR(64) NULL
);

ALTER TABLE etl_tasks_his
ADD PRIMARY KEY (version,task_name);

CREATE TABLE src_column_analysis
(
	column_name          VARCHAR(64) NOT NULL,
	data_type            VARCHAR(20) NULL,
	allow_null           boolean NULL,
	is_pk                boolean NULL,
	cn_name              VARCHAR(64) NULL,
	null_count           INTEGER NULL,
	uv_count             INTEGER NULL,
	doc_data_type        VARCHAR(20) NULL,
	doc_cn_name          VARCHAR(64) NULL,
	doc_allow_null       boolean NULL,
	doc_is_pk            boolean NULL,
	comments             VARCHAR(512) NULL,
	schema_name          VARCHAR(64) NOT NULL,
	table_name           VARCHAR(64) NOT NULL,
	column_len           INTEGER NULL,
	column_id            INTEGER NULL,
	ref_table            VARCHAR(64) NULL,
	ref_column           VARCHAR(64) NULL,
	doc_col_len          INTEGER NULL,
	doc_ref_tbl          VARCHAR(64) NULL,
	doc_ref_col          VARCHAR(64) NULL,
	uv_check             boolean NULL,
	null_check           boolean NULL,
	value_check          boolean NULL,
	ref_check            boolean NULL,
	unique_values        TEXT NULL,
	ref_ok               boolean NULL,
	sys_name             VARCHAR(64) NOT NULL
);

ALTER TABLE src_column_analysis
ADD PRIMARY KEY (column_name,schema_name,table_name,sys_name);

CREATE TABLE src_column_analysis_his
(
	column_name          VARCHAR(64) NOT NULL,
	data_type            VARCHAR(20) NULL,
	allow_null           boolean NULL,
	is_pk                boolean NULL,
	cn_name              VARCHAR(64) NULL,
	null_count           INTEGER NULL,
	uv_count             INTEGER NULL,
	doc_data_type        VARCHAR(20) NULL,
	doc_cn_name          VARCHAR(64) NULL,
	doc_allow_null       boolean NULL,
	doc_is_pk            boolean NULL,
	comments             VARCHAR(512) NULL,
	schema_name          VARCHAR(18) NOT NULL,
	table_name           VARCHAR(18) NOT NULL,
	column_len           INTEGER NULL,
	column_id            INTEGER NULL,
	ref_table            VARCHAR(64) NULL,
	ref_column           VARCHAR(64) NULL,
	doc_col_len          INTEGER NULL,
	doc_ref_tbl          VARCHAR(64) NULL,
	doc_ref_col          VARCHAR(64) NULL,
	uv_check             boolean NULL,
	null_check           boolean NULL,
	value_check          boolean NULL,
	ref_check            boolean NULL,
	unique_values        TEXT NULL,
	ref_ok               boolean NULL,
	sys_name             VARCHAR(18) NOT NULL,
	version              INTEGER NOT NULL
);

ALTER TABLE src_column_analysis_his
ADD PRIMARY KEY (column_name,schema_name,table_name,sys_name,version);

CREATE TABLE src_table_analysis
(
	comments             VARCHAR(512) NULL,
	cn_name              VARCHAR(64) NULL,
	row_count            INTEGER NULL,
	schema_name          VARCHAR(64) NOT NULL,
	table_name           VARCHAR(64) NOT NULL,
	description          VARCHAR(512) NULL,
	need_ext             boolean NULL,
	no_ext_cmt           VARCHAR(512) NULL,
	need_int             boolean NULL,
	no_int_cmt           VARCHAR(512) NULL,
	ext_cycle            VARCHAR(64) NULL,
	is_inc_ext           boolean NULL,
	ext_condition        TEXT NULL,
	stbl_name            VARCHAR(64) NULL,
	serial_no            INTEGER NULL,
	sys_name             VARCHAR(64) NOT NULL,
	load_to_fact         boolean NULL
);

ALTER TABLE src_table_analysis
ADD PRIMARY KEY (schema_name,table_name,sys_name);

CREATE TABLE src_table_analysis_his
(
	comments             VARCHAR(512) NULL,
	cn_name              VARCHAR(64) NULL,
	row_count            INTEGER NULL,
	schema_name          VARCHAR(18) NOT NULL,
	table_name           VARCHAR(18) NOT NULL,
	description          VARCHAR(512) NULL,
	need_ext             boolean NULL,
	no_ext_cmt           VARCHAR(512) NULL,
	need_int             boolean NULL,
	no_int_cmt           VARCHAR(512) NULL,
	ext_cycle            VARCHAR(64) NULL,
	is_inc_ext           boolean NULL,
	ext_condition        TEXT NULL,
	stbl_name            VARCHAR(64) NULL,
	serial_no            INTEGER NULL,
	sys_name             VARCHAR(18) NOT NULL,
	version              INTEGER NOT NULL,
	load_to_fact         boolean NULL
);

ALTER TABLE src_table_analysis_his
ADD PRIMARY KEY (schema_name,table_name,sys_name,version);

CREATE TABLE src_tbl_subject
(
	schema_name          VARCHAR(64) NOT NULL,
	table_name           VARCHAR(64) NOT NULL,
	sys_name             VARCHAR(64) NOT NULL,
	subject_name         VARCHAR(64) NOT NULL
);

ALTER TABLE src_tbl_subject
ADD PRIMARY KEY (schema_name,table_name,sys_name,subject_name);

CREATE TABLE system_properties
(
	property_name        VARCHAR(36) NOT NULL,
	peroperty_value      VARCHAR(36) NULL
);

ALTER TABLE system_properties
ADD PRIMARY KEY (property_name);

CREATE TABLE table_schema
(
	schema_name          VARCHAR(64) NOT NULL,
	comments             VARCHAR(512) NULL,
	schema_type          VARCHAR(64) NULL,
	sys_name             VARCHAR(64) NOT NULL
);

ALTER TABLE table_schema
ADD PRIMARY KEY (schema_name,sys_name);

CREATE TABLE vocabulary
(
	cn_word              VARCHAR(64) NOT NULL,
	en_word              VARCHAR(64) NULL,
	comments             VARCHAR(512) NULL
);

