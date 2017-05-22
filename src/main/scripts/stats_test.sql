create table sdata.stats_test
(
	int_col		int,
	str_col		string,
	char_col	char(10),
	vchar_col	varchar(100),
	date_col	date,
	bigint_col	bigint,
	float_col	float,
	double_col	double,
	dec_col		decimal(20,4),
	ts_col		timestamp,
	bool_col	boolean
)
partitioned by (data_dt_iso string)
row format delimited
fields terminated by '\t'
lines terminated by '\n'
;
