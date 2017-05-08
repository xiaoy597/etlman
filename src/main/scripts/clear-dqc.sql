use dg_dqc;

show tables;

insert overwrite table m07_checkerror select * from m07_checkerror where 1 = 0;
insert overwrite table m07_checkresult select * from m07_checkresult where 1 = 0;
insert overwrite table m07_checkresult_log select * from m07_checkresult_log where 1 = 0;

