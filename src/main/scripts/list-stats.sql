/*
 * 查询最新的统计信息
 *
 */
use etl_metadata;

select t.* 
from table_stats t, 
(select table_name, max(collect_time) max_time from table_stats group by table_name) t2
where t.table_name = t2.table_name and t.collect_time = t2.max_time;

select c.* 
from column_stats c, 
(select table_name, column_name, max(collect_time) max_time from column_stats group by table_name, column_name) c2
where c.table_name = c2.table_name 
and c.column_name = c2.column_name 
and c.collect_time = c2.max_time
;

select c.table_name, c.column_name, h.* 
from col_value_histogram h, column_stats c , 
(select table_name, column_name, max(collect_time) max_time
	from column_stats 
	group by table_name, column_name
) c2
where h.value_histogram_id = c.value_histogram_id 
and c.table_name = c2.table_name  and c.column_name = c2.column_name
and c.collect_time = c2.max_time
order by c.table_name asc, c.column_name asc, h.mode_value_freq desc;

