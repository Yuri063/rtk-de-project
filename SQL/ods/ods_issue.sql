-- 7.1.3 insert into ODS: ISSUE

alter table if exists {{ params.prefix }}_ods_issue truncate partition for('{{execution_date}}');
insert into {{ params.prefix }}_ods_issue (
	select 
		user_id::INT, 
		start_time::TIMESTAMP,
		end_time::TIMESTAMP,
		title,
		description,
		service
	from {{ params.prefix }}_stg_issue
	where cast(extract('year' from cast(start_time as timestamp)) as int) = {{execution_date.year}}
);
