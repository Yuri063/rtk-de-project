-- 7.1.5 insert into ODS: MDM

alter table if exists {{ params.prefix }}_ods_mdm truncate partition for('{{execution_date}}');
insert into {{ params.prefix }}_ods_mdm (
	select *
	from mdm.user
	where cast(extract('year' from cast(registered_at as timestamp)) as int) = {{execution_date.year}}
);
