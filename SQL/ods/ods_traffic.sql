-- 7.1.4 insert into ODS: TRAFFIC

alter table if exists {{ params.prefix }}_ods_traffic truncate partition for('{{execution_date}}');
insert into {{ params.prefix }}_ods_traffic (
	select 
		user_id, 			
		to_timestamp(div(timestamp, 1000))::TIMESTAMP as time_stamp,
		device_id,   
		device_ip_addr, 
		bytes_sent::BIGINT,
		bytes_received::BIGINT
	from {{ params.prefix }}_stg_traffic
	where cast(extract(year from to_timestamp(div(timestamp, 1000))) as int) = {{execution_date.year}}
);
