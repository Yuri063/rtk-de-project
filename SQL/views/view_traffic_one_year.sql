-- 7.2.5 CREATE VIEW FOR TRAFFIC

create or replace view {{ params.prefix }}_view_traffic_one_year_{{ execution_date.year }} as (

	with staging as (
		with derived_columns as (
			select
				user_id,
				time_stamp,
				device_id,
				device_ip_addr,
				bytes_sent,
				bytes_received,
				user_id::varchar as USER_KEY,
				device_id::varchar as DEVICE_KEY,
				device_ip_addr::varchar as IP_ADDR_KEY,
				'TRAFFIC - DATA LAKE'::varchar as RECORD_SOURCE
			from {{ params.prefix }}_ods_traffic
			where cast(extract('year' from time_stamp) as int) = {{ execution_date.year }}
		),
		
		hashed_columns as (
			select
				user_id,
				time_stamp,
				device_id,
				device_ip_addr,
				bytes_sent,
				bytes_received,
				USER_KEY,
				DEVICE_KEY,
				IP_ADDR_KEY,
				RECORD_SOURCE,
				
				cast((md5(nullif(upper(trim(cast(user_id as varchar))), ''))) as TEXT) as USER_PK,			
				cast((md5(nullif(upper(trim(cast(device_id as varchar))), ''))) as TEXT) as DEVICE_PK,
				cast((md5(nullif(upper(trim(cast(device_ip_addr as varchar))), ''))) as TEXT) as IP_ADDR_PK,
				cast(md5(nullif(concat_ws('||',
					coalesce(nullif(upper(trim(cast(user_id as varchar))), ''), '^^'),
					coalesce(nullif(upper(trim(cast(device_id as varchar))), ''), '^^'),
					coalesce(nullif(upper(trim(cast(device_ip_addr as varchar))), ''), '^^')					
				), '^^||^^||^^')) as TEXT) as TRAFFIC_PK,				
				cast(md5(concat_ws('||',
					coalesce(nullif(upper(trim(cast(time_stamp as varchar))), ''), '^^'),
					coalesce(nullif(upper(trim(cast(bytes_sent as varchar))), ''), '^^'),
					coalesce(nullif(upper(trim(cast(bytes_received as varchar))), ''), '^^')
				)) as TEXT) as TRAFFIC_HASHDIFF
			from derived_columns
		),
		
		columns_to_select as (
			select
				user_id,
				time_stamp,
				device_id,
				device_ip_addr,
				bytes_sent,
				bytes_received,
				USER_KEY,
				DEVICE_KEY,
				IP_ADDR_KEY,
				RECORD_SOURCE,
				USER_PK,
				DEVICE_PK,
				IP_ADDR_PK,
				TRAFFIC_PK,
				TRAFFIC_HASHDIFF
			from hashed_columns
		)
		
		select * from columns_to_select
	)
	
	select *, 			
	        '{{ execution_date }}'::timestamp as LOAD_DATE,
			time_stamp as EFFECTIVE_FROM
	from staging
);
