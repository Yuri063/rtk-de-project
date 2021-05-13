-- 5.3.6 insert SAT_TRAFFIC_DETAILS, SOURCE:TRAFFIC

with source_data as (
	select 
		TRAFFIC_PK, TRAFFIC_HASHDIFF, 
		time_stamp, bytes_sent, bytes_received,
		EFFECTIVE_FROM, 
		LOAD_DATE, RECORD_SOURCE
	from {{ params.prefix }}_view_traffic_one_year_{{ execution_date.year }}
),
update_records as (
	select 
		a.TRAFFIC_PK, a.TRAFFIC_HASHDIFF, 
		a.time_stamp, a.bytes_sent, a.bytes_received,
		a.EFFECTIVE_FROM, 
		a.LOAD_DATE, a.RECORD_SOURCE
	from {{ params.prefix }}_dds_sat_traffic_details as a
	join source_data as b
	on a.TRAFFIC_PK = b.TRAFFIC_PK
	where  a.LOAD_DATE <= b.LOAD_DATE
),
latest_records as (
	select * from (
		select TRAFFIC_PK, TRAFFIC_HASHDIFF, LOAD_DATE,
			case when rank() over (partition by TRAFFIC_PK order by LOAD_DATE desc) = 1
				then 'Y' 
				else 'N'
			end as latest
		from update_records
	) as s
	where latest = 'Y'
),	
records_to_insert as (
	select distinct 
		e.TRAFFIC_PK, e.TRAFFIC_HASHDIFF, 
		e.time_stamp, e.bytes_sent, e.bytes_received,
		e.EFFECTIVE_FROM, 
		e.LOAD_DATE, e.RECORD_SOURCE
	from source_data as e
	left join latest_records
	on latest_records.TRAFFIC_HASHDIFF = e.TRAFFIC_HASHDIFF and 
	   latest_records.TRAFFIC_PK = e.TRAFFIC_PK
	where latest_records.TRAFFIC_HASHDIFF is NULL
)	
insert into {{ params.prefix }}_dds_sat_traffic_details (
	TRAFFIC_PK, TRAFFIC_HASHDIFF, 
	time_stamp, bytes_sent, bytes_received,
	EFFECTIVE_FROM, 
	LOAD_DATE, RECORD_SOURCE)
(
	select 
		TRAFFIC_PK, TRAFFIC_HASHDIFF, 
		time_stamp, bytes_sent, bytes_received,
		EFFECTIVE_FROM, 
		LOAD_DATE, RECORD_SOURCE
	from records_to_insert
);
