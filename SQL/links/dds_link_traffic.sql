-- 5.2.5 insert LINK_TRAFFIC, SOURCE:TRAFFIC

with source_data as (
	select 
		TRAFFIC_PK,
		USER_PK, DEVICE_PK, IP_ADDR_PK,
		LOAD_DATE, RECORD_SOURCE
	from {{ params.prefix }}_view_traffic_one_year_{{ execution_date.year }}
),
records_to_insert as (
	select distinct 
		stg.TRAFFIC_PK, 
		stg.USER_PK, stg.DEVICE_PK, stg.IP_ADDR_PK,
		stg.LOAD_DATE, stg.RECORD_SOURCE
	from source_data as stg 
	left join {{ params.prefix }}_dds_link_traffic as tgt
	on stg.TRAFFIC_PK = tgt.TRAFFIC_PK
	where tgt.TRAFFIC_PK is null		
)
insert into {{ params.prefix }}_dds_link_traffic (
	TRAFFIC_PK,
	USER_PK, DEVICE_PK, IP_ADDR_PK,
	LOAD_DATE, RECORD_SOURCE)
(
	select 
		TRAFFIC_PK,
		USER_PK, DEVICE_PK, IP_ADDR_PK,
		LOAD_DATE, RECORD_SOURCE
	from records_to_insert
);
