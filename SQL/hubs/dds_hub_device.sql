-- 5.1.10 insert HUB_DEVICE, SOURCE:TRAFFIC

with row_rank_1 as (
	select * from (
		select DEVICE_PK, DEVICE_KEY, LOAD_DATE, RECORD_SOURCE,
			row_number() over (
				partition by DEVICE_PK
				order by LOAD_DATE ASC
			) as row_num
		from {{ params.prefix }}_view_traffic_one_year_{{ execution_date.year }}		
	) as h where row_num = 1
),
records_to_insert as (
		select a.DEVICE_PK, a.DEVICE_KEY, a.LOAD_DATE, a.RECORD_SOURCE
		from row_rank_1 as a
		left join {{ params.prefix }}_dds_hub_device as d
		on a.DEVICE_PK = d.DEVICE_PK
		where d.DEVICE_PK is NULL
)
insert into {{ params.prefix }}_dds_hub_device (DEVICE_PK, DEVICE_KEY, LOAD_DATE, RECORD_SOURCE)
(
	select DEVICE_PK, DEVICE_KEY, LOAD_DATE, RECORD_SOURCE
	from records_to_insert
);		
