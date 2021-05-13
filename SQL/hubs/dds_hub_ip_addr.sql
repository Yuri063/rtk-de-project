-- 5.1.10 insert HUB_IP_ADDR, SOURCE:TRAFFIC

with row_rank_1 as (
	select * from (
		select IP_ADDR_PK, IP_ADDR_KEY, LOAD_DATE, RECORD_SOURCE,
			row_number() over (
				partition by IP_ADDR_PK
				order by LOAD_DATE ASC
			) as row_num
		from {{ params.prefix }}_view_traffic_one_year_{{ execution_date.year }}		
	) as h where row_num = 1
),
records_to_insert as (
		select a.IP_ADDR_PK, a.IP_ADDR_KEY, a.LOAD_DATE, a.RECORD_SOURCE
		from row_rank_1 as a
		left join {{ params.prefix }}_dds_hub_ip_addr as d
		on a.IP_ADDR_PK = d.IP_ADDR_PK
		where d.IP_ADDR_PK is NULL
)
insert into {{ params.prefix }}_dds_hub_ip_addr (IP_ADDR_PK, IP_ADDR_KEY, LOAD_DATE, RECORD_SOURCE)
(
	select IP_ADDR_PK, IP_ADDR_KEY, LOAD_DATE, RECORD_SOURCE
	from records_to_insert
);	
