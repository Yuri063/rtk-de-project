-- 7.3.6 insert HUB_DISTRICT, SOURCE:MDM

with row_rank_1 as (
	select * from (
		select DISTRICT_PK, DISTRICT_KEY, LOAD_DATE, RECORD_SOURCE,
			row_number() over (
				partition by DISTRICT_PK
				order by LOAD_DATE ASC
			) as row_num
		from {{ params.prefix }}_view_mdm_one_year_{{ execution_date.year }}		
	) as h where row_num = 1
),
records_to_insert as (
		select a.DISTRICT_PK, a.DISTRICT_KEY, a.LOAD_DATE, a.RECORD_SOURCE
		from row_rank_1 as a
		left join {{ params.prefix }}_dds_hub_district as d
		on a.DISTRICT_PK = d.DISTRICT_PK
		where d.DISTRICT_PK is NULL
)
insert into {{ params.prefix }}_dds_hub_district (DISTRICT_PK, DISTRICT_KEY, LOAD_DATE, RECORD_SOURCE)
(
	select DISTRICT_PK, DISTRICT_KEY, LOAD_DATE, RECORD_SOURCE
	from records_to_insert
);	
	
