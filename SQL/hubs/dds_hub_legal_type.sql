-- 7.3.5 insert HUB_LEGAL_TYPE, SOURCE:MDM 

with row_rank_1 as (
	select * from (
		select LEGAL_TYPE_PK, LEGAL_TYPE_KEY, LOAD_DATE, RECORD_SOURCE,
			row_number() over (
				partition by LEGAL_TYPE_PK
				order by LOAD_DATE ASC
			) as row_num
		from {{ params.prefix }}_view_mdm_one_year_{{ execution_date.year }}		
	) as h where row_num = 1
),
records_to_insert as (
		select a.LEGAL_TYPE_PK, a.LEGAL_TYPE_KEY, a.LOAD_DATE, a.RECORD_SOURCE
		from row_rank_1 as a
		left join {{ params.prefix }}_dds_hub_legal_type as d
		on a.LEGAL_TYPE_PK = d.LEGAL_TYPE_PK
		where d.LEGAL_TYPE_PK is NULL
)
insert into {{ params.prefix }}_dds_hub_legal_type (LEGAL_TYPE_PK, LEGAL_TYPE_KEY, LOAD_DATE, RECORD_SOURCE)
(
	select LEGAL_TYPE_PK, LEGAL_TYPE_KEY, LOAD_DATE, RECORD_SOURCE
	from records_to_insert
);	
