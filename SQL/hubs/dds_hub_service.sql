-- 5.1.9 insert HUB_SERVICE, SOURCE:BILLING, ISSUE

with row_rank_1 as (
	select * from (
		select SERVICE_PK, SERVICE_KEY, LOAD_DATE, RECORD_SOURCE,
			row_number() over (
				partition by SERVICE_PK
				order by LOAD_DATE ASC
			) as row_num
		from {{ params.prefix }}_view_billing_one_year_{{ execution_date.year }}		
	) as h where row_num = 1
),
row_rank_2 as (
	select * from (
		select SERVICE_PK, SERVICE_KEY, LOAD_DATE, RECORD_SOURCE,
			row_number() over (
				partition by SERVICE_PK
				order by LOAD_DATE ASC
			) as row_num
		from {{ params.prefix }}_view_issue_one_year_{{ execution_date.year }}		
	) as h where row_num = 1
),
stage_union as (
	select SERVICE_PK, SERVICE_KEY, LOAD_DATE, RECORD_SOURCE from row_rank_1
	union all
	select SERVICE_PK, SERVICE_KEY, LOAD_DATE, RECORD_SOURCE from row_rank_2
),
raw_union as (
	select * from (
		select SERVICE_PK, SERVICE_KEY, LOAD_DATE, RECORD_SOURCE,
			row_number() over (
				partition by SERVICE_PK
				order by LOAD_DATE ASC
			) as row_num
		from stage_union
		where SERVICE_PK is not NULL
	) as h where row_num = 1	
),	
records_to_insert as (
		select a.SERVICE_PK, a.SERVICE_KEY, a.LOAD_DATE, a.RECORD_SOURCE
		from raw_union as a
		left join {{ params.prefix }}_dds_hub_service as d
		on a.SERVICE_PK = d.SERVICE_PK
		where d.SERVICE_PK is NULL
)
insert into {{ params.prefix }}_dds_hub_service (SERVICE_PK, SERVICE_KEY, LOAD_DATE, RECORD_SOURCE)
(
	select SERVICE_PK, SERVICE_KEY, LOAD_DATE, RECORD_SOURCE
	from records_to_insert
);
