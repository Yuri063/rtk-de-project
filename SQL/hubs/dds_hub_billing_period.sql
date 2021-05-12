-- 5.1.3 insert HUB_BILLING_PERIOD

with row_rank_1 as (
	select * from (
		select BILLING_PERIOD_PK, BILLING_PERIOD_KEY, LOAD_DATE, RECORD_SOURCE,
			row_number() over (
				partition by BILLING_PERIOD_PK
				order by LOAD_DATE ASC
			) as row_num
		from yfurman.project_view_payment_one_year_{{ execution_date.year }}		
	) as h where row_num = 1
),
row_rank_2 as (
	select * from (
		select BILLING_PERIOD_PK, BILLING_PERIOD_KEY, LOAD_DATE, RECORD_SOURCE,
			row_number() over (
				partition by BILLING_PERIOD_PK
				order by LOAD_DATE ASC
			) as row_num
		from yfurman.project_view_billing_one_year_{{ execution_date.year }}		
	) as h where row_num = 1
),
stage_union as (
	select BILLING_PERIOD_PK, BILLING_PERIOD_KEY, LOAD_DATE, RECORD_SOURCE from row_rank_1
	union all
	select BILLING_PERIOD_PK, BILLING_PERIOD_KEY, LOAD_DATE, RECORD_SOURCE from row_rank_2
),
raw_union as (
	select * from (
		select BILLING_PERIOD_PK, BILLING_PERIOD_KEY, LOAD_DATE, RECORD_SOURCE,
			row_number() over (
				partition by BILLING_PERIOD_PK
				order by LOAD_DATE ASC
			) as row_num
		from stage_union
		where BILLING_PERIOD_PK is not NULL
	) as h where row_num = 1	
),	
records_to_insert as (
		select a.BILLING_PERIOD_PK, a.BILLING_PERIOD_KEY, a.LOAD_DATE, a.RECORD_SOURCE
		from raw_union as a
		left join yfurman.project_dds_hub_billing_period as d
		on a.BILLING_PERIOD_PK = d.BILLING_PERIOD_PK
		where d.BILLING_PERIOD_PK is NULL
)
insert into yfurman.project_dds_hub_billing_period (BILLING_PERIOD_PK, BILLING_PERIOD_KEY, LOAD_DATE, RECORD_SOURCE)
(
	select BILLING_PERIOD_PK, BILLING_PERIOD_KEY, LOAD_DATE, RECORD_SOURCE
	from records_to_insert
);
