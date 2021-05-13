-- 5.1.7 insert HUB_BILLING_MODE, SOURCE:MDM 

with row_rank_1 as (
	select * from (
		select BILLING_MODE_PK, BILLING_MODE_KEY, LOAD_DATE, RECORD_SOURCE,
			row_number() over (
				partition by BILLING_MODE_PK
				order by LOAD_DATE ASC
			) as row_num
		from {{ params.prefix }}_view_mdm_one_year_{{ execution_date.year }}		
	) as h where row_num = 1
),
records_to_insert as (
		select a.BILLING_MODE_PK, a.BILLING_MODE_KEY, a.LOAD_DATE, a.RECORD_SOURCE
		from row_rank_1 as a
		left join {{ params.prefix }}_dds_hub_billing_mode as d
		on a.BILLING_MODE_PK = d.BILLING_MODE_PK
		where d.BILLING_MODE_PK is NULL
)
insert into {{ params.prefix }}_dds_hub_billing_mode (BILLING_MODE_PK, BILLING_MODE_KEY, LOAD_DATE, RECORD_SOURCE)
(
	select BILLING_MODE_PK, BILLING_MODE_KEY, LOAD_DATE, RECORD_SOURCE
	from records_to_insert
);
