-- 5.1.8 insert HUB_TARIFF, SOURCE:BILLING

with row_rank_1 as (
	select * from (
		select TARIFF_PK, TARIFF_KEY, LOAD_DATE, RECORD_SOURCE,
			row_number() over (
				partition by TARIFF_PK
				order by LOAD_DATE ASC
			) as row_num
		from {{ params.prefix }}_view_billing_one_year_{{ execution_date.year }}		
	) as h where row_num = 1
),
records_to_insert as (
		select a.TARIFF_PK, a.TARIFF_KEY, a.LOAD_DATE, a.RECORD_SOURCE
		from row_rank_1 as a
		left join {{ params.prefix }}_dds_hub_tariff as d
		on a.TARIFF_PK = d.TARIFF_PK
		where d.TARIFF_PK is NULL
)
insert into {{ params.prefix }}_dds_hub_tariff (TARIFF_PK, TARIFF_KEY, LOAD_DATE, RECORD_SOURCE)
(
	select TARIFF_PK, TARIFF_KEY, LOAD_DATE, RECORD_SOURCE
	from records_to_insert
);	
