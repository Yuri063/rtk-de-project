-- 5.2.3 insert LINK_BILLING, SOURCE:BILLING

with source_data as (
	select 
		BILLING_PK,
		USER_PK, BILLING_PERIOD_PK, SERVICE_PK, TARIFF_PK,
		LOAD_DATE, RECORD_SOURCE
	from {{ params.prefix }}_view_billing_one_year_{{ execution_date.year }}
),
records_to_insert as (
	select distinct 
		stg.BILLING_PK, 
		stg.USER_PK, stg.BILLING_PERIOD_PK, stg.SERVICE_PK, stg.TARIFF_PK,
		stg.LOAD_DATE, stg.RECORD_SOURCE
	from source_data as stg 
	left join {{ params.prefix }}_dds_link_billing as tgt
	on stg.BILLING_PK = tgt.BILLING_PK
	where tgt.BILLING_PK is null		
)
insert into {{ params.prefix }}_dds_link_billing (
	BILLING_PK,
	USER_PK, BILLING_PERIOD_PK, SERVICE_PK, TARIFF_PK,
	LOAD_DATE, RECORD_SOURCE)
(
	select 
		BILLING_PK,
		USER_PK, BILLING_PERIOD_PK, SERVICE_PK, TARIFF_PK,
		LOAD_DATE, RECORD_SOURCE
	from records_to_insert
);
