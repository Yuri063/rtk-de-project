-- 7.4.1 insert LINK_PAYMENT, SOURCE:PAYMENT

with source_data as (
	select 
		PAYMENT_PK,
		USER_PK, ACCOUNT_PK, BILLING_PERIOD_PK, PAY_DOC_TYPE_PK,
		LOAD_DATE, RECORD_SOURCE
	from {{ params.prefix }}_view_payment_one_year_{{ execution_date.year }}
),
records_to_insert as (
	select distinct 
		stg.PAYMENT_PK, 
		stg.USER_PK, stg.ACCOUNT_PK, stg.BILLING_PERIOD_PK, stg.PAY_DOC_TYPE_PK,
		stg.LOAD_DATE, stg.RECORD_SOURCE
	from source_data as stg 
	left join {{ params.prefix }}_dds_link_payment as tgt
	on stg.PAYMENT_PK = tgt.PAYMENT_PK
	where tgt.PAYMENT_PK is null		
)
insert into {{ params.prefix }}_dds_link_payment (
	PAYMENT_PK,
	USER_PK, ACCOUNT_PK, BILLING_PERIOD_PK, PAY_DOC_TYPE_PK,
	LOAD_DATE, RECORD_SOURCE)
(
	select 
		PAYMENT_PK,
		USER_PK, ACCOUNT_PK, BILLING_PERIOD_PK, PAY_DOC_TYPE_PK,
		LOAD_DATE, RECORD_SOURCE
	from records_to_insert
);
