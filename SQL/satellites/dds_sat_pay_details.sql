-- 5.3.2 insert SAT_PAY_DETAILS, SOURCE:PAYMENT

with source_data as (
	select 
		PAYMENT_PK, PAY_DOC_HASHDIFF, 
		pay_doc_num, pay_date, sum, 
		EFFECTIVE_FROM, 
		LOAD_DATE, RECORD_SOURCE
	from {{ params.prefix }}_view_payment_one_year_{{ execution_date.year }}
),
update_records as (
	select 
		a.PAYMENT_PK, a.PAY_DOC_HASHDIFF, 
		a.pay_doc_num, a.pay_date, a.sum,
		a.EFFECTIVE_FROM, 
		a.LOAD_DATE, a.RECORD_SOURCE
	from {{ params.prefix }}_dds_sat_pay_details as a
	join source_data as b
	on a.PAYMENT_PK = b.PAYMENT_PK
	where  a.LOAD_DATE <= b.LOAD_DATE
),
latest_records as (
	select * from (
		select PAYMENT_PK, PAY_DOC_HASHDIFF, LOAD_DATE,
			case when rank() over (partition by PAYMENT_PK order by LOAD_DATE desc) = 1
				then 'Y' 
				else 'N'
			end as latest
		from update_records
	) as s
	where latest = 'Y'
),	
records_to_insert as (
	select distinct 
		e.PAYMENT_PK, e.PAY_DOC_HASHDIFF, 
		e.pay_doc_num, e.pay_date, e.sum,
		e.EFFECTIVE_FROM, 
		e.LOAD_DATE, e.RECORD_SOURCE
	from source_data as e
	left join latest_records
	on latest_records.PAY_DOC_HASHDIFF = e.PAY_DOC_HASHDIFF and 
	   latest_records.PAYMENT_PK = e.PAYMENT_PK
	where latest_records.PAY_DOC_HASHDIFF is NULL
)	
insert into {{ params.prefix }}_dds_sat_pay_details (
	PAYMENT_PK, PAY_DOC_HASHDIFF, 
	pay_doc_num, pay_date, sum, 
	EFFECTIVE_FROM, 
	LOAD_DATE, RECORD_SOURCE)
(
	select 
		PAYMENT_PK, PAY_DOC_HASHDIFF, 
		pay_doc_num, pay_date, sum, 
		EFFECTIVE_FROM, 
		LOAD_DATE, RECORD_SOURCE
	from records_to_insert
);
