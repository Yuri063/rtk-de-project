-- 1.3.1 CREATE VIEW FOR PAYMENT

create or replace view {{ params.prefix }}_view_payment_one_year_{{ execution_date.year }} as (

	with staging as (
		with derived_columns as (
			select
				user_id,
				pay_doc_type,
				pay_doc_num,
				account,
				phone,
				billing_period,
				pay_date,
				sum,
				user_id::varchar as USER_KEY,
				account::varchar as ACCOUNT_KEY,
				billing_period::varchar as BILLING_PERIOD_KEY,
				pay_doc_type::varchar as PAY_DOC_TYPE_KEY,
				pay_doc_num::varchar as PAY_DOC_NUM_KEY,
				'PAYMENT - DATA LAKE'::varchar as RECORD_SOURCE
			from {{ params.prefix }}_ods_payment 
			where cast(extract('year' from cast(pay_date as timestamp)) as int) = {{ execution_date.year }}			
		),
		
		hashed_columns as (
			select
				user_id,
				pay_doc_type,
				pay_doc_num,
				account,
				phone,
				billing_period,
				pay_date,
				sum,
				USER_KEY,
				ACCOUNT_KEY,
				BILLING_PERIOD_KEY,
				PAY_DOC_TYPE_KEY,
				PAY_DOC_NUM_KEY,
				RECORD_SOURCE,
				
				cast((md5(nullif(upper(trim(cast(user_id as varchar))), ''))) as TEXT) as USER_PK,
				cast((md5(nullif(upper(trim(cast(account as varchar))), ''))) as TEXT) as ACCOUNT_PK,
				cast((md5(nullif(upper(trim(cast(billing_period as varchar))), ''))) as TEXT) as BILLING_PERIOD_PK,
				cast((md5(nullif(upper(trim(cast(pay_doc_type as varchar))), ''))) as TEXT) as PAY_DOC_TYPE_PK,				
				cast(md5(nullif(concat_ws('||',
					coalesce(nullif(upper(trim(cast(user_id as varchar))), ''), '^^'),
					coalesce(nullif(upper(trim(cast(account as varchar))), ''), '^^'),
					coalesce(nullif(upper(trim(cast(billing_period as varchar))), ''), '^^'),
					coalesce(nullif(upper(trim(cast(pay_doc_type as varchar))), ''), '^^')
				), '^^||^^||^^||^^')) as TEXT) as PAYMENT_PK,
				cast(md5(concat_ws('||',
					coalesce(nullif(upper(trim(cast(phone as varchar))), ''), '^^')
				)) as TEXT) as USER_HASHDIFF,
				cast(md5(concat_ws('||',
					coalesce(nullif(upper(trim(cast(pay_doc_num as varchar))), ''), '^^'),				
					coalesce(nullif(upper(trim(cast(pay_date as varchar))), ''), '^^'),					
					coalesce(nullif(upper(trim(cast(sum as varchar))), ''), '^^')					
				)) as TEXT) as PAY_DOC_HASHDIFF
			from derived_columns
		),
		
		columns_to_select as (
			select
				user_id,
				pay_doc_type,
				pay_doc_num,
				account,
				phone,
				billing_period,
				pay_date,
				sum,
				USER_KEY,
				ACCOUNT_KEY,
				BILLING_PERIOD_KEY,
				PAY_DOC_TYPE_KEY,
				PAY_DOC_NUM_KEY,
				RECORD_SOURCE,
				USER_PK,
				ACCOUNT_PK,
				BILLING_PERIOD_PK,
				PAY_DOC_TYPE_PK,
				PAYMENT_PK,				
				USER_HASHDIFF,
				PAY_DOC_HASHDIFF
			from hashed_columns
		)
		
		select * from columns_to_select
	)
	
	select *, 
			'{{ execution_date }}'::timestamp as LOAD_DATE,
			pay_date as EFFECTIVE_FROM
	from staging
);
