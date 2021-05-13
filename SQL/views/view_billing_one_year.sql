-- 1.3.3 CREATE VIEW FOR BILLING

-- create view rtk_de.yfurman.project_view_billing_one_year_{{ execution_date.year }} as (
create view {{ dag_id }}_view_billing_one_year_{{ execution_date.year }} as (

	with staging as (
		with derived_columns as (
			select
				user_id,
				billing_period,
				service,
				tariff,
				sum,
				created_at,
				user_id::varchar as USER_KEY,
				billing_period::varchar as BILLING_PERIOD_KEY,
				service::varchar as SERVICE_KEY,
				tariff::varchar as TARIFF_KEY,
				'BILLING - DATA LAKE'::varchar as RECORD_SOURCE
			-- from yfurman.project_ods_billing
			from {{ dag_id }}_ods_billing
			where cast(extract('year' from created_at) as int) = {{ execution_date.year }}
		),
		
		hashed_columns as (
			select
				user_id,
				billing_period,
				service,
				tariff,
				sum,
				created_at,
				USER_KEY,
				BILLING_PERIOD_KEY,
				SERVICE_KEY,
				TARIFF_KEY,				
				RECORD_SOURCE,
				
				cast((md5(nullif(upper(trim(cast(user_id as varchar))), ''))) as TEXT) as USER_PK,
				cast((md5(nullif(upper(trim(cast(billing_period as varchar))), ''))) as TEXT) as BILLING_PERIOD_PK,
				cast((md5(nullif(upper(trim(cast(service as varchar))), ''))) as TEXT) as SERVICE_PK,
				cast((md5(nullif(upper(trim(cast(tariff as varchar))), ''))) as TEXT) as TARIFF_PK,
				cast(md5(nullif(concat_ws('||',
					coalesce(nullif(upper(trim(cast(user_id as varchar))), ''), '^^'),
					coalesce(nullif(upper(trim(cast(billing_period as varchar))), ''), '^^'),
					coalesce(nullif(upper(trim(cast(service as varchar))), ''), '^^'),
					coalesce(nullif(upper(trim(cast(tariff as varchar))), ''), '^^')
				), '^^||^^||^^||^^')) as TEXT) as BILLING_PK,				
				cast(md5(concat_ws('||',
					coalesce(nullif(upper(trim(cast(created_at as varchar))), ''), '^^'),
					coalesce(nullif(upper(trim(cast(sum as varchar))), ''), '^^')
				)) as TEXT) as BILLING_HASHDIFF
			from derived_columns
		),
		
		columns_to_select as (
			select
				user_id,
				billing_period,
				service,
				tariff,
				sum,
				created_at,
				USER_KEY,
				BILLING_PERIOD_KEY,
				SERVICE_KEY,
				TARIFF_KEY,						
				RECORD_SOURCE,
				USER_PK,
				BILLING_PERIOD_PK,
				SERVICE_PK,
				TARIFF_PK,
				BILLING_PK,
				BILLING_HASHDIFF
			from hashed_columns
		)
		
		select * from columns_to_select
	)
	
	select *, 
			--current_timestamp as LOAD_DATE,			
	        '{{ execution_date }}'::timestamp as LOAD_DATE,
			created_at as EFFECTIVE_FROM
	from staging
);
