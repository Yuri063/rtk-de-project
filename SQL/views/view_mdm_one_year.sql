-- 7.2.2 CREATE VIEW FOR MDM

create or replace view {{ params.prefix }}_view_mdm_one_year_{{ execution_date.year }} as (

	with staging as (
		with derived_columns as (
			select
				id,
				legal_type,
				district,
				registered_at,
				billing_mode,
				is_vip,
				id::varchar as USER_KEY,
				legal_type::varchar as LEGAL_TYPE_KEY,
				district::varchar as DISTRICT_KEY,
				billing_mode::varchar as BILLING_MODE_KEY,
				'MDM - DATA LAKE'::varchar as RECORD_SOURCE
			from {{ params.prefix }}_ods_mdm
			where cast(extract('year' from registered_at) as int) = {{ execution_date.year }}
		),
		
		hashed_columns as (
			select
				id,
				legal_type,
				district,
				registered_at,
				billing_mode,
				is_vip,
				USER_KEY,
				LEGAL_TYPE_KEY,
				DISTRICT_KEY,
				BILLING_MODE_KEY,				
				RECORD_SOURCE,
				
				cast((md5(nullif(upper(trim(cast(id as varchar))), ''))) as TEXT) as USER_PK,
				cast((md5(nullif(upper(trim(cast(legal_type as varchar))), ''))) as TEXT) as LEGAL_TYPE_PK,
				cast((md5(nullif(upper(trim(cast(district as varchar))), ''))) as TEXT) as DISTRICT_PK,
				cast((md5(nullif(upper(trim(cast(billing_mode as varchar))), ''))) as TEXT) as BILLING_MODE_PK,
				cast(md5(nullif(concat_ws('||',
					coalesce(nullif(upper(trim(cast(id as varchar))), ''), '^^'),
					coalesce(nullif(upper(trim(cast(legal_type as varchar))), ''), '^^'),
					coalesce(nullif(upper(trim(cast(district as varchar))), ''), '^^'),
					coalesce(nullif(upper(trim(cast(billing_mode as varchar))), ''), '^^')
				), '^^||^^||^^||^^')) as TEXT) as MDM_PK,				
				cast(md5(concat_ws('||',
					coalesce(nullif(upper(trim(cast(registered_at as varchar))), ''), '^^'),
					coalesce(nullif(upper(trim(cast(is_vip as varchar))), ''), '^^')
				)) as TEXT) as MDM_HASHDIFF
			from derived_columns
		),
		
		columns_to_select as (
			select
				id,
				legal_type,
				district,
				registered_at,
				billing_mode,
				is_vip,
				USER_KEY,
				LEGAL_TYPE_KEY,
				DISTRICT_KEY,
				BILLING_MODE_KEY,					
				RECORD_SOURCE,
				USER_PK,
				LEGAL_TYPE_PK,
				DISTRICT_PK,
				BILLING_MODE_PK,
				MDM_PK,
				MDM_HASHDIFF
			from hashed_columns
		)
		
		select * from columns_to_select
	)
	
	select *, 			
	        '{{ execution_date }}'::timestamp as LOAD_DATE,
			registered_at as EFFECTIVE_FROM
	from staging
);
