-- 1.3.4 CREATE VIEW FOR ISSUE

create view rtk_de.yfurman.project_view_issue_one_year_{{ execution_date.year }} as (

	with staging as (
		with derived_columns as (
			select
				user_id,
				start_time,
				end_time,
				title,
				description,
				service,
				user_id::varchar as USER_KEY,
				service::varchar as SERVICE_KEY,
				'ISSUE - DATA LAKE'::varchar as RECORD_SOURCE
			from yfurman.project_ods_issue
			where cast(extract('year' from created_at) as int) = {{ execution_date.year }}
		),
		
		hashed_columns as (
			select
				user_id,
				start_time,
				end_time,
				title,
				description,
				service,
				USER_KEY,
				SERVICE_KEY,
				RECORD_SOURCE,
				
				cast((md5(nullif(upper(trim(cast(user_id as varchar))), ''))) as TEXT) as USER_PK,			
				cast((md5(nullif(upper(trim(cast(service as varchar))), ''))) as TEXT) as SERVICE_PK,
				cast(md5(nullif(concat_ws('||',
					coalesce(nullif(upper(trim(cast(user_id as varchar))), ''), '^^'),
					coalesce(nullif(upper(trim(cast(service as varchar))), ''), '^^')
				), '^^||^^')) as TEXT) as ISSUE_PK,				
				cast(md5(concat_ws('||',
					coalesce(nullif(upper(trim(cast(start_time as varchar))), ''), '^^'),
					coalesce(nullif(upper(trim(cast(end_time as varchar))), ''), '^^'),
					coalesce(nullif(upper(trim(cast(title as varchar))), ''), '^^'),
					coalesce(nullif(upper(trim(cast(description as varchar))), ''), '^^')
				)) as TEXT) as ISSUE_HASHDIFF
			from derived_columns
		),
		
		columns_to_select as (
			select
				user_id,
				start_time,
				end_time,
				title,
				description,
				service,
				USER_KEY,
				SERVICE_KEY,
				RECORD_SOURCE,
				USER_PK,
				SERVICE_PK,
				ISSUE_PK,
				ISSUE_HASHDIFF
			from hashed_columns
		)
		
		select * from columns_to_select
	)
	
	select *, 
			--current_timestamp as LOAD_DATE,			
	        '{{ execution_date.year }}'::timestamp as LOAD_DATE,
			start_time as EFFECTIVE_FROM
	from staging
);