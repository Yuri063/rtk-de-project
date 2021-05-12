-- 5.3.1 insert SAT_USER_DETAILS, SOURCE:PAYMENT

with source_data as (
	select 
		USER_PK, USER_HASHDIFF, 
		phone, 
		EFFECTIVE_FROM, 
		LOAD_DATE, RECORD_SOURCE
	from yfurman.project_view_payment_one_year_{{ execution_date.year }}
),
update_records as (
	select 
		a.USER_PK, a.USER_HASHDIFF, 
		a.phone, 
		a.EFFECTIVE_FROM, 
		a.LOAD_DATE, a.RECORD_SOURCE
	from yfurman.project_dds_sat_user_details as a
	join source_data as b
	on a.USER_PK = b.USER_PK
	where  a.LOAD_DATE <= b.LOAD_DATE
),
latest_records as (
	select * from (
		select USER_PK, USER_HASHDIFF, LOAD_DATE,
			case when rank() over (partition by USER_PK order by LOAD_DATE desc) = 1
				then 'Y' 
				else 'N'
			end as latest
		from update_records
	) as s
	where latest = 'Y'
),	
records_to_insert as (
	select distinct 
		e.USER_PK, e.USER_HASHDIFF, 
		e.phone, 
		e.EFFECTIVE_FROM, 
		e.LOAD_DATE, e.RECORD_SOURCE
	from source_data as e
	left join latest_records
	on latest_records.USER_HASHDIFF = e.USER_HASHDIFF and 
	   latest_records.USER_PK = e.USER_PK
	where latest_records.USER_HASHDIFF is NULL
)	
insert into yfurman.project_dds_sat_user_details (
	USER_PK, USER_HASHDIFF, 
	phone, 
	EFFECTIVE_FROM, 
	LOAD_DATE, RECORD_SOURCE)
(
	select 
		USER_PK, USER_HASHDIFF, 
		phone, 
		EFFECTIVE_FROM, 
		LOAD_DATE, RECORD_SOURCE
	from records_to_insert
);