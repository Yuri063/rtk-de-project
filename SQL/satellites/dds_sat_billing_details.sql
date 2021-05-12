-- 5.3.4 insert SAT_BILLING_DETAILS, SOURCE:BILLING

with source_data as (
	select 
		BILLING_PK, BILLING_HASHDIFF, 
		created_at, sum,
		EFFECTIVE_FROM, 
		LOAD_DATE, RECORD_SOURCE
	from yfurman.project_view_billing_one_year_{{ execution_date.year }}
),
update_records as (
	select 
		a.BILLING_PK, a.BILLING_HASHDIFF, 
		a.created_at, a.sum,
		a.EFFECTIVE_FROM, 
		a.LOAD_DATE, a.RECORD_SOURCE
	from yfurman.project_dds_sat_billing_details as a
	join source_data as b
	on a.BILLING_PK = b.BILLING_PK
	where  a.LOAD_DATE <= b.LOAD_DATE
),
latest_records as (
	select * from (
		select BILLING_PK, BILLING_HASHDIFF, LOAD_DATE,
			case when rank() over (partition by BILLING_PK order by LOAD_DATE desc) = 1
				then 'Y' 
				else 'N'
			end as latest
		from update_records
	) as s
	where latest = 'Y'
),	
records_to_insert as (
	select distinct 
		e.BILLING_PK, e.BILLING_HASHDIFF, 
		e.created_at, e.sum,
		e.EFFECTIVE_FROM, 
		e.LOAD_DATE, e.RECORD_SOURCE
	from source_data as e
	left join latest_records
	on latest_records.BILLING_HASHDIFF = e.BILLING_HASHDIFF and 
	   latest_records.BILLING_PK = e.BILLING_PK
	where latest_records.BILLING_HASHDIFF is NULL
)	
insert into yfurman.project_dds_sat_billing_details (
	BILLING_PK, BILLING_HASHDIFF, 
	created_at, sum,
	EFFECTIVE_FROM, 
	LOAD_DATE, RECORD_SOURCE)
(
	select 
		BILLING_PK, BILLING_HASHDIFF, 
		created_at, sum,
		EFFECTIVE_FROM, 
		LOAD_DATE, RECORD_SOURCE
	from records_to_insert
);