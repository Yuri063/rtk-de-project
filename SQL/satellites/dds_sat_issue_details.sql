-- 7.5.5 insert SAT_ISSUE_DETAILS, SOURCE:ISSUE

with source_data as (
	select 
		ISSUE_PK, ISSUE_HASHDIFF, 
		start_time, end_time, title, description, service,
		EFFECTIVE_FROM, 
		LOAD_DATE, RECORD_SOURCE
	from {{ params.prefix }}_view_issue_one_year_{{ execution_date.year }}
),
update_records as (
	select 
		a.ISSUE_PK, a.ISSUE_HASHDIFF, 
		a.start_time, a.end_time, a.title, a.description, a.service,
		a.EFFECTIVE_FROM, 
		a.LOAD_DATE, a.RECORD_SOURCE
	from {{ params.prefix }}_dds_sat_issue_details as a
	join source_data as b
	on a.ISSUE_PK = b.ISSUE_PK
	where  a.LOAD_DATE <= b.LOAD_DATE
),
latest_records as (
	select * from (
		select ISSUE_PK, ISSUE_HASHDIFF, LOAD_DATE,
			case when rank() over (partition by ISSUE_PK order by LOAD_DATE desc) = 1
				then 'Y' 
				else 'N'
			end as latest
		from update_records
	) as s
	where latest = 'Y'
),	
records_to_insert as (
	select distinct 
		e.ISSUE_PK, e.ISSUE_HASHDIFF, 
		e.start_time, e.end_time, e.title, e.description, e.service,
		e.EFFECTIVE_FROM, 
		e.LOAD_DATE, e.RECORD_SOURCE
	from source_data as e
	left join latest_records
	on latest_records.ISSUE_HASHDIFF = e.ISSUE_HASHDIFF and 
	   latest_records.ISSUE_PK = e.ISSUE_PK
	where latest_records.ISSUE_HASHDIFF is NULL
)	
insert into {{ params.prefix }}_dds_sat_issue_details (
	ISSUE_PK, ISSUE_HASHDIFF, 
	start_time, end_time, title, description, service,
	EFFECTIVE_FROM, 
	LOAD_DATE, RECORD_SOURCE)
(
	select 
		ISSUE_PK, ISSUE_HASHDIFF, 
		start_time, end_time, title, description, service,
		EFFECTIVE_FROM, 
		LOAD_DATE, RECORD_SOURCE
	from records_to_insert
);
