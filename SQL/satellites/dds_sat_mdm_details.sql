-- 5.3.3 insert SAT_USER_MDM_DETAILS, SOURCE:MDM

with source_data as (
	select 
		MDM_PK, MDM_HASHDIFF, 
		registered_at, is_vip,
		EFFECTIVE_FROM, 
		LOAD_DATE, RECORD_SOURCE
	from {{ params.prefix }}_view_mdm_one_year_{{ execution_date.year }}
),
update_records as (
	select 
		a.MDM_PK, a.MDM_HASHDIFF, 
		a.registered_at, a.is_vip,
		a.EFFECTIVE_FROM, 
		a.LOAD_DATE, a.RECORD_SOURCE
	from {{ params.prefix }}_dds_sat_mdm_details as a
	join source_data as b
	on a.MDM_PK = b.MDM_PK
	where  a.LOAD_DATE <= b.LOAD_DATE
),
latest_records as (
	select * from (
		select MDM_PK, MDM_HASHDIFF, LOAD_DATE,
			case when rank() over (partition by MDM_PK order by LOAD_DATE desc) = 1
				then 'Y' 
				else 'N'
			end as latest
		from update_records
	) as s
	where latest = 'Y'
),	
records_to_insert as (
	select distinct 
		e.MDM_PK, e.MDM_HASHDIFF, 
		e.registered_at, e.is_vip,
		e.EFFECTIVE_FROM, 
		e.LOAD_DATE, e.RECORD_SOURCE
	from source_data as e
	left join latest_records
	on latest_records.MDM_HASHDIFF = e.MDM_HASHDIFF and 
	   latest_records.MDM_PK = e.MDM_PK
	where latest_records.MDM_HASHDIFF is NULL
)	
insert into {{ params.prefix }}_dds_sat_mdm_details (
	MDM_PK, MDM_HASHDIFF, 
	registered_at, is_vip,
	EFFECTIVE_FROM, 
	LOAD_DATE, RECORD_SOURCE)
(
	select 
		MDM_PK, MDM_HASHDIFF, 
		registered_at, is_vip,
		EFFECTIVE_FROM, 
		LOAD_DATE, RECORD_SOURCE
	from records_to_insert
);
