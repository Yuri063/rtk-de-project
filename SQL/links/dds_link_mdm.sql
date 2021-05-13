-- 5.2.2 insert LINK_MDM, SOURCE:MDM

with source_data as (
	select 
		MDM_PK,
		USER_PK, LEGAL_TYPE_PK, DISTRICT_PK, BILLING_MODE_PK,
		LOAD_DATE, RECORD_SOURCE
	from {{ params.prefix }}_view_mdm_one_year_{{ execution_date.year }}
),
records_to_insert as (
	select distinct 
		stg.MDM_PK, 
		stg.USER_PK, stg.LEGAL_TYPE_PK, stg.DISTRICT_PK, stg.BILLING_MODE_PK,
		stg.LOAD_DATE, stg.RECORD_SOURCE
	from source_data as stg 
	left join {{ params.prefix }}_dds_link_mdm as tgt
	on stg.MDM_PK = tgt.MDM_PK
	where tgt.MDM_PK is null		
)
insert into {{ params.prefix }}_dds_link_mdm (
	MDM_PK,
	USER_PK, LEGAL_TYPE_PK, DISTRICT_PK, BILLING_MODE_PK,
	LOAD_DATE, RECORD_SOURCE)
(
	select 
		MDM_PK,
		USER_PK, LEGAL_TYPE_PK, DISTRICT_PK, BILLING_MODE_PK,
		LOAD_DATE, RECORD_SOURCE
	from records_to_insert
);
