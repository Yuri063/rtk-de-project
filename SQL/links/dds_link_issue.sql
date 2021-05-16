-- 7.4.4 insert LINK_ISSUE, SOURCE:ISSUE

with source_data as (
	select 
		ISSUE_PK,
		USER_PK, SERVICE_PK,
		LOAD_DATE, RECORD_SOURCE
	from {{ params.prefix }}_view_issue_one_year_{{ execution_date.year }}
),
records_to_insert as (
	select distinct 
		stg.ISSUE_PK, 
		stg.USER_PK, stg.SERVICE_PK,
		stg.LOAD_DATE, stg.RECORD_SOURCE
	from source_data as stg 
	left join {{ params.prefix }}_dds_link_issue as tgt
	on stg.ISSUE_PK = tgt.ISSUE_PK
	where tgt.ISSUE_PK is null		
)
insert into {{ params.prefix }}_dds_link_issue (
	ISSUE_PK,
	USER_PK, SERVICE_PK,
	LOAD_DATE, RECORD_SOURCE)
(
	select 
		ISSUE_PK,
		USER_PK, SERVICE_PK,
		LOAD_DATE, RECORD_SOURCE
	from records_to_insert
);
