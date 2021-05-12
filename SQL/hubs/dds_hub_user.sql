-- 5.1.1 insert HUB_USER

with row_rank_1 as (
	select * from (
		select USER_PK, USER_KEY, LOAD_DATE, RECORD_SOURCE,
			row_number() over (
				partition by USER_PK
				order by LOAD_DATE ASC
			) as row_num
		from yfurman.project_view_payment_one_year_{{ execution_date.year }}		
	) as h where row_num = 1
),	
row_rank_2 as (
	select * from (
		select USER_PK, USER_KEY, LOAD_DATE, RECORD_SOURCE,
			row_number() over (
				partition by USER_PK
				order by LOAD_DATE ASC
			) as row_num
		from yfurman.project_view_mdm_one_year_{{ execution_date.year }}		
	) as h where row_num = 1
),
row_rank_3 as (
	select * from (
		select USER_PK, USER_KEY, LOAD_DATE, RECORD_SOURCE,
			row_number() over (
				partition by USER_PK
				order by LOAD_DATE ASC
			) as row_num
		from yfurman.project_view_billing_one_year_{{ execution_date.year }}		
	) as h where row_num = 1
),
row_rank_4 as (
	select * from (
		select USER_PK, USER_KEY, LOAD_DATE, RECORD_SOURCE,
			row_number() over (
				partition by USER_PK
				order by LOAD_DATE ASC
			) as row_num
		from yfurman.project_view_issue_one_year_{{ execution_date.year }}		
	) as h where row_num = 1
),	
row_rank_5 as (
	select * from (
		select USER_PK, USER_KEY, LOAD_DATE, RECORD_SOURCE,
			row_number() over (
				partition by USER_PK
				order by LOAD_DATE ASC
			) as row_num
		from yfurman.project_view_traffic_one_year_{{ execution_date.year }}		
	) as h where row_num = 1
),	
stage_union as (
	select USER_PK, USER_KEY, LOAD_DATE, RECORD_SOURCE from row_rank_1
	union all
	select USER_PK, USER_KEY, LOAD_DATE, RECORD_SOURCE from row_rank_2
	union all
	select USER_PK, USER_KEY, LOAD_DATE, RECORD_SOURCE from row_rank_3
	union all
	select USER_PK, USER_KEY, LOAD_DATE, RECORD_SOURCE from row_rank_4		
	union all
	select USER_PK, USER_KEY, LOAD_DATE, RECORD_SOURCE from row_rank_5		
),
raw_union as (
	select * from (
		select USER_PK, USER_KEY, LOAD_DATE, RECORD_SOURCE,
			row_number() over (
				partition by USER_PK
				order by LOAD_DATE ASC
			) as row_num
		from stage_union
		where USER_PK is not NULL
	) as h where row_num = 1	
),	
records_to_insert as (
		select a.USER_PK, a.USER_KEY, a.LOAD_DATE, a.RECORD_SOURCE
		from raw_union as a
		left join yfurman.project_dds_hub_user as d
		on a.USER_PK = d.USER_PK
		where d.USER_PK is NULL
)
insert into yfurman.project_dds_hub_user (USER_PK, USER_KEY, LOAD_DATE, RECORD_SOURCE)
(
	select USER_PK, USER_KEY, LOAD_DATE, RECORD_SOURCE
	from records_to_insert
);