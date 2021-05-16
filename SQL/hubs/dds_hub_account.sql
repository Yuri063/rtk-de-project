-- 7.3.2 insert HUB_ACCOUNT

with row_rank_1 as (
		select * from (
			select ACCOUNT_PK, ACCOUNT_KEY, LOAD_DATE, RECORD_SOURCE, pay_date,
				row_number() over (
					partition by ACCOUNT_PK
					order by LOAD_DATE ASC
				) as row_num
			from {{ params.prefix }}_view_payment_one_year_{{ execution_date.year }}  		
		) as h where row_num = 1
	),	
records_to_insert as (
		select a.ACCOUNT_PK, a.ACCOUNT_KEY, a.LOAD_DATE, a.RECORD_SOURCE
		from row_rank_1 as a
		left join {{ params.prefix }}_dds_hub_account as d
		on a.ACCOUNT_PK = d.ACCOUNT_PK
		where d.ACCOUNT_PK is NULL
)
insert into {{ params.prefix }}_dds_hub_account (ACCOUNT_PK, ACCOUNT_KEY, LOAD_DATE, RECORD_SOURCE)
(
	select ACCOUNT_PK, ACCOUNT_KEY, LOAD_DATE, RECORD_SOURCE
	from records_to_insert
);
