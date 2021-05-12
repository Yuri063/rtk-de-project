-- 5.1.4 insert HUB_PAY_DOC_TYPE

with row_rank_1 as (
		select * from (
			select PAY_DOC_TYPE_PK, PAY_DOC_TYPE_KEY, LOAD_DATE, RECORD_SOURCE,
				row_number() over (
					partition by PAY_DOC_TYPE_PK
					order by LOAD_DATE ASC
				) as row_num
			from yfurman.project_view_payment_one_year_{{ execution_date.year }}  		
		) as h where row_num = 1
	),	
records_to_insert as (
		select a.PAY_DOC_TYPE_PK, a.PAY_DOC_TYPE_KEY, a.LOAD_DATE, a.RECORD_SOURCE
		from row_rank_1 as a
		left join yfurman.project_dds_hub_pay_doc_type as d
		on a.PAY_DOC_TYPE_PK = d.PAY_DOC_TYPE_PK
		where d.PAY_DOC_TYPE_PK is NULL
)
insert into yfurman.project_dds_hub_pay_doc_type (PAY_DOC_TYPE_PK, PAY_DOC_TYPE_KEY, LOAD_DATE, RECORD_SOURCE)
(
	select PAY_DOC_TYPE_PK, PAY_DOC_TYPE_KEY, LOAD_DATE, RECORD_SOURCE
	from records_to_insert
);