-- 7.1.1 insert into ODS: PAYMENT

alter table if exists {{ params.prefix }}_ods_payment truncate partition for('{{execution_date}}');
insert into {{ params.prefix }}_ods_payment (
	select 
		user_id, 
		pay_doc_type, 
		pay_doc_num::BIGINT,  
		account, 
		phone, 
		billing_period, 
		pay_date::DATE, 
		sum::DECIMAL(10,2)
	from {{ params.prefix }}_stg_payment
	where cast(extract('year' from cast(pay_date as timestamp)) as int) = {{execution_date.year}}
);
