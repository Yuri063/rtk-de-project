-- 7.1.2 insert into ODS: BILLING

alter table if exists {{ params.prefix }}_ods_billing truncate partition for('{{execution_date}}');
insert into {{ params.prefix }}_ods_billing (
	select 
		user_id, 
		billing_period,   
		service, 
		tariff, 
		sum::DECIMAL(10,2),
		created_at::TIMESTAMP
	from {{ params.prefix }}_stg_billing
	where cast(extract('year' from cast(created_at as timestamp)) as int) = {{execution_date.year}} 	
);
