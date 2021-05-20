

------------------------- DDL BLOCK -------------------------

-- 1. CREATE STAGING AND ODS

-- 1.1 CREATE STAGING FROM DATA_LAKE

-- 1.1.1 CREATE STAGING FOR PAYMENT

drop external table if exists yfurman.project_stg_payment;
create external table yfurman.project_stg_payment (
	user_id INT, 
	pay_doc_type VARCHAR, 
	pay_doc_num INT,  
	account VARCHAR, 
	phone VARCHAR, 
	billing_period VARCHAR, 
	pay_date VARCHAR, 
	sum  DOUBLE PRECISION)
location ('pxf://rt-2021-03-25-16-47-29-sfunu-final-project/payment/*/?PROFILE=gs:parquet') 
FORMAT 'CUSTOM' (FORMATTER='pxfwritable_import');
select * from yfurman.project_stg_payment limit 100;
select count(*) from yfurman.project_stg_payment;
select count(*) from (select distinct pay_doc_num, pay_doc_type from yfurman.project_stg_payment) a;
select count(*) from (select distinct pay_doc_num, pay_doc_type, account, billing_period from yfurman.project_stg_payment) a;


-- 1.1.2 CREATE STAGING FOR BILLING

drop external table if exists yfurman.project_stg_billing;
create external table yfurman.project_stg_billing (
	user_id INT, 
	billing_period VARCHAR,   
	service VARCHAR, 
	tariff VARCHAR, 
	sum VARCHAR,
	created_at VARCHAR)
location ('pxf://rt-2021-03-25-16-47-29-sfunu-final-project/billing/*/?PROFILE=gs:parquet') 
FORMAT 'CUSTOM' (FORMATTER='pxfwritable_import');
select * from yfurman.project_stg_billing limit 100;
select count(*) from yfurman.project_stg_billing;
select count(*) from (select distinct user_id, billing_period, service, tariff from yfurman.project_stg_billing) a;

-- 1.1.3 CREATE STAGING FOR ISSUE

drop external table if exists yfurman.project_stg_issue;
create external table yfurman.project_stg_issue (
	user_id VARCHAR, 
	start_time VARCHAR,
	end_time VARCHAR,
	title VARCHAR,
	description VARCHAR,
	service VARCHAR)
location ('pxf://rt-2021-03-25-16-47-29-sfunu-final-project/issue/*/?PROFILE=gs:parquet') 
FORMAT 'CUSTOM' (FORMATTER='pxfwritable_import');
select * from yfurman.project_stg_issue limit 100;
select count(*) from yfurman.project_stg_issue;
select count(*) from (select distinct user_id, title, service  from yfurman.project_stg_issue) a;

-- 1.1.4 CREATE STAGING FOR TRAFFIC

drop external table if exists yfurman.project_stg_traffic;
create external table yfurman.project_stg_traffic (
	user_id INT, 
	timestamp BIGINT,
	device_id VARCHAR,   
	device_ip_addr VARCHAR, 
	bytes_sent INT,
	bytes_received INT)
location ('pxf://rt-2021-03-25-16-47-29-sfunu-final-project/traffic/*/?PROFILE=gs:parquet') 
FORMAT 'CUSTOM' (FORMATTER='pxfwritable_import');
select * from yfurman.project_stg_traffic limit 100;
select count(*) from yfurman.project_stg_traffic;
select count(*) from (select distinct user_id, device_id, device_ip_addr  from yfurman.project_stg_traffic) a;


-- 1.2 CREATE ODS FROM STAGING

-- 1.2.1 CREATE ODS FOR PAYMENT

drop  table if exists yfurman.project_ods_payment CASCADE;

create table yfurman.project_ods_payment (
	user_id INT, 
	pay_doc_type VARCHAR, 
	pay_doc_num BIGINT,  
	account VARCHAR, 
	phone VARCHAR, 
	billing_period VARCHAR, 
	pay_date DATE, 
	sum DECIMAL(10,2)
)
distributed by (user_id)
partition by range(pay_date) (
	start (date '1990-01-01') inclusive 
	end   (date '2040-01-01') exclusive 
	every ('1 year'::interval)
);


select * from yfurman.project_ods_payment limit 100;

-- 1.2.2 CREATE ODS FOR BILLING

drop  table if exists yfurman.project_ods_billing CASCADE;

create table yfurman.project_ods_billing (
	user_id INT, 
	billing_period VARCHAR,   
	service VARCHAR, 
	tariff VARCHAR, 
	sum DECIMAL(10,2),
	created_at TIMESTAMP
)
distributed by (user_id)
partition by range(created_at) (
	start (TIMESTAMP '1990-01-01') inclusive 
	end   (TIMESTAMP '2040-01-01') exclusive 
	every ('1 year'::interval)
);

select * from yfurman.project_ods_billing limit 100;

-- 1.2.3 CREATE ODS FOR ISSUE

drop  table if exists yfurman.project_ods_issue CASCADE;

create table yfurman.project_ods_issue (
	user_id INT, 
	start_time TIMESTAMP,
	end_time TIMESTAMP,
	title VARCHAR,
	description VARCHAR,
	service VARCHAR
)
distributed by (user_id)
partition by range(start_time) (
	start (TIMESTAMP '1990-01-01') inclusive 
	end   (TIMESTAMP '2040-01-01') exclusive 
	every ('1 year'::interval)
);

select * from yfurman.project_ods_issue limit 100;

-- 1.2.4 CREATE ODS FOR TRAFFIC

drop  table if exists yfurman.project_ods_traffic CASCADE;

create table yfurman.project_ods_traffic (
	user_id INT, 			
	time_stamp TIMESTAMP,
	device_id VARCHAR,   
	device_ip_addr VARCHAR, 
	bytes_sent BIGINT,
	bytes_received BIGINT
)
distributed by (user_id)
partition by range(time_stamp) (
	start (TIMESTAMP '1990-01-01') inclusive 
	end   (TIMESTAMP '2040-01-01') exclusive 
	every ('1 year'::interval)
);

select * from yfurman.project_ods_traffic limit 100;

-- 1.2.5 CREATE ODS FOR MDM

drop  table if exists yfurman.project_ods_mdm CASCADE;

create table yfurman.project_ods_mdm (
	id INT,
	legal_type TEXT,
	district TEXT,
	registered_at TIMESTAMP,
	billing_mode TEXT,
	is_vip BOOL
)
distributed by (id)
partition by range(registered_at) (
	start (TIMESTAMP '1990-01-01') inclusive 
	end   (TIMESTAMP '2040-01-01') exclusive 
	every ('1 year'::interval)
);

select * from yfurman.project_ods_mdm limit 100;
select count(*) from yfurman.project_ods_mdm;


-- 2. CREATE HUBs

-- 2.1 CREATE HUB_USER, SOURCE:PAYMENT,MDM,BILLING,ISSUE,TRAFFIC

drop table if exists "rtk_de"."yfurman"."project_dds_hub_user";
create table yfurman.project_dds_hub_user (
	USER_PK TEXT, 
	USER_KEY VARCHAR, 
	LOAD_DATE TIMESTAMP, 
	RECORD_SOURCE VARCHAR
);

-- 2.2 CREATE HUB_ACCOUNT, SOURCE:PAYMENT

drop table if exists "rtk_de"."yfurman"."project_dds_hub_account";
create table "rtk_de"."yfurman"."project_dds_hub_account" (
	ACCOUNT_PK TEXT, 
	ACCOUNT_KEY VARCHAR, 
	LOAD_DATE TIMESTAMP, 
	RECORD_SOURCE VARCHAR
);

-- 2.3 CREATE HUB_BILLING_PERIOD, SOURCE:PAYMENT,BILLING

drop table if exists "rtk_de"."yfurman"."project_dds_hub_billing_period";
create table "rtk_de"."yfurman"."project_dds_hub_billing_period" (
	BILLING_PERIOD_PK TEXT, 
	BILLING_PERIOD_KEY VARCHAR, 
	LOAD_DATE TIMESTAMP, 
	RECORD_SOURCE VARCHAR
);

-- 2.4 CREATE HUB_PAY_DOC_TYPE, SOURCE:PAYMENT

drop table if exists "rtk_de"."yfurman"."project_dds_hub_pay_doc_type";
create table "rtk_de"."yfurman"."project_dds_hub_pay_doc_type" (
	PAY_DOC_TYPE_PK TEXT, 
	PAY_DOC_TYPE_KEY VARCHAR, 
	LOAD_DATE TIMESTAMP, 
	RECORD_SOURCE VARCHAR
);

-- 2.5 CREATE HUB_LEGAL_TYPE, SOURCE:MDM

drop table if exists "rtk_de"."yfurman"."project_dds_hub_legal_type";
create table "rtk_de"."yfurman"."project_dds_hub_legal_type" (
	LEGAL_TYPE_PK TEXT, 
	LEGAL_TYPE_KEY VARCHAR, 
	LOAD_DATE TIMESTAMP, 
	RECORD_SOURCE VARCHAR
);

-- 2.6 CREATE HUB_DISTRICT, SOURCE:MDM

drop table if exists "rtk_de"."yfurman"."project_dds_hub_district";
create table "rtk_de"."yfurman"."project_dds_hub_district" (
	DISTRICT_PK TEXT, 
	DISTRICT_KEY VARCHAR, 
	LOAD_DATE TIMESTAMP, 
	RECORD_SOURCE VARCHAR
);

-- 2.7 CREATE HUB_BILLING_MODE, SOURCE:MDM

drop table if exists "rtk_de"."yfurman"."project_dds_hub_billing_mode";
create table "rtk_de"."yfurman"."project_dds_hub_billing_mode" (
	BILLING_MODE_PK TEXT, 
	BILLING_MODE_KEY VARCHAR, 
	LOAD_DATE TIMESTAMP, 
	RECORD_SOURCE VARCHAR
);

-- 2.8 CREATE HUB_TARIFF, SOURCE:BILLING

drop table if exists "rtk_de"."yfurman"."project_dds_hub_tariff";
create table "rtk_de"."yfurman"."project_dds_hub_tariff" (
	TARIFF_PK TEXT, 
	TARIFF_KEY VARCHAR, 
	LOAD_DATE TIMESTAMP, 
	RECORD_SOURCE VARCHAR
);

-- 2.9 CREATE HUB_SERVICE, SOURCE:BILLING, ISSUE

drop table if exists "rtk_de"."yfurman"."project_dds_hub_service";
create table "rtk_de"."yfurman"."project_dds_hub_service" (
	SERVICE_PK TEXT, 
	SERVICE_KEY VARCHAR, 
	LOAD_DATE TIMESTAMP, 
	RECORD_SOURCE VARCHAR
);

-- 2.10 CREATE HUB_DEVICE, SOURCE:TRAFFIC

drop table if exists "rtk_de"."yfurman"."project_dds_hub_device";
create table "rtk_de"."yfurman"."project_dds_hub_device" (
	DEVICE_PK TEXT, 
	DEVICE_KEY VARCHAR, 
	LOAD_DATE TIMESTAMP, 
	RECORD_SOURCE VARCHAR
);

-- 2.11 CREATE HUB_IP_ADDR, SOURCE:TRAFFIC

drop table if exists "rtk_de"."yfurman"."project_dds_hub_ip_addr";
create table "rtk_de"."yfurman"."project_dds_hub_ip_addr" (
	IP_ADDR_PK TEXT, 
	IP_ADDR_KEY VARCHAR, 
	LOAD_DATE TIMESTAMP, 
	RECORD_SOURCE VARCHAR
);


-- 3. CREATE LINKs

-- 3.1 CREATE LINK_PAYMENT 

drop table if exists "rtk_de"."yfurman"."project_dds_link_payment";
create table "rtk_de"."yfurman"."project_dds_link_payment" (
	PAYMENT_PK TEXT,
	USER_PK TEXT, 
	ACCOUNT_PK TEXT, 
	BILLING_PERIOD_PK TEXT, 
	PAY_DOC_TYPE_PK TEXT,
	LOAD_DATE TIMESTAMP,
	RECORD_SOURCE VARCHAR
);

-- 3.2 CREATE LINK_MDM, SOURCE:MDM

drop table if exists "rtk_de"."yfurman"."project_dds_link_mdm";
create table "rtk_de"."yfurman"."project_dds_link_mdm" ( 
	MDM_PK TEXT,
	USER_PK TEXT, 
	LEGAL_TYPE_PK TEXT, 
	DISTRICT_PK TEXT, 
	BILLING_MODE_PK TEXT,
	LOAD_DATE TIMESTAMP,
	RECORD_SOURCE VARCHAR
);

-- 3.3 CREATE LINK_BILLING, SOURCE:BILLING

drop table if exists "rtk_de"."yfurman"."project_dds_link_billing";
create table "rtk_de"."yfurman"."project_dds_link_billing" ( 
	BILLING_PK TEXT,
	USER_PK TEXT, 
	BILLING_PERIOD_PK TEXT, 
	SERVICE_PK TEXT, 
	TARIFF_PK TEXT,
	LOAD_DATE TIMESTAMP,
	RECORD_SOURCE VARCHAR
);

-- 3.4 CREATE LINK_ISSUE, SOURCE:ISSUE

drop table if exists "rtk_de"."yfurman"."project_dds_link_issue";
create table "rtk_de"."yfurman"."project_dds_link_issue" ( 
	ISSUE_PK TEXT,
	USER_PK TEXT, 
	SERVICE_PK TEXT,
	LOAD_DATE TIMESTAMP,
	RECORD_SOURCE VARCHAR
); 

-- 3.5 CREATE LINK_TRAFFIC, SOURCE:TRAFFIC

drop table if exists "rtk_de"."yfurman"."project_dds_link_traffic";
create table "rtk_de"."yfurman"."project_dds_link_traffic" ( 
	TRAFFIC_PK TEXT,
	USER_PK TEXT, 
	DEVICE_PK TEXT, 
	IP_ADDR_PK TEXT,
	LOAD_DATE TIMESTAMP,
	RECORD_SOURCE VARCHAR
); 


-- 4. CREATE SATELLITE

-- 4.1 CREATE SAT_USER_DETAILS, SOURCE:PAYMENT

drop table if exists "rtk_de"."yfurman"."project_dds_sat_user_details";
create table "rtk_de"."yfurman"."project_dds_sat_user_details" ( 
	USER_PK TEXT, 
	USER_HASHDIFF TEXT, 
	phone VARCHAR,
	EFFECTIVE_FROM TIMESTAMP, 
	LOAD_DATE TIMESTAMP,
	RECORD_SOURCE VARCHAR
);

-- 4.2 CREATE SAT_PAY_DETAILS, SOURCE:PAYMENT

drop table if exists "rtk_de"."yfurman"."project_dds_sat_pay_details";
create table "rtk_de"."yfurman"."project_dds_sat_pay_details" ( 
	PAYMENT_PK TEXT, 
	PAY_DOC_HASHDIFF TEXT, 
	pay_doc_num BIGINT, 
	pay_date DATE, 
	sum DECIMAL(10,2),
	EFFECTIVE_FROM TIMESTAMP, 
	LOAD_DATE TIMESTAMP,
	RECORD_SOURCE VARCHAR
);


-- 4.3 CREATE SAT_USER_MDM_DETAILS, SOURCE:MDM

drop table if exists "rtk_de"."yfurman"."project_dds_sat_mdm_details";
create table "rtk_de"."yfurman"."project_dds_sat_mdm_details" (
	MDM_PK TEXT, 
	MDM_HASHDIFF TEXT, 
	registered_at TIMESTAMP, 
	is_vip BOOL,
	EFFECTIVE_FROM TIMESTAMP, 
	LOAD_DATE TIMESTAMP,
	RECORD_SOURCE VARCHAR
);

-- 4.4 CREATE SAT_BILLING_DETAILS, SOURCE:BILLING

drop table if exists "rtk_de"."yfurman"."project_dds_sat_billing_details";
create table "rtk_de"."yfurman"."project_dds_sat_billing_details" (  
	BILLING_PK TEXT, 
	BILLING_HASHDIFF TEXT, 
	created_at TIMESTAMP, 
	sum DECIMAL(10,2),
	EFFECTIVE_FROM TIMESTAMP, 
	LOAD_DATE TIMESTAMP,
	RECORD_SOURCE VARCHAR
);

-- 4.5 CREATE SAT_ISSUE_DETAILS, SOURCE:ISSUE

drop table if exists "rtk_de"."yfurman"."project_dds_sat_issue_details";
create table "rtk_de"."yfurman"."project_dds_sat_issue_details" ( 
	ISSUE_PK TEXT, 
	ISSUE_HASHDIFF TEXT, 
	start_time TIMESTAMP, 
	end_time TIMESTAMP, 
	title VARCHAR,
	description VARCHAR, 
	service VARCHAR,
	EFFECTIVE_FROM TIMESTAMP, 
	LOAD_DATE TIMESTAMP,
	RECORD_SOURCE VARCHAR
);

-- 4.6 CREATE SAT_TRAFFIC_DETAILS, SOURCE:TRAFFIC

drop table if exists "rtk_de"."yfurman"."project_dds_sat_traffic_details";
create table "rtk_de"."yfurman"."project_dds_sat_traffic_details" (
	TRAFFIC_PK TEXT, 
	TRAFFIC_HASHDIFF TEXT, 
	time_stamp TIMESTAMP, 
	bytes_sent BIGINT, 
	bytes_received BIGINT,
	EFFECTIVE_FROM TIMESTAMP, 
	LOAD_DATE TIMESTAMP,
	RECORD_SOURCE VARCHAR
);


-- 5. CREATE DIMEMSIONs TABLEs

drop  table if exists yfurman.project_report_dim_billing_year;
create table yfurman.project_report_dim_billing_year (id SERIAL primary key, billing_year_key int);

drop  table if exists yfurman.project_report_dim_legal_type;
create table yfurman.project_report_dim_legal_type (id SERIAL primary key, legal_type_key TEXT);

drop  table if exists yfurman.project_report_dim_district;
create table yfurman.project_report_dim_district (id SERIAL primary key, district_key TEXT);

drop  table if exists yfurman.project_report_dim_registration_year;
create table yfurman.project_report_dim_registration_year (id SERIAL primary key, registration_year_key int);

drop  table if exists yfurman.project_report_dim_billing_mode;
create table yfurman.project_report_dim_billing_mode (id SERIAL primary key, billing_mode_key TEXT);


-- 6. CREATE FACTS TABLE

drop  table if exists yfurman.project_report_fct;
create table yfurman.project_report_fct (
	billing_year_id int,
	legal_type_id int,
	district_id int,
	registration_year_id int,
	billing_mode_id int,
	is_vip boolean,
	payment_sum numeric,
	billing_sum numeric,
	issue_cnt int,
	traffic_amount bigint,
	constraint fk_billing_year foreign key(billing_year_id) references yfurman.project_report_dim_billing_year(id),
	constraint fk_legal_type foreign key(legal_type_id) references yfurman.project_report_dim_legal_type(id),	
	constraint fk_district foreign key(district_id) references yfurman.project_report_dim_district(id),
	constraint fk_registration_year foreign key(registration_year_id) references yfurman.project_report_dim_registration_year(id),
	constraint fk_billing_mode foreign key(billing_mode_id) references yfurman.project_report_dim_billing_mode(id)
);



------------------------- TEST BLOCK -------------------------


drop function if exists BEGIN_DATE();
create or replace function BEGIN_DATE() returns timestamp 
as $$
	select '2010-01-01'::timestamp;
$$ language sql immutable;

drop function if exists END_DATE();
create or replace function END_DATE() returns timestamp 
as $$
	select '2010-01-01'::timestamp;
$$ language sql immutable;

select BEGIN_DATE();
select END_DATE();


-- 7. TEST ETL insert 

-- 7.1 ETL for ODS

-- 7.1.1 insert into ODS: PAYMENT


do $$
declare 
	start INT := extract(year from BEGIN_DATE());
	stop  INT := extract(year from END_DATE());
	cur_date VARCHAR;
BEGIN
for year_num in start..stop
loop
	cur_date := to_char(to_date(year_num::VARCHAR, 'YYYY'), 'YYYY-MM-DD');
	execute 'alter table if exists yfurman.project_ods_payment_test truncate partition for( ''' || cur_date::text || ''')';
end LOOP;
end $$ language plpgsql;
	

truncate yfurman.project_ods_payment;

insert into yfurman.project_ods_payment (
	select 
		user_id, 
		pay_doc_type, 
		pay_doc_num::BIGINT,  
		account, 
		phone, 
		billing_period, 
		pay_date::DATE, 
		sum::DECIMAL(10,2)
	from yfurman.project_stg_payment
	where cast(extract('year' from cast(pay_date as timestamp)) as int) 
		between extract(year from BEGIN_DATE()) and extract(year from END_DATE())
);

select * from yfurman.project_ods_payment limit 100;
select count(*) from yfurman.project_ods_payment;

-- 7.1.2 insert into ODS: BILLING

-- alter table if exists yfurman.project_ods_billing truncate partition for(BEGIN_DATE());

truncate yfurman.project_ods_billing;
insert into yfurman.project_ods_billing (
	select 
		user_id, 
		billing_period,   
		service, 
		tariff, 
		sum::DECIMAL(10,2),
		created_at::TIMESTAMP
	from yfurman.project_stg_billing
	where cast(extract('year' from cast(created_at as timestamp)) as int) 
		between extract(year from BEGIN_DATE()) and extract(year from END_DATE())
);

select * from yfurman.project_ods_billing limit 100;
select count(*) from yfurman.project_ods_billing;

-- 7.1.3 insert into ODS: ISSUE

-- alter table if exists yfurman.project_ods_issue truncate partition for(BEGIN_DATE());

truncate yfurman.project_ods_issue;
insert into yfurman.project_ods_issue (
	select 
		user_id::INT, 
		start_time::TIMESTAMP,
		end_time::TIMESTAMP,
		title,
		description,
		service
	from yfurman.project_stg_issue
	where cast(extract('year' from cast(start_time as timestamp)) as int) 
		between extract(year from BEGIN_DATE()) and extract(year from END_DATE())
);

select * from yfurman.project_ods_issue limit 100;
select count(*) from yfurman.project_ods_issue;

-- 7.1.4 insert into ODS: TRAFFIC

-- alter table if exists yfurman.project_ods_traffic truncate partition for(BEGIN_DATE());

truncate yfurman.project_ods_traffic;
insert into yfurman.project_ods_traffic (
	select 
		user_id, 			
		to_timestamp(div(timestamp, 1000))::TIMESTAMP as time_stamp,
		device_id,   
		device_ip_addr, 
		bytes_sent::BIGINT,
		bytes_received::BIGINT
	from yfurman.project_stg_traffic
	where cast(extract(year from to_timestamp(div(timestamp, 1000))) as int)
		between extract(year from BEGIN_DATE()) and extract(year from END_DATE())
);

select * from yfurman.project_ods_traffic limit 100;
select count(*) from yfurman.project_ods_traffic;

-- 7.1.5 insert into ODS: MDM

-- alter table if exists yfurman.project_ods_mdm truncate partition for(BEGIN_DATE());

truncate yfurman.project_ods_mdm;
insert into yfurman.project_ods_mdm (
	select *
	from mdm.user
	where cast(extract('year' from cast(registered_at as timestamp)) as int) 
		between extract(year from BEGIN_DATE()) and extract(year from END_DATE())
);

select * from yfurman.project_ods_mdm limit 100;
select count(*) from yfurman.project_ods_mdm;


-- 7.2 CREATE VIEW FOR ODS WITH HASH FIELDS

-- 7.2.1 CREATE VIEW FOR PAYMENT

drop view if exists "yfurman"."project_view_payment_one_year";
create or replace view yfurman.project_view_payment_one_year as (

	with staging as (
		with derived_columns as (
			select
				user_id,
				pay_doc_type,
				pay_doc_num,
				account,
				phone,
				billing_period,
				pay_date,
				sum,
				user_id::varchar as USER_KEY,
				account::varchar as ACCOUNT_KEY,
				billing_period::varchar as BILLING_PERIOD_KEY,
				pay_doc_type::varchar as PAY_DOC_TYPE_KEY,
				pay_doc_num::varchar as PAY_DOC_NUM_KEY,
				'PAYMENT - DATA LAKE'::varchar as RECORD_SOURCE
			from yfurman.project_ods_payment 
			where cast(extract('year' from cast(pay_date as timestamp)) as int) 
				between extract(year from BEGIN_DATE()) and extract(year from END_DATE())
		),
		
		hashed_columns as (
			select
				user_id,
				pay_doc_type,
				pay_doc_num,
				account,
				phone,
				billing_period,
				pay_date,
				sum,
				USER_KEY,
				ACCOUNT_KEY,
				BILLING_PERIOD_KEY,
				PAY_DOC_TYPE_KEY,
				PAY_DOC_NUM_KEY,
				RECORD_SOURCE,
				
				cast((md5(nullif(upper(trim(cast(user_id as varchar))), ''))) as TEXT) as USER_PK,
				cast((md5(nullif(upper(trim(cast(account as varchar))), ''))) as TEXT) as ACCOUNT_PK,
				cast((md5(nullif(upper(trim(cast(billing_period as varchar))), ''))) as TEXT) as BILLING_PERIOD_PK,
				cast((md5(nullif(upper(trim(cast(pay_doc_type as varchar))), ''))) as TEXT) as PAY_DOC_TYPE_PK,				
				cast(md5(nullif(concat_ws('||',
					coalesce(nullif(upper(trim(cast(user_id as varchar))), ''), '^^'),
					coalesce(nullif(upper(trim(cast(account as varchar))), ''), '^^'),
					coalesce(nullif(upper(trim(cast(billing_period as varchar))), ''), '^^'),
					coalesce(nullif(upper(trim(cast(pay_doc_type as varchar))), ''), '^^')
				), '^^||^^||^^||^^')) as TEXT) as PAYMENT_PK,
				cast(md5(concat_ws('||',
					coalesce(nullif(upper(trim(cast(phone as varchar))), ''), '^^')
				)) as TEXT) as USER_HASHDIFF,
				cast(md5(concat_ws('||',
					coalesce(nullif(upper(trim(cast(pay_doc_num as varchar))), ''), '^^'),				
					coalesce(nullif(upper(trim(cast(pay_date as varchar))), ''), '^^'),					
					coalesce(nullif(upper(trim(cast(sum as varchar))), ''), '^^')					
				)) as TEXT) as PAY_DOC_HASHDIFF
			from derived_columns
		),
		
		columns_to_select as (
			select
				user_id,
				pay_doc_type,
				pay_doc_num,
				account,
				phone,
				billing_period,
				pay_date,
				sum,
				USER_KEY,
				ACCOUNT_KEY,
				BILLING_PERIOD_KEY,
				PAY_DOC_TYPE_KEY,
				PAY_DOC_NUM_KEY,
				RECORD_SOURCE,
				USER_PK,
				ACCOUNT_PK,
				BILLING_PERIOD_PK,
				PAY_DOC_TYPE_PK,
				PAYMENT_PK,				
				USER_HASHDIFF,
				PAY_DOC_HASHDIFF
			from hashed_columns
		)
		
		select * from columns_to_select
	)
	
	select *, 
			END_DATE() as LOAD_DATE,
			pay_date::timestamp as EFFECTIVE_FROM
	from staging
);

select * from yfurman.project_view_payment_one_year limit 100;

-- 7.2.2 CREATE VIEW FOR MDM

drop view if exists "rtk_de"."yfurman"."project_view_mdm_one_year";
create or replace view "rtk_de"."yfurman"."project_view_mdm_one_year" as (

	with staging as (
		with derived_columns as (
			select
				id,
				legal_type,
				district,
				registered_at,
				billing_mode,
				is_vip,
				id::varchar as USER_KEY,
				legal_type::varchar as LEGAL_TYPE_KEY,
				district::varchar as DISTRICT_KEY,
				billing_mode::varchar as BILLING_MODE_KEY,
				'MDM - DATA LAKE'::varchar as RECORD_SOURCE
			from yfurman.project_ods_mdm
			where cast(extract('year' from cast(registered_at as timestamp)) as int) 
				between extract(year from BEGIN_DATE()) and extract(year from END_DATE())
		),
		
		hashed_columns as (
			select
				id,
				legal_type,
				district,
				registered_at,
				billing_mode,
				is_vip,
				USER_KEY,
				LEGAL_TYPE_KEY,
				DISTRICT_KEY,
				BILLING_MODE_KEY,				
				RECORD_SOURCE,
				
				cast((md5(nullif(upper(trim(cast(id as varchar))), ''))) as TEXT) as USER_PK,
				cast((md5(nullif(upper(trim(cast(legal_type as varchar))), ''))) as TEXT) as LEGAL_TYPE_PK,
				cast((md5(nullif(upper(trim(cast(district as varchar))), ''))) as TEXT) as DISTRICT_PK,
				cast((md5(nullif(upper(trim(cast(billing_mode as varchar))), ''))) as TEXT) as BILLING_MODE_PK,
				cast(md5(nullif(concat_ws('||',
					coalesce(nullif(upper(trim(cast(id as varchar))), ''), '^^'),
					coalesce(nullif(upper(trim(cast(legal_type as varchar))), ''), '^^'),
					coalesce(nullif(upper(trim(cast(district as varchar))), ''), '^^'),
					coalesce(nullif(upper(trim(cast(billing_mode as varchar))), ''), '^^')
				), '^^||^^||^^||^^')) as TEXT) as MDM_PK,				
				cast(md5(concat_ws('||',
					coalesce(nullif(upper(trim(cast(registered_at as varchar))), ''), '^^'),
					coalesce(nullif(upper(trim(cast(is_vip as varchar))), ''), '^^')
				)) as TEXT) as MDM_HASHDIFF
			from derived_columns
		),
		
		columns_to_select as (
			select
				id,
				legal_type,
				district,
				registered_at,
				billing_mode,
				is_vip,
				USER_KEY,
				LEGAL_TYPE_KEY,
				DISTRICT_KEY,
				BILLING_MODE_KEY,					
				RECORD_SOURCE,
				USER_PK,
				LEGAL_TYPE_PK,
				DISTRICT_PK,
				BILLING_MODE_PK,
				MDM_PK,
				MDM_HASHDIFF
			from hashed_columns
		)
		
		select * from columns_to_select
	)
	
	select *, 
			-- current_timestamp as LOAD_DATE,			
	        --'2013-01-01'::timestamp as LOAD_DATE,
			END_DATE() as LOAD_DATE,
			registered_at as EFFECTIVE_FROM
	from staging
);

select * from yfurman.project_view_mdm_one_year limit 100;

-- 7.2.3 CREATE VIEW FOR BILLING

drop view if exists "rtk_de"."yfurman"."project_view_billing_one_year";
create or replace view "rtk_de"."yfurman"."project_view_billing_one_year" as (

	with staging as (
		with derived_columns as (
			select
				user_id,
				billing_period,
				service,
				tariff,
				sum,
				created_at,
				user_id::varchar as USER_KEY,
				billing_period::varchar as BILLING_PERIOD_KEY,
				service::varchar as SERVICE_KEY,
				tariff::varchar as TARIFF_KEY,
				'BILLING - DATA LAKE'::varchar as RECORD_SOURCE
			from yfurman.project_ods_billing
			where cast(extract('year' from cast(created_at as timestamp)) as int) 
				between extract(year from BEGIN_DATE()) and extract(year from END_DATE())
		),
		
		hashed_columns as (
			select
				user_id,
				billing_period,
				service,
				tariff,
				sum,
				created_at,
				USER_KEY,
				BILLING_PERIOD_KEY,
				SERVICE_KEY,
				TARIFF_KEY,				
				RECORD_SOURCE,
				
				cast((md5(nullif(upper(trim(cast(user_id as varchar))), ''))) as TEXT) as USER_PK,
				cast((md5(nullif(upper(trim(cast(billing_period as varchar))), ''))) as TEXT) as BILLING_PERIOD_PK,
				cast((md5(nullif(upper(trim(cast(service as varchar))), ''))) as TEXT) as SERVICE_PK,
				cast((md5(nullif(upper(trim(cast(tariff as varchar))), ''))) as TEXT) as TARIFF_PK,
				cast(md5(nullif(concat_ws('||',
					coalesce(nullif(upper(trim(cast(user_id as varchar))), ''), '^^'),
					coalesce(nullif(upper(trim(cast(billing_period as varchar))), ''), '^^'),
					coalesce(nullif(upper(trim(cast(service as varchar))), ''), '^^'),
					coalesce(nullif(upper(trim(cast(tariff as varchar))), ''), '^^')
				), '^^||^^||^^||^^')) as TEXT) as BILLING_PK,				
				cast(md5(concat_ws('||',
					coalesce(nullif(upper(trim(cast(created_at as varchar))), ''), '^^'),
					coalesce(nullif(upper(trim(cast(sum as varchar))), ''), '^^')
				)) as TEXT) as BILLING_HASHDIFF
			from derived_columns
		),
		
		columns_to_select as (
			select
				user_id,
				billing_period,
				service,
				tariff,
				sum,
				created_at,
				USER_KEY,
				BILLING_PERIOD_KEY,
				SERVICE_KEY,
				TARIFF_KEY,						
				RECORD_SOURCE,
				USER_PK,
				BILLING_PERIOD_PK,
				SERVICE_PK,
				TARIFF_PK,
				BILLING_PK,
				BILLING_HASHDIFF
			from hashed_columns
		)
		
		select * from columns_to_select
	)
	
	select *, 
			-- current_timestamp as LOAD_DATE,			
	        --'2013-01-01'::timestamp as LOAD_DATE,
			END_DATE() as LOAD_DATE,
			created_at as EFFECTIVE_FROM
	from staging
);

select * from yfurman.project_view_billing_one_year limit 100;

-- 7.2.4 CREATE VIEW FOR ISSUE

drop view if exists "rtk_de"."yfurman"."project_view_issue_one_year";
create or replace view "rtk_de"."yfurman"."project_view_issue_one_year" as (

	with staging as (
		with derived_columns as (
			select
				user_id,
				start_time,
				end_time,
				title,
				description,
				service,
				user_id::varchar as USER_KEY,
				service::varchar as SERVICE_KEY,
				'ISSUE - DATA LAKE'::varchar as RECORD_SOURCE
			from yfurman.project_ods_issue			
			where cast(extract('year' from cast(start_time as timestamp)) as int) 
				between extract(year from BEGIN_DATE()) and extract(year from END_DATE())
		),
		
		hashed_columns as (
			select
				user_id,
				start_time,
				end_time,
				title,
				description,
				service,
				USER_KEY,
				SERVICE_KEY,
				RECORD_SOURCE,
				
				cast((md5(nullif(upper(trim(cast(user_id as varchar))), ''))) as TEXT) as USER_PK,			
				cast((md5(nullif(upper(trim(cast(service as varchar))), ''))) as TEXT) as SERVICE_PK,
				cast(md5(nullif(concat_ws('||',
					coalesce(nullif(upper(trim(cast(user_id as varchar))), ''), '^^'),
					coalesce(nullif(upper(trim(cast(service as varchar))), ''), '^^')
				), '^^||^^')) as TEXT) as ISSUE_PK,				
				cast(md5(concat_ws('||',
					coalesce(nullif(upper(trim(cast(start_time as varchar))), ''), '^^'),
					coalesce(nullif(upper(trim(cast(end_time as varchar))), ''), '^^'),
					coalesce(nullif(upper(trim(cast(title as varchar))), ''), '^^'),
					coalesce(nullif(upper(trim(cast(description as varchar))), ''), '^^')
				)) as TEXT) as ISSUE_HASHDIFF
			from derived_columns
		),
		
		columns_to_select as (
			select
				user_id,
				start_time,
				end_time,
				title,
				description,
				service,
				USER_KEY,
				SERVICE_KEY,
				RECORD_SOURCE,
				USER_PK,
				SERVICE_PK,
				ISSUE_PK,
				ISSUE_HASHDIFF
			from hashed_columns
		)
		
		select * from columns_to_select
	)
	
	select *, 
			-- current_timestamp as LOAD_DATE,			
	        --'2013-01-01'::timestamp as LOAD_DATE,
			END_DATE() as LOAD_DATE,
			start_time as EFFECTIVE_FROM
	from staging
);

select * from yfurman.project_view_issue_one_year limit 100;

-- 7.2.5 CREATE VIEW FOR TRAFFIC

drop view if exists "rtk_de"."yfurman"."project_view_traffic_one_year";
create or replace view "rtk_de"."yfurman"."project_view_traffic_one_year" as (

	with staging as (
		with derived_columns as (
			select
				user_id,
				time_stamp,
				device_id,
				device_ip_addr,
				bytes_sent,
				bytes_received,
				user_id::varchar as USER_KEY,
				device_id::varchar as DEVICE_KEY,
				device_ip_addr::varchar as IP_ADDR_KEY,
				'TRAFFIC - DATA LAKE'::varchar as RECORD_SOURCE
			from yfurman.project_ods_traffic
			--where cast(extract('year' from created_at) as int) = 2013
			where cast(extract('year' from cast(time_stamp as timestamp)) as int) 
				between extract(year from BEGIN_DATE()) and extract(year from END_DATE())
		),
		
		hashed_columns as (
			select
				user_id,
				time_stamp,
				device_id,
				device_ip_addr,
				bytes_sent,
				bytes_received,
				USER_KEY,
				DEVICE_KEY,
				IP_ADDR_KEY,
				RECORD_SOURCE,
				
				cast((md5(nullif(upper(trim(cast(user_id as varchar))), ''))) as TEXT) as USER_PK,			
				cast((md5(nullif(upper(trim(cast(device_id as varchar))), ''))) as TEXT) as DEVICE_PK,
				cast((md5(nullif(upper(trim(cast(device_ip_addr as varchar))), ''))) as TEXT) as IP_ADDR_PK,
				cast(md5(nullif(concat_ws('||',
					coalesce(nullif(upper(trim(cast(user_id as varchar))), ''), '^^'),
					coalesce(nullif(upper(trim(cast(device_id as varchar))), ''), '^^'),
					coalesce(nullif(upper(trim(cast(device_ip_addr as varchar))), ''), '^^')					
				), '^^||^^||^^')) as TEXT) as TRAFFIC_PK,				
				cast(md5(concat_ws('||',
					coalesce(nullif(upper(trim(cast(time_stamp as varchar))), ''), '^^'),
					coalesce(nullif(upper(trim(cast(bytes_sent as varchar))), ''), '^^'),
					coalesce(nullif(upper(trim(cast(bytes_received as varchar))), ''), '^^')
				)) as TEXT) as TRAFFIC_HASHDIFF
			from derived_columns
		),
		
		columns_to_select as (
			select
				user_id,
				time_stamp,
				device_id,
				device_ip_addr,
				bytes_sent,
				bytes_received,
				USER_KEY,
				DEVICE_KEY,
				IP_ADDR_KEY,
				RECORD_SOURCE,
				USER_PK,
				DEVICE_PK,
				IP_ADDR_PK,
				TRAFFIC_PK,
				TRAFFIC_HASHDIFF
			from hashed_columns
		)
		
		select * from columns_to_select
	)
	
	select *, 
			-- current_timestamp as LOAD_DATE,			
	        --'2013-01-01'::timestamp as LOAD_DATE,
			END_DATE() as LOAD_DATE,
			time_stamp as EFFECTIVE_FROM
	from staging
);

select * from yfurman.project_view_traffic_one_year limit 100;



-- 7.3 ETL for HUBs

-- 7.3.1 insert HUB_USER, SOURCE:PAYMENT, BILLING, ISSUE, TRAFFIC, MDM 

with row_rank_1 as (
	select * from (
		select USER_PK, USER_KEY, LOAD_DATE, RECORD_SOURCE,
			row_number() over (
				partition by USER_PK
				order by LOAD_DATE ASC
			) as row_num
		from yfurman.project_view_payment_one_year		
	) as h where row_num = 1
),	
row_rank_2 as (
	select * from (
		select USER_PK, USER_KEY, LOAD_DATE, RECORD_SOURCE,
			row_number() over (
				partition by USER_PK
				order by LOAD_DATE ASC
			) as row_num
		from yfurman.project_view_mdm_one_year		
	) as h where row_num = 1
),
row_rank_3 as (
	select * from (
		select USER_PK, USER_KEY, LOAD_DATE, RECORD_SOURCE,
			row_number() over (
				partition by USER_PK
				order by LOAD_DATE ASC
			) as row_num
		from yfurman.project_view_billing_one_year		
	) as h where row_num = 1
),
row_rank_4 as (
	select * from (
		select USER_PK, USER_KEY, LOAD_DATE, RECORD_SOURCE,
			row_number() over (
				partition by USER_PK
				order by LOAD_DATE ASC
			) as row_num
		from yfurman.project_view_issue_one_year		
	) as h where row_num = 1
),	
row_rank_5 as (
	select * from (
		select USER_PK, USER_KEY, LOAD_DATE, RECORD_SOURCE,
			row_number() over (
				partition by USER_PK
				order by LOAD_DATE ASC
			) as row_num
		from yfurman.project_view_traffic_one_year		
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

select * from yfurman.project_dds_hub_user limit 10;
select count(*) from yfurman.project_dds_hub_user;

-- 7.3.2 insert HUB_ACCOUNT, SOURCE:PAYMENT

with row_rank_1 as (
		select * from (
			select ACCOUNT_PK, ACCOUNT_KEY, LOAD_DATE, RECORD_SOURCE, pay_date,
				row_number() over (
					partition by ACCOUNT_PK
					order by LOAD_DATE ASC
				) as row_num
			from yfurman.project_view_payment_one_year  		
		) as h where row_num = 1
	),	
records_to_insert as (
		select a.ACCOUNT_PK, a.ACCOUNT_KEY, a.LOAD_DATE, a.RECORD_SOURCE
		from row_rank_1 as a
		left join yfurman.project_dds_hub_account as d
		on a.ACCOUNT_PK = d.ACCOUNT_PK
		where d.ACCOUNT_PK is NULL
)
insert into yfurman.project_dds_hub_account (ACCOUNT_PK, ACCOUNT_KEY, LOAD_DATE, RECORD_SOURCE)
(
	select ACCOUNT_PK, ACCOUNT_KEY, LOAD_DATE, RECORD_SOURCE
	from records_to_insert
);

select * from yfurman.project_dds_hub_account limit 10;
select count(*) from yfurman.project_dds_hub_account;

-- 7.3.3 insert HUB_BILLING_PERIOD, SOURCE:PAYMENT, BILLING 

with row_rank_1 as (
	select * from (
		select BILLING_PERIOD_PK, BILLING_PERIOD_KEY, LOAD_DATE, RECORD_SOURCE,
			row_number() over (
				partition by BILLING_PERIOD_PK
				order by LOAD_DATE ASC
			) as row_num
		from yfurman.project_view_payment_one_year		
	) as h where row_num = 1
),
row_rank_2 as (
	select * from (
		select BILLING_PERIOD_PK, BILLING_PERIOD_KEY, LOAD_DATE, RECORD_SOURCE,
			row_number() over (
				partition by BILLING_PERIOD_PK
				order by LOAD_DATE ASC
			) as row_num
		from yfurman.project_view_billing_one_year		
	) as h where row_num = 1
),
stage_union as (
	select BILLING_PERIOD_PK, BILLING_PERIOD_KEY, LOAD_DATE, RECORD_SOURCE from row_rank_1
	union all
	select BILLING_PERIOD_PK, BILLING_PERIOD_KEY, LOAD_DATE, RECORD_SOURCE from row_rank_2
),
raw_union as (
	select * from (
		select BILLING_PERIOD_PK, BILLING_PERIOD_KEY, LOAD_DATE, RECORD_SOURCE,
			row_number() over (
				partition by BILLING_PERIOD_PK
				order by LOAD_DATE ASC
			) as row_num
		from stage_union
		where BILLING_PERIOD_PK is not NULL
	) as h where row_num = 1	
),	
records_to_insert as (
		select a.BILLING_PERIOD_PK, a.BILLING_PERIOD_KEY, a.LOAD_DATE, a.RECORD_SOURCE
		from raw_union as a
		left join yfurman.project_dds_hub_billing_period as d
		on a.BILLING_PERIOD_PK = d.BILLING_PERIOD_PK
		where d.BILLING_PERIOD_PK is NULL
)
insert into yfurman.project_dds_hub_billing_period (BILLING_PERIOD_PK, BILLING_PERIOD_KEY, LOAD_DATE, RECORD_SOURCE)
(
	select BILLING_PERIOD_PK, BILLING_PERIOD_KEY, LOAD_DATE, RECORD_SOURCE
	from records_to_insert
);

select * from yfurman.project_dds_hub_billing_period limit 10;
select count(*) from yfurman.project_dds_hub_billing_period;

-- 7.3.4 insert HUB_PAY_DOC_TYPE, SOURCE:PAYMENT

with row_rank_1 as (
		select * from (
			select PAY_DOC_TYPE_PK, PAY_DOC_TYPE_KEY, LOAD_DATE, RECORD_SOURCE,
				row_number() over (
					partition by PAY_DOC_TYPE_PK
					order by LOAD_DATE ASC
				) as row_num
			from yfurman.project_view_payment_one_year  		
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

select * from yfurman.project_dds_hub_pay_doc_type limit 10;
select count(*) from yfurman.project_dds_hub_pay_doc_type;

-- 7.3.5 insert HUB_LEGAL_TYPE, SOURCE:MDM 

with row_rank_1 as (
	select * from (
		select LEGAL_TYPE_PK, LEGAL_TYPE_KEY, LOAD_DATE, RECORD_SOURCE,
			row_number() over (
				partition by LEGAL_TYPE_PK
				order by LOAD_DATE ASC
			) as row_num
		from yfurman.project_view_mdm_one_year		
	) as h where row_num = 1
),
records_to_insert as (
		select a.LEGAL_TYPE_PK, a.LEGAL_TYPE_KEY, a.LOAD_DATE, a.RECORD_SOURCE
		from row_rank_1 as a
		left join yfurman.project_dds_hub_legal_type as d
		on a.LEGAL_TYPE_PK = d.LEGAL_TYPE_PK
		where d.LEGAL_TYPE_PK is NULL
)
insert into yfurman.project_dds_hub_legal_type (LEGAL_TYPE_PK, LEGAL_TYPE_KEY, LOAD_DATE, RECORD_SOURCE)
(
	select LEGAL_TYPE_PK, LEGAL_TYPE_KEY, LOAD_DATE, RECORD_SOURCE
	from records_to_insert
);	

select * from yfurman.project_dds_hub_legal_type limit 10;
select count(*) from yfurman.project_dds_hub_legal_type;

-- 7.3.6 insert HUB_DISTRICT, SOURCE:MDM

with row_rank_1 as (
	select * from (
		select DISTRICT_PK, DISTRICT_KEY, LOAD_DATE, RECORD_SOURCE,
			row_number() over (
				partition by DISTRICT_PK
				order by LOAD_DATE ASC
			) as row_num
		from yfurman.project_view_mdm_one_year		
	) as h where row_num = 1
),
records_to_insert as (
		select a.DISTRICT_PK, a.DISTRICT_KEY, a.LOAD_DATE, a.RECORD_SOURCE
		from row_rank_1 as a
		left join yfurman.project_dds_hub_district as d
		on a.DISTRICT_PK = d.DISTRICT_PK
		where d.DISTRICT_PK is NULL
)
insert into yfurman.project_dds_hub_district (DISTRICT_PK, DISTRICT_KEY, LOAD_DATE, RECORD_SOURCE)
(
	select DISTRICT_PK, DISTRICT_KEY, LOAD_DATE, RECORD_SOURCE
	from records_to_insert
);	
	

select * from yfurman.project_dds_hub_district limit 10;
select count(*) from yfurman.project_dds_hub_district;

-- 7.3.7 insert HUB_BILLING_MODE, SOURCE:MDM 

with row_rank_1 as (
	select * from (
		select BILLING_MODE_PK, BILLING_MODE_KEY, LOAD_DATE, RECORD_SOURCE,
			row_number() over (
				partition by BILLING_MODE_PK
				order by LOAD_DATE ASC
			) as row_num
		from yfurman.project_view_mdm_one_year		
	) as h where row_num = 1
),
records_to_insert as (
		select a.BILLING_MODE_PK, a.BILLING_MODE_KEY, a.LOAD_DATE, a.RECORD_SOURCE
		from row_rank_1 as a
		left join yfurman.project_dds_hub_billing_mode as d
		on a.BILLING_MODE_PK = d.BILLING_MODE_PK
		where d.BILLING_MODE_PK is NULL
)
insert into yfurman.project_dds_hub_billing_mode (BILLING_MODE_PK, BILLING_MODE_KEY, LOAD_DATE, RECORD_SOURCE)
(
	select BILLING_MODE_PK, BILLING_MODE_KEY, LOAD_DATE, RECORD_SOURCE
	from records_to_insert
);

select * from yfurman.project_dds_hub_billing_mode limit 10;
select count(*) from yfurman.project_dds_hub_billing_mode;

-- 7.3.8 insert HUB_TARIFF, SOURCE:BILLING

with row_rank_1 as (
	select * from (
		select TARIFF_PK, TARIFF_KEY, LOAD_DATE, RECORD_SOURCE,
			row_number() over (
				partition by TARIFF_PK
				order by LOAD_DATE ASC
			) as row_num
		from yfurman.project_view_billing_one_year		
	) as h where row_num = 1
),
records_to_insert as (
		select a.TARIFF_PK, a.TARIFF_KEY, a.LOAD_DATE, a.RECORD_SOURCE
		from row_rank_1 as a
		left join yfurman.project_dds_hub_tariff as d
		on a.TARIFF_PK = d.TARIFF_PK
		where d.TARIFF_PK is NULL
)
insert into yfurman.project_dds_hub_tariff (TARIFF_PK, TARIFF_KEY, LOAD_DATE, RECORD_SOURCE)
(
	select TARIFF_PK, TARIFF_KEY, LOAD_DATE, RECORD_SOURCE
	from records_to_insert
);	

select * from yfurman.project_dds_hub_tariff limit 10;
select count(*) from yfurman.project_dds_hub_tariff;

-- 7.3.9 insert HUB_SERVICE, SOURCE:BILLING, ISSUE

with row_rank_1 as (
	select * from (
		select SERVICE_PK, SERVICE_KEY, LOAD_DATE, RECORD_SOURCE,
			row_number() over (
				partition by SERVICE_PK
				order by LOAD_DATE ASC
			) as row_num
		from yfurman.project_view_billing_one_year		
	) as h where row_num = 1
),
row_rank_2 as (
	select * from (
		select SERVICE_PK, SERVICE_KEY, LOAD_DATE, RECORD_SOURCE,
			row_number() over (
				partition by SERVICE_PK
				order by LOAD_DATE ASC
			) as row_num
		from yfurman.project_view_issue_one_year
	) as h where row_num = 1
),
stage_union as (
	select SERVICE_PK, SERVICE_KEY, LOAD_DATE, RECORD_SOURCE from row_rank_1
	union all
	select SERVICE_PK, SERVICE_KEY, LOAD_DATE, RECORD_SOURCE from row_rank_2
),
raw_union as (
	select * from (
		select SERVICE_PK, SERVICE_KEY, LOAD_DATE, RECORD_SOURCE,
			row_number() over (
				partition by SERVICE_PK
				order by LOAD_DATE ASC
			) as row_num
		from stage_union
		where SERVICE_PK is not NULL
	) as h where row_num = 1	
),	
records_to_insert as (
		select a.SERVICE_PK, a.SERVICE_KEY, a.LOAD_DATE, a.RECORD_SOURCE
		from raw_union as a
		left join yfurman.project_dds_hub_service as d
		on a.SERVICE_PK = d.SERVICE_PK
		where d.SERVICE_PK is NULL
)
insert into yfurman.project_dds_hub_service (SERVICE_PK, SERVICE_KEY, LOAD_DATE, RECORD_SOURCE)
(
	select SERVICE_PK, SERVICE_KEY, LOAD_DATE, RECORD_SOURCE
	from records_to_insert
);

select * from yfurman.project_dds_hub_service limit 10;
select count(*) from yfurman.project_dds_hub_service;

-- 7.3.10 insert HUB_DEVICE, SOURCE:TRAFFIC

with row_rank_1 as (
	select * from (
		select DEVICE_PK, DEVICE_KEY, LOAD_DATE, RECORD_SOURCE,
			row_number() over (
				partition by DEVICE_PK
				order by LOAD_DATE ASC
			) as row_num
		from yfurman.project_view_traffic_one_year		
	) as h where row_num = 1
),
records_to_insert as (
		select a.DEVICE_PK, a.DEVICE_KEY, a.LOAD_DATE, a.RECORD_SOURCE
		from row_rank_1 as a
		left join yfurman.project_dds_hub_device as d
		on a.DEVICE_PK = d.DEVICE_PK
		where d.DEVICE_PK is NULL
)
insert into yfurman.project_dds_hub_device (DEVICE_PK, DEVICE_KEY, LOAD_DATE, RECORD_SOURCE)
(
	select DEVICE_PK, DEVICE_KEY, LOAD_DATE, RECORD_SOURCE
	from records_to_insert
);		

select * from yfurman.project_dds_hub_device limit 10;
select count(*) from yfurman.project_dds_hub_device;

-- 7.3.11 insert HUB_IP_ADDR, SOURCE:TRAFFIC

with row_rank_1 as (
	select * from (
		select IP_ADDR_PK, IP_ADDR_KEY, LOAD_DATE, RECORD_SOURCE,
			row_number() over (
				partition by IP_ADDR_PK
				order by LOAD_DATE ASC
			) as row_num
		from yfurman.project_view_traffic_one_year		
	) as h where row_num = 1
),
records_to_insert as (
		select a.IP_ADDR_PK, a.IP_ADDR_KEY, a.LOAD_DATE, a.RECORD_SOURCE
		from row_rank_1 as a
		left join yfurman.project_dds_hub_ip_addr as d
		on a.IP_ADDR_PK = d.IP_ADDR_PK
		where d.IP_ADDR_PK is NULL
)
insert into yfurman.project_dds_hub_ip_addr (IP_ADDR_PK, IP_ADDR_KEY, LOAD_DATE, RECORD_SOURCE)
(
	select IP_ADDR_PK, IP_ADDR_KEY, LOAD_DATE, RECORD_SOURCE
	from records_to_insert
);	

select * from yfurman.project_dds_hub_ip_addr limit 10;
select count(*) from yfurman.project_dds_hub_ip_addr;



-- 7.4 ETL for LINKs

-- 7.4.1 insert LINK_PAYMENT, SOURCE:PAYMENT

with source_data as (
	select 
		PAYMENT_PK,
		USER_PK, ACCOUNT_PK, BILLING_PERIOD_PK, PAY_DOC_TYPE_PK,
		LOAD_DATE, RECORD_SOURCE
	from yfurman.project_view_payment_one_year
),
records_to_insert as (
	select distinct 
		stg.PAYMENT_PK, 
		stg.USER_PK, stg.ACCOUNT_PK, stg.BILLING_PERIOD_PK, stg.PAY_DOC_TYPE_PK,
		stg.LOAD_DATE, stg.RECORD_SOURCE
	from source_data as stg 
	left join yfurman.project_dds_link_payment as tgt
	on stg.PAYMENT_PK = tgt.PAYMENT_PK
	where tgt.PAYMENT_PK is null		
)
insert into yfurman.project_dds_link_payment (
	PAYMENT_PK,
	USER_PK, ACCOUNT_PK, BILLING_PERIOD_PK, PAY_DOC_TYPE_PK,
	LOAD_DATE, RECORD_SOURCE)
(
	select 
		PAYMENT_PK,
		USER_PK, ACCOUNT_PK, BILLING_PERIOD_PK, PAY_DOC_TYPE_PK,
		LOAD_DATE, RECORD_SOURCE
	from records_to_insert
);

select * from yfurman.project_dds_link_payment limit 10;
select count(*) from yfurman.project_dds_link_payment;


-- 7.4.2 insert LINK_MDM, SOURCE:MDM

with source_data as (
	select 
		MDM_PK,
		USER_PK, LEGAL_TYPE_PK, DISTRICT_PK, BILLING_MODE_PK,
		LOAD_DATE, RECORD_SOURCE
	from yfurman.project_view_mdm_one_year
),
records_to_insert as (
	select distinct 
		stg.MDM_PK, 
		stg.USER_PK, stg.LEGAL_TYPE_PK, stg.DISTRICT_PK, stg.BILLING_MODE_PK,
		stg.LOAD_DATE, stg.RECORD_SOURCE
	from source_data as stg 
	left join yfurman.project_dds_link_mdm as tgt
	on stg.MDM_PK = tgt.MDM_PK
	where tgt.MDM_PK is null		
)
insert into yfurman.project_dds_link_mdm (
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

select * from yfurman.project_dds_link_mdm limit 10;
select count(*) from yfurman.project_dds_link_mdm;

-- 7.4.3 insert LINK_BILLING, SOURCE:BILLING

with source_data as (
	select 
		BILLING_PK,
		USER_PK, BILLING_PERIOD_PK, SERVICE_PK, TARIFF_PK,
		LOAD_DATE, RECORD_SOURCE
	from yfurman.project_view_billing_one_year
),
records_to_insert as (
	select distinct 
		stg.BILLING_PK, 
		stg.USER_PK, stg.BILLING_PERIOD_PK, stg.SERVICE_PK, stg.TARIFF_PK,
		stg.LOAD_DATE, stg.RECORD_SOURCE
	from source_data as stg 
	left join yfurman.project_dds_link_billing as tgt
	on stg.BILLING_PK = tgt.BILLING_PK
	where tgt.BILLING_PK is null		
)
insert into yfurman.project_dds_link_billing (
	BILLING_PK,
	USER_PK, BILLING_PERIOD_PK, SERVICE_PK, TARIFF_PK,
	LOAD_DATE, RECORD_SOURCE)
(
	select 
		BILLING_PK,
		USER_PK, BILLING_PERIOD_PK, SERVICE_PK, TARIFF_PK,
		LOAD_DATE, RECORD_SOURCE
	from records_to_insert
);

select * from yfurman.project_dds_link_billing limit 10;
select count(*) from yfurman.project_dds_link_billing;

-- 7.4.4 insert LINK_ISSUE, SOURCE:ISSUE

with source_data as (
	select 
		ISSUE_PK,
		USER_PK, SERVICE_PK,
		LOAD_DATE, RECORD_SOURCE
	from yfurman.project_view_issue_one_year
),
records_to_insert as (
	select distinct 
		stg.ISSUE_PK, 
		stg.USER_PK, stg.SERVICE_PK,
		stg.LOAD_DATE, stg.RECORD_SOURCE
	from source_data as stg 
	left join yfurman.project_dds_link_issue as tgt
	on stg.ISSUE_PK = tgt.ISSUE_PK
	where tgt.ISSUE_PK is null		
)
insert into yfurman.project_dds_link_issue (
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

select * from yfurman.project_dds_link_issue limit 10;
select count(*) from yfurman.project_dds_link_issue;

-- 7.4.5 insert LINK_TRAFFIC, SOURCE:TRAFFIC

with source_data as (
	select 
		TRAFFIC_PK,
		USER_PK, DEVICE_PK, IP_ADDR_PK,
		LOAD_DATE, RECORD_SOURCE
	from yfurman.project_view_traffic_one_year
),
records_to_insert as (
	select distinct 
		stg.TRAFFIC_PK, 
		stg.USER_PK, stg.DEVICE_PK, stg.IP_ADDR_PK,
		stg.LOAD_DATE, stg.RECORD_SOURCE
	from source_data as stg 
	left join yfurman.project_dds_link_traffic as tgt
	on stg.TRAFFIC_PK = tgt.TRAFFIC_PK
	where tgt.TRAFFIC_PK is null		
)
insert into yfurman.project_dds_link_traffic (
	TRAFFIC_PK,
	USER_PK, DEVICE_PK, IP_ADDR_PK,
	LOAD_DATE, RECORD_SOURCE)
(
	select 
		TRAFFIC_PK,
		USER_PK, DEVICE_PK, IP_ADDR_PK,
		LOAD_DATE, RECORD_SOURCE
	from records_to_insert
);

select * from yfurman.project_dds_link_traffic limit 10;
select count(*) from yfurman.project_dds_link_traffic;


-- 7.5 ETL for SATELLITEs

-- 7.5.1 insert SAT_USER_DETAILS, SOURCE:PAYMENT

with source_data as (
	select * from (
		select 
			USER_PK, USER_HASHDIFF, 
			phone, 
			EFFECTIVE_FROM, 
			LOAD_DATE, RECORD_SOURCE,
			lag(phone) OVER (PARTITION BY USER_PK ORDER BY EFFECTIVE_FROM) AS prev_state
		from yfurman.project_view_payment_one_year
	) as sd where phone IS DISTINCT FROM prev_state
),
update_records as (
	select 
		a.USER_PK, a.USER_HASHDIFF, 
		a.phone, 
		a.EFFECTIVE_FROM, 
		a.LOAD_DATE, a.RECORD_SOURCE
	from yfurman.project_dds_sat_user_details as a
	join source_data as b
	on a.USER_PK = b.USER_PK
	where  a.LOAD_DATE <= b.LOAD_DATE
),
latest_records as (
	select * from (
		select USER_PK, USER_HASHDIFF, LOAD_DATE,
			case when rank() over (partition by USER_PK order by LOAD_DATE desc) = 1
				then 'Y' 
				else 'N'
			end as latest
		from update_records
	) as s
	where latest = 'Y'
),	
records_to_insert as (
	select distinct 
		e.USER_PK, e.USER_HASHDIFF, 
		e.phone, 
		e.EFFECTIVE_FROM, 
		e.LOAD_DATE, e.RECORD_SOURCE
	from source_data as e
	left join latest_records
	on latest_records.USER_HASHDIFF = e.USER_HASHDIFF and 
	   latest_records.USER_PK = e.USER_PK
	where latest_records.USER_HASHDIFF is NULL
)	
insert into yfurman.project_dds_sat_user_details (
	USER_PK, USER_HASHDIFF, 
	phone, 
	EFFECTIVE_FROM, 
	LOAD_DATE, RECORD_SOURCE)
(
	select 
		USER_PK, USER_HASHDIFF, 
		phone, 
		EFFECTIVE_FROM, 
		LOAD_DATE, RECORD_SOURCE
	from records_to_insert
);

select * from yfurman.project_dds_sat_user_details limit 10;
select count(*) from yfurman.project_dds_sat_user_details;

-- 7.5.2 insert SAT_PAY_DETAILS, SOURCE:PAYMENT

with source_data as (
	select 
		PAYMENT_PK, PAY_DOC_HASHDIFF, 
		pay_doc_num, pay_date, sum, 
		EFFECTIVE_FROM, 
		LOAD_DATE, RECORD_SOURCE
	from yfurman.project_view_payment_one_year
),
update_records as (
	select 
		a.PAYMENT_PK, a.PAY_DOC_HASHDIFF, 
		a.pay_doc_num, a.pay_date, a.sum,
		a.EFFECTIVE_FROM, 
		a.LOAD_DATE, a.RECORD_SOURCE
	from yfurman.project_dds_sat_pay_details as a
	join source_data as b
	on a.PAYMENT_PK = b.PAYMENT_PK
	where  a.LOAD_DATE <= b.LOAD_DATE
),
latest_records as (
	select * from (
		select PAYMENT_PK, PAY_DOC_HASHDIFF, LOAD_DATE,
			case when rank() over (partition by PAYMENT_PK order by LOAD_DATE desc) = 1
				then 'Y' 
				else 'N'
			end as latest
		from update_records
	) as s
	where latest = 'Y'
),	
records_to_insert as (
	select distinct 
		e.PAYMENT_PK, e.PAY_DOC_HASHDIFF, 
		e.pay_doc_num, e.pay_date, e.sum,
		e.EFFECTIVE_FROM, 
		e.LOAD_DATE, e.RECORD_SOURCE
	from source_data as e
	left join latest_records
	on latest_records.PAY_DOC_HASHDIFF = e.PAY_DOC_HASHDIFF and 
	   latest_records.PAYMENT_PK = e.PAYMENT_PK
	where latest_records.PAY_DOC_HASHDIFF is NULL
)	
insert into yfurman.project_dds_sat_pay_details (
	PAYMENT_PK, PAY_DOC_HASHDIFF, 
	pay_doc_num, pay_date, sum, 
	EFFECTIVE_FROM, 
	LOAD_DATE, RECORD_SOURCE)
(
	select 
		PAYMENT_PK, PAY_DOC_HASHDIFF, 
		pay_doc_num, pay_date, sum, 
		EFFECTIVE_FROM, 
		LOAD_DATE, RECORD_SOURCE
	from records_to_insert
);

select * from yfurman.project_dds_sat_pay_details limit 10;
select count(*) from yfurman.project_dds_sat_pay_details;

-- 7.5.3 insert SAT_USER_MDM_DETAILS, SOURCE:MDM

with source_data as (
	select 
		MDM_PK, MDM_HASHDIFF, 
		registered_at, is_vip,
		EFFECTIVE_FROM, 
		LOAD_DATE, RECORD_SOURCE
	from yfurman.project_view_mdm_one_year
),
update_records as (
	select 
		a.MDM_PK, a.MDM_HASHDIFF, 
		a.registered_at, a.is_vip,
		a.EFFECTIVE_FROM, 
		a.LOAD_DATE, a.RECORD_SOURCE
	from yfurman.project_dds_sat_mdm_details as a
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
insert into yfurman.project_dds_sat_mdm_details (
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

select * from yfurman.project_dds_sat_mdm_details limit 100;
select count(*) from yfurman.project_dds_sat_mdm_details;

-- 7.5.4 insert SAT_BILLING_DETAILS, SOURCE:BILLING

with source_data as (
	select 
		BILLING_PK, BILLING_HASHDIFF, 
		created_at, sum,
		EFFECTIVE_FROM, 
		LOAD_DATE, RECORD_SOURCE
	from yfurman.project_view_billing_one_year
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

select * from yfurman.project_dds_sat_billing_details limit 100;
select count(*) from yfurman.project_dds_sat_billing_details;

-- 7.5.5 insert SAT_ISSUE_DETAILS, SOURCE:ISSUE

with source_data as (
	select 
		ISSUE_PK, ISSUE_HASHDIFF, 
		start_time, end_time, title, description, service,
		EFFECTIVE_FROM, 
		LOAD_DATE, RECORD_SOURCE
	from yfurman.project_view_issue_one_year
),
update_records as (
	select 
		a.ISSUE_PK, a.ISSUE_HASHDIFF, 
		a.start_time, a.end_time, a.title, a.description, a.service,
		a.EFFECTIVE_FROM, 
		a.LOAD_DATE, a.RECORD_SOURCE
	from yfurman.project_dds_sat_issue_details as a
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
insert into yfurman.project_dds_sat_issue_details (
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

select * from yfurman.project_dds_sat_issue_details limit 10;
select * from yfurman.project_dds_sat_issue_details where extract(year from start_time) between 2013 and 2019;
select count(*) from yfurman.project_dds_sat_issue_details;
select count(*) from yfurman.project_dds_sat_issue_details where extract(year from start_time) between 2013 and 2019;

select count(*) from (select distinct user_id from yfurman.project_ods_issue) poi;

-- 7.5.6 insert SAT_TRAFFIC_DETAILS, SOURCE:TRAFFIC

with source_data as (
	select 
		TRAFFIC_PK, TRAFFIC_HASHDIFF, 
		time_stamp, bytes_sent, bytes_received,
		EFFECTIVE_FROM, 
		LOAD_DATE, RECORD_SOURCE
	from yfurman.project_view_traffic_one_year
),
update_records as (
	select 
		a.TRAFFIC_PK, a.TRAFFIC_HASHDIFF, 
		a.time_stamp, a.bytes_sent, a.bytes_received,
		a.EFFECTIVE_FROM, 
		a.LOAD_DATE, a.RECORD_SOURCE
	from yfurman.project_dds_sat_traffic_details as a
	join source_data as b
	on a.TRAFFIC_PK = b.TRAFFIC_PK
	where  a.LOAD_DATE <= b.LOAD_DATE
),
latest_records as (
	select * from (
		select TRAFFIC_PK, TRAFFIC_HASHDIFF, LOAD_DATE,
			case when rank() over (partition by TRAFFIC_PK order by LOAD_DATE desc) = 1
				then 'Y' 
				else 'N'
			end as latest
		from update_records
	) as s
	where latest = 'Y'
),	
records_to_insert as (
	select distinct 
		e.TRAFFIC_PK, e.TRAFFIC_HASHDIFF, 
		e.time_stamp, e.bytes_sent, e.bytes_received,
		e.EFFECTIVE_FROM, 
		e.LOAD_DATE, e.RECORD_SOURCE
	from source_data as e
	left join latest_records
	on latest_records.TRAFFIC_HASHDIFF = e.TRAFFIC_HASHDIFF and 
	   latest_records.TRAFFIC_PK = e.TRAFFIC_PK
	where latest_records.TRAFFIC_HASHDIFF is NULL
)	
insert into yfurman.project_dds_sat_traffic_details (
	TRAFFIC_PK, TRAFFIC_HASHDIFF, 
	time_stamp, bytes_sent, bytes_received,
	EFFECTIVE_FROM, 
	LOAD_DATE, RECORD_SOURCE)
(
	select 
		TRAFFIC_PK, TRAFFIC_HASHDIFF, 
		time_stamp, bytes_sent, bytes_received,
		EFFECTIVE_FROM, 
		LOAD_DATE, RECORD_SOURCE
	from records_to_insert
);


select * from yfurman.project_dds_sat_traffic_details limit 10;
select * from yfurman.project_dds_sat_traffic_details where extract(year from time_stamp) between 2013 and 2019;
select count(*) from yfurman.project_dds_sat_traffic_details;
select count(*) from yfurman.project_dds_sat_traffic_details where extract(year from time_stamp) between 2013 and 2019;

select count(*) from (select distinct user_id from yfurman.project_ods_traffic) pot;



-- 8. TEST CREATE DATAMART

-- 8.1 CREATE TMP REPORT_TABLE

drop  table if exists yfurman.project_report_tmp;
create table yfurman.project_report_tmp as (
		with row_billing as (
			select * from (
				select BILLING_PK, sum, EFFECTIVE_FROM, LOAD_DATE, RECORD_SOURCE,
					row_number() over (
						partition by BILLING_PK
						order by EFFECTIVE_FROM DESC
					) as row_num
				from yfurman.project_dds_sat_billing_details		
			) as h where row_num = 1
		),
		
		billing_sum_data as (
			select USER_PK, BILLING_PERIOD_PK, sum(sum) as bill_sum
			from row_billing rb
			join yfurman.project_dds_link_billing lb on rb.BILLING_PK = lb.BILLING_PK
			group by USER_PK, BILLING_PERIOD_PK
		)

		,
		payment_sum_data as (
			select USER_PK, BILLING_PERIOD_PK, sum(sum) as pay_sum
			from yfurman.project_dds_sat_pay_details spd
			join yfurman.project_dds_link_payment lp on spd.PAYMENT_PK = lp.PAYMENT_PK
			group by USER_PK, BILLING_PERIOD_PK
		)

		,		
		issue_sum_data as (
			select 
				USER_PK, 
				to_char(start_time, 'YYYY-MM') as BILLING_PERIOD_KEY, 
				count(*) as issue_count
			from yfurman.project_dds_sat_issue_details sid
			join yfurman.project_dds_link_issue li on sid.ISSUE_PK = li.ISSUE_PK
			group by USER_PK, BILLING_PERIOD_KEY
		)

		,		
		traffic_sum_data as (
			select 
				USER_PK, 
				to_char(time_stamp, 'YYYY-MM') as BILLING_PERIOD_KEY, 
				sum(bytes_sent) as traff_out,
				sum(bytes_received) as traff_in
			from yfurman.project_dds_sat_traffic_details std
			join yfurman.project_dds_link_traffic lt on std.TRAFFIC_PK = lt.TRAFFIC_PK
			group by USER_PK, BILLING_PERIOD_KEY
		)
		,
		raw_user_period as (
			select USER_PK, USER_KEY,
					BILLING_PERIOD_PK, BILLING_PERIOD_KEY
			from yfurman.project_dds_hub_user, yfurman.project_dds_hub_billing_period
		),
		
		raw_data as (
			select 
				LEGAL_TYPE_KEY as legal_type,
				DISTRICT_KEY as district,
				BILLING_MODE_KEY as billing_mode,
				extract(year from registered_at) as registration_year,
				is_vip,
				extract(year from to_date(rup.BILLING_PERIOD_KEY, 'YYYY-MM')) as billing_year,
				pay_sum,
				bill_sum,
				issue_count,
				traff_out,
				traff_in
			from raw_user_period rup
			left join payment_sum_data psd on rup.USER_PK = psd.USER_PK
									 and rup.BILLING_PERIOD_PK = psd.BILLING_PERIOD_PK
			left join billing_sum_data bsd on rup.USER_PK = bsd.USER_PK
									 and rup.BILLING_PERIOD_PK = bsd.BILLING_PERIOD_PK
			left join issue_sum_data isd   on rup.USER_PK = isd.USER_PK
									      and rup.BILLING_PERIOD_KEY = isd.BILLING_PERIOD_KEY						
			left join traffic_sum_data tsd on rup.USER_PK = tsd.USER_PK
									      and rup.BILLING_PERIOD_KEY = tsd.BILLING_PERIOD_KEY						
			left join yfurman.project_dds_link_mdm lm on rup.USER_PK = lm.USER_PK
			left join yfurman.project_dds_hub_legal_type hlt on lm.LEGAL_TYPE_PK = hlt.LEGAL_TYPE_PK
			left join yfurman.project_dds_hub_district hd on lm.DISTRICT_PK = hd.DISTRICT_PK
			left join yfurman.project_dds_hub_billing_mode hbm on lm.BILLING_MODE_PK = hbm.BILLING_MODE_PK
			left join yfurman.project_dds_sat_mdm_details smd on lm.MDM_PK = smd.MDM_PK			
		)	
				
		select billing_year, legal_type, district, billing_mode, registration_year, is_vip, 
			   sum(pay_sum) as payment_sum, 
			   sum(bill_sum) as billing_sum,  
			   sum(issue_count) as issue_cnt,
			   sum(traff_out + traff_in) as traffic_amount
		from raw_data
		group by billing_year, legal_type, district, billing_mode, registration_year, is_vip
		order by billing_year, legal_type, district, billing_mode, registration_year, is_vip
);

select * from yfurman.project_report_tmp limit 100;
select count(*) from yfurman.project_report_tmp;


-- 8.2 INSERT INTO DEMENSION TABLEs

insert into yfurman.project_report_dim_billing_year(billing_year_key)
select distinct billing_year as billing_year_key 
from yfurman.project_report_tmp a
left join yfurman.project_report_dim_billing_year b on b.billing_year_key = a.billing_year
where b.billing_year_key is null;

select * from yfurman.project_report_dim_billing_year limit 100;

insert into yfurman.project_report_dim_legal_type(legal_type_key)
select distinct legal_type as legal_type_key 
from yfurman.project_report_tmp a
left join yfurman.project_report_dim_legal_type b on b.legal_type_key = a.legal_type
where b.legal_type_key is null;

select * from yfurman.project_report_dim_legal_type limit 100;

insert into yfurman.project_report_dim_district(district_key)
select distinct district as district_key 
from yfurman.project_report_tmp a
left join yfurman.project_report_dim_district b on b.district_key = a.district
where b.district_key is null;

select * from yfurman.project_report_dim_district limit 100;

insert into yfurman.project_report_dim_registration_year(registration_year_key)
select distinct registration_year as registration_year_key 
from yfurman.project_report_tmp a
left join yfurman.project_report_dim_registration_year b on b.registration_year_key = a.registration_year
where b.registration_year_key is null;

select * from yfurman.project_report_dim_registration_year limit 100;

insert into yfurman.project_report_dim_billing_mode(billing_mode_key)
select distinct billing_mode as billing_mode_key 
from yfurman.project_report_tmp a
left join yfurman.project_report_dim_billing_mode b on b.billing_mode_key = a.billing_mode
where b.billing_mode_key is null;

select * from yfurman.project_report_dim_billing_mode limit 100;

-- 8.3 INSERT INTO FACTS TABLE

insert into yfurman.project_report_fct(
			billing_year_id,
			legal_type_id,
			district_id,
			registration_year_id,
			billing_mode_id,
			is_vip,
			payment_sum,
			billing_sum,
			issue_cnt,
			traffic_amount
		)
select biy.id, lt.id, d.id, ry.id, bm.id, is_vip, 
	   raw.payment_sum, raw.billing_sum, raw.issue_cnt, raw.traffic_amount
from yfurman.project_report_tmp raw
join yfurman.project_report_dim_billing_year biy on raw.billing_year = biy.billing_year_key
join yfurman.project_report_dim_legal_type lt on raw.legal_type = lt.legal_type_key
join yfurman.project_report_dim_district d on raw.district = d.district_key
join yfurman.project_report_dim_registration_year ry on raw.registration_year = ry.registration_year_key
join yfurman.project_report_dim_billing_mode bm on raw.billing_mode = bm.billing_mode_key;

select * from yfurman.project_report_fct limit 100;

-- 9. DATA CONSISTENCY CHECK

select to_char(current_timestamp, 'YYYY-MM');

select sum(payment_sum) from yfurman.project_report_tmp;
select sum(billing_sum) from yfurman.project_report_tmp;
select sum(issue_cnt) from yfurman.project_report_tmp;
select sum(traffic_amount) from yfurman.project_report_tmp;

select sum(payment_sum) from yfurman.project_report_fct;
select sum(billing_sum) from yfurman.project_report_fct;
select sum(issue_cnt) from yfurman.project_report_fct;
select sum(traffic_amount) from yfurman.project_report_fct;


select sum(sum) from yfurman.project_ods_payment; 
select sum(sum) from yfurman.project_ods_billing; 

select sum(sum) from yfurman.project_ods_billing
where extract(year from to_date(billing_period, 'YYYY-MM')) 
between extract(year from BEGIN_DATE()) and extract(year from END_DATE());

select count(*) from yfurman.project_dds_sat_traffic_details 
where extract(year from time_stamp) 
between extract(year from BEGIN_DATE()) and extract(year from END_DATE());

select sum(bytes_sent + bytes_received) from yfurman.project_dds_sat_traffic_details
where extract(year from time_stamp) 
between extract(year from BEGIN_DATE()) and extract(year from END_DATE());

select sum(bytes_sent + bytes_received) from yfurman.project_ods_traffic
where extract(year from time_stamp) 
between extract(year from BEGIN_DATE()) and extract(year from END_DATE());

select sum(sum) from yfurman.project_stg_payment 
where extract(year from to_date(billing_period, 'YYYY-MM')) 
between extract(year from BEGIN_DATE()) and extract(year from END_DATE());

select sum(sum) from yfurman.project_stg_payment;
select sum(sum) from yfurman.project_dds_sat_pay_details;
