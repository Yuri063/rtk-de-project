-- 9. CREATE TMP TABLE (for ONE YEAR)

drop  table if exists {{ params.prefix }}_report_tmp_{{ execution_date.year }};
create table {{ params.prefix }}_report_tmp_{{ execution_date.year }} as (
		with row_billing as (
			select * from (
				select BILLING_PK, sum, EFFECTIVE_FROM, LOAD_DATE, RECORD_SOURCE,
					row_number() over (
						partition by BILLING_PK
						order by EFFECTIVE_FROM DESC
					) as row_num
				from {{ params.prefix }}_dds_sat_billing_details		
			) as h where row_num = 1
		),
		billing_sum_data as (
			select USER_PK, BILLING_PERIOD_PK, sum(sum) as bill_sum
			from row_billing rb
			join {{ params.prefix }}_dds_link_billing lb on rb.BILLING_PK = lb.BILLING_PK
			group by USER_PK, BILLING_PERIOD_PK
		),

		payment_sum_data as (
			select USER_PK, BILLING_PERIOD_PK, sum(sum) as pay_sum
			from {{ params.prefix }}_dds_sat_pay_details spd
			join {{ params.prefix }}_dds_link_payment lp on spd.PAYMENT_PK = lp.PAYMENT_PK
			group by USER_PK, BILLING_PERIOD_PK
		),
		
		issue_sum_data as (
			select 
				USER_PK, 
				to_char(start_time, 'YYYY-MM') as BILLING_PERIOD_KEY, 
				count(*) as issue_count
			from {{ params.prefix }}_dds_sat_issue_details sid
			join {{ params.prefix }}_dds_link_issue li on sid.ISSUE_PK = li.ISSUE_PK
			group by USER_PK, BILLING_PERIOD_KEY
		),
		
		traffic_sum_data as (
			select 
				USER_PK, 
				to_char(time_stamp, 'YYYY-MM') as BILLING_PERIOD_KEY, 
				sum(bytes_sent) as traff_out,
				sum(bytes_received) as traff_in
			from {{ params.prefix }}_dds_sat_traffic_details std
			join {{ params.prefix }}_dds_link_traffic lt on std.TRAFFIC_PK = lt.TRAFFIC_PK
			group by USER_PK, BILLING_PERIOD_KEY
		),

		raw_user_period as (
			select USER_PK, USER_KEY,
					BILLING_PERIOD_PK, BILLING_PERIOD_KEY
			from {{ params.prefix }}_dds_hub_user, {{ params.prefix }}_dds_hub_billing_period
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
			left join {{ params.prefix }}_dds_link_mdm lm on rup.USER_PK = lm.USER_PK
			left join {{ params.prefix }}_dds_hub_legal_type hlt on lm.LEGAL_TYPE_PK = hlt.LEGAL_TYPE_PK
			left join {{ params.prefix }}_dds_hub_district hd on lm.DISTRICT_PK = hd.DISTRICT_PK
			left join {{ params.prefix }}_dds_hub_billing_mode hbm on lm.BILLING_MODE_PK = hbm.BILLING_MODE_PK
			left join {{ params.prefix }}_dds_sat_mdm_details smd on lm.MDM_PK = smd.MDM_PK			

			-- where extract(year from to_date(rup.BILLING_PERIOD_KEY, 'YYYY-MM')) = {{ execution_date.year }}			

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
