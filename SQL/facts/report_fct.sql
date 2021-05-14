-- 11. INSERT INTO FACTS TABLE

truncate {{ params.prefix }}_report_fct;

insert into {{ params.prefix }}_report_fct(
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
from {{ params.prefix }}_report_tmp_{{ execution_date.year }} raw
join {{ params.prefix }}_report_dim_billing_year biy on raw.billing_year = biy.billing_year_key
join {{ params.prefix }}_report_dim_legal_type lt on raw.legal_type = lt.legal_type_key
join {{ params.prefix }}_report_dim_district d on raw.district = d.district_key
join {{ params.prefix }}_report_dim_registration_year ry on raw.registration_year = ry.registration_year_key
join {{ params.prefix }}_report_dim_billing_mode bm on raw.billing_mode = bm.billing_mode_key;
