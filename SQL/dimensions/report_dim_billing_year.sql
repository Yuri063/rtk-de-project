-- 10.1 INSERT INTO DEMENSION TABLEs REPORT_DIM_BILLING_YEAR

insert into {{ params.prefix }}_report_dim_billing_year(billing_year_key)
select distinct billing_year as billing_year_key 
from {{ params.prefix }}_report_tmp_{{ execution_date.year }} a
left join {{ params.prefix }}_report_dim_billing_year b on b.billing_year_key = a.billing_year
where b.billing_year_key is null;
