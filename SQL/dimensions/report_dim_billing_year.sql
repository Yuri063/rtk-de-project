-- 10.1 INSERT INTO DEMENSION TABLEs REPORT_DIM_BILLING_YEAR

insert into yfurman.project_report_dim_billing_year(billing_year_key)
select distinct billing_year as billing_year_key 
from yfurman.project_report_tmp_{{ execution_date.year }} a
left join yfurman.project_report_dim_billing_year b on b.billing_year_key = a.billing_year
where b.billing_year_key is null;