-- 10.5 INSERT INTO DEMENSION TABLEs REPORT_DIM_BILLING_MODE

insert into yfurman.project_report_dim_billing_mode(billing_mode_key)
select distinct billing_mode as billing_mode_key 
from yfurman.project_report_tmp_{{ execution_date.year }} a
left join yfurman.project_report_dim_billing_mode b on b.billing_mode_key = a.billing_mode
where b.billing_mode_key is null;