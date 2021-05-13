-- 10.5 INSERT INTO DEMENSION TABLEs REPORT_DIM_BILLING_MODE

insert into {{ params.prefix }}_report_dim_billing_mode(billing_mode_key)
select distinct billing_mode as billing_mode_key 
from {{ params.prefix }}_report_tmp_{{ execution_date.year }} a
left join {{ params.prefix }}_report_dim_billing_mode b on b.billing_mode_key = a.billing_mode
where b.billing_mode_key is null;
