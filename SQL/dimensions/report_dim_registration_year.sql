-- 10.4 INSERT INTO DEMENSION TABLEs REPORT_DIM_REGISTRATION_YEAR

insert into {{ params.prefix }}_report_dim_registration_year(registration_year_key)
select distinct registration_year as registration_year_key 
from {{ params.prefix }}_report_tmp_{{ execution_date.year }} a
left join {{ params.prefix }}_report_dim_registration_year b on b.registration_year_key = a.registration_year
where b.registration_year_key is null;
