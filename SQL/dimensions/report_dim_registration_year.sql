-- 10.4 INSERT INTO DEMENSION TABLEs REPORT_DIM_REGISTRATION_YEAR

insert into yfurman.project_report_dim_registration_year(registration_year_key)
select distinct registration_year as registration_year_key 
from yfurman.project_report_tmp_{{ execution_date.year }} a
left join yfurman.project_report_dim_registration_year b on b.registration_year_key = a.registration_year
where b.registration_year_key is null;