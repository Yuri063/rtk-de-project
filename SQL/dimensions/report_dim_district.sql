-- 10.3 INSERT INTO DEMENSION TABLEs REPORT_DIM_DISTRICT

insert into yfurman.project_report_dim_district(district_key)
select distinct district as district_key 
from yfurman.project_report_tmp_{{ execution_date.year }} a
left join yfurman.project_report_dim_district b on b.district_key = a.district
where b.district_key is null;