-- 8.2.2 INSERT INTO DEMENSION TABLEs REPORT_DIM_LEGAL_TYPE

insert into {{ params.prefix }}_report_dim_legal_type(legal_type_key)
select distinct legal_type as legal_type_key 
from {{ params.prefix }}_report_tmp_{{ execution_date.year }} a
left join {{ params.prefix }}_report_dim_legal_type b on b.legal_type_key = a.legal_type
where b.legal_type_key is null;
