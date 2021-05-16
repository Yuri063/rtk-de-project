-- 9.1 DROP REPORT_TMP

drop  table if exists {{ params.prefix }}_report_tmp_{{ execution_date.year }};
