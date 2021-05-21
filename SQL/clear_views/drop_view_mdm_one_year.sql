-- 9.4 DROP VIEW_MDM_ONE_YEAR

drop view if exists {{ params.prefix }}_view_mdm_one_year__{{ execution_date.year }};
