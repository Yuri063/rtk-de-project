-- 9.6 DROP VIEW_TRAFFIC_ONE_YEAR

drop view if exists {{ params.prefix }}_view_traffic_one_year__{{ execution_date.year }};
