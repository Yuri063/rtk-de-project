-- 9.2 DROP VIEW_BILLING_ONE_YEAR

drop view if exists {{ params.prefix }}_view_billing_one_year__{{ execution_date.year }};
