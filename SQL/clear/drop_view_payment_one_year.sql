-- 9.5 DROP VIEW_PAYMENT_ONE_YEAR

drop view if exists {{ params.prefix }}_view_payment_one_year__{{ execution_date.year }};
