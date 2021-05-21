-- 9.3 DROP VIEW_ISSUE_ONE_YEAR

drop view if exists {{ params.prefix }}_view_issue_one_year__{{ execution_date.year }};
