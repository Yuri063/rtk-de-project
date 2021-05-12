-- 0. INITIAL VARIABLES

create or replace function BEGIN_DATE() returns timestamp 
as $$
	select {{ execution_date.year }}::timestamp;
$$ language sql immutable;

create or replace function END_DATE() returns timestamp 
as $$
	select {{ execution_date.year }}::timestamp;
$$ language sql immutable;


