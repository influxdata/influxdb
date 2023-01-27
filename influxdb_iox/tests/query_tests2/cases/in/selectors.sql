-- Demonstrate the use of flux "selector" aggregates via SQL
-- IOX_SETUP: AllTypes

-- select * so the tests are more self describing / understandable
SELECT * from m;

---- Note: all the tests below tests float, int, uint, string, and bool types and then a few together

--------
-- FIRST Selector
--------
select selector_first(float_field, time) from m;
select selector_first(int_field, time) from m;
select selector_first(uint_field, time) from m;
select selector_first(string_field, time) from m;
select selector_first(bool_field, time) from m;

-- Test extracting value and time subfields
select selector_first(float_field, time)['value'], selector_first(float_field, time)['time'] from m;
-- Also with subqueries
select f['value'], f['time'] from (select selector_first(float_field, time) as f from m) as sq;


--------
-- LAST Selector
--------
select selector_last(float_field, time) from m;
select selector_last(int_field, time) from m;
select selector_last(uint_field, time) from m;
select selector_last(string_field, time) from m;
select selector_last(bool_field, time) from m;

-- Test extracting value and time subfields
select selector_last(float_field, time)['value'], selector_last(float_field, time)['time'] from m;
-- Also with subqueries
select f['value'], f['time'] from (select selector_last(float_field, time) as f from m) as sq;


--------
-- MIN Selector
--------
select selector_min(float_field, time) from m;
select selector_min(int_field, time) from m;
select selector_min(uint_field, time) from m;
select selector_min(string_field, time) from m;
select selector_min(bool_field, time) from m;

-- Test extracting value and time subfields
select selector_min(float_field, time)['value'], selector_min(float_field, time)['time'] from m;
-- Also with subqueries
select f['value'], f['time'] from (select selector_min(float_field, time) as f from m) as sq;


--------
-- MAX Selector
--------
select selector_max(float_field, time) from m;
select selector_max(int_field, time) from m;
select selector_max(uint_field, time) from m;
select selector_max(string_field, time) from m;
select selector_max(bool_field, time) from m;

-- Test extracting value and time subfields
select selector_max(float_field, time)['value'], selector_max(float_field, time)['time'] from m;
-- Also with subqueries
select f['value'], f['time'] from (select selector_max(float_field, time) as f from m) as sq;
