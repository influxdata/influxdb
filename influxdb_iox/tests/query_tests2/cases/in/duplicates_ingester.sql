-- Test for predicate push down explains
-- IOX_SETUP: OneMeasurementFourChunksWithDuplicatesWithIngester

-- IOX_COMPARE: sorted
select time, state, city, min_temp, max_temp, area from h2o order by time, state, city;

-- Plan with order by
-- IOX_COMPARE: uuid
explain select time, state, city, min_temp, max_temp, area from h2o order by time, state, city;

-- plan without order by
-- IOX_COMPARE: uuid
EXPLAIN select time, state, city, min_temp, max_temp, area from h2o;

-- Union plan
-- IOX_COMPARE: uuid
EXPLAIN select state as name from h2o UNION ALL select city as name from h2o;

-- count(*) plan that ensures that row count statistics are not used (because we don't know how many rows overlap)
select count(*) from h2o;
