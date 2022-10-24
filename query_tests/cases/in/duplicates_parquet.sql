-- Test for predicate push down explains
-- IOX_SETUP: OneMeasurementFourChunksWithDuplicatesParquetOnly

-- Plan with order by
explain select time, state, city, min_temp, max_temp, area from h2o order by time, state, city;


-- plan without order by
EXPLAIN select time, state, city, min_temp, max_temp, area from h2o;

-- Union plan
EXPLAIN select state as name from h2o UNION ALL select city as name from h2o;

-- count(*) plan that ensures that row count statistics are not used (because we don't know how many rows overlap)
select count(*) from h2o;
