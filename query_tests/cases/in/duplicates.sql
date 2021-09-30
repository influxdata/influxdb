-- Test for predicate push down explains
-- IOX_SETUP: OneMeasurementFourChunksWithDuplicates

-- Plan with order by
explain select time, state, city, min_temp, max_temp, area from h2o order by time, state, city;


-- plan without order by
EXPLAIN select time, state, city, min_temp, max_temp, area from h2o;

-- Union plan
EXPLAIN select state as name from h2o UNION ALL select city as name from h2o;
