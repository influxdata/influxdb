-- IOX_SETUP: TwoMeasurementsManyFieldsTwoChunks

-- validate we have access to information schema for listing system tables
-- IOX_COMPARE: sorted
SELECT * from h2o;
-- Plan will look like:
--  . Two overlapped chunks overlap, one parquet and one from ingester
--  . The parquet chunk already sorted -> no need the sort operator on top of scan
--  . The ingester chunk is not sorted -> need the sort operator on top
-- IOX_COMPARE: uuid
EXPLAIN SELECT * from h2o;

-- Only selct fields and time
select temp, other_temp, time from h2o;
-- IOX_COMPARE: uuid
EXPLAIN select temp, other_temp, time from h2o;

