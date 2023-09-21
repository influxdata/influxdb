-- IOX_SETUP: ManyFieldsSeveralChunks

-- validate we have access to information schema for listing system tables
-- IOX_COMPARE: sorted
SELECT * from h2o;
-- Plan will look like:
--  . Two chunks (one parquet and one from ingester) neither overlap nor contain duplicate
--     --> scan in one scan node
--  . Three parquet chunks overlapped -> dedup without resort
-- IOX_COMPARE: uuid
EXPLAIN SELECT * from h2o;

-- Only selct fields and time
-- IOX_COMPARE: sorted
select temp, other_temp, time from h2o;
-- IOX_COMPARE: uuid
EXPLAIN select temp, other_temp, time from h2o;

-- early pruning
-- IOX_COMPARE: uuid
EXPLAIN SELECT * from h2o where time >= to_timestamp('1970-01-01T00:00:00.000000250+00:00');
