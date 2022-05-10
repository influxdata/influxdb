-- IOX_SETUP: ManyFieldsSeveralChunks

-- validate we have access to information schema for listing system tables
-- IOX_COMPARE: sorted
SELECT * from h2o;
-- Plan will look like:
--  . Two chunks (one parquet and one from ingester) neither overlap nor contain duplicate
--     --> scan in one scan node
--  . Three parquet chunks overlapped -> dedup without resort
EXPLAIN SELECT * from h2o;

-- Only selct fields and time
select temp, other_temp, time from h2o;
EXPLAIN select temp, other_temp, time from h2o;
