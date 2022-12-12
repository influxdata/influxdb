-- Basic query tests
-- IOX_SETUP: TwoMeasurements

-- query data
SELECT * from cpu;

SELECT min(region) from cpu;

-- projection
-- expect that to get a subset of the columns and in the order specified
SELECT "user", region from cpu;

-- predicate on CPU
SELECT * from cpu where time > to_timestamp('1970-01-01T00:00:00.000000120+00:00');

-- predicate on CPU with explicit coercion (cast string to timestamp)
SELECT * from cpu where time > '1970-01-01T00:00:00'::timestamp ORDER BY time;

-- predicate on CPU with automatic coercion (comparing time to string)
SELECT * from cpu where time > '1970-01-01T00:00:00' ORDER BY time;

-- projection and predicate
-- expect that to get a subset of the columns and in the order specified
SELECT "user", region from cpu where time > to_timestamp('1970-01-01T00:00:00.000000120+00:00');

-- basic grouping
SELECT count(*) from cpu group by region;

-- select from a different measurement
SELECT * from disk;

-- MEDIAN should work
select MEDIAN("user"), region from cpu group by region;
