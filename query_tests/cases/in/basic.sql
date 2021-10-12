-- Basic query tests
-- IOX_SETUP: TwoMeasurements

-- query data
SELECT * from cpu;


-- BUG: https://github.com/influxdata/influxdb_iox/issues/2776
--        "+----------------+",
--        "| MIN(cpu.region |",
--        "+----------------+",
--        "| west           |",
--        "+----------------+",
--SELECT min(region) from cpu;

-- projection
-- expect that to get a subset of the columns and in the order specified
SELECT user, region from cpu;

-- predicate on CPU
SELECT * from cpu where time > to_timestamp('1970-01-01T00:00:00.000000120+00:00');

-- projection and predicate
-- expect that to get a subset of the columns and in the order specified
SELECT user, region from cpu where time > to_timestamp('1970-01-01T00:00:00.000000120+00:00');

-- basic grouping
SELECT count(*) from cpu group by region;


-- select from a different measurement
SELECT * from disk;
