-- Demonstrate soft deleted rows will not be return to queries
-- IOX_SETUP: OneDeleteMultiExprsOneChunk

-- select *
SELECT * from cpu;

SELECT time, bar from cpu;

SELECT bar from cpu;

SELECT count(time), count(*), count(bar), min(bar), max(bar), min(time), max(time)  from cpu;

SELECT count(time)  from cpu;

SELECT count(foo) from cpu;

SELECT count(bar) from cpu;

SELECT count(*) from cpu;

SELECT min(bar) from cpu;

SELECT foo from cpu;

-- BUG: https://github.com/influxdata/influxdb_iox/issues/2776
-- SELECT min(foo) from cpu;
-- SELECT max(foo) from cpu

-- BUG: https://github.com/influxdata/influxdb_iox/issues/2779
--  inconsistent format returned
-- SELECT min(time) from cpu;
-- SELECT max(time) from cpu;

SELECT time from cpu;

SELECT max(bar) from cpu;

--------------------------------------------------------
-- With selection predicate

SELECT * from cpu where bar >= 1.0 order by bar, foo, time;

SELECT foo from cpu where bar >= 1.0 order by foo;

SELECT time, bar from cpu where bar >= 1.0 order by bar, time;

SELECT * from cpu where foo = 'you' order by bar, foo, time;

SELECT min(bar) as mi, max(time) as ma from cpu where foo = 'you' order by mi, ma


