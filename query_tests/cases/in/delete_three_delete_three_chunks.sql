-- Demonstrate soft deleted rows will not be return to queries
-- IOX_SETUP: ThreeDeleteThreeChunks

-- select *
SELECT * from cpu order by foo, bar, time;

SELECT time, bar from cpu order by bar, time;

SELECT bar from cpu order by bar;

SELECT count(time) as t, count(*) as c, count(bar) as b, min(bar) as mi, min(time) as mt, max(time) as mat from cpu order by t, c, b, mi, mt, mat;

SELECT count(time)  from cpu;

SELECT count(foo) from cpu;

SELECT count(bar) from cpu;

SELECT count(*) from cpu;

SELECT min(bar) from cpu;

SELECT foo from cpu order by foo;

-- BUG: https://github.com/influxdata/influxdb_iox/issues/2776
-- SELECT min(foo) from cpu;
-- SELECT max(foo) from cpu

-- BUG: https://github.com/influxdata/influxdb_iox/issues/2779
--  inconsistent format returned
-- SELECT min(time) from cpu;
-- SELECT max(time) from cpu;

SELECT time from cpu order by time;

SELECT max(bar) from cpu;

SELECT min(time), max(time) from cpu;

--------------------------------------------------------
-- With selection predicate

SELECT * from cpu where bar != 1.0 order by bar, foo, time;

SELECT * from cpu where foo = 'me' and bar > 2.0 order by bar, foo, time;

SELECT * from cpu where bar = 1 order by bar, foo, time;

SELECT * from cpu where foo = 'me' and (bar > 2 or bar = 1.0) order by bar, foo, time;

SELECT * from cpu where foo = 'you' and (bar > 3.0 or bar = 1) order by bar, foo, time;

SELECT min(bar) from cpu where foo = 'me' and (bar > 2 or bar = 1.0);

--  BUG: https://github.com/influxdata/influxdb_iox/issues/2779
-- SELECT max(foo) from cpu where foo = 'me' and (bar > 2 or bar = 1.0);

SELECT min(time) from cpu where foo = 'me' and (bar > 2 or bar = 1.0);

SELECT count(bar) from cpu where foo = 'me' and (bar > 2 or bar = 1.0);

SELECT count(time) from cpu where foo = 'me' and (bar > 2 or bar = 1.0);

SELECT count(*) from cpu where foo = 'me' and (bar > 2 or bar = 1.0);

----------
SELECT * from cpu where bar >= 1.0 order by bar, foo, time;

SELECT foo from cpu where bar >= 1.0 order by foo;

SELECT time, bar from cpu where bar >= 1.0 order by bar, time;

SELECT * from cpu where foo = 'you' order by bar, foo, time;

SELECT min(bar) as mi, max(time) as ma from cpu where foo = 'you' order by mi, ma;