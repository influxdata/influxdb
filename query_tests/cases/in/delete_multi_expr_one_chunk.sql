-- Demonstrate soft deleted rows will not be return to queries
-- IOX_SETUP: OneDeleteMultiExprsOneChunk

-- select *
SELECT * from cpu order by bar, foo, time;

SELECT time, bar from cpu order by time, bar;

SELECT bar from cpu order by bar;

SELECT count(time), count(*), count(bar), min(bar), max(bar), min(time), max(time)  from cpu;

SELECT count(time)  from cpu;

SELECT count(foo) from cpu;

SELECT count(bar) from cpu;

SELECT count(*) from cpu;

SELECT min(bar) from cpu;

-- IOX_COMPARE: sorted
SELECT foo from cpu;

SELECT min(foo) as min_foo from cpu order by min_foo;
SELECT max(foo) as max_foo from cpu order by max_foo;

SELECT min(foo) as min_foo from cpu group by time order by min_foo;
SELECT max(foo) as max_foo from cpu group by time order by max_foo;
SELECT time, max(foo) as max_foo from cpu group by time order by time, max_foo;

SELECT min(foo) as min_foo from cpu group by bar order by min_foo;
SELECT bar, max(foo) as max_foo from cpu group by bar order by bar, max_foo;
SELECT max(foo) as max_foo from cpu group by time order by max_foo;

SELECT min(time) as min_time from cpu order by min_time;
SELECT max(time) as max_time from cpu order by max_time;

SELECT min(time) as min_time from cpu group by bar order by min_time;
SELECT bar, min(time) as min_time from cpu group by bar order by bar, min_time;
SELECT max(time) as max_time from cpu group by foo order by max_time;
SELECT foo, max(time) as max_time from cpu group by foo order by foo, max_time;

-- IOX_COMPARE: sorted
SELECT time from cpu;

SELECT max(bar) from cpu order by 1;

--------------------------------------------------------
-- With selection predicate

SELECT * from cpu where bar >= 1.0 order by bar, foo, time;

SELECT foo from cpu where bar >= 1.0 order by foo;

SELECT time, bar from cpu where bar >= 1.0 order by bar, time;

SELECT * from cpu where foo = 'you' order by bar, foo, time;

SELECT min(bar) as mi, max(time) as ma from cpu where foo = 'you' order by mi, ma
