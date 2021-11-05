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

SELECT min(foo) from cpu;
SELECT max(foo) from cpu;

-- SELECT min(foo) from cpu group by time;
-- SELECT max(foo) from cpu group by time;
-- SELECT time, max(foo) from cpu group by time;

SELECT min(foo) from cpu group by bar;
SELECT bar, max(foo) from cpu group by bar;
-- Todo: Test not work in this framework. Exact same test works in sql.rs
-- SELECT max(foo) from cpu group by time; 

SELECT min(time) from cpu;
SELECT max(time) from cpu;

SELECT min(time) from cpu group by bar;
SELECT bar, min(time) from cpu group by bar;
-- Todo: Test not work in this framework. Exact same test works in sql.rs
-- SELECT max(time) from cpu group by foo;

SELECT time from cpu;

SELECT max(bar) from cpu;

--------------------------------------------------------
-- With selection predicate

SELECT * from cpu where bar >= 1.0 order by bar, foo, time;

SELECT foo from cpu where bar >= 1.0 order by foo;

SELECT time, bar from cpu where bar >= 1.0 order by bar, time;

SELECT * from cpu where foo = 'you' order by bar, foo, time;

SELECT min(bar) as mi, max(time) as ma from cpu where foo = 'you' order by mi, ma


