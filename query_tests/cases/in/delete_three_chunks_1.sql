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
