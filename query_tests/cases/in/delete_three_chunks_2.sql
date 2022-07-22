-- Demonstrate soft deleted rows will not be return to queries
-- IOX_SETUP: ThreeDeleteThreeChunks

SELECT min(foo) from cpu;
SELECT max(foo) from cpu;

SELECT min(time) from cpu;
SELECT max(time) from cpu;

-- IOX_COMPARE: sorted
SELECT foo, min(time) from cpu group by foo;
SELECT bar, max(time) as max_time from cpu group by bar order by bar, max_time;
SELECT max(time) as max_time from cpu group by bar order by max_time;

SELECT time from cpu order by time;

SELECT max(bar) from cpu;

SELECT min(time), max(time) from cpu;
