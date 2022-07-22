-- Demonstrate soft deleted rows will not be return to queries
-- IOX_SETUP: ThreeDeleteThreeChunks

--------------------------------------------------------
-- With selection predicate

SELECT * from cpu where bar != 1.0 order by bar, foo, time;

SELECT * from cpu where foo = 'me' and bar > 2.0 order by bar, foo, time;

SELECT * from cpu where bar = 1 order by bar, foo, time;

SELECT * from cpu where foo = 'me' and (bar > 2 or bar = 1.0) order by bar, foo, time;

SELECT * from cpu where foo = 'you' and (bar > 3.0 or bar = 1) order by bar, foo, time;

SELECT min(bar) from cpu where foo = 'me' and (bar > 2 or bar = 1.0);

SELECT max(foo) from cpu where foo = 'me' and (bar > 2 or bar = 1.0);

SELECT min(time) from cpu where foo = 'me' and (bar > 2 or bar = 1.0);

SELECT count(bar) from cpu where foo = 'me' and (bar > 2 or bar = 1.0);

SELECT count(time) from cpu where foo = 'me' and (bar > 2 or bar = 1.0);

SELECT count(*) from cpu where foo = 'me' and (bar > 2 or bar = 1.0);
