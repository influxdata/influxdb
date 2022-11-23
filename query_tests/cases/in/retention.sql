-- Test for retention policy
 -- IOX_SETUP: ThreeChunksWithRetention

 -- should return 3 rows
 SELECT * FROM cpu order by host, load, time;

 -- should see only 2 chunks with predicate pushed down to ParquetExec
 -- IOX_COMPARE: uuid
 EXPLAIN SELECT * FROM cpu order by host, load, time;

 -- should return 2 rows
 SELECT * FROM cpu WHERE host != 'b' ORDER BY host,time;

 -- should see only 2 chunks with predicate pushed down to ParquetExec
 -- IOX_COMPARE: uuid
 EXPLAIN SELECT * FROM cpu WHERE host != 'b' ORDER BY host,time;
