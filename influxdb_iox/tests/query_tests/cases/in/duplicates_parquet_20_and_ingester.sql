-- Test setup for running with 20 parquet files and overlapping ingester data
-- IOX_SETUP: TwentySortedParquetFilesAndIngester


-- each parquet file has either 2 rows, one with f=1 and the other with f=2
-- and then there are 50 that have a single row with f=3
select count(*), sum(f) from m;

-- Use sum to avoid count(*) optimization
-- IOX_COMPARE: uuid
EXPLAIN select count(*), sum(f) from m;
