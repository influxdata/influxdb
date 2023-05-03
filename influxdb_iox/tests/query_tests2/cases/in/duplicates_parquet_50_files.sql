-- Test setup for running with 50 parquet files
-- IOX_SETUP: FiftySortedSameParquetFiles


-- each parquet file has either 2 rows, one with f=1 and the other with f=2
-- and then there are 50 that have a single row with f=3
select count(1), sum(f1) from m;

-- All 50 files are sorted but since it is larger than max_parquet_fanout which is set 40,
-- we do not use the presort and add a SortExec
-- WHen running this test, a warning "cannot use pre-sorted parquet files, fan-out too wide" is printed
-- IOX_COMPARE: uuid
EXPLAIN select count(1), sum(f1) from m;

