-- Add to help the compactor when it searches for undeleted parquet files.
-- IOX_OTHER_CHECKSUM: 4b8295a25aa051620c8fe5fa64b914901a2f4af4d343d2735cde484aa018691e22d274aab84200ba2830fbe51833ab1a

-- By default we often only have 5min to finish our statements. The `CREATE INDEX CONCURRENTLY` however takes longer.
-- In our prod test this took about 15min, but better be safe than sorry.
-- IOX_NO_TRANSACTION
SET statement_timeout TO '60min';

-- IOX_STEP_BOUNDARY

-- remove potentially invalid index
-- IOX_NO_TRANSACTION
DROP INDEX CONCURRENTLY IF EXISTS parquet_file_partition_delete_idx;

-- IOX_STEP_BOUNDARY

-- While `CONCURRENTLY` means it runs parallel to other writes, this command will only finish after the index was
-- successfully built.
-- IOX_NO_TRANSACTION
CREATE INDEX CONCURRENTLY parquet_file_partition_delete_idx ON parquet_file (partition_id) WHERE to_delete IS NULL;
