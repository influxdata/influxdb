-- Add to help the querier when it searches for undeleted parquet files.
-- IOX_OTHER_CHECKSUM: ddc52db62ed446e4a8fe30af7bced52724bcf93e6e6c6cac8cbd3783bf7312595cfc44dc6aa282db9ac58e4ecfb08268

-- By default we often only have 5min to finish our statements. The `CREATE INDEX CONCURRENTLY` however takes longer.
-- In our prod test this took about 15min, but better be safe than sorry.
-- IOX_NO_TRANSACTION
SET statement_timeout TO '60min';

-- IOX_STEP_BOUNDARY

-- remove potentially invalid index
-- IOX_NO_TRANSACTION
DROP INDEX CONCURRENTLY IF EXISTS parquet_file_table_delete_idx;

-- IOX_STEP_BOUNDARY

-- While `CONCURRENTLY` means it runs parallel to other writes, this command will only finish after the index was
-- successfully built.
-- IOX_NO_TRANSACTION
CREATE INDEX CONCURRENTLY parquet_file_table_delete_idx ON parquet_file (table_id) WHERE to_delete IS NULL;
