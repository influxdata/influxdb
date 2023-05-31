-- Add to help the querier when it searches for undeleted parquet files.

-- By default we often only have 5min to finish our statements. The `CREATE INDEX CONCURRENTLY` however takes longer.
-- In our prod test this took about 15min, but better be safe than sorry.
-- IOX_NO_TRANSACTION
SET statement_timeout TO '60min';

-- IOX_STEP_BOUNDARY

-- While `CONCURRENTLY` means it runs parallel to other writes, this command will only finish after the index was
-- successfully built.
-- IOX_NO_TRANSACTION
CREATE INDEX CONCURRENTLY IF NOT EXISTS parquet_file_table_delete_idx ON parquet_file (table_id) WHERE to_delete IS NULL;
