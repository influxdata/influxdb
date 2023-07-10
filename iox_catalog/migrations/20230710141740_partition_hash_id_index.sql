-- By default, we often only have 5min to finish our statements. The `CREATE INDEX CONCURRENTLY`,
-- however, can take longer.
-- IOX_NO_TRANSACTION
SET statement_timeout TO '60min';

-- IOX_STEP_BOUNDARY

-- IOX_NO_TRANSACTION
CREATE INDEX CONCURRENTLY IF NOT EXISTS parquet_file_partition_hash_id_idx
ON parquet_file (partition_hash_id)
WHERE partition_hash_id IS NOT NULL;
