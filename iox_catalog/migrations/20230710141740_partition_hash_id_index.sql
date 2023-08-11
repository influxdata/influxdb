-- By default, we often only have 5min to finish our statements. The `CREATE INDEX CONCURRENTLY`,
-- however, can take longer.
-- IOX_OTHER_CHECKSUM: 2ee2416cc206254f5b8a5497a3cfc5bcb2146759416cd4cb6a83ae34d3e0141387eb733e61f224076d5af0e3c6016e7b

-- IOX_NO_TRANSACTION
SET statement_timeout TO '60min';

-- IOX_STEP_BOUNDARY

-- remove potentially invalid index
-- IOX_NO_TRANSACTION
DROP INDEX CONCURRENTLY IF EXISTS parquet_file_partition_hash_id_idx;

-- IOX_STEP_BOUNDARY

-- IOX_NO_TRANSACTION
CREATE INDEX CONCURRENTLY parquet_file_partition_hash_id_idx
ON parquet_file (partition_hash_id)
WHERE partition_hash_id IS NOT NULL;
