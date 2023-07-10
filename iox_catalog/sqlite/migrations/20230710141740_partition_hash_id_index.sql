CREATE INDEX IF NOT EXISTS parquet_file_partition_hash_id_idx
ON parquet_file (partition_hash_id)
WHERE partition_hash_id IS NOT NULL;
