-- Remove unused indices
DROP INDEX IF EXISTS parquet_file_partition_created_idx;
DROP INDEX IF EXISTS parquet_file_shard_compaction_delete_created_idx;
DROP INDEX IF EXISTS parquet_file_shard_compaction_delete_idx;
