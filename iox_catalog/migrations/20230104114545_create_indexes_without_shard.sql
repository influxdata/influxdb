-- This index will be used for selecting partitions with parquet files created after a given time
CREATE INDEX IF NOT EXISTS parquet_file_partition_created_idx ON parquet_file (partition_id, created_at);
