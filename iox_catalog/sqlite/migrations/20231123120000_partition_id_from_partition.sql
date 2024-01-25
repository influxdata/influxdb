-- Populate partition ID where omitted (#9338)

UPDATE parquet_file SET partition_id = partition.id
FROM partition
WHERE parquet_file.partition_id is NULL
  AND parquet_file.partition_hash_id = partition.hash_id;

-- Ideally would SET NOT NULL but not supported by SQLite
