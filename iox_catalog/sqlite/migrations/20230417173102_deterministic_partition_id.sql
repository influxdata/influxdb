-- Add a new, nullable hash ID column to the partition table
ALTER TABLE
  partition
  ADD COLUMN hash_id BLOB;

-- If it's specified, it must be unique
CREATE UNIQUE INDEX IF NOT EXISTS partition_hash_id_unique ON partition (hash_id);

-- Add a new, nullable foreign key column to the parquet_file table referencing the partition table
ALTER TABLE
  parquet_file
  ADD COLUMN partition_hash_id bytea
  REFERENCES partition (hash_id);
