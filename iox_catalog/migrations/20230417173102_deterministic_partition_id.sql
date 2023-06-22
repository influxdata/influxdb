-- Add a new, nullable hash ID column to the partition table
ALTER TABLE
  IF EXISTS partition
  ADD COLUMN hash_id bytea;

-- If it's specified, it must be unique
ALTER TABLE
  IF EXISTS partition
  ADD CONSTRAINT partition_hash_id_unique UNIQUE (hash_id);

-- Add a new, nullable foreign key column to the parquet_file table referencing the partition table
ALTER TABLE
  IF EXISTS parquet_file
  ADD COLUMN partition_hash_id bytea
  REFERENCES partition (hash_id);

