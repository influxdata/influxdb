ALTER TABLE
  IF EXISTS parquet_file
ADD
  COLUMN parquet_metadata bytea NOT NULL DEFAULT '';
