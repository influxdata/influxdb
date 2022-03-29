-- Add indexes to support querying for and deleting Parquet Files marked for deletion before
-- a specified time.

CREATE INDEX IF NOT EXISTS parquet_file_deleted_at_idx ON parquet_file (to_delete);
