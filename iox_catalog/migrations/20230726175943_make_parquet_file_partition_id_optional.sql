DROP TRIGGER IF EXISTS update_partition ON parquet_file;

ALTER TABLE parquet_file
ALTER COLUMN partition_id
DROP NOT NULL;

CREATE OR REPLACE FUNCTION update_partition_on_new_file_at()
RETURNS TRIGGER
LANGUAGE PLPGSQL
AS $$
BEGIN
    UPDATE partition
    SET new_file_at = NEW.created_at
    WHERE (NEW.partition_id IS NULL OR id = NEW.partition_id)
      AND (NEW.partition_hash_id IS NULL OR hash_id = NEW.partition_hash_id);

    RETURN NEW;
END;
$$;

CREATE TRIGGER update_partition
    AFTER INSERT ON parquet_file
    FOR EACH ROW
    EXECUTE PROCEDURE update_partition_on_new_file_at();
