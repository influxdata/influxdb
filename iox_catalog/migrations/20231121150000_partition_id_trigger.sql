-- Forward compatibility for old writers that fail to populate partition.id

-- FUNCTION that updates the partition field in the parquet_file table when the set_partition_id trigger is fired
CREATE OR REPLACE FUNCTION update_partition_id()
RETURNS TRIGGER
LANGUAGE PLPGSQL
AS $$
BEGIN
SELECT partition.id INTO NEW.partition_id
    FROM partition WHERE partition.hash_id = NEW.partition_hash_id;
RETURN NEW;
END;
$$;

-- TRIGGER that fires the update_partition_id function when a new file is added to the parquet_file table
CREATE TRIGGER set_partition_id
    BEFORE INSERT ON parquet_file
    FOR EACH ROW
    WHEN (NEW.partition_id IS NULL)
    EXECUTE PROCEDURE update_partition_id();
