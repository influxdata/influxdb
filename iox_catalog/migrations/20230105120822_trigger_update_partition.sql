-- A new field in the partition that stores the latest time a new file is added to the partition
ALTER TABLE partition ADD COLUMN IF NOT EXISTS new_file_at bigint;

-- FUNTION that updates the new_file_at field in the partition table when the update_partition trigger is fired
CREATE OR REPLACE FUNCTION update_partition_on_new_file_at()
RETURNS TRIGGER 
LANGUAGE PLPGSQL
AS $$
BEGIN 
    UPDATE partition SET new_file_at = EXTRACT(EPOCH FROM now() ) * 1000000000 WHERE id = NEW.partition_id;

    RETURN NEW;
END;
$$;

-- TRIGGER that fires the update_partition_on_new_file_at function when a new file is added to the parquet_file table
CREATE TRIGGER update_partition 
    AFTER INSERT ON parquet_file 
    FOR EACH ROW 
    EXECUTE PROCEDURE update_partition_on_new_file_at();
    

