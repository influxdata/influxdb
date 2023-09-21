-- FUNTION that updates the new_file_at field in the partition table when the update_partition trigger is fired
-- The field new_file_at signals when the last file was added to the partition for compaction.

CREATE OR REPLACE FUNCTION update_partition_on_new_file_at()
RETURNS TRIGGER 
LANGUAGE PLPGSQL
AS $$
BEGIN
    UPDATE partition SET new_file_at = NEW.created_at WHERE id = NEW.partition_id;    

    RETURN NEW;
END;
$$;