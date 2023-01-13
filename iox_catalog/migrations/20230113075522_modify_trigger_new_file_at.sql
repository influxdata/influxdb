-- FUNTION that updates the new_file_at field in the partition table when the update_partition trigger is fired
-- The field new_file_at signals when the last file was added to the partition for compaction. However, 
-- since compaction level 2 is the final stage, its creation should not signal further compaction

CREATE OR REPLACE FUNCTION update_partition_on_new_file_at()
RETURNS TRIGGER 
LANGUAGE PLPGSQL
AS $$
BEGIN
    IF NEW.compaction_level < 2 THEN
        UPDATE partition SET new_file_at = NEW.created_at WHERE id = NEW.partition_id;    
    END IF;

    RETURN NEW;
END;
$$;