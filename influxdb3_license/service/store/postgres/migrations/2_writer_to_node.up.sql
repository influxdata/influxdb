BEGIN;

-- Rename the writer_id column to node_id
ALTER TABLE licenses RENAME COLUMN writer_id TO node_id;

COMMIT;