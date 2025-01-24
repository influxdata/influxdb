BEGIN;

-- Rename the node_id column back to writer_id
ALTER TABLE licenses RENAME COLUMN node_id TO writer_id;

COMMIT;