ALTER TABLE partition DROP COLUMN sort_key_ids;
ALTER TABLE partition ADD COLUMN sort_key_ids INTEGER[] NOT NULL;
