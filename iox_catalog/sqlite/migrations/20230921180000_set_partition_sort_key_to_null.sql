ALTER TABLE partition DROP COLUMN sort_key;
ALTER TABLE partition ADD COLUMN sort_key TEXT[];
