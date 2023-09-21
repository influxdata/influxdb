UPDATE partition SET sort_key_arr=string_to_array(COALESCE(sort_key, ''),',') WHERE sort_key_arr IS NULL;
ALTER TABLE partition ALTER COLUMN sort_key_arr SET NOT NULL;
ALTER TABLE partition RENAME COLUMN sort_key TO sort_key_old;
ALTER TABLE partition RENAME COLUMN sort_key_arr TO sort_key;
