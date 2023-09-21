-- This column is ment to be later manually populated with:
--
-- UPDATE partition SET sort_key_arr=string_to_array(CASE WHEN sort_key IS NULL THEN '' ELSE sort_key END,',');
--
-- before merging https://github.com/influxdata/influxdb_iox/pull/4801/
-- (which will rename `sort_key_arr` back to `sort_key`)
--
-- Then, once the migration is done we should add a NOT NULL constraint.

ALTER TABLE partition ADD COLUMN sort_key_arr TEXT[];
