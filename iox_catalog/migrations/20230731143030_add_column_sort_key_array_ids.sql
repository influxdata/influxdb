-- https://github.com/influxdata/influxdb_iox/issues/6401

ALTER TABLE partition ADD COLUMN IF NOT EXISTS sort_key_ids BIGINT[];
