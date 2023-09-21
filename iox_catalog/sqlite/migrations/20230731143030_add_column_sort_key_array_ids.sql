-- https://github.com/influxdata/influxdb_iox/issues/6401

ALTER TABLE partition ADD COLUMN sort_key_ids INTEGER[];