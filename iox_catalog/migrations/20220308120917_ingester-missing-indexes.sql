-- Add indexes to support JOINs in TableRepo::get_table_persist_info
-- https://github.com/influxdata/influxdb_iox/blob/aaec1c7828139e47296665c84aeea4c974026118/iox_catalog/src/postgres.rs#L717-L734
CREATE INDEX IF NOT EXISTS table_name_namespace_idx ON table_name (namespace_id);

CREATE INDEX IF NOT EXISTS parquet_file_table_idx ON parquet_file (table_id);
