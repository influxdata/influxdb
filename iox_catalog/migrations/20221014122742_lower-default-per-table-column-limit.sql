-- Lower the defualt per-table column limit.
--
-- https://github.com/influxdata/influxdb_iox/issues/5858
ALTER TABLE
    namespace ALTER max_columns_per_table
SET
    DEFAULT 200;
