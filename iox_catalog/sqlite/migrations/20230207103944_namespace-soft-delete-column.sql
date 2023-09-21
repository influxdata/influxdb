-- Add a soft-deletion timestamp to the "namespace" table.
--
-- <https://github.com/influxdata/influxdb_iox/issues/6492>
ALTER TABLE
    namespace
ADD
    COLUMN deleted_at numeric DEFAULT NULL;

CREATE INDEX namespace_deleted_at_idx ON namespace (deleted_at);