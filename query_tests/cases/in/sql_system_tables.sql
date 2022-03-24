-- IOX_SETUP: OldTwoMeasurementsManyFieldsOneRubChunk

--
-- system tables reflect the state of chunks, so don't run them
-- with different chunk configurations.
--

-- validate we have access to information schema for listing system tables
-- IOX_COMPARE: sorted
SELECT * from information_schema.tables where table_schema = 'system';

-- ensures the tables / plumbing are hooked up (so no need to
-- test timestamps, etc)
-- IOX_COMPARE: sorted
SELECT partition_key, table_name, storage, memory_bytes, row_count from system.chunks;


-- IOX_COMPARE: sorted
SELECT * from system.columns;
