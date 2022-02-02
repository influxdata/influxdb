-- IOX_SETUP: TwoMeasurementsManyFieldsTwoChunks

--
-- system tables reflect the state of chunks, so don't run them
-- with different chunk configurations.
--


-- ensures the tables / plumbing are hooked up (so no need to
-- test timestamps, etc)
-- IOX_COMPARE: sorted
SELECT partition_key, table_name, column_name, storage, row_count, null_count, min_value, max_value, memory_bytes from system.chunk_columns;
