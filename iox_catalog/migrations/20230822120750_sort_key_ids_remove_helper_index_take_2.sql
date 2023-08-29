-- Remove helper index for the migration from name-based partition sort keys to ID-based sort keys

-- By default we often only have 5min to finish our statements.
-- IOX_NO_TRANSACTION
SET statement_timeout TO '60min';

-- IOX_STEP_BOUNDARY

-- remove potentially invalid index
-- IOX_NO_TRANSACTION
DROP INDEX CONCURRENTLY IF EXISTS partition_sort_key_name_to_id_helper;
