-- Add helper index for the migration from name-based partition sort keys to ID-based sort keys

-- By default we often only have 5min to finish our statements.
-- IOX_NO_TRANSACTION
SET statement_timeout TO '60min';

-- IOX_STEP_BOUNDARY

-- remove potentially invalid index
-- IOX_NO_TRANSACTION
DROP INDEX CONCURRENTLY IF EXISTS partition_sort_key_name_to_id_helper;

-- IOX_STEP_BOUNDARY

-- While `CONCURRENTLY` means it runs parallel to other writes, this command will only finish after the index was
-- successfully built.
-- IOX_NO_TRANSACTION
CREATE INDEX CONCURRENTLY partition_sort_key_name_to_id_helper ON partition (id) WHERE sort_key_ids IS NULL;
