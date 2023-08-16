-- Add to help the compactor when it searches for partitions with files created recently.
-- IOX_OTHER_CHECKSUM: 7a353a9a9876a691c6df91af2f2774bb0a43e5f86aa65ca470c16e413ef1741f4a75505ad4e56f1ecb206c2097aad90f

-- By default we often only have 5min to finish our statements. The `CREATE INDEX CONCURRENTLY` however takes longer.
-- In our prod test this took about 15min, but better be safe than sorry.
-- IOX_NO_TRANSACTION
SET statement_timeout TO '60min';

-- IOX_STEP_BOUNDARY

-- remove potentially invalid index
-- IOX_NO_TRANSACTION
DROP INDEX CONCURRENTLY IF EXISTS partition_new_file_at_idx;

-- IOX_STEP_BOUNDARY

-- While `CONCURRENTLY` means it runs parallel to other writes, this command will only finish after the index was
-- successfully built.
-- IOX_NO_TRANSACTION
CREATE INDEX CONCURRENTLY partition_new_file_at_idx ON partition (new_file_at);
