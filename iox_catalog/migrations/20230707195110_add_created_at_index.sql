-- Add to help the compactor when it searches for partitions with files created recently.

-- By default we often only have 5min to finish our statements. The `CREATE INDEX CONCURRENTLY` however takes longer.
-- In our prod test this took about 15min, but better be safe than sorry.
-- IOX_NO_TRANSACTION
SET statement_timeout TO '60min';

-- IOX_STEP_BOUNDARY

-- While `CONCURRENTLY` means it runs parallel to other writes, this command will only finish after the index was
-- successfully built.
-- IOX_NO_TRANSACTION
CREATE INDEX CONCURRENTLY IF NOT EXISTS partition_new_file_at_idx ON partition (new_file_at);
