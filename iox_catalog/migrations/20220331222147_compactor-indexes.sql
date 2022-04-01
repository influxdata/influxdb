-- Add indexes for looking for level 0 files by sequencer id for compactor
CREATE INDEX IF NOT EXISTS parquet_file_sequencer_compaction_delete_idx ON parquet_file (sequencer_id, compaction_level, to_delete);

-- Add indexes for looking up parquet files by partition for compaction
CREATE INDEX IF NOT EXISTS parquet_file_partition_idx ON parquet_file (partition_id);
