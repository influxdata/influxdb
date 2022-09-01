-- Rename sequencer to shard
ALTER TABLE IF EXISTS sequencer RENAME TO shard;
ALTER TABLE IF EXISTS shard RENAME CONSTRAINT sequencer_unique TO shard_unique;
ALTER TABLE IF EXISTS partition RENAME sequencer_id TO shard_id;
ALTER TABLE IF EXISTS tombstone RENAME sequencer_id TO shard_id;
ALTER TABLE IF EXISTS parquet_file RENAME sequencer_id TO shard_id;
ALTER INDEX IF EXISTS parquet_file_sequencer_compaction_delete_idx
  RENAME TO parquet_file_shard_compaction_delete_idx;

-- Rename kafka_partition to shard_index
ALTER TABLE IF EXISTS shard RENAME kafka_partition TO shard_index;

-- Rename kafka_topic to topic
ALTER TABLE IF EXISTS kafka_topic RENAME TO topic;
ALTER TABLE IF EXISTS topic RENAME CONSTRAINT kafka_topic_name_unique TO topic_name_unique;
ALTER TABLE IF EXISTS namespace RENAME kafka_topic_id TO topic_id;
ALTER TABLE IF EXISTS shard RENAME kafka_topic_id TO topic_id;
