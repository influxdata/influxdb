-- Drop the foreign key constraints referencing the various 
-- placeholder kafka columns
ALTER TABLE IF EXISTS namespace DROP CONSTRAINT IF EXISTS namespace_kafka_topic_id_fkey, DROP CONSTRAINT IF EXISTS namespace_query_pool_id_fkey;
ALTER TABLE IF EXISTS parquet_file DROP CONSTRAINT IF EXISTS parquet_file_sequencer_id_fkey;
ALTER TABLE IF EXISTS partition DROP CONSTRAINT IF EXISTS partition_sequencer_id_fkey;
ALTER TABLE IF EXISTS tombstone DROP CONSTRAINT IF EXISTS tombstone_sequencer_id_fkey;
-- Allow the ID columns in these tables to be nullable
ALTER TABLE IF EXISTS namespace ALTER COLUMN topic_id DROP NOT NULL, ALTER COLUMN query_pool_id DROP NOT NULL;
ALTER TABLE IF EXISTS parquet_file ALTER COLUMN shard_id DROP NOT NULL;
ALTER TABLE IF EXISTS partition ALTER COLUMN shard_id DROP NOT NULL;
ALTER TABLE IF EXISTS tombstone ALTER COLUMN shard_id DROP NOT NULL;