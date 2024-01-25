-- Remove unused indices
DROP INDEX IF EXISTS parquet_file_partition_created_idx;
DROP INDEX IF EXISTS parquet_file_shard_compaction_delete_created_idx;
DROP INDEX IF EXISTS parquet_file_shard_compaction_delete_idx;
DROP INDEX IF EXISTS parquet_file_partition_idx;
DROP INDEX IF EXISTS parquet_file_table_idx;
-- Remove the columns referring to Kafka based information
ALTER TABLE namespace DROP COLUMN query_pool_id;
ALTER TABLE namespace DROP COLUMN topic_id;
ALTER TABLE parquet_file DROP COLUMN shard_id;
ALTER TABLE partition DROP COLUMN shard_id;
-- The tombstone table had a redundant unique constraint on shard_id which
-- prevents it from being dropped in SQLite, we have to copy the data into a new
-- table, delete the old one and then recreate the table.
CREATE TABLE tombstone_temp AS SELECT * FROM tombstone;
DROP TABLE tombstone;
CREATE TABLE tombstone (
    id                   INTEGER
        constraint tombstone_pkey
            primary key autoincrement,
    table_id             numeric not null
        references table_name
            on delete cascade,
    shard_id             numeric not null,
    sequence_number      numeric not null,
    min_time             numeric not null,
    max_time             numeric not null,
    serialized_predicate text    not null,
    constraint tombstone_unique
        unique (table_id, sequence_number)
);
INSERT INTO tombstone SELECT * FROM tombstone_temp;
ALTER TABLE tombstone DROP COLUMN shard_id;
DROP TABLE tombstone_temp;
-- Remove the now unreferenced, unused tables 
DROP TABLE IF EXISTS topic;
DROP TABLE IF EXISTS query_pool;
DROP TABLE IF EXISTS shard;
DROP TABLE IF EXISTS sharding_rule_override;