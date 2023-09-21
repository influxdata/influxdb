CREATE TABLE parquet_file_temp
AS SELECT * FROM parquet_file;

DROP TABLE parquet_file;

CREATE TABLE parquet_file
(
    id                  INTEGER
        constraint parquet_file_pkey
            primary key autoincrement,
    shard_id            numeric            not null
        constraint parquet_file_sequencer_id_fkey
            references shard,
    table_id            numeric            not null
        references table_name,
    partition_id        numeric
        references partition,
    partition_hash_id bytea
      references partition (hash_id),

    object_store_id     uuid               not null
        constraint parquet_location_unique
            unique,
    max_sequence_number numeric,
    min_time            numeric,
    max_time            numeric,
    to_delete           numeric,
    row_count           numeric  default 0 not null,
    file_size_bytes     numeric  default 0 not null,
    compaction_level    smallint default 0 not null,
    created_at          numeric,
    namespace_id        numeric            not null
        references namespace
            on delete cascade,
    column_set          numeric[]          not null,
    max_l0_created_at   numeric  default 0 not null
);

create index if not exists parquet_file_deleted_at_idx
    on parquet_file (to_delete);

create index if not exists parquet_file_partition_idx
    on parquet_file (partition_id);

create index if not exists parquet_file_table_idx
    on parquet_file (table_id);

create index if not exists parquet_file_shard_compaction_delete_idx
    on parquet_file (shard_id, compaction_level, to_delete);

create index if not exists parquet_file_shard_compaction_delete_created_idx
    on parquet_file (shard_id, compaction_level, to_delete, created_at);

create index if not exists parquet_file_partition_created_idx
    on parquet_file (partition_id, created_at);

CREATE INDEX IF NOT EXISTS parquet_file_partition_hash_id_idx
ON parquet_file (partition_hash_id)
WHERE partition_hash_id IS NOT NULL;

create trigger if not exists update_partition
    after insert
    on parquet_file
    for each row
begin
    UPDATE partition
    SET new_file_at = NEW.created_at
    WHERE (NEW.partition_id IS NULL OR id = NEW.partition_id)
       AND (NEW.partition_hash_id IS NULL OR hash_id = NEW.partition_hash_id);
end;

create trigger if not exists update_billing
    after insert
    on parquet_file
    for each row
begin
    INSERT INTO billing_summary (namespace_id, total_file_size_bytes)
    VALUES (NEW.namespace_id, NEW.file_size_bytes)
    ON CONFLICT (namespace_id) DO UPDATE
        SET total_file_size_bytes = billing_summary.total_file_size_bytes + NEW.file_size_bytes
    WHERE billing_summary.namespace_id = NEW.namespace_id;
end;

create trigger if not exists decrement_summary
    after update
    on parquet_file
    for each row
    when OLD.to_delete IS NULL AND NEW.to_delete IS NOT NULL
begin
    UPDATE billing_summary
    SET total_file_size_bytes = billing_summary.total_file_size_bytes - OLD.file_size_bytes
    WHERE billing_summary.namespace_id = OLD.namespace_id;
end;

INSERT INTO parquet_file
SELECT * FROM parquet_file_temp;

DROP TABLE parquet_file_temp;
