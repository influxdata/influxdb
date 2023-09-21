create table if not exists topic
(
    id   INTEGER not null
        constraint kafka_topic_pkey
            primary key autoincrement,
    name VARCHAR not null
        constraint topic_name_unique unique
);

create table if not exists query_pool
(
    id   INTEGER NOT NULL
        constraint query_pool_pkey
            primary key autoincrement,
    name varchar not null
        constraint query_pool_name_unique
            unique
);

create table if not exists namespace
(
    id                    INTEGER
        constraint namespace_pkey
            primary key autoincrement,
    name                  varchar               not null
        constraint namespace_name_unique
            unique,
    topic_id              numeric               not null
        constraint namespace_kafka_topic_id_fkey
            references topic,
    query_pool_id         numeric               not null
        references query_pool,
    max_tables            integer default 10000 not null,
    max_columns_per_table integer default 200   not null,
    retention_period_ns   numeric
);

create table if not exists table_name
(
    id           INTEGER
        constraint table_name_pkey
            primary key autoincrement,
    namespace_id numeric not null
        references namespace
            on delete cascade,
    name         varchar not null,
    constraint table_name_unique
        unique (namespace_id, name)
);


create index if not exists table_name_namespace_idx
    on table_name (namespace_id);

create table if not exists column_name
(
    id          INTEGER
        constraint column_name_pkey
            primary key autoincrement,
    table_id    numeric  not null
        references table_name
            on delete cascade,
    name        varchar  not null,
    column_type smallint not null,
    constraint column_name_unique
        unique (table_id, name)
);


create index if not exists column_name_table_idx
    on column_name (table_id);

create table if not exists shard
(
    id                              INTEGER
        constraint sequencer_pkey
            primary key autoincrement,
    topic_id                        numeric not null
        constraint sequencer_kafka_topic_id_fkey
            references topic,
    shard_index                     integer not null,
    min_unpersisted_sequence_number numeric,
    constraint shard_unique
        unique (topic_id, shard_index)
);


create table if not exists sharding_rule_override
(
    id           INTEGER
        constraint sharding_rule_override_pkey
            primary key autoincrement,
    namespace_id numeric not null
        references namespace,
    table_id     numeric not null
        references table_name,
    column_id    numeric not null
        references column_name
);


create table if not exists partition
(
    id                        INTEGER
        constraint partition_pkey
            primary key autoincrement,
    shard_id                  numeric not null
        constraint partition_sequencer_id_fkey
            references shard,
    table_id                  numeric not null
        references table_name
            on delete cascade,
    partition_key             varchar not null,
    sort_key                  text [] not null,
    persisted_sequence_number numeric,
    to_delete                 numeric,
    new_file_at               numeric,
    constraint partition_key_unique
        unique (table_id, partition_key)
);


create table if not exists parquet_file
(
    id                  INTEGER
        constraint parquet_file_pkey
            primary key autoincrement,
    shard_id            numeric            not null
        constraint parquet_file_sequencer_id_fkey
            references shard,
    table_id            numeric            not null
        references table_name,
    partition_id        numeric            not null
        references partition,
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

create table if not exists tombstone
(
    id                   INTEGER
        constraint tombstone_pkey
            primary key autoincrement,
    table_id             numeric not null
        references table_name
            on delete cascade,
    shard_id             numeric not null
        constraint tombstone_sequencer_id_fkey
            references shard,
    sequence_number      numeric not null,
    min_time             numeric not null,
    max_time             numeric not null,
    serialized_predicate text    not null,
    constraint tombstone_unique
        unique (table_id, shard_id, sequence_number)
);


create table if not exists processed_tombstone
(
    tombstone_id    INTEGER not null
        references tombstone,
    parquet_file_id numeric not null
        references parquet_file
            on delete cascade,
    primary key (tombstone_id, parquet_file_id)
);


create table if not exists skipped_compactions
(
    partition_id                       INTEGER not null
        constraint skipped_compactions_pkey
            primary key
        references partition
            on delete cascade,
    reason                             text    not null,
    skipped_at                         numeric not null,
    num_files                          numeric,
    limit_num_files                    numeric,
    estimated_bytes                    numeric,
    limit_bytes                        numeric,
    limit_num_files_first_in_partition numeric
);


create table if not exists billing_summary
(
    namespace_id          integer not null
        constraint billing_summary_pkey
            primary key
        references namespace
            on delete cascade,
    total_file_size_bytes numeric not null
);


create index if not exists billing_summary_namespace_idx
    on billing_summary (namespace_id);

