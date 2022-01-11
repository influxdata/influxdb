-- Add migration script here
-- iox_shared schema
BEGIN;

CREATE TABLE IF NOT EXISTS public.kafka_topics
(
    id INT GENERATED ALWAYS AS IDENTITY,
    name VARCHAR NOT NULL,
    PRIMARY KEY (id),
    CONSTRAINT kafka_topic_name_unique UNIQUE (name)
    );

CREATE TABLE IF NOT EXISTS public.query_pools
(
    id SMALLINT GENERATED ALWAYS AS IDENTITY,
    name VARCHAR NOT NULL,
    connection_string VARCHAR NOT NULL,
    PRIMARY KEY (id),
    CONSTRAINT query_pool_name_unique UNIQUE (name)
    );

CREATE TABLE IF NOT EXISTS public.namespaces
(
    id INT GENERATED ALWAYS AS IDENTITY,
    name VARCHAR NOT NULL,
    retention_duration VARCHAR,
    kafka_topic_id integer NOT NULL,
    query_pool_id integer NOT NULL,
    PRIMARY KEY (id),
    CONSTRAINT namespace_name_unique UNIQUE (name)
    );

CREATE TABLE IF NOT EXISTS public.table_names
(
    id INT GENERATED ALWAYS AS IDENTITY,
    namespace_id integer NOT NULL,
    name VARCHAR NOT NULL,
    PRIMARY KEY (id),
    CONSTRAINT table_name_unique UNIQUE (namespace_id, name)
    );

CREATE TABLE IF NOT EXISTS public.column_names
(
    id INT GENERATED ALWAYS AS IDENTITY,
    table_id INT NOT NULL,
    name VARCHAR NOT NULL,
    data_type SMALLINT NOT NULL,
    PRIMARY KEY (id),
    CONSTRAINT column_name_unique UNIQUE (table_id, name)
    );

CREATE TABLE IF NOT EXISTS public.sequencers
(
    id SMALLINT GENERATED ALWAYS AS IDENTITY,
    kafka_topic_id INT NOT NULL,
    kafka_partition INT NOT NULL,
    min_unpersisted_sequence_number BIGINT,
    PRIMARY KEY (id)
    );

CREATE TABLE IF NOT EXISTS public.sharding_rule_overrides
(
    id INT GENERATED ALWAYS AS IDENTITY,
    namespace_id INT NOT NULL,
    table_id INT NOT NULL,
    column_id INT NOT NULL,
    PRIMARY KEY (id)
    );

CREATE TABLE IF NOT EXISTS public.partitions
(
    id INT GENERATED ALWAYS AS IDENTITY,
    sequencer_id SMALLINT NOT NULL,
    table_id INT NOT NULL,
    partition_key VARCHAR NOT NULL,
    PRIMARY KEY (id),
    CONSTRAINT partition_key_unique UNIQUE (table_id, partition_key)
    );

CREATE TABLE IF NOT EXISTS public.parquet_files
(
    id BIGINT GENERATED ALWAYS AS IDENTITY,
    sequencer_id SMALLINT NOT NULL,
    table_id INT NOT NULL,
    partition_id INT NOT NULL,
    file_location VARCHAR NOT NULL,
    min_sequence_number BIGINT,
    max_sequence_number BIGINT,
    min_time INT,
    max_time INT,
    to_delete BOOLEAN,
    PRIMARY KEY (id),
    CONSTRAINT parquet_location_unique UNIQUE (file_location)
    );

CREATE TABLE IF NOT EXISTS public.delete_tombstones
(
    id INT GENERATED ALWAYS AS IDENTITY,
    table_id INT NOT NULL,
    sequencer_id SMALLINT NOT NULL,
    partition_id INT NOT NULL,
    sequence_number BIGINT NOT NULL,
    min_time INT NOT NULL,
    max_time INT NOT NULL,
    serialized_predicate TEXT NOT NULL,
    PRIMARY KEY (id)
    );

CREATE TABLE IF NOT EXISTS public.processed_tombstones
(
    delete_tombstone_id INT NOT NULL,
    parquet_file_id BIGINT NOT NULL,
    PRIMARY KEY (delete_tombstone_id, parquet_file_id)
    );

ALTER TABLE IF EXISTS public.namespaces
    ADD FOREIGN KEY (kafka_topic_id)
    REFERENCES public.kafka_topics (id) MATCH SIMPLE
    ON UPDATE NO ACTION
       ON DELETE NO ACTION
	NOT VALID;

ALTER TABLE IF EXISTS public.namespaces
    ADD FOREIGN KEY (query_pool_id)
    REFERENCES public.query_pools (id) MATCH SIMPLE
    ON UPDATE NO ACTION
       ON DELETE NO ACTION
	NOT VALID;

ALTER TABLE IF EXISTS public.table_names
    ADD FOREIGN KEY (namespace_id)
    REFERENCES public.namespaces (id) MATCH SIMPLE
    ON UPDATE NO ACTION
       ON DELETE NO ACTION
	NOT VALID;

ALTER TABLE IF EXISTS public.column_names
    ADD FOREIGN KEY (table_id)
    REFERENCES public.table_names (id) MATCH SIMPLE
    ON UPDATE NO ACTION
       ON DELETE NO ACTION
	NOT VALID;

ALTER TABLE IF EXISTS public.sequencers
    ADD FOREIGN KEY (kafka_topic_id)
    REFERENCES public.kafka_topics (id) MATCH SIMPLE
    ON UPDATE NO ACTION
       ON DELETE NO ACTION
	NOT VALID;

ALTER TABLE IF EXISTS public.sharding_rule_overrides
    ADD FOREIGN KEY (namespace_id)
    REFERENCES public.namespaces (id) MATCH SIMPLE
    ON UPDATE NO ACTION
       ON DELETE NO ACTION
	NOT VALID;

ALTER TABLE IF EXISTS public.sharding_rule_overrides
    ADD FOREIGN KEY (table_id)
    REFERENCES public.table_names (id) MATCH SIMPLE
    ON UPDATE NO ACTION
       ON DELETE NO ACTION
	NOT VALID;

ALTER TABLE IF EXISTS public.sharding_rule_overrides
    ADD FOREIGN KEY (column_id)
    REFERENCES public.column_names (id) MATCH SIMPLE
    ON UPDATE NO ACTION
       ON DELETE NO ACTION
	NOT VALID;

ALTER TABLE IF EXISTS public.partitions
    ADD FOREIGN KEY (sequencer_id)
    REFERENCES public.sequencers (id) MATCH SIMPLE
    ON UPDATE NO ACTION
       ON DELETE NO ACTION
	NOT VALID;

ALTER TABLE IF EXISTS public.partitions
    ADD FOREIGN KEY (table_id)
    REFERENCES public.table_names (id) MATCH SIMPLE
    ON UPDATE NO ACTION
       ON DELETE NO ACTION
	NOT VALID;

ALTER TABLE IF EXISTS public.parquet_files
    ADD FOREIGN KEY (sequencer_id)
    REFERENCES public.sequencers (id) MATCH SIMPLE
    ON UPDATE NO ACTION
       ON DELETE NO ACTION
	NOT VALID;

ALTER TABLE IF EXISTS public.parquet_files
    ADD FOREIGN KEY (table_id)
    REFERENCES public.table_names (id) MATCH SIMPLE
    ON UPDATE NO ACTION
       ON DELETE NO ACTION
	NOT VALID;

ALTER TABLE IF EXISTS public.parquet_files
    ADD FOREIGN KEY (partition_id)
    REFERENCES public.partitions (id) MATCH SIMPLE
    ON UPDATE NO ACTION
       ON DELETE NO ACTION
	NOT VALID;

ALTER TABLE IF EXISTS public.delete_tombstones
    ADD FOREIGN KEY (sequencer_id)
    REFERENCES public.sequencers (id) MATCH SIMPLE
    ON UPDATE NO ACTION
       ON DELETE NO ACTION
	NOT VALID;

ALTER TABLE IF EXISTS public.delete_tombstones
    ADD FOREIGN KEY (table_id)
    REFERENCES public.table_names (id) MATCH SIMPLE
    ON UPDATE NO ACTION
       ON DELETE NO ACTION
	NOT VALID;

ALTER TABLE IF EXISTS public.delete_tombstones
    ADD FOREIGN KEY (partition_id)
    REFERENCES public.partitions (id) MATCH SIMPLE
    ON UPDATE NO ACTION
       ON DELETE NO ACTION
	NOT VALID;

ALTER TABLE IF EXISTS public.processed_tombstones
    ADD FOREIGN KEY (delete_tombstone_id)
    REFERENCES public.delete_tombstones (id) MATCH SIMPLE
    ON UPDATE NO ACTION
       ON DELETE NO ACTION
	NOT VALID;

ALTER TABLE IF EXISTS public.processed_tombstones
    ADD FOREIGN KEY (parquet_file_id)
    REFERENCES public.parquet_files (id) MATCH SIMPLE
    ON UPDATE NO ACTION
       ON DELETE NO ACTION
	NOT VALID;
END;
