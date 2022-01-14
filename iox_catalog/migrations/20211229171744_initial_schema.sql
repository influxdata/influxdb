-- Add migration script here
-- iox_shared schema
BEGIN;

CREATE SCHEMA IF NOT EXISTS iox_catalog;

CREATE TABLE IF NOT EXISTS iox_catalog.kafka_topic
(
    id INT GENERATED ALWAYS AS IDENTITY,
    name VARCHAR NOT NULL,
    PRIMARY KEY (id),
    CONSTRAINT kafka_topic_name_unique UNIQUE (name)
    );

CREATE TABLE IF NOT EXISTS iox_catalog.query_pool
(
    id SMALLINT GENERATED ALWAYS AS IDENTITY,
    name VARCHAR NOT NULL,
    PRIMARY KEY (id),
    CONSTRAINT query_pool_name_unique UNIQUE (name)
    );

CREATE TABLE IF NOT EXISTS iox_catalog.namespace
(
    id INT GENERATED ALWAYS AS IDENTITY,
    name VARCHAR NOT NULL,
    retention_duration VARCHAR,
    kafka_topic_id integer NOT NULL,
    query_pool_id integer NOT NULL,
    PRIMARY KEY (id),
    CONSTRAINT namespace_name_unique UNIQUE (name)
    );

CREATE TABLE IF NOT EXISTS iox_catalog.table_name
(
    id INT GENERATED ALWAYS AS IDENTITY,
    namespace_id integer NOT NULL,
    name VARCHAR NOT NULL,
    PRIMARY KEY (id),
    CONSTRAINT table_name_unique UNIQUE (namespace_id, name)
    );

CREATE TABLE IF NOT EXISTS iox_catalog.column_name
(
    id INT GENERATED ALWAYS AS IDENTITY,
    table_id INT NOT NULL,
    name VARCHAR NOT NULL,
    column_type SMALLINT NOT NULL,
    PRIMARY KEY (id),
    CONSTRAINT column_name_unique UNIQUE (table_id, name)
    );

CREATE TABLE IF NOT EXISTS iox_catalog.sequencer
(
    id SMALLINT GENERATED ALWAYS AS IDENTITY,
    kafka_topic_id INT NOT NULL,
    kafka_partition INT NOT NULL,
    min_unpersisted_sequence_number BIGINT,
    PRIMARY KEY (id),
    CONSTRAINT sequencer_unique UNIQUE (kafka_topic_id, kafka_partition)
    );

CREATE TABLE IF NOT EXISTS iox_catalog.sharding_rule_override
(
    id INT GENERATED ALWAYS AS IDENTITY,
    namespace_id INT NOT NULL,
    table_id INT NOT NULL,
    column_id INT NOT NULL,
    PRIMARY KEY (id)
    );

CREATE TABLE IF NOT EXISTS iox_catalog.partition
(
    id BIGINT GENERATED ALWAYS AS IDENTITY,
    sequencer_id SMALLINT NOT NULL,
    table_id INT NOT NULL,
    partition_key VARCHAR NOT NULL,
    PRIMARY KEY (id),
    CONSTRAINT partition_key_unique UNIQUE (table_id, partition_key)
    );

CREATE TABLE IF NOT EXISTS iox_catalog.parquet_file
(
    id BIGINT GENERATED ALWAYS AS IDENTITY,
    sequencer_id SMALLINT NOT NULL,
    table_id INT NOT NULL,
    partition_id INT NOT NULL,
    file_location VARCHAR NOT NULL,
    min_sequence_number BIGINT,
    max_sequence_number BIGINT,
    min_time BIGINT,
    max_time BIGINT,
    to_delete BOOLEAN,
    PRIMARY KEY (id),
    CONSTRAINT parquet_location_unique UNIQUE (file_location)
    );

CREATE TABLE IF NOT EXISTS iox_catalog.tombstone
(
    id BIGINT GENERATED ALWAYS AS IDENTITY,
    table_id INT NOT NULL,
    sequencer_id SMALLINT NOT NULL,
    sequence_number BIGINT NOT NULL,
    min_time BIGINT NOT NULL,
    max_time BIGINT NOT NULL,
    serialized_predicate TEXT NOT NULL,
    PRIMARY KEY (id)
    );

CREATE TABLE IF NOT EXISTS iox_catalog.processed_tombstone
(
    tombstone_id BIGINT NOT NULL,
    parquet_file_id BIGINT NOT NULL,
    PRIMARY KEY (tombstone_id, parquet_file_id)
    );

ALTER TABLE IF EXISTS iox_catalog.namespace
    ADD FOREIGN KEY (kafka_topic_id)
    REFERENCES iox_catalog.kafka_topic (id) MATCH SIMPLE
    ON UPDATE NO ACTION
       ON DELETE NO ACTION
	NOT VALID;

ALTER TABLE IF EXISTS iox_catalog.namespace
    ADD FOREIGN KEY (query_pool_id)
    REFERENCES iox_catalog.query_pool (id) MATCH SIMPLE
    ON UPDATE NO ACTION
       ON DELETE NO ACTION
	NOT VALID;

ALTER TABLE IF EXISTS iox_catalog.table_name
    ADD FOREIGN KEY (namespace_id)
    REFERENCES iox_catalog.namespace (id) MATCH SIMPLE
    ON UPDATE NO ACTION
       ON DELETE NO ACTION
	NOT VALID;

ALTER TABLE IF EXISTS iox_catalog.column_name
    ADD FOREIGN KEY (table_id)
    REFERENCES iox_catalog.table_name (id) MATCH SIMPLE
    ON UPDATE NO ACTION
       ON DELETE NO ACTION
	NOT VALID;

ALTER TABLE IF EXISTS iox_catalog.sequencer
    ADD FOREIGN KEY (kafka_topic_id)
    REFERENCES iox_catalog.kafka_topic (id) MATCH SIMPLE
    ON UPDATE NO ACTION
       ON DELETE NO ACTION
	NOT VALID;

ALTER TABLE IF EXISTS iox_catalog.sharding_rule_override
    ADD FOREIGN KEY (namespace_id)
    REFERENCES iox_catalog.namespace (id) MATCH SIMPLE
    ON UPDATE NO ACTION
       ON DELETE NO ACTION
	NOT VALID;

ALTER TABLE IF EXISTS iox_catalog.sharding_rule_override
    ADD FOREIGN KEY (table_id)
    REFERENCES iox_catalog.table_name (id) MATCH SIMPLE
    ON UPDATE NO ACTION
       ON DELETE NO ACTION
	NOT VALID;

ALTER TABLE IF EXISTS iox_catalog.sharding_rule_override
    ADD FOREIGN KEY (column_id)
    REFERENCES iox_catalog.column_name (id) MATCH SIMPLE
    ON UPDATE NO ACTION
       ON DELETE NO ACTION
	NOT VALID;

ALTER TABLE IF EXISTS iox_catalog.partition
    ADD FOREIGN KEY (sequencer_id)
    REFERENCES iox_catalog.sequencer (id) MATCH SIMPLE
    ON UPDATE NO ACTION
       ON DELETE NO ACTION
	NOT VALID;

ALTER TABLE IF EXISTS iox_catalog.partition
    ADD FOREIGN KEY (table_id)
    REFERENCES iox_catalog.table_name (id) MATCH SIMPLE
    ON UPDATE NO ACTION
       ON DELETE NO ACTION
	NOT VALID;

ALTER TABLE IF EXISTS iox_catalog.parquet_file
    ADD FOREIGN KEY (sequencer_id)
    REFERENCES iox_catalog.sequencer (id) MATCH SIMPLE
    ON UPDATE NO ACTION
       ON DELETE NO ACTION
	NOT VALID;

ALTER TABLE IF EXISTS iox_catalog.parquet_file
    ADD FOREIGN KEY (table_id)
    REFERENCES iox_catalog.table_name (id) MATCH SIMPLE
    ON UPDATE NO ACTION
       ON DELETE NO ACTION
	NOT VALID;

ALTER TABLE IF EXISTS iox_catalog.parquet_file
    ADD FOREIGN KEY (partition_id)
    REFERENCES iox_catalog.partition (id) MATCH SIMPLE
    ON UPDATE NO ACTION
       ON DELETE NO ACTION
	NOT VALID;

ALTER TABLE IF EXISTS iox_catalog.tombstone
    ADD FOREIGN KEY (sequencer_id)
    REFERENCES iox_catalog.sequencer (id) MATCH SIMPLE
    ON UPDATE NO ACTION
       ON DELETE NO ACTION
	NOT VALID;

ALTER TABLE IF EXISTS iox_catalog.tombstone
    ADD FOREIGN KEY (table_id)
    REFERENCES iox_catalog.table_name (id) MATCH SIMPLE
    ON UPDATE NO ACTION
       ON DELETE NO ACTION
	NOT VALID;

ALTER TABLE IF EXISTS iox_catalog.processed_tombstone
    ADD FOREIGN KEY (tombstone_id)
    REFERENCES iox_catalog.tombstone (id) MATCH SIMPLE
    ON UPDATE NO ACTION
       ON DELETE NO ACTION
	NOT VALID;

ALTER TABLE IF EXISTS iox_catalog.processed_tombstone
    ADD FOREIGN KEY (parquet_file_id)
    REFERENCES iox_catalog.parquet_file (id) MATCH SIMPLE
    ON UPDATE NO ACTION
       ON DELETE NO ACTION
	NOT VALID;
END;
