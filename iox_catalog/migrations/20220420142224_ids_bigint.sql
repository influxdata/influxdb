----------------------------------------
-- kafka_topic_id
ALTER TABLE
    IF EXISTS kafka_topic
    ALTER COLUMN id
    TYPE BIGINT
;

ALTER TABLE
    IF EXISTS namespace
    ALTER COLUMN kafka_topic_id
    TYPE BIGINT
;

ALTER TABLE
    IF EXISTS sequencer
    ALTER COLUMN kafka_topic_id
    TYPE BIGINT
;
----------------------------------------

----------------------------------------
-- query_pool_id
ALTER TABLE
    IF EXISTS query_pool
    ALTER COLUMN id
    TYPE BIGINT
;

ALTER TABLE
    IF EXISTS namespace
    ALTER COLUMN query_pool_id
    TYPE BIGINT
;
----------------------------------------

----------------------------------------
-- namespace_id
ALTER TABLE
    IF EXISTS namespace
    ALTER COLUMN id
    TYPE BIGINT
;

ALTER TABLE
    IF EXISTS table_name
    ALTER COLUMN namespace_id
    TYPE BIGINT
;

ALTER TABLE
    IF EXISTS sharding_rule_override
    ALTER COLUMN namespace_id
    TYPE BIGINT
;

ALTER TABLE
    IF EXISTS parquet_file
    ALTER COLUMN namespace_id
    TYPE BIGINT
;
----------------------------------------

----------------------------------------
-- table_id
ALTER TABLE
    IF EXISTS table_name
    ALTER COLUMN id
    TYPE BIGINT
;

ALTER TABLE
    IF EXISTS column_name
    ALTER COLUMN table_id
    TYPE BIGINT
;

ALTER TABLE
    IF EXISTS sharding_rule_override
    ALTER COLUMN table_id
    TYPE BIGINT
;

ALTER TABLE
    IF EXISTS partition
    ALTER COLUMN table_id
    TYPE BIGINT
;

ALTER TABLE
    IF EXISTS parquet_file
    ALTER COLUMN table_id
    TYPE BIGINT
;

ALTER TABLE
    IF EXISTS tombstone
    ALTER COLUMN table_id
    TYPE BIGINT
;
----------------------------------------

----------------------------------------
-- column_id
ALTER TABLE
    IF EXISTS column_name
    ALTER COLUMN id
    TYPE BIGINT
;

ALTER TABLE
    IF EXISTS sharding_rule_override
    ALTER COLUMN column_id
    TYPE BIGINT
;
----------------------------------------

----------------------------------------
-- sharding_rule_override_id
ALTER TABLE
    IF EXISTS sharding_rule_override
    ALTER COLUMN id
    TYPE BIGINT
;
----------------------------------------

----------------------------------------
-- sequencer_id
ALTER TABLE
    IF EXISTS sequencer
    ALTER COLUMN id
    TYPE BIGINT
;

ALTER TABLE
    IF EXISTS partition
    ALTER COLUMN sequencer_id
    TYPE BIGINT
;

ALTER TABLE
    IF EXISTS parquet_file
    ALTER COLUMN sequencer_id
    TYPE BIGINT
;

ALTER TABLE
    IF EXISTS tombstone
    ALTER COLUMN sequencer_id
    TYPE BIGINT
;
----------------------------------------
