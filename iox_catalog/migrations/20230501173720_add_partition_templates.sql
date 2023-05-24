ALTER TABLE
    IF EXISTS namespace
    ADD COLUMN partition_template JSONB;

ALTER TABLE
    IF EXISTS table_name
    ADD COLUMN partition_template JSONB;
