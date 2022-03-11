ALTER TABLE
    IF EXISTS parquet_file
    ADD
    COLUMN compaction_level smallint NOT NULL DEFAULT 0,
    ADD
    COLUMN created_at bigint;
