ALTER TABLE 
    IF EXISTS parquet_file 
    ADD COLUMN max_l0_created_at BIGINT NOT NULL DEFAULT 0; 