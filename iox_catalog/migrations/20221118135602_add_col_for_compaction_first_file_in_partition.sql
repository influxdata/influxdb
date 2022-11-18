ALTER TABLE IF EXISTS skipped_compactions
    ADD COLUMN IF NOT EXISTS limit_num_files_first_in_partition BIGINT DEFAULT NULL;
