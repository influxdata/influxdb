ALTER TABLE parquet_file
    DROP CONSTRAINT parquet_file_namespace_id_fkey;
ALTER TABLE parquet_file
    ADD CONSTRAINT parquet_file_namespace_id_fkey
    FOREIGN KEY (namespace_id) REFERENCES namespace(id)
    ON DELETE CASCADE;

ALTER TABLE processed_tombstone
    DROP CONSTRAINT processed_tombstone_parquet_file_id_fkey;
ALTER TABLE processed_tombstone
    ADD CONSTRAINT processed_tombstone_parquet_file_id_fkey
    FOREIGN KEY (parquet_file_id) REFERENCES parquet_file(id)
    ON DELETE CASCADE;

ALTER TABLE tombstone
    DROP CONSTRAINT tombstone_table_id_fkey;
ALTER TABLE tombstone
    ADD CONSTRAINT tombstone_table_id_fkey
    FOREIGN KEY (table_id) REFERENCES table_name(id)
    ON DELETE CASCADE;

ALTER TABLE table_name
    DROP CONSTRAINT table_name_namespace_id_fkey;
ALTER TABLE table_name
    ADD CONSTRAINT table_name_namespace_id_fkey
    FOREIGN KEY (namespace_id) REFERENCES namespace(id)
    ON DELETE CASCADE;

ALTER TABLE column_name
    DROP CONSTRAINT column_name_table_id_fkey;
ALTER TABLE column_name
    ADD CONSTRAINT column_name_table_id_fkey
    FOREIGN KEY (table_id) REFERENCES table_name(id)
    ON DELETE CASCADE;

ALTER TABLE partition
    DROP CONSTRAINT partition_table_id_fkey;
ALTER TABLE partition
    ADD CONSTRAINT partition_table_id_fkey
    FOREIGN KEY (table_id) REFERENCES table_name(id)
    ON DELETE CASCADE;

ALTER TABLE billing_summary
    DROP CONSTRAINT billing_summary_namespace_id_fkey;
ALTER TABLE billing_summary
    ADD CONSTRAINT billing_summary_namespace_id_fkey
    FOREIGN KEY (namespace_id) REFERENCES namespace(id)
    ON DELETE CASCADE;
