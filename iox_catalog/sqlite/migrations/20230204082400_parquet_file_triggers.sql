create trigger if not exists update_partition
    after insert
    on parquet_file
    for each row
    when NEW.compaction_level < 2
begin
    UPDATE partition set new_file_at = NEW.created_at WHERE id = NEW.partition_id;
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