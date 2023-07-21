-- update new_file_at for all compactions, not just L0 & L1
drop trigger update_partition;
create trigger if not exists update_partition
    after insert
    on parquet_file
    for each row
begin
    UPDATE partition set new_file_at = NEW.created_at WHERE id = NEW.partition_id;
end;
