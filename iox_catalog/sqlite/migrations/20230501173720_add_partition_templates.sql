ALTER TABLE
    namespace
ADD COLUMN partition_template TEXT;

ALTER TABLE
    table_name
ADD COLUMN partition_template TEXT;
