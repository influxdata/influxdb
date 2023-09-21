-- set up target column
-- NOTE: proper foreign key arrays are NOT supported at the moment, see https://stackoverflow.com/a/50441059
ALTER TABLE parquet_file ADD COLUMN column_set_new BIGINT[];

-- convert
UPDATE parquet_file
SET column_set_new=sub.column_set_new
FROM (
    SELECT
        parquet_file.id AS id,
        array_agg(column_name.id) AS column_set_new
    FROM (
        SELECT
            id,
            table_id,
            unnest(column_set) AS column_name
        FROM parquet_file
    ) AS parquet_file
    JOIN column_name
        ON parquet_file.table_id = column_name.table_id AND parquet_file.column_name = column_name.name
    GROUP BY parquet_file.id
) AS sub
WHERE parquet_file.id = sub.id;

-- start to enforce target column constraints
ALTER TABLE parquet_file ALTER COLUMN column_set_new SET NOT NULL;

-- replace old column
ALTER TABLE parquet_file DROP COLUMN column_set;
ALTER TABLE parquet_file RENAME COLUMN column_set_new TO column_set;
