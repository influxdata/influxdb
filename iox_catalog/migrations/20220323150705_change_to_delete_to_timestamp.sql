ALTER TABLE
    IF EXISTS parquet_file
    ALTER COLUMN to_delete
    TYPE BIGINT
    -- If to_delete is set to true, assign it a zero-but-not-null timestamp since we don't have the
    -- actual time it was marked to_delete, but we want it to be deleted. If to_delete is false or
    -- null, set it to null.
    USING CASE WHEN to_delete THEN 0 ELSE NULL END
;
