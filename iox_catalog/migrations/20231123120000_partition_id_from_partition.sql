-- Actual batched migration that converts parquet file partition ids

-- By default we often only have 5min to finish our statements.
-- IOX_NO_TRANSACTION
SET statement_timeout TO '60min';

-- IOX_STEP_BOUNDARY

-- IOX_NO_TRANSACTION
DO
$$
    DECLARE
        pos       integer;
        max       integer;
        processed integer;
    BEGIN
        SELECT coalesce(min(id), 0), coalesce(max(id), 0)
        INTO pos, max
        FROM parquet_file;

        -- loop that migrates parquet_file in batches
        LOOP
            -- update batch:
            RAISE NOTICE 'Processing rows from %', pos;
            UPDATE parquet_file
            SET partition_id=partition.id
            FROM partition
            WHERE parquet_file.partition_hash_id = partition.hash_id
              AND parquet_file.partition_id is NULL
              AND parquet_file.id >= pos
              AND parquet_file.id < pos + 100000;

            pos = pos + 100000;

            GET DIAGNOSTICS processed = ROW_COUNT;

            -- commit update
            COMMIT;

            -- check remaining work
            RAISE NOTICE 'Updated: % rows', processed;
            IF pos > max THEN
                EXIT;
            END IF;
        END LOOP;
    END
$$ LANGUAGE plpgsql;
