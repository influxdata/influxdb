-- Actual batched migration that converts partition sort IDs

-- By default we often only have 5min to finish our statements.
-- IOX_NO_TRANSACTION
SET statement_timeout TO '60min';

-- IOX_STEP_BOUNDARY

-- IOX_NO_TRANSACTION
DO $$
DECLARE
    rem integer;
BEGIN
    -- fixed-point loop that migrates partition in batches
    LOOP
        -- update batch: non-empty keys
        UPDATE partition
        SET sort_key_ids=sub.sort_key_ids
        FROM (
            SELECT
                partition.id AS id,
                array_agg(column_name.id ORDER BY partition.idx) AS sort_key_ids
            FROM (
                SELECT
                    id,
                    table_id,
                    unnest(sort_key) AS column_name,
                    generate_series(1, array_length(sort_key, 1)) as idx
                FROM (SELECT id, table_id, sort_key FROM partition WHERE sort_key_ids IS NULL LIMIT 1000) AS partition
            ) AS partition
            JOIN column_name
                ON partition.table_id = column_name.table_id AND partition.column_name = column_name.name
            GROUP BY partition.id
        ) AS sub
        WHERE partition.id = sub.id;

        -- commit update
        COMMIT;

        -- update batch: empty keys
        UPDATE partition
        SET sort_key_ids=ARRAY[]::BIGINT[]
        FROM (
            SELECT id FROM partition WHERE sort_key_ids IS NULL AND cardinality(sort_key) = 0 LIMIT 1000
        ) AS sub
        WHERE partition.id = sub.id;

        -- commit update
        COMMIT;

        -- check remaining work
        -- do this AT THE END of the loop so that the loop body runs at least once even for an empty database
        SELECT COUNT(*) INTO STRICT rem FROM partition WHERE sort_key_ids IS NULL;
        RAISE NOTICE 'Remaining: %', rem;
        IF rem = 0 THEN
            EXIT;
        END IF;
    END LOOP;
END
$$ LANGUAGE plpgsql;
