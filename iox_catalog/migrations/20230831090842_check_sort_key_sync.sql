-- sanity check for https://github.com/influxdata/influxdb_iox/issues/8570
DO $$
DECLARE
    rem integer;
BEGIN
    SELECT COUNT(*) INTO STRICT rem FROM partition WHERE cardinality(sort_key_ids) != cardinality(sort_key);
    IF rem != 0 THEN
        RAISE EXCEPTION 'Number of not correctly migrated entries: %, see https://github.com/influxdata/influxdb_iox/issues/8570', rem;
    END IF;
END
$$ LANGUAGE plpgsql;
