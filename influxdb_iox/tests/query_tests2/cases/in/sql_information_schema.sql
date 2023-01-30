-- IOX_SETUP: TwoMeasurementsManyFields

-- validate we have access to information schema for listing table names (w/o system tables)
-- IOX_COMPARE: sorted
SELECT * from information_schema.tables where table_schema != 'system';

-- validate we have access to information schema for listing columns names/types
-- IOX_COMPARE: sorted
SELECT * from information_schema.columns where table_name = 'h2o' OR table_name = 'o2';

-- validate we have access to SHOW TABLE for listing columns names
-- IOX_COMPARE: sorted
SHOW TABLES;

-- validate we have access to SHOW COLUMNs for listing columns names
-- IOX_COMPARE: sorted
SHOW COLUMNS FROM h2o;
