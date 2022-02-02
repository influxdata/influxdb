-- IOX_SETUP: TwoMeasurementsManyFields

-- validate we have access to information schema for listing table names
-- IOX_COMPARE: sorted
SELECT * from information_schema.tables;

-- validate we have access to information schema for listing columns names/types
-- IOX_COMPARE: sorted
SELECT * from information_schema.columns where table_name = 'h2o' OR table_name = 'o2';

-- validate we have access to SHOW SCHEMA for listing columns names
-- IOX_COMPARE: sorted
SHOW COLUMNS FROM h2o;
