-- IOX_SETUP: TwoMeasurementsManyFieldsTwoChunks

-- validate we have access to information schema for listing system tables
-- IOX_COMPARE: sorted
SELECT * from information_schema.tables where table_schema = 'system';

-- IOX_COMPARE: sorted
SELECT issue_time, query_type, query_text, success FROM system.queries;
