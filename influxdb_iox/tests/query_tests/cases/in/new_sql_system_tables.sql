-- IOX_SETUP: TwoMeasurementsManyFieldsTwoChunks

-- validate we have access to information schema for listing system tables
-- IOX_COMPARE: sorted
SELECT * from information_schema.tables where table_schema = 'system';

-- IOX_COMPARE: sorted
-- Note the issue_time changes, so we can't display it directly
-- Instead check that it is reasonable and non null
SELECT issue_time <= now(), query_type, query_text, success FROM system.queries;
