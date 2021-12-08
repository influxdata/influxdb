-- IOX_SETUP: OneMeasurementAllChunksDropped

-- list information schema (show that all the chunks were dropped)
SELECT * from information_schema.tables where table_schema = 'iox';
