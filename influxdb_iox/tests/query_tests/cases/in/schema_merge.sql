-- IOX_SETUP: MultiChunkSchemaMerge

-- IOX_COMPARE: sorted
SELECT * from cpu;

-- Merge a subset
-- IOX_COMPARE: sorted
SELECT host, region, system from cpu;