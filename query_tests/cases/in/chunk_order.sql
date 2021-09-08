-- Test that deduplication respects chunk ordering
-- IOX_SETUP: ChunkOrder

-- query data
SELECT * from cpu order by time;
