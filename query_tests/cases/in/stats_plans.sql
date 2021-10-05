-- Demonstrate plans that are optimized using statistics
-- IOX_SETUP: TwoMeasurementsManyFieldsOneRubChunk

-- This plan should not scan data as it reads from a RUB chunk (no duplicate) and no soft deleted data
EXPLAIN SELECT count(*) from h2o;

-- However, this plan will still need to scan data given the predicate
EXPLAIN SELECT count(*) from h2o where temp > 70.0 and temp < 72.0;

