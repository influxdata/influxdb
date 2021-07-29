-- Demonstrate plans that are optimized using statistics
-- IOX_SETUP: TwoMeasurementsManyFieldsOneChunk

-- This plan should not scan data
EXPLAIN SELECT count(*) from h2o;

-- However, this plan will still need to scan data given the predicate
EXPLAIN SELECT count(*) from h2o where temp > 70.0 and temp < 72.0;
