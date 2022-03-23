-- Demonstrate plans that are not optimized using statistics
-- IOX_SETUP: OldTwoMeasurementsManyFieldsOneRubChunk

-- This plan should scan data as it reads from chunk with delete predicates
EXPLAIN SELECT count(*) from h2o;

-- NOTE: This test should have "IOX_SETUP: ThreeDeleteThreeChunks" but becasue of  Bug: https://github.com/influxdata/influxdb_iox/issues/2745
-- make it OldTwoMeasurementsManyFieldsOneRubChunk
-- Also, the query should be "EXPLAIN SELECT count(*) from cpu;"

