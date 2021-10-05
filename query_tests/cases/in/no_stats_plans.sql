-- Demonstrate plans that are not optimized using statistics
-- IOX_SETUP: ThreeDeleteThreeChunks

-- This plan should scan data as it reads from chunk with delete predicates
EXPLAIN SELECT count(*) from cpu;
