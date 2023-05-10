-- Illustrate the issue reported in <https://github.com/influxdata/influxdb_iox/issues/6066>.
-- IOX_SETUP: TwoChunksDedupWeirdnessParquet


--------------------------------------------------------------------------------
-- query everything data
SELECT * FROM "table" ORDER BY tag;

-- explain the above
-- IOX_COMPARE: uuid
EXPLAIN SELECT * FROM "table" ORDER BY tag;


--------------------------------------------------------------------------------
-- predicates on tags
SELECT * FROM "table" WHERE tag='A';

-- explain the above
-- IOX_COMPARE: uuid
EXPLAIN SELECT * FROM "table" WHERE tag='A';


--------------------------------------------------------------------------------
-- predicates on fields
SELECT * FROM "table" WHERE foo=1 AND bar=2;

-- explain the above
-- IOX_COMPARE: uuid
EXPLAIN SELECT * FROM "table" WHERE foo=1 AND bar=2;


--------------------------------------------------------------------------------
-- predicates on time
SELECT * FROM "table" WHERE time=to_timestamp('1970-01-01T00:00:00.000000000+00:00') ORDER BY tag;

-- explain the above
-- IOX_COMPARE: uuid
EXPLAIN SELECT * FROM "table" WHERE time=to_timestamp('1970-01-01T00:00:00.000000000+00:00') ORDER BY tag;


--------------------------------------------------------------------------------
-- mixed predicates
SELECT * FROM "table" WHERE tag='A' AND foo=1 AND time=to_timestamp('1970-01-01T00:00:00.000000000+00:00');

-- explain the above
-- IOX_COMPARE: uuid
EXPLAIN SELECT * FROM "table" WHERE tag='A' AND foo=1 AND time=to_timestamp('1970-01-01T00:00:00.000000000+00:00');
