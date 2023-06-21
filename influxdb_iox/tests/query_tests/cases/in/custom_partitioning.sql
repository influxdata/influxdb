-- Tests w/ custom partitioning
-- IOX_SETUP: CustomPartitioning

----------------------------------------
-- table1
----------------------------------------
-- Partition template source: implicit when data was written
-- Partition template: tag1|tag2
-- #Partitions: 4

-- all: all 4 parquet files
-- IOX_COMPARE: sorted
SELECT * FROM "table1";
-- IOX_COMPARE: uuid
EXPLAIN SELECT * FROM "table1";

-- select based on tag1: 2 parquet files, others are pruned using the decoded partition key
-- IOX_COMPARE: sorted
SELECT * FROM "table1" WHERE tag1 = 'v1a';
-- IOX_COMPARE: uuid
EXPLAIN SELECT * FROM "table1" WHERE tag1 = 'v1a';

-- select based on tag2: not part of the partition key, so all parquet files scanned
-- IOX_COMPARE: sorted
SELECT * FROM "table1" WHERE tag2 = 'v2a';
-- IOX_COMPARE: uuid
EXPLAIN SELECT * FROM "table1" WHERE tag2 = 'v2a';

-- select based on tag3: 2 parquet files, others are pruned using the decoded partition key
-- IOX_COMPARE: sorted
SELECT * FROM "table1" WHERE tag3 = 'v3a';
-- IOX_COMPARE: uuid
EXPLAIN SELECT * FROM "table1" WHERE tag3 = 'v3a';

-- select single partition
-- IOX_COMPARE: sorted
SELECT * FROM "table1" WHERE tag1 = 'v1a' AND tag3 = 'v3a';
-- IOX_COMPARE: uuid
EXPLAIN SELECT * FROM "table1" WHERE tag1 = 'v1a' AND tag3 = 'v3a';


----------------------------------------
-- table2
----------------------------------------
-- Partition template source: implicit when table was created via API (no override)
-- Partition template: tag1|tag2
-- #Partitions: 4

-- all: all 4 parquet files
-- IOX_COMPARE: sorted
SELECT * FROM "table2";
-- IOX_COMPARE: uuid
EXPLAIN SELECT * FROM "table2";

-- select based on tag1: 2 parquet files, others are pruned using the decoded partition key
-- IOX_COMPARE: sorted
SELECT * FROM "table2" WHERE tag1 = 'v1a';
-- IOX_COMPARE: uuid
EXPLAIN SELECT * FROM "table2" WHERE tag1 = 'v1a';

-- select based on tag2: not part of the partition key, so all parquet files scanned
-- IOX_COMPARE: sorted
SELECT * FROM "table2" WHERE tag2 = 'v2a';
-- IOX_COMPARE: uuid
EXPLAIN SELECT * FROM "table2" WHERE tag2 = 'v2a';

-- select based on tag3: 2 parquet files, others are pruned using the decoded partition key
-- IOX_COMPARE: sorted
SELECT * FROM "table2" WHERE tag3 = 'v3a';
-- IOX_COMPARE: uuid
EXPLAIN SELECT * FROM "table2" WHERE tag3 = 'v3a';

-- select single partition
-- IOX_COMPARE: sorted
SELECT * FROM "table2" WHERE tag1 = 'v1a' AND tag3 = 'v3a';
-- IOX_COMPARE: uuid
EXPLAIN SELECT * FROM "table2" WHERE tag1 = 'v1a' AND tag3 = 'v3a';


----------------------------------------
-- table3
----------------------------------------
-- Partition template source: explicit override when table was created via API
-- Partition template: tag2
-- #Partitions: 2

-- all: all 2 parquet files
-- IOX_COMPARE: sorted
SELECT * FROM "table3";
-- IOX_COMPARE: uuid
EXPLAIN SELECT * FROM "table3";

-- select based on tag1: not part of the partition key, so all parquet files scanned
-- IOX_COMPARE: sorted
SELECT * FROM "table3" WHERE tag1 = 'v1a';
-- IOX_COMPARE: uuid
EXPLAIN SELECT * FROM "table3" WHERE tag1 = 'v1a';

-- select based on tag2: 1 parquet files, others are pruned using the decoded partition key
-- IOX_COMPARE: sorted
SELECT * FROM "table3" WHERE tag2 = 'v2a';
-- IOX_COMPARE: uuid
EXPLAIN SELECT * FROM "table3" WHERE tag2 = 'v2a';

-- select based on tag3: not part of the partition key, so all parquet files scanned
-- IOX_COMPARE: sorted
SELECT * FROM "table3" WHERE tag3 = 'v3a';
-- IOX_COMPARE: uuid
EXPLAIN SELECT * FROM "table3" WHERE tag3 = 'v3a';
