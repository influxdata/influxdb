ALTER TABLE test_table_1 RENAME TO _test_table_1_old;

CREATE TABLE test_table_1 (
  org_id TEXT NOT NULL PRIMARY KEY,
  created_at TIMESTAMP,
  updated_at TIMESTAMP
);

INSERT INTO test_table_1 (org_id, updated_at, created_at)
	SELECT id, updated_at, created_at
	FROM _test_table_1_old;

DROP TABLE _test_table_1_old;
