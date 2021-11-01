CREATE TABLE notebooks (
  id TEXT NOT NULL PRIMARY KEY,
  org_id TEXT NOT NULL,
  name TEXT NOT NULL,
  spec TEXT NOT NULL,
  created_at TIMESTAMP,
  updated_at TIMESTAMP
);
