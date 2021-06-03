-- The user_version should match the "000X" from the file name
-- Ex: 0001_create_notebooks_table should have a user_verison of 1
PRAGMA user_version=1;

-- Create the initial table to store notebooks
CREATE TABLE notebooks (
  id TEXT NOT NULL PRIMARY KEY,
  org_id TEXT NOT NULL,
  name TEXT NOT NULL,
  spec TEXT NOT NULL,
  created_at TIMESTAMP,
  updated_at TIMESTAMP
);
