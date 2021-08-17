-- The user_version should match the "000X" from the file name
-- Ex: 0001_create_notebooks_table should have a user_verison of 1
PRAGMA user_version=3;

CREATE TABLE remotes (
    id VARCHAR(16) NOT NULL PRIMARY KEY,
    org_id VARCHAR(16) NOT NULL,
    name TEXT NOT NULL,
    description TEXT,
    remote_url TEXT NOT NULL,
    remote_api_token TEXT NOT NULL,
    remote_org_id VARCHAR(16) NOT NULL,
    allow_insecure_tls BOOLEAN NOT NULL,
    created_at TIMESTAMP NOT NULL,
    updated_at TIMESTAMP NOT NULL,

    CONSTRAINT remotes_uniq_orgid_name UNIQUE (org_id, name)
);

-- Create indexes on lookup patterns we expect to be common
CREATE INDEX idx_remote_url_per_org ON remotes (org_id, remote_url);
