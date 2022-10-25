-- Adds the "NOT NULL" to remote_org_id
ALTER TABLE remotes RENAME TO _remotes_old;

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

INSERT INTO remotes (
    id,
    org_id,
    name,
    description,
    remote_url,
    remote_api_token,
    remote_org_id,
    allow_insecure_tls,
    created_at,
    updated_at
) SELECT * FROM _remotes_old;
DROP TABLE _remotes_old;

-- Create indexes on lookup patterns we expect to be common
CREATE INDEX idx_remote_url_per_org ON remotes (org_id, remote_url);