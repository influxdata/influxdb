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
) SELECT * FROM _remotes_old WHERE remote_org_id IS NOT NULL;

-- Edit the replications table as the remotes table key has changed
ALTER TABLE replications RENAME TO _replications_old;

CREATE TABLE replications
(
    id                       VARCHAR(16) NOT NULL PRIMARY KEY,
    org_id                   VARCHAR(16) NOT NULL,
    name                     TEXT        NOT NULL,
    description              TEXT,
    remote_id                VARCHAR(16) NOT NULL,
    local_bucket_id          VARCHAR(16) NOT NULL,
    remote_bucket_id         VARCHAR(16),
    remote_bucket_name       TEXT DEFAULT '',
    max_queue_size_bytes     INTEGER     NOT NULL,
    max_age_seconds          INTEGER     NOT NULL,
    latest_response_code     INTEGER,
    latest_error_message     TEXT,
    drop_non_retryable_data  BOOLEAN     NOT NULL,
    created_at               TIMESTAMP   NOT NULL,
    updated_at               TIMESTAMP   NOT NULL,

    CONSTRAINT replications_uniq_orgid_name UNIQUE (org_id, name),
    CONSTRAINT replications_one_of_id_name CHECK (remote_bucket_id IS NOT NULL OR remote_bucket_name != '')
 );

INSERT INTO replications (
    id,
    org_id,
    name,
    description,
    remote_id,
    local_bucket_id,
    remote_bucket_id,
    remote_bucket_name,
    max_queue_size_bytes,
    max_age_seconds,
    latest_response_code,
    latest_error_message,
    drop_non_retryable_data,
    created_at,
    updated_at
) SELECT * FROM _replications_old;
DROP TABLE _replications_old;
DROP TABLE _remotes_old;

-- The DROP _remotes has to be at the end due to the FK from replications remote_id to remotes id.
-- The replications table will follow the ALTER TABLE and FK to _remotes until we
-- reinsert. By putting the DROP after all the data is re-entered, it will stay consistent throughout the process.

-- Create indexes on lookup patterns we expect to be common
CREATE INDEX idx_local_bucket_id_per_org ON replications (org_id, local_bucket_id);
-- Create indexes on lookup patterns we expect to be common
CREATE INDEX idx_remote_url_per_org ON remotes (org_id, remote_url);
