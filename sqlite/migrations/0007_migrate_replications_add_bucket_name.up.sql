-- Removes the "NOT NULL" from `remote_bucket_id` and adds `remote_bucket_name`.
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
    CONSTRAINT replications_one_of_id_name CHECK (remote_bucket_id IS NOT NULL OR remote_bucket_name != ''),
    FOREIGN KEY (remote_id) REFERENCES remotes (id)
);

INSERT INTO replications (
    id,
    org_id,
    name,
    description,
    remote_id,
    local_bucket_id,
    remote_bucket_id,
    max_queue_size_bytes,
    max_age_seconds,
    latest_response_code,
    latest_error_message,
    drop_non_retryable_data,
    created_at,updated_at
) SELECT * FROM _replications_old;
DROP TABLE _replications_old;

-- Create indexes on lookup patterns we expect to be common
CREATE INDEX idx_local_bucket_id_per_org ON replications (org_id, local_bucket_id);
CREATE INDEX idx_remote_id_per_org ON replications (org_id, remote_id);