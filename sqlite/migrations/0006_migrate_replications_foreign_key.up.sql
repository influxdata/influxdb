PRAGMA foreign_keys=off;

-- Removes the "ON DELETE CASCADE" from the foreign key constraint
ALTER TABLE replications RENAME TO _replications_old;

CREATE TABLE replications
(
    id                       VARCHAR(16) NOT NULL PRIMARY KEY,
    org_id                   VARCHAR(16) NOT NULL,
    name                     TEXT        NOT NULL,
    description              TEXT,
    remote_id                VARCHAR(16) NOT NULL,
    local_bucket_id          VARCHAR(16) NOT NULL,
    remote_bucket_id         VARCHAR(16) NOT NULL,
    max_queue_size_bytes     INTEGER     NOT NULL,
    max_age_seconds          INTEGER     NOT NULL,
    latest_response_code     INTEGER,
    latest_error_message     TEXT,
    drop_non_retryable_data  BOOLEAN     NOT NULL,
    created_at               TIMESTAMP   NOT NULL,
    updated_at               TIMESTAMP   NOT NULL,

    CONSTRAINT replications_uniq_orgid_name UNIQUE (org_id, name),
    FOREIGN KEY (remote_id) REFERENCES remotes (id)
);

INSERT INTO replications SELECT * FROM _replications_old;
DROP TABLE _replications_old;

-- Create indexes on lookup patterns we expect to be common
CREATE INDEX idx_local_bucket_id_per_org ON replications (org_id, local_bucket_id);
CREATE INDEX idx_remote_id_per_org ON replications (org_id, remote_id);

PRAGMA foreign_keys=on;
