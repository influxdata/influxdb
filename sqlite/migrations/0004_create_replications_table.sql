-- The user_version should match the "000X" from the file name
-- Ex: 0001_create_notebooks_table should have a user_verison of 1
PRAGMA user_version=4;

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
    current_queue_size_bytes INTEGER     NOT NULL,
    latest_response_code     INTEGER,
    latest_error_message     TEXT,

    CONSTRAINT replications_uniq_orgid_name UNIQUE (org_id, name),
    FOREIGN KEY (remote_id) REFERENCES remotes(id) ON DELETE CASCADE
);

-- Create indexes on lookup patterns we expect to be common
CREATE INDEX idx_local_bucket_id_per_org ON replications (org_id, local_bucket_id);
CREATE INDEX idx_remote_id_per_org ON replications (org_id, remote_id);
