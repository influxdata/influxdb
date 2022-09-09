CREATE TABLE IF NOT EXISTS skipped_compactions (
    partition_id BIGINT REFERENCES PARTITION (id) ON DELETE CASCADE,
    reason TEXT NOT NULL,
    skipped_at BIGINT NOT NULL,
    PRIMARY KEY (partition_id)
);
