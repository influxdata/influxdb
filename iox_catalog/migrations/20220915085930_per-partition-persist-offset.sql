/*
 Add a per-partition persistence watermark (inclusive).

 https://github.com/influxdata/influxdb_iox/issues/5638
 */
ALTER TABLE
    "partition"
ADD
    COLUMN "persisted_sequence_number" BIGINT NOT NULL DEFAULT 0;
