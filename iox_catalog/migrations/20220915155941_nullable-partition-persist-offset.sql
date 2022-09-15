-- Make persisted_sequence_number nullable.
--
-- NULL == no persisted data for this partition
ALTER TABLE
    "partition" DROP COLUMN "persisted_sequence_number";

-- Remove implicit NULL / default of 0.
ALTER TABLE
    "partition"
ADD
    COLUMN "persisted_sequence_number" BIGINT NULL DEFAULT NULL;
