use crate::influxdata::iox::compactor::v1 as proto;
use data_types::SkippedCompaction;

impl From<SkippedCompaction> for proto::SkippedCompaction {
    fn from(skipped_compaction: SkippedCompaction) -> Self {
        let SkippedCompaction {
            partition_id,
            reason,
            skipped_at,
            estimated_bytes,
            limit_bytes,
            num_files,
            limit_num_files,
            limit_num_files_first_in_partition,
        } = skipped_compaction;

        Self {
            partition_id: partition_id.get(),
            reason,
            skipped_at: skipped_at.get(),
            estimated_bytes,
            limit_bytes,
            num_files,
            limit_num_files,
            limit_num_files_first_in_partition: Some(limit_num_files_first_in_partition),
        }
    }
}
