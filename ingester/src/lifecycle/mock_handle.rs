//! A mock [`LifecycleHandle`] impl for testing.

use data_types::{NamespaceId, PartitionId, SequenceNumber, ShardId, TableId};

use super::LifecycleHandle;

/// Special [`LifecycleHandle`] that never persists and always accepts more data.
///
/// This is useful to control persists manually.
#[derive(Debug, Default, Clone, Copy)]
pub struct NoopLifecycleHandle;

impl LifecycleHandle for NoopLifecycleHandle {
    fn log_write(
        &self,
        _partition_id: PartitionId,
        _shard_id: ShardId,
        _namespace_id: NamespaceId,
        _table_id: TableId,
        _sequence_number: SequenceNumber,
        _bytes_written: usize,
        _rows_written: usize,
    ) -> bool {
        // do NOT pause ingest
        false
    }

    fn can_resume_ingest(&self) -> bool {
        true
    }
}
