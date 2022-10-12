//! A mock [`LifecycleHandle`] impl for testing.

use std::sync::Arc;

use data_types::{NamespaceId, PartitionId, SequenceNumber, ShardId, TableId};
use parking_lot::Mutex;

use super::LifecycleHandle;

/// A set of arguments captured from a call to
/// [`MockLifecycleHandle::log_write()`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[allow(missing_docs)]
pub struct MockLifecycleCall {
    pub partition_id: PartitionId,
    pub shard_id: ShardId,
    pub namespace_id: NamespaceId,
    pub table_id: TableId,
    pub sequence_number: SequenceNumber,
    pub bytes_written: usize,
    pub rows_written: usize,
}

/// A mock [`LifecycleHandle`] implementation that records calls made to
/// [`Self::log_write()`] and never blocks ingest, always accepting more data.
///
/// # Cloning
///
/// Cloning a [`MockLifecycleHandle`] will clone the inner state - calls to all
/// cloned instances are reported in a call to [`Self::get_log_calls()`].
#[derive(Debug, Default, Clone)]
pub struct MockLifecycleHandle {
    log_calls: Arc<Mutex<Vec<MockLifecycleCall>>>,
}

impl MockLifecycleHandle {
    /// Returns the ordered [`Self::log_write()`] calls made to this mock.
    pub fn get_log_calls(&self) -> Vec<MockLifecycleCall> {
        self.log_calls.lock().clone()
    }
}

impl LifecycleHandle for MockLifecycleHandle {
    fn log_write(
        &self,
        partition_id: PartitionId,
        shard_id: ShardId,
        namespace_id: NamespaceId,
        table_id: TableId,
        sequence_number: SequenceNumber,
        bytes_written: usize,
        rows_written: usize,
    ) -> bool {
        self.log_calls.lock().push(MockLifecycleCall {
            partition_id,
            shard_id,
            namespace_id,
            table_id,
            sequence_number,
            bytes_written,
            rows_written,
        });

        // do NOT pause ingest
        false
    }

    fn can_resume_ingest(&self) -> bool {
        true
    }
}
