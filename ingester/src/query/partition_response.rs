//! The per-partition data nested in a query [`QueryResponse`].
//!
//! [`QueryResponse`]: super::response::QueryResponse

use arrow::record_batch::RecordBatch;
use data_types::TransitionPartitionId;

/// Response data for a single partition.
#[derive(Debug)]
pub(crate) struct PartitionResponse {
    /// Stream of snapshots.
    batches: Vec<RecordBatch>,

    /// Partition ID.
    id: TransitionPartitionId,

    /// Count of persisted Parquet files for this partition by this ingester instance.
    completed_persistence_count: u64,
}

impl PartitionResponse {
    pub(crate) fn new(
        data: Vec<RecordBatch>,
        id: TransitionPartitionId,
        completed_persistence_count: u64,
    ) -> Self {
        Self {
            batches: data,
            id,
            completed_persistence_count,
        }
    }

    pub(crate) fn id(&self) -> &TransitionPartitionId {
        &self.id
    }

    pub(crate) fn completed_persistence_count(&self) -> u64 {
        self.completed_persistence_count
    }

    pub(crate) fn into_record_batches(self) -> Vec<RecordBatch> {
        self.batches
    }
}
