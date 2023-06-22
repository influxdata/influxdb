//! The per-partition data nested in a query [`QueryResponse`].
//!
//! [`QueryResponse`]: super::response::QueryResponse

use arrow::record_batch::RecordBatch;
use data_types::{PartitionHashId, PartitionId};

/// Response data for a single partition.
#[derive(Debug)]
pub(crate) struct PartitionResponse {
    /// Stream of snapshots.
    batches: Vec<RecordBatch>,

    /// Partition ID.
    id: PartitionId,

    /// Partition hash ID, if stored in the database.
    partition_hash_id: Option<PartitionHashId>,

    /// Count of persisted Parquet files for this partition by this ingester instance.
    completed_persistence_count: u64,
}

impl PartitionResponse {
    pub(crate) fn new(
        data: Vec<RecordBatch>,
        id: PartitionId,
        partition_hash_id: Option<PartitionHashId>,
        completed_persistence_count: u64,
    ) -> Self {
        Self {
            batches: data,
            id,
            partition_hash_id,
            completed_persistence_count,
        }
    }

    pub(crate) fn id(&self) -> PartitionId {
        self.id
    }

    pub(crate) fn partition_hash_id(&self) -> Option<&PartitionHashId> {
        self.partition_hash_id.as_ref()
    }

    pub(crate) fn completed_persistence_count(&self) -> u64 {
        self.completed_persistence_count
    }

    pub(crate) fn into_record_batches(self) -> Vec<RecordBatch> {
        self.batches
    }
}
