//! The per-partition data nested in a query [`QueryResponse`].
//!
//! [`QueryResponse`]: super::response::QueryResponse

use data_types::{PartitionId, SequenceNumber};
use datafusion::physical_plan::SendableRecordBatchStream;

/// Response data for a single partition.
pub(crate) struct PartitionResponse {
    /// Stream of snapshots.
    batches: SendableRecordBatchStream,

    /// Partition ID.
    id: PartitionId,

    /// Max sequence number persisted
    max_persisted_sequence_number: Option<SequenceNumber>,

    /// Count of persisted Parquet files for this partition by this ingester instance.
    completed_persistence_count: u64,
}

impl std::fmt::Debug for PartitionResponse {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PartitionResponse")
            .field("batches", &"<SNAPSHOT STREAM>")
            .field("partition_id", &self.id)
            .field("max_persisted", &self.max_persisted_sequence_number)
            .field(
                "completed_persistence_count",
                &self.completed_persistence_count,
            )
            .finish()
    }
}

impl PartitionResponse {
    pub(crate) fn new(
        batches: SendableRecordBatchStream,
        id: PartitionId,
        max_persisted_sequence_number: Option<SequenceNumber>,
        completed_persistence_count: u64,
    ) -> Self {
        Self {
            batches,
            id,
            max_persisted_sequence_number,
            completed_persistence_count,
        }
    }

    pub(crate) fn id(&self) -> PartitionId {
        self.id
    }

    pub(crate) fn max_persisted_sequence_number(&self) -> Option<SequenceNumber> {
        self.max_persisted_sequence_number
    }

    pub(crate) fn completed_persistence_count(&self) -> u64 {
        self.completed_persistence_count
    }

    pub(crate) fn into_record_batch_stream(self) -> SendableRecordBatchStream {
        self.batches
    }
}
