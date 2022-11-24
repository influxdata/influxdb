//! The per-partition data nested in a query [`Response`].
//!
//! [`Response`]: super::response::Response

use std::pin::Pin;

use arrow::error::ArrowError;
use data_types::{PartitionId, SequenceNumber};
use datafusion::physical_plan::SendableRecordBatchStream;
use futures::Stream;

/// Stream of [`RecordBatch`].
///
/// [`RecordBatch`]: arrow::record_batch::RecordBatch
pub(crate) type RecordBatchStream =
    Pin<Box<dyn Stream<Item = Result<SendableRecordBatchStream, ArrowError>> + Send>>;

/// Response data for a single partition.
pub(crate) struct PartitionResponse {
    /// Stream of snapshots.
    batches: RecordBatchStream,

    /// Partition ID.
    id: PartitionId,

    /// Max sequence number persisted
    max_persisted_sequence_number: Option<SequenceNumber>,
}

impl std::fmt::Debug for PartitionResponse {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PartitionResponse")
            .field("batches", &"<SNAPSHOT STREAM>")
            .field("partition_id", &self.id)
            .field("max_persisted", &self.max_persisted_sequence_number)
            .finish()
    }
}

impl PartitionResponse {
    pub(crate) fn new(
        batches: RecordBatchStream,
        id: PartitionId,
        max_persisted_sequence_number: Option<SequenceNumber>,
    ) -> Self {
        Self {
            batches,
            id,
            max_persisted_sequence_number,
        }
    }

    pub(crate) fn id(&self) -> PartitionId {
        self.id
    }

    pub(crate) fn max_persisted_sequence_number(&self) -> Option<SequenceNumber> {
        self.max_persisted_sequence_number
    }

    pub(crate) fn into_record_batch_stream(self) -> RecordBatchStream {
        self.batches
    }
}
