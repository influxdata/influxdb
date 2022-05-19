//! Interface for reconciling Ingester and catalog state

use crate::ingester::IngesterPartition;
use data_types::{
    ParquetFileWithMetadata, PartitionId, SequenceNumber, SequencerId, Tombstone, TombstoneId,
};
use std::{ops::Deref, sync::Arc};

/// Information about an ingester partition.
///
/// This is mostly the same as [`IngesterPartition`] but allows easier mocking.
pub trait IngesterPartitionInfo {
    fn partition_id(&self) -> PartitionId;
    fn sequencer_id(&self) -> SequencerId;
    fn parquet_max_sequence_number(&self) -> Option<SequenceNumber>;
    fn tombstone_max_sequence_number(&self) -> Option<SequenceNumber>;
}

impl IngesterPartitionInfo for Arc<IngesterPartition> {
    fn partition_id(&self) -> PartitionId {
        self.deref().partition_id()
    }

    fn sequencer_id(&self) -> SequencerId {
        self.deref().sequencer_id()
    }

    fn parquet_max_sequence_number(&self) -> Option<SequenceNumber> {
        self.deref().parquet_max_sequence_number()
    }

    fn tombstone_max_sequence_number(&self) -> Option<SequenceNumber> {
        self.deref().tombstone_max_sequence_number()
    }
}

/// Information about a parquet file.
///
/// This is mostly the same as [`ParquetFileWithMetadata`] but allows easier mocking.
pub trait ParquetFileInfo {
    fn partition_id(&self) -> PartitionId;
    fn min_sequence_number(&self) -> SequenceNumber;
    fn max_sequence_number(&self) -> SequenceNumber;
}

impl ParquetFileInfo for ParquetFileWithMetadata {
    fn partition_id(&self) -> PartitionId {
        self.partition_id
    }

    fn min_sequence_number(&self) -> SequenceNumber {
        self.min_sequence_number
    }

    fn max_sequence_number(&self) -> SequenceNumber {
        self.max_sequence_number
    }
}

/// Information about a tombstone.
///
/// This is mostly the same as [`Tombstone`] but allows easier mocking.
pub trait TombstoneInfo {
    fn id(&self) -> TombstoneId;
    fn sequencer_id(&self) -> SequencerId;
    fn sequence_number(&self) -> SequenceNumber;
}

impl TombstoneInfo for Tombstone {
    fn id(&self) -> TombstoneId {
        self.id
    }

    fn sequencer_id(&self) -> SequencerId {
        self.sequencer_id
    }

    fn sequence_number(&self) -> SequenceNumber {
        self.sequence_number
    }
}
