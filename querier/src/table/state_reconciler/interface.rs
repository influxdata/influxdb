//! Interface for reconciling Ingester and catalog state

use crate::ingester::IngesterPartition;
use data_types::{PartitionId, SequenceNumber, SequencerId, Tombstone, TombstoneId};
use parquet_file::chunk::DecodedParquetFile;
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

impl IngesterPartitionInfo for IngesterPartition {
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

impl<T> IngesterPartitionInfo for Arc<T>
where
    T: IngesterPartitionInfo,
{
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
/// This is mostly the same as [`DecodedParquetFile`] but allows easier mocking.
pub trait ParquetFileInfo {
    fn partition_id(&self) -> PartitionId;
    fn min_sequence_number(&self) -> SequenceNumber;
    fn max_sequence_number(&self) -> SequenceNumber;
}

impl ParquetFileInfo for Arc<DecodedParquetFile> {
    fn partition_id(&self) -> PartitionId {
        self.parquet_file.partition_id
    }

    fn min_sequence_number(&self) -> SequenceNumber {
        self.parquet_file.min_sequence_number
    }

    fn max_sequence_number(&self) -> SequenceNumber {
        self.parquet_file.max_sequence_number
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

impl TombstoneInfo for Arc<Tombstone> {
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
