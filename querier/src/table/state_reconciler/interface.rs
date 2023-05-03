//! Interface for reconciling Ingester and catalog state

use crate::{ingester::IngesterPartition, parquet::QuerierParquetChunk};
use data_types::{CompactionLevel, ParquetFile, PartitionId};
use std::{ops::Deref, sync::Arc};

/// Information about an ingester partition.
///
/// This is mostly the same as [`IngesterPartition`] but allows easier mocking.
pub trait IngesterPartitionInfo {
    fn partition_id(&self) -> PartitionId;
}

impl IngesterPartitionInfo for IngesterPartition {
    fn partition_id(&self) -> PartitionId {
        self.deref().partition_id()
    }
}

impl<T> IngesterPartitionInfo for Arc<T>
where
    T: IngesterPartitionInfo,
{
    fn partition_id(&self) -> PartitionId {
        self.deref().partition_id()
    }
}

/// Information about a parquet file.
///
/// This is mostly the same as [`ParquetFile`] but allows easier mocking.
pub trait ParquetFileInfo {
    fn partition_id(&self) -> PartitionId;
    fn compaction_level(&self) -> CompactionLevel;
}

impl ParquetFileInfo for Arc<ParquetFile> {
    fn partition_id(&self) -> PartitionId {
        self.partition_id
    }

    fn compaction_level(&self) -> CompactionLevel {
        self.compaction_level
    }
}

impl ParquetFileInfo for QuerierParquetChunk {
    fn partition_id(&self) -> PartitionId {
        self.meta().partition_id()
    }

    fn compaction_level(&self) -> CompactionLevel {
        self.meta().compaction_level()
    }
}
