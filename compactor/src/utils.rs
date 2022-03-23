//! Helpers of the Compactor

use crate::{compact::LEVEL_UPGRADE_THRESHOLD_NANO, query::QueryableParquetChunk};
use arrow::record_batch::RecordBatch;
use data_types2::{ParquetFile, ParquetFileId, ParquetFileParams, Tombstone, TombstoneId};
use iox_object_store::IoxObjectStore;
use object_store::DynObjectStore;
use parquet_file::{
    chunk::{new_parquet_chunk, ChunkMetrics, DecodedParquetFile},
    metadata::{IoxMetadata, IoxParquetMetaData},
};
use std::{collections::HashSet, sync::Arc};
use time::TimeProvider;

/// Wrapper of a group of parquet files and their tombstones that overlap in time and should be
/// considered during compaction.
pub struct GroupWithTombstones {
    /// Each file with the set of tombstones relevant to it
    pub(crate) parquet_files: Vec<ParquetFileWithTombstone>,
    /// All tombstones relevant to any of the files in the group
    pub(crate) tombstones: Vec<Tombstone>,
}

impl GroupWithTombstones {
    /// Return all tombstone ids
    pub fn tombstone_ids(&self) -> HashSet<TombstoneId> {
        self.tombstones.iter().map(|t| t.id).collect()
    }
}

/// Wrapper of a parquet file and its tombstones
#[allow(missing_docs)]
#[derive(Debug, Clone)]
pub struct ParquetFileWithTombstone {
    pub(crate) data: Arc<ParquetFile>,
    pub(crate) tombstones: Vec<Tombstone>,
}

impl ParquetFileWithTombstone {
    /// Return all tombstone ids
    pub fn tombstone_ids(&self) -> HashSet<TombstoneId> {
        self.tombstones.iter().map(|t| t.id).collect()
    }

    /// Return true if there is no tombstone
    pub fn no_tombstones(&self) -> bool {
        self.tombstones.is_empty()
    }

    /// Check if the parquet file is old enough to upgarde its level
    pub fn level_upgradable(&self, time_provider: Arc<dyn TimeProvider>) -> bool {
        if time_provider.now().timestamp_nanos() - self.data.created_at.get()
            > LEVEL_UPGRADE_THRESHOLD_NANO
        {
            return true;
        }

        false
    }

    /// Return id of this parquet file
    pub fn parquet_file_id(&self) -> ParquetFileId {
        self.data.id
    }

    /// Add more tombstones
    pub fn add_tombstones(&mut self, tombstones: Vec<Tombstone>) {
        self.tombstones.extend(tombstones);
    }

    /// Convert to a QueryableParquetChunk
    pub fn to_queryable_parquet_chunk(
        &self,
        object_store: Arc<DynObjectStore>,
        table_name: String,
        partition_key: String,
    ) -> QueryableParquetChunk {
        let decoded_parquet_file = DecodedParquetFile::new((*self.data).clone());
        let root_path = IoxObjectStore::root_path_for(&*object_store, self.data.object_store_id);
        let iox_object_store = IoxObjectStore::existing(object_store, root_path);
        let parquet_chunk = new_parquet_chunk(
            &decoded_parquet_file,
            Arc::from(table_name.clone()),
            Arc::from(partition_key),
            ChunkMetrics::new_unregistered(), // TODO: need to add metrics
            Arc::new(iox_object_store),
        );

        QueryableParquetChunk::new(
            table_name,
            Arc::new(parquet_chunk),
            Arc::new(decoded_parquet_file.iox_metadata),
            &self.tombstones,
        )
    }

    /// Return iox metadata of the parquet file
    pub fn iox_metadata(&self) -> IoxMetadata {
        let decoded_parquet_file = DecodedParquetFile::new((*self.data).clone());
        decoded_parquet_file.iox_metadata
    }
}

/// Struct holding output of a compacted stream
pub struct CompactedData {
    pub(crate) data: Vec<RecordBatch>,
    pub(crate) meta: IoxMetadata,
    pub(crate) tombstone_ids: HashSet<TombstoneId>,
}

impl CompactedData {
    /// Initialize compacted data
    pub fn new(
        data: Vec<RecordBatch>,
        meta: IoxMetadata,
        tombstone_ids: HashSet<TombstoneId>,
    ) -> Self {
        Self {
            data,
            meta,
            tombstone_ids,
        }
    }
}

/// Information needed to update the catalog after compacting a group of files
#[derive(Debug)]
pub struct CatalogUpdate {
    pub(crate) meta: IoxMetadata,
    pub(crate) tombstone_ids: HashSet<TombstoneId>,
    pub(crate) parquet_file: ParquetFileParams,
}

impl CatalogUpdate {
    /// Initialize with data received from a persist to object storage
    pub fn new(
        meta: IoxMetadata,
        file_size: usize,
        md: IoxParquetMetaData,
        tombstone_ids: HashSet<TombstoneId>,
    ) -> Self {
        let parquet_file = meta.to_parquet_file(file_size, &md);
        Self {
            meta,
            tombstone_ids,
            parquet_file,
        }
    }
}
