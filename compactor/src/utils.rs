//! Helpers of the Compactor

use crate::query::QueryableParquetChunk;
use arrow::record_batch::RecordBatch;
use data_types2::{ParquetFile, ParquetFileId, Tombstone, TombstoneId};
use iox_object_store::IoxObjectStore;
use object_store::ObjectStoreImpl;
use parquet_file::{
    chunk::{new_parquet_chunk, ChunkMetrics, DecodedParquetFile},
    metadata::IoxMetadata,
};
use std::{collections::HashSet, sync::Arc};

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
    pub fn level_upgradable(&self) -> bool {
        // TODO: need to wait for creation_time added
        // if time_provider.now() - self.data.creation_time > LEVEL_UPGRADE_THRESHOLD_NANO
        true
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
        object_store: Arc<ObjectStoreImpl>,
        table_name: String,
        partition_key: String,
    ) -> QueryableParquetChunk {
        let decoded_parquet_file = DecodedParquetFile::new((*self.data).clone());
        let root_path = IoxObjectStore::root_path_for(&object_store, self.data.object_store_id);
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
}

impl CompactedData {
    /// Initialize compacted data
    pub fn new(data: Vec<RecordBatch>, meta: IoxMetadata) -> Self {
        Self { data, meta }
    }
}
