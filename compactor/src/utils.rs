//! Helpers of the Compactor

use crate::query::QueryableParquetChunk;
use arrow::record_batch::RecordBatch;
use data_types::{
    ParquetFileId, ParquetFileParams, ParquetFileWithMetadata, Timestamp, Tombstone, TombstoneId,
};
use object_store::DynObjectStore;
use observability_deps::tracing::*;
use parquet_file::{
    chunk::{new_parquet_chunk, ChunkMetrics, DecodedParquetFile},
    metadata::{IoxMetadata, IoxParquetMetaData},
};
use schema::sort::SortKey;
use std::{
    collections::{BTreeMap, HashSet},
    sync::Arc,
};

/// Wrapper of a group of parquet files and their tombstones that overlap in time and should be
/// considered during compaction.
#[derive(Debug)]
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

/// Wrapper of group of parquet files with their min time and total size
#[derive(Debug, Clone, PartialEq)]
pub struct GroupWithMinTimeAndSize {
    /// Parquet files and their metadata
    pub(crate) parquet_files: Vec<ParquetFileWithMetadata>,

    /// min time of all parquet_files
    pub(crate) min_time: Timestamp,

    /// total size of all file
    pub(crate) total_file_size_bytes: i64,
}

/// Wrapper of a parquet file and its tombstones
#[allow(missing_docs)]
#[derive(Debug, Clone)]
pub struct ParquetFileWithTombstone {
    pub(crate) data: Arc<ParquetFileWithMetadata>,
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

    /// Return id of this parquet file
    pub fn parquet_file_id(&self) -> ParquetFileId {
        self.data.id
    }

    /// Return all tombstones in btree map format
    pub fn tombstones(&self) -> BTreeMap<TombstoneId, Tombstone> {
        self.tombstones
            .iter()
            .map(|ts| (ts.id, ts.clone()))
            .collect()
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
        sort_key: Option<SortKey>,
        partition_sort_key: Option<SortKey>,
    ) -> QueryableParquetChunk {
        let decoded_parquet_file = DecodedParquetFile::new((*self.data).clone());

        let parquet_chunk = new_parquet_chunk(
            &decoded_parquet_file,
            ChunkMetrics::new_unregistered(), // TODO: need to add metrics
            object_store,
        );

        debug!(
            parquet_file_id=?decoded_parquet_file.parquet_file.id,
            parquet_file_sequencer_id=?decoded_parquet_file.parquet_file.sequencer_id,
            parquet_file_namespace_id=?decoded_parquet_file.parquet_file.namespace_id,
            parquet_file_table_id=?decoded_parquet_file.parquet_file.table_id,
            parquet_file_partition_id=?decoded_parquet_file.parquet_file.partition_id,
            parquet_file_object_store_id=?decoded_parquet_file.parquet_file.object_store_id,
            "built parquet chunk from metadata"
        );

        QueryableParquetChunk::new(
            table_name,
            self.data.partition_id,
            Arc::new(parquet_chunk),
            &self.tombstones,
            self.data.min_sequence_number,
            self.data.max_sequence_number,
            self.data.min_time,
            self.data.max_time,
            sort_key,
            partition_sort_key,
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
    pub(crate) tombstones: BTreeMap<TombstoneId, Tombstone>,
}

impl CompactedData {
    /// Initialize compacted data
    pub fn new(
        data: Vec<RecordBatch>,
        meta: IoxMetadata,
        tombstones: BTreeMap<TombstoneId, Tombstone>,
    ) -> Self {
        Self {
            data,
            meta,
            tombstones,
        }
    }
}

/// Information needed to update the catalog after compacting a group of files
#[derive(Debug)]
pub struct CatalogUpdate {
    pub(crate) meta: IoxMetadata,
    pub(crate) tombstones: BTreeMap<TombstoneId, Tombstone>,
    pub(crate) parquet_file: ParquetFileParams,
}

impl CatalogUpdate {
    /// Initialize with data received from a persist to object storage
    pub fn new(
        meta: IoxMetadata,
        file_size: usize,
        md: IoxParquetMetaData,
        tombstones: BTreeMap<TombstoneId, Tombstone>,
    ) -> Self {
        let parquet_file = meta.to_parquet_file(file_size, &md);
        Self {
            meta,
            tombstones,
            parquet_file,
        }
    }
}
