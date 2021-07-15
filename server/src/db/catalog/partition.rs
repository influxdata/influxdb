//! The catalog representation of a Partition

use super::chunk::{CatalogChunk, ChunkStage};
use crate::db::catalog::metrics::PartitionMetrics;
use chrono::{DateTime, Utc};
use data_types::{
    chunk_metadata::{ChunkAddr, ChunkLifecycleAction, ChunkSummary},
    partition_metadata::{PartitionAddr, PartitionSummary},
};
use internal_types::schema::Schema;
use observability_deps::tracing::info;
use persistence_windows::persistence_windows::PersistenceWindows;
use snafu::Snafu;
use std::{
    collections::{btree_map::Entry, BTreeMap},
    fmt::Display,
    sync::Arc,
};
use tracker::RwLock;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("chunk not found: {}", chunk))]
    ChunkNotFound { chunk: ChunkAddr },

    #[snafu(display(
        "cannot drop chunk {} with in-progress lifecycle action: {}",
        chunk,
        action
    ))]
    LifecycleInProgress {
        chunk: ChunkAddr,
        action: ChunkLifecycleAction,
    },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

/// IOx Catalog Partition
///
/// A partition contains multiple Chunks for a given table
#[derive(Debug)]
pub struct Partition {
    addr: PartitionAddr,

    /// The chunks that make up this partition, indexed by id
    chunks: BTreeMap<u32, Arc<RwLock<CatalogChunk>>>,

    /// When this partition was created
    created_at: DateTime<Utc>,

    /// the last time at which write was made to this
    /// partition. Partition::new initializes this to now.
    last_write_at: DateTime<Utc>,

    /// What the next chunk id is
    next_chunk_id: u32,

    /// Partition metrics
    metrics: PartitionMetrics,

    /// Ingest tracking for persisting data from memory to Parquet
    persistence_windows: Option<PersistenceWindows>,
}

impl Partition {
    /// Create a new partition catalog object.
    ///
    /// This function is not pub because `Partition`s should be created using the interfaces on
    /// [`Catalog`](crate::db::catalog::Catalog) and not instantiated directly.
    pub(super) fn new(addr: PartitionAddr, metrics: PartitionMetrics) -> Self {
        let now = Utc::now();
        Self {
            addr,
            chunks: Default::default(),
            created_at: now,
            last_write_at: now,
            next_chunk_id: 0,
            metrics,
            persistence_windows: None,
        }
    }

    /// Return the address of this Partition
    pub fn addr(&self) -> &PartitionAddr {
        &self.addr
    }

    /// Return the db name of this Partition
    pub fn db_name(&self) -> &str {
        &self.addr.db_name
    }

    /// Return the partition_key of this Partition
    pub fn key(&self) -> &str {
        &self.addr.partition_key
    }

    /// Return the table name of this partition
    pub fn table_name(&self) -> &str {
        &self.addr.table_name
    }

    /// Update the last write time to now
    pub fn update_last_write_at(&mut self) {
        self.last_write_at = Utc::now();
    }

    /// Return the time at which this partition was created
    pub fn created_at(&self) -> DateTime<Utc> {
        self.created_at
    }

    /// Return the time at which the last write was written to this partititon
    pub fn last_write_at(&self) -> DateTime<Utc> {
        self.last_write_at
    }

    /// Create a new Chunk in the open state.
    ///
    /// This will add a new chunk to the catalog and increases the chunk ID counter for that
    /// table-partition combination.
    ///
    /// Returns an error if the chunk is empty.
    pub fn create_open_chunk(
        &mut self,
        chunk: mutable_buffer::chunk::MBChunk,
    ) -> Arc<RwLock<CatalogChunk>> {
        assert_eq!(chunk.table_name().as_ref(), self.table_name());

        let chunk_id = self.next_chunk_id;
        assert_ne!(self.next_chunk_id, u32::MAX, "Chunk ID Overflow");
        self.next_chunk_id += 1;

        let addr = ChunkAddr::new(&self.addr, chunk_id);

        let chunk = CatalogChunk::new_open(addr, chunk, self.metrics.new_chunk_metrics());
        let chunk = Arc::new(self.metrics.new_chunk_lock(chunk));

        if self.chunks.insert(chunk_id, Arc::clone(&chunk)).is_some() {
            // A fundamental invariant has been violated - abort
            panic!("chunk already existed with id {}", chunk_id)
        }

        chunk
    }

    /// Create a new read buffer chunk
    pub fn create_rub_chunk(
        &mut self,
        chunk: read_buffer::RBChunk,
        schema: Arc<Schema>,
    ) -> Arc<RwLock<CatalogChunk>> {
        let chunk_id = self.next_chunk_id;
        assert_ne!(self.next_chunk_id, u32::MAX, "Chunk ID Overflow");
        self.next_chunk_id += 1;

        let addr = ChunkAddr::new(&self.addr, chunk_id);
        info!(%addr, row_count=chunk.rows(), "inserting RUB chunk to catalog");

        let chunk = Arc::new(self.metrics.new_chunk_lock(CatalogChunk::new_rub_chunk(
            addr,
            chunk,
            schema,
            self.metrics.new_chunk_metrics(),
        )));

        if self.chunks.insert(chunk_id, Arc::clone(&chunk)).is_some() {
            // A fundamental invariant has been violated - abort
            panic!("chunk already existed with id {}", chunk_id)
        }
        chunk
    }

    /// Create new chunk that is only in object store (= parquet file).
    ///
    /// The table-specific chunk ID counter will be set to `max(current, chunk_id + 1)`.
    ///
    /// Returns the previous chunk with the given chunk_id if any
    pub fn insert_object_store_only_chunk(
        &mut self,
        chunk_id: u32,
        chunk: Arc<parquet_file::chunk::ParquetChunk>,
    ) -> Arc<RwLock<CatalogChunk>> {
        assert_eq!(chunk.table_name(), self.table_name());

        let addr = ChunkAddr::new(&self.addr, chunk_id);

        let chunk = Arc::new(
            self.metrics
                .new_chunk_lock(CatalogChunk::new_object_store_only(
                    addr,
                    chunk,
                    self.metrics.new_chunk_metrics(),
                )),
        );

        self.next_chunk_id = self.next_chunk_id.max(chunk_id + 1);
        match self.chunks.entry(chunk_id) {
            Entry::Vacant(vacant) => Arc::clone(vacant.insert(chunk)),
            Entry::Occupied(_) => panic!("chunk with id {} already exists", chunk_id),
        }
    }

    /// Drop the specified chunk
    pub fn drop_chunk(&mut self, chunk_id: u32) -> Result<Arc<RwLock<CatalogChunk>>> {
        match self.chunks.entry(chunk_id) {
            Entry::Vacant(_) => Err(Error::ChunkNotFound {
                chunk: ChunkAddr::new(&self.addr, chunk_id),
            }),
            Entry::Occupied(occupied) => {
                {
                    let chunk = occupied.get().read();
                    if let Some(action) = chunk.lifecycle_action() {
                        if action.metadata() != &ChunkLifecycleAction::Dropping {
                            return Err(Error::LifecycleInProgress {
                                chunk: chunk.addr().clone(),
                                action: *action.metadata(),
                            });
                        }
                    }
                }
                Ok(occupied.remove())
            }
        }
    }

    /// Drop the specified chunk even if it has an in-progress lifecycle action
    pub fn force_drop_chunk(&mut self, chunk_id: u32) {
        self.chunks.remove(&chunk_id);
    }

    /// Return the first currently open chunk, if any
    pub fn open_chunk(&self) -> Option<Arc<RwLock<CatalogChunk>>> {
        self.chunks
            .values()
            .find(|chunk| {
                let chunk = chunk.read();
                matches!(chunk.stage(), ChunkStage::Open { .. })
            })
            .cloned()
    }

    /// Return an immutable chunk reference by chunk id
    pub fn chunk(&self, chunk_id: u32) -> Option<&Arc<RwLock<CatalogChunk>>> {
        self.chunks.get(&chunk_id)
    }

    /// Return a iterator over chunks in this partition
    pub fn chunks(&self) -> impl Iterator<Item = &Arc<RwLock<CatalogChunk>>> {
        self.chunks.values()
    }

    /// Return a iterator over chunks in this partition with their ids
    pub fn keyed_chunks(&self) -> impl Iterator<Item = (u32, &Arc<RwLock<CatalogChunk>>)> {
        self.chunks.iter().map(|(a, b)| (*a, b))
    }

    /// Return a PartitionSummary for this partition
    pub fn summary(&self) -> PartitionSummary {
        PartitionSummary::from_table_summaries(
            self.addr.partition_key.to_string(),
            self.chunks
                .values()
                .map(|x| x.read().table_summary().as_ref().clone()),
        )
    }

    /// Return chunk summaries for all chunks in this partition
    pub fn chunk_summaries(&self) -> impl Iterator<Item = ChunkSummary> + '_ {
        self.chunks().map(|x| x.read().summary())
    }

    pub fn metrics(&self) -> &PartitionMetrics {
        &self.metrics
    }

    pub fn persistence_windows(&self) -> Option<&PersistenceWindows> {
        self.persistence_windows.as_ref()
    }

    pub fn persistence_windows_mut(&mut self) -> Option<&mut PersistenceWindows> {
        self.persistence_windows.as_mut()
    }

    pub fn set_persistence_windows(&mut self, windows: PersistenceWindows) {
        self.persistence_windows = Some(windows);
    }
}

impl Display for Partition {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.addr.fmt(f)
    }
}
