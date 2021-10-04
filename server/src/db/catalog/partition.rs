//! The catalog representation of a Partition

use super::chunk::{CatalogChunk, Error as ChunkError};
use crate::db::catalog::metrics::PartitionMetrics;
use chrono::{DateTime, Utc};
use data_types::{
    chunk_metadata::{ChunkAddr, ChunkId, ChunkLifecycleAction, ChunkOrder, ChunkSummary},
    partition_metadata::{PartitionAddr, PartitionSummary},
};
use hashbrown::HashMap;
use internal_types::schema::Schema;
use observability_deps::tracing::info;
use persistence_windows::{
    min_max_sequence::OptionalMinMaxSequence, persistence_windows::PersistenceWindows,
};
use predicate::delete_predicate::DeletePredicate;
use snafu::{OptionExt, Snafu};
use std::{collections::BTreeMap, fmt::Display, sync::Arc};
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

    #[snafu(display("creating new mutable buffer chunk failed: {}", source))]
    CreateOpenChunk { source: ChunkError },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

/// Provides ordered iteration of a collection of chunks
#[derive(Debug, Default)]
struct ChunkCollection {
    /// The chunks that make up this partition, indexed by order and id.
    ///
    /// This is the order that chunks should be iterated and locks acquired
    chunks: BTreeMap<(ChunkOrder, ChunkId), Arc<RwLock<CatalogChunk>>>,

    /// Provides a lookup from `ChunkId` to the corresponding `ChunkOrder`
    chunk_orders: HashMap<ChunkId, ChunkOrder>,
}

impl ChunkCollection {
    /// Returns an iterator over the chunks in this collection
    /// ordered by `ChunkOrder` and then `ChunkId`
    fn iter(&self) -> impl Iterator<Item = (ChunkId, ChunkOrder, &Arc<RwLock<CatalogChunk>>)> + '_ {
        self.chunks
            .iter()
            .map(|((order, id), chunk)| (*id, *order, chunk))
    }

    /// Returns an iterator over the chunks in this collection
    /// ordered by `ChunkOrder` and then `ChunkId`
    fn values(&self) -> impl Iterator<Item = &Arc<RwLock<CatalogChunk>>> + '_ {
        self.chunks.values()
    }

    /// Gets a chunk by `ChunkId`
    fn get(&self, id: ChunkId) -> Option<(&Arc<RwLock<CatalogChunk>>, ChunkOrder)> {
        let order = *self.chunk_orders.get(&id)?;
        let chunk = self.chunks.get(&(order, id)).unwrap();
        Some((chunk, order))
    }

    /// Inserts a new chunk
    ///
    /// # Panics
    ///
    /// Panics if a chunk already exists with the given id
    fn insert(
        &mut self,
        id: ChunkId,
        order: ChunkOrder,
        chunk: Arc<RwLock<CatalogChunk>>,
    ) -> &Arc<RwLock<CatalogChunk>> {
        match self.chunk_orders.entry(id) {
            hashbrown::hash_map::Entry::Occupied(_) => {
                panic!("chunk already found with id: {}", id)
            }
            hashbrown::hash_map::Entry::Vacant(v) => v.insert(order),
        };

        match self.chunks.entry((order, id)) {
            std::collections::btree_map::Entry::Occupied(_) => unreachable!(),
            std::collections::btree_map::Entry::Vacant(v) => v.insert(chunk),
        }
    }

    /// Remove a chunk with the given ID, returns None if the chunk doesn't exist
    fn remove(&mut self, id: ChunkId) -> Option<Arc<RwLock<CatalogChunk>>> {
        let order = self.chunk_orders.remove(&id)?;
        Some(self.chunks.remove(&(order, id)).unwrap())
    }

    /// Returns `true` if the collection contains no chunks
    fn is_empty(&self) -> bool {
        self.chunk_orders.is_empty()
    }
}

/// IOx Catalog Partition
///
/// A partition contains multiple Chunks for a given table
#[derive(Debug)]
pub struct Partition {
    addr: PartitionAddr,

    /// The chunks that make up this partition
    chunks: ChunkCollection,

    /// When this partition was created
    created_at: DateTime<Utc>,

    /// the last time at which write was made to this
    /// partition. Partition::new initializes this to now.
    last_write_at: DateTime<Utc>,

    /// What the next chunk id is
    next_chunk_id: ChunkId,

    /// Partition metrics
    metrics: Arc<PartitionMetrics>,

    /// Ingest tracking for persisting data from memory to Parquet
    persistence_windows: Option<PersistenceWindows>,

    /// Tracks next chunk order in this partition.
    next_chunk_order: ChunkOrder,
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
            next_chunk_id: ChunkId::new(0),
            metrics: Arc::new(metrics),
            persistence_windows: None,
            next_chunk_order: ChunkOrder::MIN,
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
        time_of_write: DateTime<Utc>,
    ) -> &Arc<RwLock<CatalogChunk>> {
        assert_eq!(chunk.table_name().as_ref(), self.table_name());

        let chunk_id = self.next_chunk_id();
        let chunk_order = self.next_chunk_order();

        let addr = ChunkAddr::new(&self.addr, chunk_id);

        let chunk = CatalogChunk::new_open(
            addr,
            chunk,
            time_of_write,
            self.metrics.new_chunk_metrics(),
            chunk_order,
        );
        let chunk = Arc::new(self.metrics.new_chunk_lock(chunk));
        self.chunks.insert(chunk_id, chunk_order, chunk)
    }

    /// Create a new read buffer chunk.
    ///
    /// Returns ID and chunk.
    pub fn create_rub_chunk(
        &mut self,
        chunk: read_buffer::RBChunk,
        time_of_first_write: DateTime<Utc>,
        time_of_last_write: DateTime<Utc>,
        schema: Arc<Schema>,
        delete_predicates: Vec<Arc<DeletePredicate>>,
        chunk_order: ChunkOrder,
    ) -> (ChunkId, &Arc<RwLock<CatalogChunk>>) {
        let chunk_id = self.next_chunk_id();
        assert!(
            chunk_order < self.next_chunk_order,
            "chunk order for new RUB chunk ({}) is out of range [0, {})",
            chunk_order,
            self.next_chunk_order
        );

        let addr = ChunkAddr::new(&self.addr, chunk_id);
        info!(%addr, row_count=chunk.rows(), "inserting RUB chunk to catalog");

        let chunk = Arc::new(self.metrics.new_chunk_lock(CatalogChunk::new_rub_chunk(
            addr,
            chunk,
            time_of_first_write,
            time_of_last_write,
            schema,
            self.metrics.new_chunk_metrics(),
            delete_predicates,
            chunk_order,
        )));

        let chunk = self.chunks.insert(chunk_id, chunk_order, chunk);
        (chunk_id, chunk)
    }

    /// Create new chunk that is only in object store (= parquet file).
    ///
    /// The partition-specific chunk ID counter will be set to `max(current, chunk_id + 1)`.
    ///
    /// The partition-specific chunk order counter will be set to `max(current, chunk_order + 1)`.
    ///
    /// Returns the previous chunk with the given chunk_id if any
    pub fn insert_object_store_only_chunk(
        &mut self,
        chunk_id: ChunkId,
        chunk: Arc<parquet_file::chunk::ParquetChunk>,
        time_of_first_write: DateTime<Utc>,
        time_of_last_write: DateTime<Utc>,
        delete_predicates: Vec<Arc<DeletePredicate>>,
        chunk_order: ChunkOrder,
    ) -> &Arc<RwLock<CatalogChunk>> {
        assert_eq!(chunk.table_name(), self.table_name());

        let addr = ChunkAddr::new(&self.addr, chunk_id);

        let chunk = Arc::new(
            self.metrics
                .new_chunk_lock(CatalogChunk::new_object_store_only(
                    addr,
                    chunk,
                    time_of_first_write,
                    time_of_last_write,
                    self.metrics.new_chunk_metrics(),
                    delete_predicates,
                    chunk_order,
                )),
        );

        let chunk = self.chunks.insert(chunk_id, chunk_order, chunk);

        // only update internal state when we know that insertion is OK
        self.next_chunk_id = self.next_chunk_id.max(chunk_id.next());
        self.next_chunk_order = self.next_chunk_order.max(chunk_order.next());

        chunk
    }

    /// Drop the specified chunk
    pub fn drop_chunk(&mut self, chunk_id: ChunkId) -> Result<Arc<RwLock<CatalogChunk>>> {
        match self.chunks.get(chunk_id) {
            None => Err(Error::ChunkNotFound {
                chunk: ChunkAddr::new(&self.addr, chunk_id),
            }),
            Some((chunk, _)) => {
                {
                    let chunk = chunk.read();
                    if let Some(action) = chunk.lifecycle_action() {
                        if action.metadata() != &ChunkLifecycleAction::Dropping {
                            return Err(Error::LifecycleInProgress {
                                chunk: chunk.addr().clone(),
                                action: *action.metadata(),
                            });
                        }
                    }
                }
                Ok(self.chunks.remove(chunk_id).unwrap())
            }
        }
    }

    /// Drop the specified chunk even if it has an in-progress lifecycle action
    /// returning the dropped chunk
    pub fn force_drop_chunk(&mut self, chunk_id: ChunkId) -> Result<Arc<RwLock<CatalogChunk>>> {
        self.chunks.remove(chunk_id).context(ChunkNotFound {
            chunk: ChunkAddr::new(&self.addr, chunk_id),
        })
    }

    /// Return the first currently open chunk, if any
    pub fn open_chunk(&self) -> Option<Arc<RwLock<CatalogChunk>>> {
        self.chunks
            .values()
            .find(|chunk| chunk.read().stage().is_open())
            .cloned()
    }

    /// Return an immutable chunk and its order reference by chunk id.
    pub fn chunk(&self, chunk_id: ChunkId) -> Option<(&Arc<RwLock<CatalogChunk>>, ChunkOrder)> {
        self.chunks.get(chunk_id)
    }

    /// Return chunks in this partition.
    ///
    /// Note that chunks are guaranteed ordered by chunk order and then ID.
    pub fn chunks(&self) -> impl Iterator<Item = &Arc<RwLock<CatalogChunk>>> {
        self.chunks.values()
    }

    /// Return chunks in this partition with their order and ids.
    ///
    /// Note that chunks are guaranteed ordered by chunk order and ID.
    pub fn keyed_chunks(
        &self,
    ) -> impl Iterator<Item = (ChunkId, ChunkOrder, &Arc<RwLock<CatalogChunk>>)> + '_ {
        self.chunks.iter()
    }

    /// Return a PartitionSummary for this partition. If the partition
    /// has no chunks, returns None.
    pub fn summary(&self) -> Option<PartitionSummary> {
        if self.chunks.is_empty() {
            None
        } else {
            Some(PartitionSummary::from_table_summaries(
                self.addr.partition_key.to_string(),
                self.chunks
                    .values()
                    .map(|chunk| chunk.read().table_summary().as_ref().clone()),
            ))
        }
    }

    /// Return chunk summaries for all chunks in this partition
    pub fn chunk_summaries(&self) -> impl Iterator<Item = ChunkSummary> + '_ {
        self.chunks.values().map(|x| x.read().summary())
    }

    /// Return reference to partition-specific metrics.
    pub fn metrics(&self) -> &PartitionMetrics {
        &self.metrics
    }

    /// Return immutable reference to current persistence window, if any.
    pub fn persistence_windows(&self) -> Option<&PersistenceWindows> {
        self.persistence_windows.as_ref()
    }

    /// Return mutable reference to current persistence window, if any.
    pub fn persistence_windows_mut(&mut self) -> Option<&mut PersistenceWindows> {
        self.persistence_windows.as_mut()
    }

    /// Set persistence window to new value.
    pub fn set_persistence_windows(&mut self, windows: PersistenceWindows) {
        self.persistence_windows = Some(windows);
    }

    /// Construct sequencer numbers out of contained persistence window, if any.
    pub fn sequencer_numbers(&self) -> Option<BTreeMap<u32, OptionalMinMaxSequence>> {
        self.persistence_windows
            .as_ref()
            .map(|persistence_windows| persistence_windows.sequencer_numbers())
    }

    fn next_chunk_id(&mut self) -> ChunkId {
        let res = self.next_chunk_id;
        self.next_chunk_id = self.next_chunk_id.next();
        res
    }

    fn next_chunk_order(&mut self) -> ChunkOrder {
        let res = self.next_chunk_order;
        self.next_chunk_order = self.next_chunk_order.next();
        res
    }
}

impl Display for Partition {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.addr.fmt(f)
    }
}

#[cfg(test)]
mod tests {
    use entry::test_helpers::lp_to_entry;
    use mutable_buffer::chunk::{ChunkMetrics, MBChunk};

    use crate::db::catalog::metrics::CatalogMetrics;

    use super::*;

    #[test]
    fn chunks_are_returned_in_order() {
        let addr = PartitionAddr {
            db_name: "d".into(),
            table_name: "t".into(),
            partition_key: "p".into(),
        };
        let registry = Arc::new(metric::Registry::new());
        let catalog_metrics = Arc::new(CatalogMetrics::new(
            Arc::clone(&addr.db_name),
            Arc::clone(&registry),
        ));
        let table_metrics = Arc::new(catalog_metrics.new_table_metrics("t"));
        let partition_metrics = table_metrics.new_partition_metrics();

        let t = Utc::now();

        // Make three chunks
        let mut partition = Partition::new(addr, partition_metrics);
        for _ in 0..3 {
            partition.create_open_chunk(make_mb_chunk("t"), t);
        }

        // should be in ascending order
        let expected_ids = vec![ChunkId::new(0), ChunkId::new(1), ChunkId::new(2)];

        let ids = partition
            .chunks()
            .into_iter()
            .map(|c| c.read().id())
            .collect::<Vec<_>>();
        assert_eq!(ids, expected_ids);

        let ids = partition
            .keyed_chunks()
            .into_iter()
            .map(|(id, _order, _chunk)| id)
            .collect::<Vec<_>>();
        assert_eq!(ids, expected_ids);
    }

    fn make_mb_chunk(table_name: &str) -> MBChunk {
        let entry = lp_to_entry(&format!("{} bar=1 10", table_name));
        let write = entry.partition_writes().unwrap().remove(0);
        let batch = write.table_batches().remove(0);

        MBChunk::new(ChunkMetrics::new_unregistered(), batch, None).unwrap()
    }
}
