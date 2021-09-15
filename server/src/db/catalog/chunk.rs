use std::sync::Arc;

use chrono::{DateTime, Utc};
use snafu::Snafu;

use data_types::{
    chunk_metadata::{
        ChunkAddr, ChunkColumnSummary, ChunkLifecycleAction, ChunkOrder, ChunkStorage,
        ChunkSummary, DetailedChunkSummary,
    },
    instant::to_approximate_datetime,
    partition_metadata::TableSummary,
    write_summary::TimestampSummary,
};
use internal_types::{access::AccessRecorder, schema::Schema};
use mutable_buffer::chunk::{snapshot::ChunkSnapshot as MBChunkSnapshot, MBChunk};
use observability_deps::tracing::debug;
use parquet_file::chunk::ParquetChunk;
use predicate::predicate::Predicate;
use read_buffer::RBChunk;
use tracker::{TaskRegistration, TaskTracker};

use crate::db::catalog::metrics::{StorageRecorder, TimestampHistogram};
use parking_lot::Mutex;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display(
        "Internal Error: unexpected chunk state for {} during {}. Expected {}, got {}",
        chunk,
        operation,
        expected,
        actual
    ))]
    InternalChunkState {
        chunk: ChunkAddr,
        operation: String,
        expected: String,
        actual: String,
    },

    #[snafu(display(
        "Internal Error: A lifecycle action '{}' is already in progress for {}",
        lifecycle_action,
        chunk,
    ))]
    LifecycleActionAlreadyInProgress {
        chunk: ChunkAddr,
        lifecycle_action: String,
    },

    #[snafu(display(
        "Internal Error: Unexpected chunk state for {}. Expected {}, got {}",
        chunk,
        expected,
        actual
    ))]
    UnexpectedLifecycleAction {
        chunk: ChunkAddr,
        expected: String,
        actual: String,
    },

    #[snafu(display(
        "Internal Error: Cannot clear a lifecycle action '{}' for chunk {} that is still running",
        action,
        chunk
    ))]
    IncompleteLifecycleAction { chunk: ChunkAddr, action: String },
}
pub type Result<T, E = Error> = std::result::Result<T, E>;

// Closed chunks have cached information about their schema and statistics
#[derive(Debug, Clone)]
pub struct ChunkMetadata {
    /// The TableSummary, including statistics, for the table in this Chunk
    pub table_summary: Arc<TableSummary>,

    /// The schema for the table in this Chunk
    pub schema: Arc<Schema>,

    /// Delete predicates of this chunk
    pub delete_predicates: Arc<Vec<Predicate>>,
}

/// Different memory representations of a frozen chunk.
#[derive(Debug)]
pub enum ChunkStageFrozenRepr {
    /// Snapshot from the Mutable Buffer, freshly created from the former _open_ chunk. Not ideal
    /// for memory consumption but good enough for the frozen stage. Should ideally be converted
    /// into the [`ReadBuffer`](ChunkStageFrozenRepr::ReadBuffer) rather quickly.
    MutableBufferSnapshot(Arc<MBChunkSnapshot>),

    /// Read Buffer that is optimized for in-memory data processing.
    ReadBuffer(Arc<RBChunk>),
}

/// Represents the current lifecycle stage a chunk is in.
///
/// # Stages
/// - **Open:** A chunk can receive new data. It is not persisted.
/// - **Frozen:** A chunk cannot receive new data. It is not persisted.
/// - **Persisted:** A chunk cannot receive new data. It is persisted.
///
/// # Stage Transitions
/// State changes look like this:
///
/// ```text
///      new           compact         restore
///       │             ▲   │             │
///       │             │   │             │
/// ┌─────▼─────┐   ┌───┴───▼───┐   ┌─────▼─────┐
/// │           │   │           │   │           │
/// │   Open    ├───►  Frozen   ├──►│ Persisted │
/// │           │   │           │   │           │
/// └─────┬─────┘   └─────┬─────┘   └─────┬─────┘
///       │               │               │
///       │               ▼               │
///       └────────────►drop◄─────────────┘
/// ```
///
/// A chunk stage lifecycle is linear, i.e. it can never go back. Also note that the _peristed_
/// stage is the only one that can be restored on node startup (from the persisted catalog).
/// Furthermore, multiple _frozen_ chunks can be compacted into a single one. Nodes at any stage
/// can be dropped via API calls and according to lifecycle policies.
///
/// A chunk can be in-transit when there is a lifecycle job active. A lifecycle job can change the
/// stage once finished (according to the diagram shown above). The chunk stage is considered
/// unchanged as long as the job is running.
#[derive(Debug)]
pub enum ChunkStage {
    /// A chunk in an _open_ stage.
    ///
    /// Chunks in this stage are writable (= can receive new data) and are not preserved.
    Open {
        /// Mutable Buffer that receives writes.
        mb_chunk: MBChunk,
    },

    /// A chunk in a _frozen_ stage.
    ///
    /// Chunks in this stage cannot be modified but are not yet persisted. They can, however, be
    /// compacted, which will take two or more chunks and creates a single new frozen chunk.
    Frozen {
        /// Metadata (statistics, schema) about this chunk
        meta: Arc<ChunkMetadata>,

        /// Internal memory representation of the frozen chunk.
        representation: ChunkStageFrozenRepr,
    },

    /// Chunk in a _persisted_ stage.
    ///
    /// Chunk cannot receive new data. It is persisted.
    Persisted {
        /// Metadata (statistics, schema) about this chunk
        meta: Arc<ChunkMetadata>,

        /// Parquet chunk that lives immutable within the object store.
        parquet: Arc<ParquetChunk>,

        /// In-memory version of the parquet data.
        read_buffer: Option<Arc<RBChunk>>,
    },
}

impl ChunkStage {
    pub fn name(&self) -> &'static str {
        match self {
            Self::Open { .. } => "Open",
            Self::Frozen { .. } => "Frozen",
            Self::Persisted { .. } => "Persisted",
        }
    }
}

/// The catalog representation of a Chunk in IOx. Note that a chunk
/// may exist in several physical locations at any given time (e.g. in
/// mutable buffer and in read buffer)
///
/// # State Handling
/// The actual chunk _state_ consistest of multiple parts. First there is the
/// [lifecycle _stage_](ChunkStage) which captures the grant movement of a chunk from a unoptimized
/// mutable object to an optimized immutable one. Within these stages there are multiple ways to
/// represent or cache data. This fact is captured by the _stage_-specific chunk _representation_
/// (e.g. a persisted chunk may have data cached in-memory).
#[derive(Debug)]
pub struct CatalogChunk {
    addr: ChunkAddr,

    /// The lifecycle stage this chunk is in.
    stage: ChunkStage,

    /// The active lifecycle task if any
    ///
    /// This is stored as a TaskTracker to allow monitoring the progress of the
    /// action, detecting if the task failed, waiting for the task to complete
    /// or even triggering graceful termination of it
    lifecycle_action: Option<TaskTracker<ChunkLifecycleAction>>,

    /// The metrics for this chunk
    ///
    /// Wrapped in a mutex to allow updating metrics without exclusive access to CatalogChunk
    metrics: Mutex<ChunkMetrics>,

    /// Record access to this chunk's data by queries and writes
    access_recorder: AccessRecorder,

    /// The earliest time at which data contained within this chunk was written
    /// into IOx. Note due to the compaction, etc... this may not be the chunk
    /// that data was originally written into
    time_of_first_write: DateTime<Utc>,

    /// The latest time at which data contained within this chunk was written
    /// into IOx. Note due to the compaction, etc... this may not be the chunk
    /// that data was originally written into
    time_of_last_write: DateTime<Utc>,

    /// Time at which this chunk was marked as closed. Note this is
    /// not the same as the timestamps on the data itself
    time_closed: Option<DateTime<Utc>>,

    /// Order of this chunk relative to other overlapping chunks.
    order: ChunkOrder,
}

macro_rules! unexpected_state {
    ($SELF: expr, $OP: expr, $EXPECTED: expr, $STATE: expr) => {
        InternalChunkState {
            chunk: $SELF.addr.clone(),
            operation: $OP,
            expected: $EXPECTED,
            actual: $STATE.name(),
        }
        .fail()
    };
}

#[derive(Debug)]
pub struct ChunkMetrics {
    /// Chunk storage metrics
    pub(super) chunk_storage: StorageRecorder,

    /// Chunk row count metrics
    pub(super) row_count: StorageRecorder,

    /// Catalog memory metrics
    pub(super) memory_metrics: StorageRecorder,

    /// Track ingested timestamps
    pub(super) timestamp_histogram: Option<TimestampHistogram>,
}

impl ChunkMetrics {
    /// Creates an instance of ChunkMetrics that isn't registered with a central
    /// metrics registry. Observations made to instruments on this ChunkMetrics instance
    /// will therefore not be visible to other ChunkMetrics instances or metric instruments
    /// created on a metrics domain, and vice versa
    pub fn new_unregistered() -> Self {
        Self {
            chunk_storage: StorageRecorder::new_unregistered(),
            row_count: StorageRecorder::new_unregistered(),
            memory_metrics: StorageRecorder::new_unregistered(),
            timestamp_histogram: Default::default(),
        }
    }
}

impl CatalogChunk {
    /// Creates a new open chunk from the provided MUB chunk.
    ///
    /// Panics if the provided chunk is empty, otherwise creates a new open chunk.
    pub(super) fn new_open(
        addr: ChunkAddr,
        chunk: mutable_buffer::chunk::MBChunk,
        time_of_write: DateTime<Utc>,
        metrics: ChunkMetrics,
        order: ChunkOrder,
    ) -> Self {
        assert_eq!(chunk.table_name(), &addr.table_name);

        let stage = ChunkStage::Open { mb_chunk: chunk };

        let chunk = Self {
            addr,
            stage,
            lifecycle_action: None,
            metrics: Mutex::new(metrics),
            access_recorder: Default::default(),
            time_of_first_write: time_of_write,
            time_of_last_write: time_of_write,
            time_closed: None,
            order,
        };
        chunk.update_metrics();
        chunk
    }

    /// Creates a new RUB chunk from the provided RUB chunk and metadata
    ///
    /// Panics if the provided chunk is empty
    #[allow(clippy::too_many_arguments)]
    pub(super) fn new_rub_chunk(
        addr: ChunkAddr,
        chunk: read_buffer::RBChunk,
        time_of_first_write: DateTime<Utc>,
        time_of_last_write: DateTime<Utc>,
        schema: Arc<Schema>,
        metrics: ChunkMetrics,
        delete_predicates: Arc<Vec<Predicate>>,
        order: ChunkOrder,
    ) -> Self {
        let stage = ChunkStage::Frozen {
            meta: Arc::new(ChunkMetadata {
                table_summary: Arc::new(chunk.table_summary()),
                schema,
                delete_predicates: Arc::clone(&delete_predicates),
            }),
            representation: ChunkStageFrozenRepr::ReadBuffer(Arc::new(chunk)),
        };

        let chunk = Self {
            addr,
            stage,
            lifecycle_action: None,
            metrics: Mutex::new(metrics),
            access_recorder: Default::default(),
            time_of_first_write,
            time_of_last_write,
            time_closed: None,
            order,
        };
        chunk.update_metrics();
        chunk
    }

    /// Creates a new chunk that is only registered via an object store reference (= only exists in
    /// parquet).
    pub(super) fn new_object_store_only(
        addr: ChunkAddr,
        chunk: Arc<parquet_file::chunk::ParquetChunk>,
        time_of_first_write: DateTime<Utc>,
        time_of_last_write: DateTime<Utc>,
        metrics: ChunkMetrics,
        delete_predicates: Arc<Vec<Predicate>>,
        order: ChunkOrder,
    ) -> Self {
        assert_eq!(chunk.table_name(), addr.table_name.as_ref());

        // Cache table summary + schema
        let meta = Arc::new(ChunkMetadata {
            table_summary: Arc::clone(chunk.table_summary()),
            schema: chunk.schema(),
            delete_predicates,
        });

        let stage = ChunkStage::Persisted {
            parquet: chunk,
            read_buffer: None,
            meta,
        };

        let chunk = Self {
            addr,
            stage,
            lifecycle_action: None,
            metrics: Mutex::new(metrics),
            access_recorder: Default::default(),
            time_of_first_write,
            time_of_last_write,
            time_closed: None,
            order,
        };
        chunk.update_metrics();
        chunk
    }

    pub fn addr(&self) -> &ChunkAddr {
        &self.addr
    }

    pub fn id(&self) -> u32 {
        self.addr.chunk_id
    }

    pub fn key(&self) -> &str {
        self.addr.partition_key.as_ref()
    }

    pub fn table_name(&self) -> Arc<str> {
        Arc::clone(&self.addr.table_name)
    }

    pub fn stage(&self) -> &ChunkStage {
        &self.stage
    }

    /// Returns the AccessRecorder used to record access to this chunk's data by queries
    pub fn access_recorder(&self) -> &AccessRecorder {
        &self.access_recorder
    }

    pub fn lifecycle_action(&self) -> Option<&TaskTracker<ChunkLifecycleAction>> {
        self.lifecycle_action.as_ref()
    }

    pub fn is_in_lifecycle(&self, lifecycle_action: ChunkLifecycleAction) -> bool {
        self.lifecycle_action
            .as_ref()
            .map_or(false, |action| action.metadata() == &lifecycle_action)
    }

    pub fn time_of_first_write(&self) -> DateTime<Utc> {
        self.time_of_first_write
    }

    pub fn time_of_last_write(&self) -> DateTime<Utc> {
        self.time_of_last_write
    }

    pub fn time_closed(&self) -> Option<DateTime<Utc>> {
        self.time_closed
    }

    pub fn order(&self) -> ChunkOrder {
        self.order
    }

    /// Updates `self.metrics` to match the contents of `self.stage`
    pub fn update_metrics(&self) {
        let mut metrics = self.metrics.lock();

        match &self.stage {
            ChunkStage::Open { mb_chunk } => {
                metrics.memory_metrics.set_mub_only(mb_chunk.size());
                metrics.row_count.set_mub_only(mb_chunk.rows());
                metrics.chunk_storage.set_mub_only(1);
            }
            ChunkStage::Frozen { representation, .. } => match representation {
                ChunkStageFrozenRepr::MutableBufferSnapshot(snapshot) => {
                    metrics.memory_metrics.set_mub_only(snapshot.size());
                    metrics.row_count.set_mub_only(snapshot.rows());
                    metrics.chunk_storage.set_mub_only(1);
                }
                ChunkStageFrozenRepr::ReadBuffer(rb_chunk) => {
                    metrics.memory_metrics.set_rub_only(rb_chunk.size());
                    metrics.row_count.set_rub_only(rb_chunk.rows() as usize);
                    metrics.chunk_storage.set_rub_only(1);
                }
            },
            ChunkStage::Persisted {
                parquet,
                read_buffer: Some(read_buffer),
                ..
            } => {
                metrics
                    .memory_metrics
                    .set_rub_and_object_store_only(read_buffer.size(), parquet.size());
                metrics
                    .row_count
                    .set_rub_and_object_store_only(read_buffer.rows() as usize, parquet.rows());
                metrics.chunk_storage.set_rub_and_object_store_only(1, 1);
            }
            ChunkStage::Persisted { parquet, .. } => {
                metrics.memory_metrics.set_object_store_only(parquet.size());
                metrics.row_count.set_object_store_only(parquet.rows());
                metrics.chunk_storage.set_object_store_only(1);
            }
        }
    }

    pub fn add_delete_predicate(&mut self, delete_predicate: &Predicate) -> Result<()> {
        debug!(
            ?delete_predicate,
            "Input delete predicate to CatalogChunk add_delete_predicate"
        );
        match &mut self.stage {
            ChunkStage::Open { mb_chunk: _ } => {
                // Freeze/close this chunk and add delete_predicate to its frozen one
                self.freeze_with_predicate(delete_predicate)?;
            }
            ChunkStage::Frozen { meta, .. } => {
                // Add the delete_predicate into the chunk's metadata
                let mut del_preds: Vec<Predicate> = (*meta.delete_predicates).clone();
                del_preds.push(delete_predicate.clone());
                *meta = Arc::new(ChunkMetadata {
                    table_summary: Arc::clone(&meta.table_summary),
                    schema: Arc::clone(&meta.schema),
                    delete_predicates: Arc::new(del_preds),
                });
            }
            ChunkStage::Persisted { meta, .. } => {
                // Add the delete_predicate into the chunk's metadata
                let mut del_preds: Vec<Predicate> = (*meta.delete_predicates).clone();
                del_preds.push(delete_predicate.clone());
                *meta = Arc::new(ChunkMetadata {
                    table_summary: Arc::clone(&meta.table_summary),
                    schema: Arc::clone(&meta.schema),
                    delete_predicates: Arc::new(del_preds),
                });
            }
        }

        Ok(())
    }

    pub fn delete_predicates(&mut self) -> Arc<Vec<Predicate>> {
        match &self.stage {
            ChunkStage::Open { mb_chunk: _ } => {
                // no delete predicate for open chunk
                debug!("delete_predicates of Open chunk is empty");
                Arc::new(vec![])
            }
            ChunkStage::Frozen { meta, .. } => {
                let preds = &meta.delete_predicates;
                debug!(?preds, "delete_predicates of Frozen chunk");
                Arc::clone(&meta.delete_predicates)
            }
            ChunkStage::Persisted { meta, .. } => {
                let preds = &meta.delete_predicates;
                debug!(?preds, "delete_predicates of Persisted chunk");
                Arc::clone(&meta.delete_predicates)
            }
        }
    }

    /// Record a write of row data to this chunk
    ///
    /// `time_of_write` is the wall clock time of the write
    /// `timestamps` is a summary of the row timestamps contained in the write
    pub fn record_write(&mut self, time_of_write: DateTime<Utc>, timestamps: &TimestampSummary) {
        {
            let metrics = self.metrics.lock();
            if let Some(timestamp_histogram) = metrics.timestamp_histogram.as_ref() {
                timestamp_histogram.add(timestamps)
            }
        }
        self.access_recorder.record_access_now();

        self.time_of_first_write = self.time_of_first_write.min(time_of_write);

        // DateTime<Utc> isn't necessarily monotonic
        self.time_of_last_write = self.time_of_last_write.max(time_of_write);

        self.update_metrics();
    }

    /// Returns the storage and the number of rows
    pub fn storage(&self) -> (usize, ChunkStorage) {
        match &self.stage {
            ChunkStage::Open { mb_chunk, .. } => (mb_chunk.rows(), ChunkStorage::OpenMutableBuffer),
            ChunkStage::Frozen { representation, .. } => match &representation {
                ChunkStageFrozenRepr::MutableBufferSnapshot(repr) => {
                    (repr.rows(), ChunkStorage::ClosedMutableBuffer)
                }
                ChunkStageFrozenRepr::ReadBuffer(repr) => {
                    (repr.rows() as usize, ChunkStorage::ReadBuffer)
                }
            },
            ChunkStage::Persisted {
                parquet,
                read_buffer,
                ..
            } => {
                let rows = parquet.rows() as usize;
                let storage = if read_buffer.is_some() {
                    ChunkStorage::ReadBufferAndObjectStore
                } else {
                    ChunkStorage::ObjectStoreOnly
                };
                (rows, storage)
            }
        }
    }

    /// Return ChunkSummary metadata for this chunk
    pub fn summary(&self) -> ChunkSummary {
        let (row_count, storage) = self.storage();

        let lifecycle_action = self
            .lifecycle_action
            .as_ref()
            .map(|tracker| *tracker.metadata());

        let time_of_last_access = self
            .access_recorder
            .get_metrics()
            .last_access()
            .map(to_approximate_datetime);

        ChunkSummary {
            partition_key: Arc::clone(&self.addr.partition_key),
            table_name: Arc::clone(&self.addr.table_name),
            id: self.addr.chunk_id,
            storage,
            lifecycle_action,
            memory_bytes: self.memory_bytes(),
            object_store_bytes: self.object_store_bytes(),
            row_count,
            time_of_last_access,
            time_of_first_write: self.time_of_first_write,
            time_of_last_write: self.time_of_last_write,
            time_closed: self.time_closed,
            order: self.order,
        }
    }

    /// Return information about the storage in this Chunk
    pub fn detailed_summary(&self) -> DetailedChunkSummary {
        let inner = self.summary();

        fn to_summary(v: (&str, usize)) -> ChunkColumnSummary {
            ChunkColumnSummary {
                name: v.0.into(),
                memory_bytes: v.1,
            }
        }

        let columns: Vec<ChunkColumnSummary> = match &self.stage {
            ChunkStage::Open { mb_chunk, .. } => mb_chunk.column_sizes().map(to_summary).collect(),
            ChunkStage::Frozen { representation, .. } => match &representation {
                ChunkStageFrozenRepr::MutableBufferSnapshot(repr) => {
                    repr.column_sizes().map(to_summary).collect()
                }
                ChunkStageFrozenRepr::ReadBuffer(repr) => repr.column_sizes(),
            },
            ChunkStage::Persisted { read_buffer, .. } => {
                if let Some(read_buffer) = &read_buffer {
                    read_buffer.column_sizes()
                } else {
                    // TODO parquet statistics
                    vec![]
                }
            }
        };

        DetailedChunkSummary { inner, columns }
    }

    /// Return the summary information about the table stored in this Chunk
    pub fn table_summary(&self) -> Arc<TableSummary> {
        match &self.stage {
            ChunkStage::Open { mb_chunk, .. } => {
                // The stats for open chunks change so can't be cached
                Arc::new(mb_chunk.table_summary())
            }
            ChunkStage::Frozen { meta, .. } => Arc::clone(&meta.table_summary),
            ChunkStage::Persisted { meta, .. } => Arc::clone(&meta.table_summary),
        }
    }

    /// Returns an approximation of the amount of process memory consumed by the chunk
    pub fn memory_bytes(&self) -> usize {
        match &self.stage {
            ChunkStage::Open { mb_chunk, .. } => mb_chunk.size(),
            ChunkStage::Frozen { representation, .. } => match &representation {
                ChunkStageFrozenRepr::MutableBufferSnapshot(repr) => repr.size(),
                ChunkStageFrozenRepr::ReadBuffer(repr) => repr.size(),
            },
            ChunkStage::Persisted {
                parquet,
                read_buffer,
                ..
            } => {
                let mut size = parquet.size();
                if let Some(read_buffer) = &read_buffer {
                    size += read_buffer.size();
                }
                size
            }
        }
    }

    /// Returns the number of bytes of object storage consumed by this chunk
    pub fn object_store_bytes(&self) -> usize {
        match &self.stage {
            ChunkStage::Open { .. } => 0,
            ChunkStage::Frozen { .. } => 0,
            ChunkStage::Persisted { parquet, .. } => parquet.file_size_bytes(),
        }
    }

    /// Returns a mutable reference to the mutable buffer storage for chunks in the Open state
    pub fn mutable_buffer(&mut self) -> Result<&mut MBChunk> {
        match &mut self.stage {
            ChunkStage::Open { mb_chunk, .. } => Ok(mb_chunk),
            stage => unexpected_state!(self, "mutable buffer reference", "Open or Closed", stage),
        }
    }

    /// Set chunk to _frozen_ state.
    ///
    /// This only works for chunks in the _open_ stage (chunk is converted) and the _frozen_ stage
    /// (no-op) and will fail for other stages.
    pub fn freeze_with_predicate(&mut self, delete_predicate: &Predicate) -> Result<()> {
        self.freeze_with_delete_predicates(vec![delete_predicate.clone()])
    }

    fn freeze_with_delete_predicates(&mut self, delete_predicates: Vec<Predicate>) -> Result<()> {
        match &self.stage {
            ChunkStage::Open { mb_chunk, .. } => {
                debug!(%self.addr, row_count=mb_chunk.rows(), "freezing chunk");
                assert!(self.time_closed.is_none());

                self.time_closed = Some(Utc::now());
                let (s, _) = mb_chunk.snapshot();

                // Cache table summary + schema
                let metadata = ChunkMetadata {
                    table_summary: Arc::new(mb_chunk.table_summary()),
                    schema: s.full_schema(),
                    delete_predicates: Arc::new(delete_predicates),
                };

                self.stage = ChunkStage::Frozen {
                    representation: ChunkStageFrozenRepr::MutableBufferSnapshot(Arc::clone(&s)),
                    meta: Arc::new(metadata),
                };
                self.update_metrics();

                Ok(())
            }
            &ChunkStage::Frozen { .. } => {
                // already frozen => no-op
                Ok(())
            }
            _ => {
                unexpected_state!(self, "setting closed", "Open or Frozen", &self.stage)
            }
        }
    }

    pub fn freeze(&mut self) -> Result<()> {
        self.freeze_with_delete_predicates(vec![])
    }

    /// Set the chunk to the Moving state, returning a handle to the underlying storage
    ///
    /// If called on an open chunk will first [`freeze`](Self::freeze) the chunk
    pub fn set_moving(&mut self, registration: &TaskRegistration) -> Result<Arc<MBChunkSnapshot>> {
        // This ensures the closing logic is consistent but doesn't break code that
        // assumes a chunk can be moved from open
        if matches!(self.stage, ChunkStage::Open { .. }) {
            self.freeze()?;
        }

        match &self.stage {
            ChunkStage::Frozen { representation, .. } => match &representation {
                ChunkStageFrozenRepr::MutableBufferSnapshot(repr) => {
                    let chunk = Arc::clone(repr);
                    self.set_lifecycle_action(ChunkLifecycleAction::Moving, registration)?;

                    Ok(chunk)
                }
                ChunkStageFrozenRepr::ReadBuffer(_) => InternalChunkState {
                    chunk: self.addr.clone(),
                    operation: "setting moving",
                    expected: "Frozen with MutableBufferSnapshot",
                    actual: "Frozen with ReadBuffer",
                }
                .fail(),
            },
            _ => {
                unexpected_state!(self, "setting closed", "Open or Closed", &self.stage)
            }
        }
    }

    /// Set the chunk to be compacting
    pub fn set_compacting(&mut self, registration: &TaskRegistration) -> Result<()> {
        match &self.stage {
            ChunkStage::Open { .. } | ChunkStage::Frozen { .. } => {
                self.set_lifecycle_action(ChunkLifecycleAction::Compacting, registration)?;
                self.freeze()?;
                Ok(())
            }
            ChunkStage::Persisted { .. } => {
                unexpected_state!(self, "setting compacting", "Open or Frozen", &self.stage)
            }
        }
    }

    /// Set the chunk in the Moved state, setting the underlying storage handle to db, and
    /// discarding the underlying mutable buffer storage.
    pub fn set_moved(&mut self, chunk: Arc<RBChunk>, schema: Arc<Schema>) -> Result<()> {
        match &mut self.stage {
            ChunkStage::Frozen {
                meta,
                representation,
                ..
            } => {
                // after moved, the chunk is sorted and its schema needs to get updated
                *meta = Arc::new(ChunkMetadata {
                    table_summary: Arc::clone(&meta.table_summary),
                    schema,
                    delete_predicates: Arc::clone(&meta.delete_predicates),
                });

                match &representation {
                    ChunkStageFrozenRepr::MutableBufferSnapshot(_) => {
                        *representation = ChunkStageFrozenRepr::ReadBuffer(chunk);
                        self.update_metrics();
                        self.finish_lifecycle_action(ChunkLifecycleAction::Moving)?;
                        Ok(())
                    }
                    ChunkStageFrozenRepr::ReadBuffer(_) => InternalChunkState {
                        chunk: self.addr.clone(),
                        operation: "setting moved",
                        expected: "Frozen with MutableBufferSnapshot",
                        actual: "Frozen with ReadBuffer",
                    }
                    .fail(),
                }
            }
            _ => {
                unexpected_state!(self, "setting moved", "Moving", self.stage)
            }
        }
    }

    /// Start lifecycle action that should move the chunk into the _persisted_ stage.
    pub fn set_writing_to_object_store(&mut self, registration: &TaskRegistration) -> Result<()> {
        // This ensures the closing logic is consistent but doesn't break code that
        // assumes a chunk can be moved from open
        if matches!(self.stage, ChunkStage::Open { .. }) {
            self.freeze()?;
        }

        match &self.stage {
            ChunkStage::Frozen { .. } => {
                self.set_lifecycle_action(ChunkLifecycleAction::Persisting, registration)?;
                Ok(())
            }
            _ => {
                unexpected_state!(self, "setting object store", "Moved", self.stage)
            }
        }
    }

    /// Set the chunk to the MovedToObjectStore state, returning a handle to the underlying storage
    pub fn set_written_to_object_store(&mut self, chunk: Arc<ParquetChunk>) -> Result<()> {
        match &self.stage {
            ChunkStage::Frozen {
                representation,
                meta,
                ..
            } => {
                let meta = Arc::clone(meta);
                match &representation {
                    ChunkStageFrozenRepr::MutableBufferSnapshot(_) => {
                        // Should always be in RUB once persisted
                        InternalChunkState {
                            chunk: self.addr.clone(),
                            operation: "setting object store",
                            expected: "Frozen with ReadBuffer",
                            actual: "Frozen with MutableBufferSnapshot",
                        }
                        .fail()
                    }
                    ChunkStageFrozenRepr::ReadBuffer(repr) => {
                        let db = Arc::clone(repr);
                        self.finish_lifecycle_action(ChunkLifecycleAction::Persisting)?;

                        self.stage = ChunkStage::Persisted {
                            meta,
                            parquet: chunk,
                            read_buffer: Some(db),
                        };
                        self.update_metrics();
                        Ok(())
                    }
                }
            }
            _ => {
                unexpected_state!(
                    self,
                    "setting object store",
                    "MovingToObjectStore",
                    self.stage
                )
            }
        }
    }

    pub fn set_unload_from_read_buffer(&mut self) -> Result<Arc<RBChunk>> {
        match &mut self.stage {
            ChunkStage::Persisted { read_buffer, .. } => {
                if let Some(rub_chunk) = read_buffer.take() {
                    self.update_metrics();

                    Ok(rub_chunk)
                } else {
                    // TODO: do we really need to error here or should unloading an unloaded chunk
                    // be a no-op?
                    InternalChunkState {
                        chunk: self.addr.clone(),
                        operation: "setting unload",
                        expected: "Persisted with ReadBuffer",
                        actual: "Persisted without ReadBuffer",
                    }
                    .fail()
                }
            }
            _ => {
                unexpected_state!(self, "setting unload", "Persisted", &self.stage)
            }
        }
    }

    /// Start lifecycle action that should result in the chunk being dropped from memory and (if persisted) from object store.
    pub fn set_dropping(&mut self, registration: &TaskRegistration) -> Result<()> {
        self.set_lifecycle_action(ChunkLifecycleAction::Dropping, registration)?;

        // set memory metrics to 0 to stop accounting for this chunk within the catalog
        self.metrics.lock().memory_metrics.set_to_zero();

        Ok(())
    }

    /// Set the chunk's in progress lifecycle action or return an error if already in-progress
    fn set_lifecycle_action(
        &mut self,
        lifecycle_action: ChunkLifecycleAction,
        registration: &TaskRegistration,
    ) -> Result<()> {
        if let Some(lifecycle_action) = &self.lifecycle_action {
            return Err(Error::LifecycleActionAlreadyInProgress {
                chunk: self.addr.clone(),
                lifecycle_action: lifecycle_action.metadata().name().to_string(),
            });
        }
        self.lifecycle_action = Some(registration.clone().into_tracker(lifecycle_action));
        Ok(())
    }

    /// Clear the chunk's lifecycle action or return an error if it doesn't match that provided
    fn finish_lifecycle_action(&mut self, lifecycle_action: ChunkLifecycleAction) -> Result<()> {
        match &self.lifecycle_action {
            Some(actual) if actual.metadata() == &lifecycle_action => {}
            actual => {
                return Err(Error::UnexpectedLifecycleAction {
                    chunk: self.addr.clone(),
                    expected: lifecycle_action.name().to_string(),
                    actual: actual
                        .as_ref()
                        .map(|x| x.metadata().name())
                        .unwrap_or("None")
                        .to_string(),
                })
            }
        }
        self.lifecycle_action = None;
        Ok(())
    }

    /// Abort the current lifecycle action if any
    ///
    /// Returns an error if the lifecycle action is still running
    pub fn clear_lifecycle_action(&mut self) -> Result<()> {
        if let Some(tracker) = &self.lifecycle_action {
            if !tracker.is_complete() {
                return Err(Error::IncompleteLifecycleAction {
                    chunk: self.addr.clone(),
                    action: tracker.metadata().name().to_string(),
                });
            }
            self.lifecycle_action = None;

            // Some lifecycle actions (e.g. Drop) modify the memory metrics so that the catalog accounts chunks w/
            // actions correctly. When clearing out that action, we need to restore the pre-action state. The easiest
            // (and stateless) way to to do that is just to call the update method. Since clearing lifecycle actions
            // should be a rather rare event, the cost of this is negligible.
            self.update_metrics();
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::logical_plan::{col, lit};

    use entry::test_helpers::lp_to_entry;
    use mutable_buffer::chunk::ChunkMetrics as MBChunkMetrics;
    use parquet_file::{
        chunk::ParquetChunk,
        test_utils::{
            make_chunk as make_parquet_chunk_with_store, make_iox_object_store, TestSize,
        },
    };
    use predicate::predicate::PredicateBuilder;

    #[test]
    fn test_new_open() {
        let chunk = make_open_chunk();
        assert!(matches!(chunk.stage(), &ChunkStage::Open { .. }));
    }

    #[tokio::test]
    async fn test_freeze() {
        let mut chunk = make_open_chunk();

        // close it
        chunk.freeze().unwrap();
        assert!(matches!(chunk.stage(), &ChunkStage::Frozen { .. }));

        // closing a second time is a no-op
        chunk.freeze().unwrap();
        assert!(matches!(chunk.stage(), &ChunkStage::Frozen { .. }));

        // closing a chunk in persisted state will fail
        let mut chunk = make_persisted_chunk().await;
        assert_eq!(
            chunk.freeze().unwrap_err().to_string(),
            "Internal Error: unexpected chunk state for Chunk('db':'table1':'part1':0) \
            during setting closed. Expected Open or Frozen, got Persisted"
        );
    }

    #[tokio::test]
    async fn set_compacting_freezes_chunk() {
        let mut chunk = make_open_chunk();
        let registration = TaskRegistration::new();

        assert!(chunk.time_closed.is_none());
        assert!(matches!(chunk.stage, ChunkStage::Open { .. }));
        chunk.set_compacting(&registration).unwrap();
        assert!(chunk.time_closed.is_some());
        assert!(matches!(chunk.stage, ChunkStage::Frozen { .. }));
    }

    #[tokio::test]
    async fn test_drop() {
        let mut chunk = make_open_chunk();
        let memory_metrics = chunk.metrics.lock().memory_metrics.reporter();

        // size should not be zero
        let size_before = memory_metrics.total();
        assert_ne!(size_before, 0);

        // start dropping it
        let registration = TaskRegistration::new();
        chunk.set_dropping(&registration).unwrap();

        // size should now be reported as zero
        assert_eq!(memory_metrics.total(), 0);

        // if the lifecycle action is cleared, it should reset the size
        registration.into_tracker(1).cancel();
        chunk.clear_lifecycle_action().unwrap();
        assert_eq!(memory_metrics.total(), size_before);

        // when the lifecycle action cannot be set (e.g. due to an action already in progress), do NOT zero out the size
        let registration = TaskRegistration::new();
        chunk.set_compacting(&registration).unwrap();
        let size_before = memory_metrics.total();
        chunk.set_dropping(&registration).unwrap_err();
        assert_eq!(memory_metrics.total(), size_before);
    }

    #[test]
    fn test_lifecycle_action() {
        let mut chunk = make_open_chunk();
        let registration = TaskRegistration::new();

        // no action to begin with
        assert!(chunk.lifecycle_action().is_none());

        // set some action
        chunk
            .set_lifecycle_action(ChunkLifecycleAction::Moving, &registration)
            .unwrap();
        assert_eq!(
            *chunk.lifecycle_action().unwrap().metadata(),
            ChunkLifecycleAction::Moving
        );

        // setting an action while there is one running fails
        assert_eq!(
            chunk
                .set_lifecycle_action(ChunkLifecycleAction::Moving, &registration)
                .unwrap_err()
                .to_string(),
            "Internal Error: A lifecycle action \'Moving to the Read Buffer\' is already in \
            progress for Chunk('db':'table1':'part1':0)"
        );

        // finishing the wrong action fails
        assert_eq!(
            chunk
                .finish_lifecycle_action(ChunkLifecycleAction::Compacting)
                .unwrap_err()
                .to_string(),
            "Internal Error: Unexpected chunk state for Chunk('db':'table1':'part1':0). Expected \
            Compacting, got Moving to the Read Buffer"
        );

        // finish some action
        chunk
            .finish_lifecycle_action(ChunkLifecycleAction::Moving)
            .unwrap();

        // finishing w/o any action in progress will fail
        assert_eq!(
            chunk
                .finish_lifecycle_action(ChunkLifecycleAction::Moving)
                .unwrap_err()
                .to_string(),
            "Internal Error: Unexpected chunk state for Chunk('db':'table1':'part1':0). Expected \
            Moving to the Read Buffer, got None"
        );

        // now we can set another action
        chunk
            .set_lifecycle_action(ChunkLifecycleAction::Compacting, &registration)
            .unwrap();
        assert_eq!(
            *chunk.lifecycle_action().unwrap().metadata(),
            ChunkLifecycleAction::Compacting
        );
    }

    #[test]
    fn test_clear_lifecycle_action() {
        let mut chunk = make_open_chunk();
        let registration = TaskRegistration::new();

        // clearing w/o any action in-progress works
        chunk.clear_lifecycle_action().unwrap();

        // set some action
        chunk
            .set_lifecycle_action(ChunkLifecycleAction::Moving, &registration)
            .unwrap();

        // clearing now fails because task is still in progress
        assert_eq!(
            chunk.clear_lifecycle_action().unwrap_err().to_string(),
            "Internal Error: Cannot clear a lifecycle action 'Moving to the Read Buffer' for chunk Chunk('db':'table1':'part1':0) that is still running",
        );

        // "finish" task
        registration.into_tracker(1).cancel();

        // clearing works now
        chunk.clear_lifecycle_action().unwrap();
    }

    #[test]
    fn test_add_delete_predicate_open_chunk() {
        let mut chunk = make_open_chunk();
        let registration = TaskRegistration::new();

        // no delete_predicate yet
        let del_preds = chunk.delete_predicates();
        assert_eq!(del_preds.len(), 0);

        // Build delete predicate and expected output
        let expr1 = col("city").eq(lit("Boston"));
        let del_pred1 = PredicateBuilder::new()
            .table("test")
            .timestamp_range(1, 100)
            .add_expr(expr1)
            .build();
        let mut expected_exprs1 = vec![];
        let e = col("city").eq(lit("Boston"));
        expected_exprs1.push(e);

        // Add a delete predicate into a chunk the open chunk = delete simulation for open chunk
        chunk.add_delete_predicate(&del_pred1).unwrap();
        // chunk must be in frozen stage now
        assert_eq!(chunk.stage().name(), "Frozen");
        // chunk must have a delete predicate
        let del_preds = chunk.delete_predicates();
        assert_eq!(del_preds.len(), 1);
        // verify delete predicate value
        let pred = &del_preds[0];
        if let Some(range) = pred.range {
            assert_eq!(range.start, 1); // start time
            assert_eq!(range.end, 100); // stop time
        } else {
            panic!("No time range set for delete predicate");
        }
        assert_eq!(pred.exprs, expected_exprs1);

        // Move the chunk
        chunk.set_moving(&registration).unwrap();
        // The chunk still should be frozen
        assert_eq!(chunk.stage().name(), "Frozen");
        // let add more delete predicate = simulate second delete
        // Build delete predicate and expected output
        let expr2 = col("cost").not_eq(lit(15));
        let del_pred2 = PredicateBuilder::new()
            .table("test")
            .timestamp_range(20, 50)
            .add_expr(expr2)
            .build();
        let mut expected_exprs2 = vec![];
        let e = col("cost").not_eq(lit(15));
        expected_exprs2.push(e);
        chunk.add_delete_predicate(&del_pred2).unwrap();
        // chunk still must be in frozen stage now
        assert_eq!(chunk.stage().name(), "Frozen");
        // chunk must have 2 delete predicates
        let del_preds = chunk.delete_predicates();
        assert_eq!(del_preds.len(), 2);
        // verify the second delete predicate value
        let pred = &del_preds[1];
        if let Some(range) = pred.range {
            assert_eq!(range.start, 20); // start time
            assert_eq!(range.end, 50); // stop time
        } else {
            panic!("No time range set for delete predicate");
        }
        assert_eq!(pred.exprs, expected_exprs2);
    }

    fn make_mb_chunk(table_name: &str) -> MBChunk {
        let entry = lp_to_entry(&format!("{} bar=1 10", table_name));
        let write = entry.partition_writes().unwrap().remove(0);
        let batch = write.table_batches().remove(0);

        MBChunk::new(MBChunkMetrics::new_unregistered(), batch, None).unwrap()
    }

    async fn make_parquet_chunk(addr: ChunkAddr) -> ParquetChunk {
        let iox_object_store = make_iox_object_store().await;
        make_parquet_chunk_with_store(iox_object_store, "foo", addr, TestSize::Full).await
    }

    fn chunk_addr() -> ChunkAddr {
        ChunkAddr {
            db_name: Arc::from("db"),
            table_name: Arc::from("table1"),
            partition_key: Arc::from("part1"),
            chunk_id: 0,
        }
    }

    fn make_open_chunk() -> CatalogChunk {
        let addr = chunk_addr();
        let mb_chunk = make_mb_chunk(&addr.table_name);
        let time_of_write = Utc::now();

        CatalogChunk::new_open(
            addr,
            mb_chunk,
            time_of_write,
            ChunkMetrics::new_unregistered(),
            ChunkOrder::new(5),
        )
    }

    async fn make_persisted_chunk() -> CatalogChunk {
        let addr = chunk_addr();
        let now = Utc::now();

        // assemble ParquetChunk
        let parquet_chunk = make_parquet_chunk(addr.clone()).await;

        CatalogChunk::new_object_store_only(
            addr,
            Arc::new(parquet_chunk),
            now,
            now,
            ChunkMetrics::new_unregistered(),
            Arc::new(vec![] as Vec<Predicate>),
            ChunkOrder::new(6),
        )
    }
}
