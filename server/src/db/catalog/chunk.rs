use std::sync::Arc;

use chrono::{DateTime, Utc};
use data_types::{
    chunk_metadata::{ChunkColumnSummary, ChunkStorage, ChunkSummary, DetailedChunkSummary},
    partition_metadata::TableSummary,
};
use mutable_buffer::chunk::{snapshot::ChunkSnapshot as MBChunkSnapshot, Chunk as MBChunk};
use parquet_file::chunk::Chunk as ParquetChunk;
use read_buffer::Chunk as ReadBufferChunk;

use super::{ChunkIsEmpty, Error, InternalChunkState, Result};
use metrics::{Counter, Histogram, KeyValue};
use snafu::ensure;
use tracker::{TaskRegistration, TaskTracker};

/// Any lifecycle action currently in progress for this chunk
#[derive(Debug, Copy, Clone, PartialEq)]
pub enum ChunkLifecycleAction {
    /// Chunk is in the process of being moved to the read buffer
    Moving,

    /// Chunk is in the process of being written to object storage
    Persisting,

    /// Chunk is in the process of being compacted
    Compacting,
}

impl ChunkLifecycleAction {
    pub fn name(&self) -> &'static str {
        match self {
            Self::Moving => "Moving to the Read Buffer",
            Self::Persisting => "Persisting to Object Storage",
            Self::Compacting => "Compacting",
        }
    }
}

/// A chunk in an _open_ stage.
///
/// Chunks in this stage are writable (= can receive new data) and are never preserved.
#[derive(Debug)]
pub struct ChunkStageOpen {
    /// Mutable Buffer that receives writes.
    pub mb_chunk: MBChunk,
}

/// Different memory representations of a frozen chunk.
#[derive(Debug)]
pub enum ChunkStageFrozenRepr {
    /// Snapshot from the Mutable Buffer, freshly created from the former _open_ chunk. Not ideal for memory consumption
    /// but good enough for the frozen stage. Should ideally be converted into the
    /// [`ReadBuffer`](ChunkStageFrozenRepr::ReadBuffer) rather quickly.
    MutableBufferSnapshot(Arc<MBChunkSnapshot>),

    /// Read Buffer that is optimized for in-memory data processing.
    ReadBuffer(Arc<ReadBufferChunk>),
}

/// A chunk in an _frozen stage.
///
/// Chunks in this stage cannot be modified but are not yet persisted. They can however be compacted which will take two
/// or more chunks and creates a single new frozen chunk.
#[derive(Debug)]
pub struct ChunkStageFrozen {
    /// Internal memory representation of the frozen chunk.
    pub representation: ChunkStageFrozenRepr,
}

/// Chunk in _persisted_ stage.
#[derive(Debug)]
pub struct ChunkStagePersisted {
    /// Parquet chunk that lives immutable within the object store.
    pub parquet: Arc<ParquetChunk>,
    
    /// In-memory version of the parquet data.
    pub read_buffer: Option<Arc<ReadBufferChunk>>,
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
/// A chunk stage lifecycle is linear, i.e. it can never go back. Also note that the _peristed_ stage is the only one
/// that can be restored on node startup (from the persisted catalog). Furthermore, multiple _frozen_ chunks can be
/// compacted into a single one. Nodes at any stage can be dropped via API calls and according to lifecycle policies.
///
/// A chunk can be in-transit when there is a lifecycle job active. A lifecycle job can change the stage once finished
/// (according to the diagram shown above). The chunk stage is considered unchanged as long as the job is running.
#[derive(Debug)]
pub enum ChunkStage {
    /// Chunk can receive new data. It is not persisted.
    Open(ChunkStageOpen),

    /// Chunk cannot receive new data. It is not persisted.
    Frozen(ChunkStageFrozen),

    /// Chunk cannot receive new data. It is persisted.
    Persisted(ChunkStagePersisted),
}

impl ChunkStage {
    pub fn name(&self) -> &'static str {
        match self {
            Self::Open(_) => "Open",
            Self::Frozen(_) => "Frozen",
            Self::Persisted(_) => "Persisted",
        }
    }
}

/// The catalog representation of a Chunk in IOx. Note that a chunk
/// may exist in several physical locations at any given time (e.g. in
/// mutable buffer and in read buffer)
///
/// # State Handling
/// The actual chunk _state_ consistest of multiple parts. First there is the [lifecycle _stage_](ChunkStage)
/// which captures the grant movement of a chunk from a unoptimized mutable object to an optimized immutable one. Within
/// these stages there are multiple ways to represent or cache data. This fact is captured by the _stage_-specific chunk
/// _representation_ (e.g. a persisted chunk may have data cached in-memory).
#[derive(Debug)]
pub struct Chunk {
    /// What partition does the chunk belong to?
    partition_key: Arc<str>,

    /// What table does the chunk belong to?
    table_name: Arc<str>,

    /// The ID of the chunk
    id: u32,

    /// The lifecycle stage this chunk is in.
    stage: ChunkStage,

    /// The active lifecycle task if any
    ///
    /// This is stored as a TaskTracker to allow monitoring the progress of the
    /// action, detecting if the task failed, waiting for the task to complete
    /// or even triggering graceful termination of it
    lifecycle_action: Option<TaskTracker<ChunkLifecycleAction>>,

    /// The metrics for this chunk
    metrics: ChunkMetrics,

    /// The TableSummary, including statistics, for the table in this
    /// Chunk, if known
    table_summary: Option<Arc<TableSummary>>,

    /// Time at which the first data was written into this chunk. Note
    /// this is not the same as the timestamps on the data itself
    time_of_first_write: Option<DateTime<Utc>>,

    /// Most recent time at which data write was initiated into this
    /// chunk. Note this is not the same as the timestamps on the data
    /// itself
    time_of_last_write: Option<DateTime<Utc>>,

    /// Time at which this chunk was maked as closed. Note this is
    /// not the same as the timestamps on the data itself
    time_closed: Option<DateTime<Utc>>,
}

macro_rules! unexpected_state {
    ($SELF: expr, $OP: expr, $EXPECTED: expr, $STATE: expr) => {
        InternalChunkState {
            partition_key: $SELF.partition_key.as_ref(),
            table_name: $SELF.table_name.as_ref(),
            chunk_id: $SELF.id,
            operation: $OP,
            expected: $EXPECTED,
            actual: $STATE.name(),
        }
        .fail()
    };
}

#[derive(Debug)]
pub struct ChunkMetrics {
    pub(super) state: Counter,
    pub(super) immutable_chunk_size: Histogram,
}

impl ChunkMetrics {
    /// Creates an instance of ChunkMetrics that isn't registered with a central
    /// metrics registry. Observations made to instruments on this ChunkMetrics instance
    /// will therefore not be visible to other ChunkMetrics instances or metric instruments
    /// created on a metrics domain, and vice versa
    pub fn new_unregistered() -> Self {
        Self {
            state: Counter::new_unregistered(),
            immutable_chunk_size: Histogram::new_unregistered(),
        }
    }
}

impl Chunk {
    /// Creates a new open chunk from the provided MUB chunk.
    ///
    /// Returns an error if the provided chunk is empty, otherwise creates a new open chunk and records a write at the
    /// current time.
    ///
    /// Apart from [`new_object_store_only`](Self::new_object_store_only) this is the only way to create new chunks.
    pub(crate) fn new_open(
        chunk_id: u32,
        partition_key: impl AsRef<str>,
        chunk: mutable_buffer::chunk::Chunk,
        metrics: ChunkMetrics,
    ) -> Result<Self> {
        ensure!(
            chunk.rows() > 0,
            ChunkIsEmpty {
                partition_key: partition_key.as_ref(),
                chunk_id,
            }
        );

        let table_name = Arc::clone(&chunk.table_name());
        let table_summary = None;
        let stage = ChunkStage::Open(ChunkStageOpen { mb_chunk: chunk });
        metrics
            .state
            .inc_with_labels(&[KeyValue::new("state", "open")]);

        let mut chunk = Self {
            partition_key: Arc::from(partition_key.as_ref()),
            table_name,
            id: chunk_id,
            stage,
            lifecycle_action: None,
            metrics,
            table_summary,
            time_of_first_write: None,
            time_of_last_write: None,
            time_closed: None,
        };
        chunk.record_write();
        Ok(chunk)
    }

    /// Creates a new chunk that is only registered via an object store reference (= only exists in parquet).
    ///
    /// Apart from [`new_open`](Self::new_open) this is the only way to create new chunks.
    pub(crate) fn new_object_store_only(
        chunk_id: u32,
        partition_key: impl AsRef<str>,
        chunk: Arc<parquet_file::chunk::Chunk>,
        metrics: ChunkMetrics,
    ) -> Self {
        // workaround until https://github.com/influxdata/influxdb_iox/issues/1295 is fixed
        let table_name = Arc::from(
            chunk
                .table_names(None)
                .next()
                .expect("chunk must have exactly 1 table")
                .as_ref(),
        );
        // Cache table summary
        let table_summary = Some(Arc::clone(chunk.table_summary()));
        let stage = ChunkStage::Persisted(ChunkStagePersisted {
            parquet: chunk,
            read_buffer: None,
        });

        Self {
            partition_key: Arc::from(partition_key.as_ref()),
            table_name,
            id: chunk_id,
            stage,
            lifecycle_action: None,
            metrics,
            table_summary,
            time_of_first_write: None,
            time_of_last_write: None,
            time_closed: None,
        }
    }

    /// Used for testing
    #[cfg(test)]
    pub(crate) fn set_timestamps(
        &mut self,
        time_of_first_write: Option<DateTime<Utc>>,
        time_of_last_write: Option<DateTime<Utc>>,
    ) {
        self.time_of_first_write = time_of_first_write;
        self.time_of_last_write = time_of_last_write;
    }

    pub fn id(&self) -> u32 {
        self.id
    }

    pub fn key(&self) -> &str {
        self.partition_key.as_ref()
    }

    pub fn table_name(&self) -> &str {
        self.table_name.as_ref()
    }

    pub fn stage(&self) -> &ChunkStage {
        &self.stage
    }

    pub fn lifecycle_action(&self) -> Option<&ChunkLifecycleAction> {
        self.lifecycle_action.as_ref().map(|x| x.metadata())
    }

    pub fn time_of_first_write(&self) -> Option<DateTime<Utc>> {
        self.time_of_first_write
    }

    pub fn time_of_last_write(&self) -> Option<DateTime<Utc>> {
        self.time_of_last_write
    }

    pub fn time_closed(&self) -> Option<DateTime<Utc>> {
        self.time_closed
    }

    /// Update the write timestamps for this chunk
    pub fn record_write(&mut self) {
        let now = Utc::now();
        if self.time_of_first_write.is_none() {
            self.time_of_first_write = Some(now);
        }
        self.time_of_last_write = Some(now);
    }

    /// Return ChunkSummary metadata for this chunk
    pub fn summary(&self) -> ChunkSummary {
        let (row_count, storage) = match &self.stage {
            ChunkStage::Open(stage) => (stage.mb_chunk.rows(), ChunkStorage::OpenMutableBuffer),
            ChunkStage::Frozen(stage) => match &stage.representation {
                ChunkStageFrozenRepr::MutableBufferSnapshot(repr) => {
                    (repr.rows(), ChunkStorage::ClosedMutableBuffer)
                }
                ChunkStageFrozenRepr::ReadBuffer(repr) => {
                    (repr.rows() as usize, ChunkStorage::ReadBuffer)
                }
            },
            ChunkStage::Persisted(stage) => {
                let rows = stage.parquet.rows() as usize;
                let storage = if stage.read_buffer.is_some() {
                    ChunkStorage::ReadBufferAndObjectStore
                } else {
                    ChunkStorage::ObjectStoreOnly
                };
                (rows, storage)
            }
        };

        ChunkSummary {
            partition_key: Arc::clone(&self.partition_key),
            table_name: Arc::clone(&self.table_name),
            id: self.id,
            storage,
            estimated_bytes: self.size(),
            row_count,
            time_of_first_write: self.time_of_first_write,
            time_of_last_write: self.time_of_last_write,
            time_closed: self.time_closed,
        }
    }

    /// Return information about the storage in this Chunk
    pub fn detailed_summary(&self) -> DetailedChunkSummary {
        let inner = self.summary();

        fn to_summary(v: (&str, usize)) -> ChunkColumnSummary {
            ChunkColumnSummary {
                name: v.0.into(),
                estimated_bytes: v.1,
            }
        }

        let columns: Vec<ChunkColumnSummary> = match &self.stage {
            ChunkStage::Open(stage) => stage.mb_chunk.column_sizes().map(to_summary).collect(),
            ChunkStage::Frozen(stage) => match &stage.representation {
                ChunkStageFrozenRepr::MutableBufferSnapshot(repr) => {
                    repr.column_sizes().map(to_summary).collect()
                }
                ChunkStageFrozenRepr::ReadBuffer(repr) => repr.column_sizes(&self.table_name),
            },
            ChunkStage::Persisted(stage) => {
                if let Some(read_buffer) = &stage.read_buffer {
                    read_buffer.column_sizes(&self.table_name)
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
            ChunkStage::Open(stage) => {
                // The stats for open chunks change so can't be cached
                Arc::new(stage.mb_chunk.table_summary())
            }
            _ => {
                let table_summary = self
                    .table_summary
                    .as_ref()
                    .expect("Table summary not set for non open chunk");
                Arc::clone(table_summary)
            }
        }
    }

    /// Returns an approximation of the amount of process memory consumed by the
    /// chunk
    pub fn size(&self) -> usize {
        match &self.stage {
            ChunkStage::Open(stage) => stage.mb_chunk.size(),
            ChunkStage::Frozen(stage) => match &stage.representation {
                ChunkStageFrozenRepr::MutableBufferSnapshot(repr) => repr.size(),
                ChunkStageFrozenRepr::ReadBuffer(repr) => repr.size(),
            },
            ChunkStage::Persisted(stage) => {
                let mut size = stage.parquet.size();
                if let Some(read_buffer) = &stage.read_buffer {
                    size += read_buffer.size();
                }
                size
            }
        }
    }

    /// Returns a mutable reference to the mutable buffer storage for
    /// chunks in the Open state
    ///
    /// Must be in open or closed state
    pub fn mutable_buffer(&mut self) -> Result<&mut MBChunk> {
        match &mut self.stage {
            ChunkStage::Open(stage) => Ok(&mut stage.mb_chunk),
            stage => unexpected_state!(self, "mutable buffer reference", "Open or Closed", stage),
        }
    }

    /// Set the chunk to the Closed state
    pub fn set_closed(&mut self) -> Result<Arc<MBChunkSnapshot>> {
        match &self.stage {
            ChunkStage::Open(stage) => {
                assert!(self.time_closed.is_none());
                self.time_closed = Some(Utc::now());
                let s = stage.mb_chunk.snapshot();
                self.metrics
                    .state
                    .inc_with_labels(&[KeyValue::new("state", "closed")]);

                self.metrics.immutable_chunk_size.observe_with_labels(
                    stage.mb_chunk.size() as f64,
                    &[KeyValue::new("state", "closed")],
                );

                // Cache table summary
                self.table_summary = Some(Arc::new(stage.mb_chunk.table_summary()));

                self.stage = ChunkStage::Frozen(ChunkStageFrozen {
                    representation: ChunkStageFrozenRepr::MutableBufferSnapshot(Arc::clone(&s)),
                });
                Ok(s)
            }
            _ => {
                unexpected_state!(self, "setting closed", "Open or Closed", &self.stage)
            }
        }
    }

    /// Set the chunk to the Moving state, returning a handle to the underlying
    /// storage
    ///
    /// If called on an open chunk will first close the chunk
    pub fn set_moving(&mut self, registration: &TaskRegistration) -> Result<Arc<MBChunkSnapshot>> {
        // This ensures the closing logic is consistent but doesn't break code that
        // assumes a chunk can be moved from open
        if matches!(self.stage, ChunkStage::Open(_)) {
            self.set_closed()?;
        }

        match &self.stage {
            ChunkStage::Frozen(stage) => match &stage.representation {
                ChunkStageFrozenRepr::MutableBufferSnapshot(repr) => {
                    let chunk = Arc::clone(repr);
                    self.set_lifecycle_action(ChunkLifecycleAction::Moving, registration)?;

                    self.metrics
                        .state
                        .inc_with_labels(&[KeyValue::new("state", "moving")]);

                    self.metrics.immutable_chunk_size.observe_with_labels(
                        chunk.size() as f64,
                        &[KeyValue::new("state", "moving")],
                    );

                    Ok(chunk)
                }
                ChunkStageFrozenRepr::ReadBuffer(_) => InternalChunkState {
                    partition_key: self.partition_key.as_ref(),
                    table_name: self.table_name.as_ref(),
                    chunk_id: self.id,
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

    /// Set the chunk in the Moved state, setting the underlying
    /// storage handle to db, and discarding the underlying mutable buffer
    /// storage.
    pub fn set_moved(&mut self, chunk: Arc<ReadBufferChunk>) -> Result<()> {
        match &mut self.stage {
            ChunkStage::Frozen(stage) => match &stage.representation {
                ChunkStageFrozenRepr::MutableBufferSnapshot(_) => {
                    self.metrics
                        .state
                        .inc_with_labels(&[KeyValue::new("state", "moved")]);

                    self.metrics.immutable_chunk_size.observe_with_labels(
                        chunk.size() as f64,
                        &[KeyValue::new("state", "moved")],
                    );

                    stage.representation = ChunkStageFrozenRepr::ReadBuffer(chunk);
                    self.finish_lifecycle_action(ChunkLifecycleAction::Moving)?;
                    Ok(())
                }
                ChunkStageFrozenRepr::ReadBuffer(_) => InternalChunkState {
                    partition_key: self.partition_key.as_ref(),
                    table_name: self.table_name.as_ref(),
                    chunk_id: self.id,
                    operation: "setting moved",
                    expected: "Frozen with MutableBufferSnapshot",
                    actual: "Frozen with ReadBuffer",
                }
                .fail(),
            },
            _ => {
                unexpected_state!(self, "setting moved", "Moving", self.stage)
            }
        }
    }

    /// Set the chunk to the MovingToObjectStore state
    pub fn set_writing_to_object_store(
        &mut self,
        registration: &TaskRegistration,
    ) -> Result<Arc<ReadBufferChunk>> {
        match &self.stage {
            ChunkStage::Frozen(stage) => {
                match &stage.representation {
                    ChunkStageFrozenRepr::MutableBufferSnapshot(_) => {
                        // TODO: ideally we would support all Frozen representations
                        InternalChunkState {
                            partition_key: self.partition_key.as_ref(),
                            table_name: self.table_name.as_ref(),
                            chunk_id: self.id,
                            operation: "setting object store",
                            expected: "Frozen with ReadBuffer",
                            actual: "Frozen with MutableBufferSnapshot",
                        }
                        .fail()
                    }
                    ChunkStageFrozenRepr::ReadBuffer(repr) => {
                        let db = Arc::clone(repr);
                        self.set_lifecycle_action(ChunkLifecycleAction::Persisting, registration)?;
                        self.metrics
                            .state
                            .inc_with_labels(&[KeyValue::new("state", "writing_os")]);
                        Ok(db)
                    }
                }
            }
            _ => {
                unexpected_state!(self, "setting object store", "Moved", self.stage)
            }
        }
    }

    /// Set the chunk to the MovedToObjectStore state, returning a handle to the
    /// underlying storage
    pub fn set_written_to_object_store(&mut self, chunk: Arc<ParquetChunk>) -> Result<()> {
        match &self.stage {
            ChunkStage::Frozen(stage) => {
                match &stage.representation {
                    ChunkStageFrozenRepr::MutableBufferSnapshot(_) => {
                        // TODO: ideally we would support all Frozen representations
                        InternalChunkState {
                            partition_key: self.partition_key.as_ref(),
                            table_name: self.table_name.as_ref(),
                            chunk_id: self.id,
                            operation: "setting object store",
                            expected: "Frozen with ReadBuffer",
                            actual: "Frozen with MutableBufferSnapshot",
                        }
                        .fail()
                    }
                    ChunkStageFrozenRepr::ReadBuffer(repr) => {
                        let db = Arc::clone(&repr);
                        self.finish_lifecycle_action(ChunkLifecycleAction::Persisting)?;

                        self.metrics
                            .state
                            .inc_with_labels(&[KeyValue::new("state", "rub_and_os")]);

                        self.metrics.immutable_chunk_size.observe_with_labels(
                            (chunk.size() + db.size()) as f64,
                            &[KeyValue::new("state", "rub_and_os")],
                        );

                        self.stage = ChunkStage::Persisted(ChunkStagePersisted {
                            parquet: chunk,
                            read_buffer: Some(db),
                        });
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

    pub fn set_unload_from_read_buffer(&mut self) -> Result<Arc<ReadBufferChunk>> {
        match &mut self.stage {
            ChunkStage::Persisted(stage) => {
                if let Some(read_buffer) = &stage.read_buffer {
                    self.metrics
                        .state
                        .inc_with_labels(&[KeyValue::new("state", "os")]);

                    self.metrics.immutable_chunk_size.observe_with_labels(
                        stage.parquet.size() as f64,
                        &[KeyValue::new("state", "os")],
                    );

                    let rub_chunk = Arc::clone(read_buffer);
                    stage.read_buffer = None;
                    Ok(rub_chunk)
                } else {
                    // TODO: do we really need to error here or should unloading an unloaded chunk be a no-op?
                    InternalChunkState {
                        partition_key: self.partition_key.as_ref(),
                        table_name: self.table_name.as_ref(),
                        chunk_id: self.id,
                        operation: "setting unload",
                        expected: "Persisted with ReadBuffer",
                        actual: "Persisted without ReadBuffer",
                    }
                    .fail()
                }
            }
            _ => {
                unexpected_state!(self, "setting unload", "WrittenToObjectStore", &self.stage)
            }
        }
    }

    /// Set the chunk's in progress lifecycle action or return an error if already in-progress
    fn set_lifecycle_action(
        &mut self,
        lifecycle_action: ChunkLifecycleAction,
        registration: &TaskRegistration,
    ) -> Result<()> {
        if let Some(lifecycle_action) = &self.lifecycle_action {
            return Err(Error::LifecycleActionAlreadyInProgress {
                partition_key: self.partition_key.to_string(),
                table_name: self.table_name.to_string(),
                chunk_id: self.id,
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
                    partition_key: self.partition_key.to_string(),
                    table_name: self.table_name.to_string(),
                    chunk_id: self.id,
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
}
