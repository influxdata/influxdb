use super::DbChunk;
use crate::{
    catalog::{chunk::CatalogChunk, partition::Partition},
    Db,
};
use ::lifecycle::{LifecycleDb, LifecyclePolicy};
use data_types::{
    chunk_metadata::{ChunkAddr, ChunkId, ChunkLifecycleAction, ChunkOrder, ChunkStorage},
    database_rules::LifecycleRules,
    error::ErrorLogger,
    job::Job,
    partition_metadata::{PartitionAddr, Statistics},
    DatabaseName,
};
use datafusion::physical_plan::SendableRecordBatchStream;
use futures::{
    future::{BoxFuture, Shared},
    FutureExt, TryFutureExt,
};
use internal_types::access::AccessMetrics;
use lifecycle::{
    LifecycleChunk, LifecyclePartition, LifecycleReadGuard, LifecycleWriteGuard, LockableChunk,
    LockablePartition,
};
use observability_deps::tracing::{info, trace, warn};
use parking_lot::Mutex;
use persistence_windows::persistence_windows::FlushHandle;
use query::QueryChunkMeta;
use schema::{merge::SchemaMerger, Schema, TIME_COLUMN_NAME};
use std::{future::Future, sync::Arc};
use time::{Time, TimeProvider};
use tokio::task::JoinError;
use tokio_util::sync::CancellationToken;
use tracker::{RwLock, TaskTracker};

pub(crate) use compact::compact_chunks;
pub(crate) use drop::{drop_chunk, drop_partition};
pub(crate) use error::{Error, Result};
pub(crate) use persist::persist_chunks;
pub(crate) use unload::unload_read_buffer_chunk;

mod compact;
pub(crate) mod compact_object_store;
mod drop;
mod error;
mod load;
mod persist;
mod unload;
mod write;

/// A newtype wrapper around `Arc<Db>` to workaround trait orphan rules
#[derive(Debug, Clone)]
pub struct ArcDb(Arc<Db>);

impl ArcDb {
    pub fn new(db: Arc<Db>) -> Self {
        Self(db)
    }
}

///
/// A `LockableCatalogChunk` combines a `CatalogChunk` with its owning `Db`
///
/// This provides the `lifecycle::LockableChunk` trait which can be used to lock
/// the chunk, determine what to do, and then optionally trigger an action, all
/// without allowing concurrent modification
///
#[derive(Debug, Clone)]
pub struct LockableCatalogChunk {
    pub db: Arc<Db>,
    pub chunk: Arc<RwLock<CatalogChunk>>,
    pub id: ChunkId,
    pub order: ChunkOrder,
}

impl LockableChunk for LockableCatalogChunk {
    type Chunk = CatalogChunk;

    type Job = Job;

    type Error = Error;

    fn read(&self) -> LifecycleReadGuard<'_, Self::Chunk, Self> {
        LifecycleReadGuard::new(self.clone(), self.chunk.as_ref())
    }

    fn write(&self) -> LifecycleWriteGuard<'_, Self::Chunk, Self> {
        LifecycleWriteGuard::new(self.clone(), self.chunk.as_ref())
    }

    fn unload_read_buffer(
        chunk: LifecycleWriteGuard<'_, Self::Chunk, Self>,
    ) -> Result<(), Self::Error> {
        info!(%chunk, "unloading from readbuffer");

        let _ = self::unload::unload_read_buffer_chunk(chunk)?;
        Ok(())
    }

    fn load_read_buffer(
        chunk: LifecycleWriteGuard<'_, Self::Chunk, Self>,
    ) -> Result<TaskTracker<Self::Job>, Self::Error> {
        info!(%chunk, "loading chunk to read buffer");
        let (tracker, fut) = load::load_chunk(chunk)?;
        let _ = tokio::spawn(async move { fut.await.log_if_error("compacting chunks") });
        Ok(tracker)
    }

    fn id(&self) -> ChunkId {
        self.id
    }

    fn order(&self) -> ChunkOrder {
        self.order
    }
}

/// A newtype wrapper around persistence_windows::FlushHandle
///
/// Represents the context for flushing data out of the PersistenceWindows
#[derive(Debug)]
pub struct CatalogPersistHandle(FlushHandle);

impl lifecycle::PersistHandle for CatalogPersistHandle {
    fn timestamp(&self) -> Time {
        self.0.timestamp()
    }
}

///
/// A `LockableCatalogPartition` combines a `Partition` with its owning `Db`
///
/// This provides the `lifecycle::LockablePartition` trait which can be used to lock
/// the chunk, determine what to do, and then optionally trigger an action, all
/// without allowing concurrent modification
///
#[derive(Debug, Clone)]
pub struct LockableCatalogPartition {
    pub db: Arc<Db>,
    pub partition: Arc<RwLock<Partition>>,
}

impl LockableCatalogPartition {
    pub fn new(db: Arc<Db>, partition: Arc<RwLock<Partition>>) -> Self {
        Self { db, partition }
    }
}

impl LockablePartition for LockableCatalogPartition {
    type Partition = Partition;

    type Chunk = LockableCatalogChunk;

    type PersistHandle = CatalogPersistHandle;

    type Error = super::lifecycle::Error;

    fn read(&self) -> LifecycleReadGuard<'_, Partition, Self> {
        LifecycleReadGuard::new(self.clone(), self.partition.as_ref())
    }

    fn write(&self) -> LifecycleWriteGuard<'_, Partition, Self> {
        LifecycleWriteGuard::new(self.clone(), self.partition.as_ref())
    }

    fn chunk(
        s: &LifecycleReadGuard<'_, Partition, Self>,
        chunk_id: ChunkId,
    ) -> Option<Self::Chunk> {
        s.chunk(chunk_id)
            .map(|(chunk, order)| LockableCatalogChunk {
                db: Arc::clone(&s.data().db),
                chunk: Arc::clone(chunk),
                id: chunk_id,
                order,
            })
    }

    fn chunks(s: &LifecycleReadGuard<'_, Partition, Self>) -> Vec<Self::Chunk> {
        s.keyed_chunks()
            .into_iter()
            .map(|(id, order, chunk)| LockableCatalogChunk {
                db: Arc::clone(&s.data().db),
                chunk: Arc::clone(chunk),
                id,
                order,
            })
            .collect()
    }

    fn compact_chunks(
        partition: LifecycleWriteGuard<'_, Partition, Self>,
        chunks: Vec<LifecycleWriteGuard<'_, CatalogChunk, Self::Chunk>>,
    ) -> Result<TaskTracker<Job>, Self::Error> {
        info!(table=%partition.table_name(), partition=%partition.partition_key(), "compacting chunks");
        let (tracker, fut) = compact::compact_chunks(partition, chunks)?;
        let _ = tokio::spawn(async move { fut.await.log_if_error("compacting chunks") });
        Ok(tracker)
    }

    fn compact_object_store_chunks(
        partition: LifecycleWriteGuard<'_, Partition, Self>,
        chunks: Vec<LifecycleWriteGuard<'_, CatalogChunk, Self::Chunk>>,
    ) -> Result<TaskTracker<Job>, Self::Error> {
        info!(table=%partition.table_name(), partition=%partition.partition_key(), "compacting object store chunks");
        let (tracker, fut) = compact_object_store::compact_object_store_chunks(partition, chunks)?;
        let _ =
            tokio::spawn(async move { fut.await.log_if_error("compacting object store chunks") });
        Ok(tracker)
    }

    fn prepare_persist(
        partition: &mut LifecycleWriteGuard<'_, Self::Partition, Self>,
        force: bool,
    ) -> Option<Self::PersistHandle> {
        let window = partition.persistence_windows_mut().unwrap();
        let handle = match force {
            true => window.flush_all_handle(),
            false => window.flush_handle(),
        };
        trace!(?handle, "preparing for persist");
        Some(CatalogPersistHandle(handle?))
    }

    fn persist_chunks(
        partition: LifecycleWriteGuard<'_, Partition, Self>,
        chunks: Vec<LifecycleWriteGuard<'_, CatalogChunk, Self::Chunk>>,
        handle: Self::PersistHandle,
    ) -> Result<TaskTracker<Job>, Self::Error> {
        info!(table=%partition.table_name(), partition=%partition.partition_key(), "persisting chunks");
        let (tracker, fut) = persist::persist_chunks(partition, chunks, handle.0)?;
        let _ = tokio::spawn(async move { fut.await.log_if_error("persisting chunks") });
        Ok(tracker)
    }

    fn drop_chunk(
        partition: LifecycleWriteGuard<'_, Self::Partition, Self>,
        chunk: LifecycleWriteGuard<'_, CatalogChunk, Self::Chunk>,
    ) -> Result<TaskTracker<Job>, Self::Error> {
        info!(
            table=%partition.table_name(),
            partition=%partition.partition_key(),
            chunk_id=%chunk.addr().chunk_id.get(),
            "drop chunk",
        );
        let (tracker, fut) = drop::drop_chunk(partition, chunk)?;
        let _ = tokio::spawn(async move { fut.await.log_if_error("drop chunk") });
        Ok(tracker)
    }
}

impl LifecycleDb for ArcDb {
    type Chunk = LockableCatalogChunk;
    type Partition = LockableCatalogPartition;

    fn buffer_size(&self) -> usize {
        self.0.catalog.metrics().memory().total()
    }

    fn rules(&self) -> LifecycleRules {
        self.0.rules.read().lifecycle_rules.clone()
    }

    fn partitions(&self) -> Vec<Self::Partition> {
        self.0
            .catalog
            .partitions()
            .into_iter()
            .map(|partition| LockableCatalogPartition::new(Arc::clone(&self.0), partition))
            .collect()
    }

    fn name(&self) -> DatabaseName<'static> {
        self.0.rules.read().name.clone()
    }

    fn time_provider(&self) -> &Arc<dyn TimeProvider> {
        &self.0.time_provider
    }
}

impl LifecyclePartition for Partition {
    fn partition_key(&self) -> &str {
        self.key()
    }

    fn is_persisted(&self) -> bool {
        self.persistence_windows()
            .map(|w| w.minimum_unpersisted_age().is_none())
            .unwrap_or(true)
    }

    fn persistable_row_count(&self) -> usize {
        self.persistence_windows()
            .map(|w| w.persistable_row_count())
            .unwrap_or(0)
    }

    fn minimum_unpersisted_age(&self) -> Option<Time> {
        self.persistence_windows()
            .and_then(|w| w.minimum_unpersisted_age())
    }
}

impl LifecycleChunk for CatalogChunk {
    fn lifecycle_action(&self) -> Option<&TaskTracker<ChunkLifecycleAction>> {
        self.lifecycle_action()
    }

    fn clear_lifecycle_action(&mut self) {
        self.clear_lifecycle_action()
            .expect("failed to clear lifecycle action")
    }

    fn min_timestamp(&self) -> Time {
        let table_summary = self.table_summary();
        let col = table_summary
            .columns
            .iter()
            .find(|x| x.name == TIME_COLUMN_NAME)
            .expect("time column expected");

        let min = match &col.stats {
            Statistics::I64(stats) => stats.min.expect("time column cannot be empty"),
            _ => panic!("unexpected time column type"),
        };

        Time::from_timestamp_nanos(min)
    }

    fn access_metrics(&self) -> AccessMetrics {
        self.access_recorder().get_metrics()
    }

    fn time_of_last_write(&self) -> Time {
        self.time_of_last_write()
    }

    fn addr(&self) -> &ChunkAddr {
        self.addr()
    }

    fn storage(&self) -> ChunkStorage {
        self.storage().1
    }

    fn row_count(&self) -> usize {
        self.storage().0
    }
}

/// Executes a plan and collects the results into a read buffer chunk
// This is an async function but has been desugared manually because it's hitting
// https://github.com/rust-lang/rust/issues/63033
fn collect_rub(
    stream: SendableRecordBatchStream,
    partition_addr: &PartitionAddr,
    metric_registry: &metric::Registry,
) -> impl futures::Future<Output = Result<Option<read_buffer::RBChunk>>> {
    use futures::{future, StreamExt, TryStreamExt};

    let db_name = partition_addr.db_name.to_string();
    let table_name = partition_addr.table_name.to_string();
    let chunk_metrics = read_buffer::ChunkMetrics::new(metric_registry, db_name);

    async move {
        let mut adapted_stream = stream.try_filter(|batch| future::ready(batch.num_rows() > 0));

        let first_batch = match adapted_stream.next().await {
            Some(rb_result) => rb_result?,
            // At least one RecordBatch is required to create a read_buffer::Chunk
            None => return Ok(None),
        };
        let mut chunk = read_buffer::RBChunk::new(table_name, first_batch, chunk_metrics);

        adapted_stream
            .try_for_each(|batch| {
                chunk.upsert_table(batch);
                future::ready(Ok(()))
            })
            .await?;

        Ok(Some(chunk))
    }
}

/// Return the merged schema for the chunks that are being
/// reorganized.
///
/// This is infallable because the schemas of chunks within a
/// partition are assumed to be compatible because that schema was
/// enforced as part of writing into the partition
fn merge_schemas(chunks: &[Arc<DbChunk>]) -> Arc<Schema> {
    let mut merger = SchemaMerger::new();
    for db_chunk in chunks {
        merger = merger
            .merge(&db_chunk.schema())
            .expect("schemas compatible");
    }
    Arc::new(merger.build())
}

/// A lifecycle worker manages a background task that drives a [`LifecyclePolicy`]
#[derive(Debug)]
pub struct LifecycleWorker {
    /// Future that resolves when the background worker exits
    join: Shared<BoxFuture<'static, Result<(), Arc<JoinError>>>>,

    /// Shared worker state
    state: Arc<WorkerState>,
}

#[derive(Debug)]
struct WorkerState {
    policy: Mutex<LifecyclePolicy<ArcDb>>,

    shutdown: CancellationToken,
}

impl LifecycleWorker {
    /// Creates a new `LifecycleWorker`
    ///
    /// The worker starts with persistence suppressed, persistence must be enabled
    /// by a call to [`LifecycleWorker::unsuppress_persistence`]
    pub fn new(db: Arc<Db>) -> Self {
        let db = ArcDb::new(db);
        let shutdown = CancellationToken::new();

        let policy = LifecyclePolicy::new_suppress_persistence(db);

        let state = Arc::new(WorkerState {
            policy: Mutex::new(policy),
            shutdown,
        });

        let join = tokio::spawn(background_worker(Arc::clone(&state)))
            .map_err(Arc::new)
            .boxed()
            .shared();

        Self { join, state }
    }

    /// Stop suppressing persistence and allow it if the database rules allow it.
    pub fn unsuppress_persistence(&self) {
        self.state.policy.lock().unsuppress_persistence()
    }

    /// Triggers shutdown of this `WriteBufferConsumer`
    pub fn shutdown(&self) {
        self.state.shutdown.cancel()
    }

    /// Waits for the background worker of this `Database` to exit
    pub fn join(&self) -> impl Future<Output = Result<(), Arc<JoinError>>> {
        self.join.clone()
    }
}

impl Drop for LifecycleWorker {
    fn drop(&mut self) {
        if !self.state.shutdown.is_cancelled() {
            warn!("lifecycle worker dropped without calling shutdown()");
            self.state.shutdown.cancel();
        }

        if self.join.clone().now_or_never().is_none() {
            warn!("lifecycle worker dropped without waiting for worker termination");
        }
    }
}

async fn background_worker(state: Arc<WorkerState>) {
    loop {
        let fut = state.policy.lock().check_for_work();
        tokio::select! {
            _ = fut => {},
            _ = state.shutdown.cancelled() => {
                info!("lifecycle worker shutting down");
                break
            }
        }
    }
}
