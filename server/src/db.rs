//! This module contains the main IOx Database object which has the
//! instances of the mutable buffer, read buffer, and object store

pub(crate) use crate::db::chunk::DbChunk;
use crate::{
    db::{
        access::QueryCatalogAccess,
        catalog::{
            chunk::{CatalogChunk, ChunkStage},
            partition::Partition,
            Catalog, TableNameFilter,
        },
        lifecycle::{LockableCatalogChunk, LockableCatalogPartition},
    },
    write_buffer::WriteBuffer,
    JobRegistry,
};
use ::lifecycle::{LockableChunk, LockablePartition};
use async_trait::async_trait;
use chrono::Utc;
use data_types::{
    chunk_metadata::ChunkSummary,
    database_rules::DatabaseRules,
    partition_metadata::{PartitionSummary, TableSummary},
    server_id::ServerId,
};
use datafusion::catalog::{catalog::CatalogProvider, schema::SchemaProvider};
use entry::{Entry, SequencedEntry};
use metrics::KeyValue;
use mutable_buffer::chunk::{ChunkMetrics as MutableBufferChunkMetrics, MBChunk};
use mutable_buffer::persistence_windows::PersistenceWindows;
use object_store::{path::parsed::DirsAndFileName, ObjectStore};
use observability_deps::tracing::{debug, error, info};
use parking_lot::RwLock;
use parquet_file::{
    catalog::{CheckpointData, PreservedCatalog},
    cleanup::{delete_files as delete_parquet_files, get_unreferenced_parquet_files},
};
use query::{exec::Executor, predicate::Predicate, QueryDatabase};
use rand_distr::{Distribution, Poisson};
use snafu::{ensure, ResultExt, Snafu};
use std::{
    any::Any,
    collections::HashMap,
    num::NonZeroUsize,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    time::{Duration, Instant},
};

pub mod access;
pub mod catalog;
mod chunk;
mod lifecycle;
pub mod load;
pub mod pred;
mod process_clock;
mod streams;
mod system_tables;

#[allow(clippy::large_enum_variant)]
#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(context(false))]
    CatalogError { source: catalog::Error },

    #[snafu(context(false))]
    PartitionError { source: catalog::partition::Error },

    #[snafu(display("Lifecycle error: {}", source))]
    LifecycleError { source: lifecycle::Error },

    #[snafu(display("Error freeinzing chunk while rolling over partition: {}", source))]
    FreezingChunk { source: catalog::chunk::Error },

    #[snafu(display("Error sending entry to write buffer"))]
    WriteBufferError {
        source: Box<dyn std::error::Error + Sync + Send>,
    },

    #[snafu(display("Cannot write to this database: no mutable buffer configured"))]
    DatabaseNotWriteable {},

    #[snafu(display("Hard buffer size limit reached"))]
    HardLimitReached {},

    #[snafu(display("Can not write entry {}:{} : {}", partition_key, chunk_id, source))]
    WriteEntry {
        partition_key: String,
        chunk_id: u32,
        source: mutable_buffer::chunk::Error,
    },

    #[snafu(display("Can not write entry to new MUB chunk {} : {}", partition_key, source))]
    WriteEntryInitial {
        partition_key: String,
        source: mutable_buffer::chunk::Error,
    },

    #[snafu(display(
        "Storing sequenced entry failed with the following error(s), and possibly more: {}",
        errors.iter().map(ToString::to_string).collect::<Vec<_>>().join(", ")
    ))]
    StoreSequencedEntryFailures { errors: Vec<Error> },

    #[snafu(display("Error building sequenced entry: {}", source))]
    SequencedEntryError { source: entry::SequencedEntryError },

    #[snafu(display("background task cancelled: {}", source))]
    TaskCancelled { source: futures::future::Aborted },

    #[snafu(display("error finding min/max time on table batch: {}", source))]
    TableBatchTimeError { source: entry::Error },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

/// This is the main IOx Database object. It is the root object of any
/// specific InfluxDB IOx instance
///
///
/// The data in a `Db` is structured in this way:
///
/// ┌───────────────────────────────────────────────┐
/// │                                               │
/// │    ┌────────────────┐                         │
/// │    │    Database    │                         │
/// │    └────────────────┘                         │
/// │             │ one partition per               │
/// │             │ partition_key                   │
/// │             ▼                                 │
/// │    ┌────────────────┐                         │
/// │    │   Partition    │                         │
/// │    └────────────────┘                         │
/// │             │  multiple Tables (measurements) │
/// │             ▼                                 │
/// │    ┌────────────────┐                         │
/// │    │     Table      │                         │
/// │    └────────────────┘                         │
/// │             │  one open Chunk                 │
/// │             │  zero or more closed            │
/// │             ▼  Chunks                         │
/// │    ┌────────────────┐                         │
/// │    │     Chunk      │                         │
/// │    └────────────────┘                         │
/// │             │  multiple Colums                │
/// │             ▼                                 │
/// │    ┌────────────────┐                         │
/// │    │     Column     │                         │
/// │    └────────────────┘                         │
/// │                              MutableBuffer    │
/// │                                               │
/// └───────────────────────────────────────────────┘
///
/// Each row of data is routed into a particular partitions based on
/// column values in that row. The partition's open chunk is updated
/// with the new data.
///
/// The currently open chunk in a partition can be rolled over. When
/// this happens, the chunk is closed (becomes read-only) and stops
/// taking writes. Any new writes to the same partition will create a
/// new active open chunk.
///
/// Catalog Usage: the state of the catalog and the state of the `Db`
/// must remain in sync. If they are ever out of sync, the IOx system
/// should be shutdown and forced through a "recovery" to correctly
/// reconcile the state.
///
/// Ensuring the Catalog and Db remain in sync is accomplished by
/// manipulating the catalog state alongside the state in the `Db`
/// itself. The catalog state can be observed (but not mutated) by things
/// outside of the Db
#[derive(Debug)]
pub struct Db {
    pub rules: RwLock<DatabaseRules>,

    pub server_id: ServerId, // this is also the Query Server ID

    /// Interface to use for persistence
    pub store: Arc<ObjectStore>,

    /// Executor for running queries
    exec: Arc<Executor>,

    /// Preserved catalog (data in object store).
    preserved_catalog: Arc<PreservedCatalog>,

    /// The catalog holds chunks of data under partitions for the database.
    /// The underlying chunks may be backed by different execution engines
    /// depending on their stage in the data lifecycle. Currently there are
    /// three backing engines for Chunks:
    ///
    ///  - The Mutable Buffer where chunks are mutable but also queryable;
    ///  - The Read Buffer where chunks are immutable and stored in an optimised
    ///    compressed form for small footprint and fast query execution; and
    ///  - The Parquet Buffer where chunks are backed by Parquet file data.
    catalog: Arc<Catalog>,

    /// A handle to the global jobs registry for long running tasks
    jobs: Arc<JobRegistry>,

    /// The metrics registry to inject into created components in the Db.
    metrics_registry: Arc<metrics::MetricRegistry>,

    /// Catalog interface for query
    catalog_access: Arc<QueryCatalogAccess>,

    /// Process clock used in establishing a partial ordering of operations via a Lamport Clock.
    ///
    /// Value is nanoseconds since the Unix Epoch.
    process_clock: process_clock::ProcessClock,

    /// Number of iterations of the worker lifecycle loop for this Db
    worker_iterations_lifecycle: AtomicUsize,

    /// Number of iterations of the worker cleanup loop for this Db
    worker_iterations_cleanup: AtomicUsize,

    /// Metric labels
    metric_labels: Vec<KeyValue>,

    /// Optionally buffer writes
    write_buffer: Option<Arc<dyn WriteBuffer>>,

    /// Lock that prevents the cleanup job from deleting files that are written but not yet added to the preserved
    /// catalog.
    ///
    /// The cleanup job needs exclusive access and hence will acquire a write-guard. Creating parquet files and creating
    /// catalog transaction only needs shared access and hence will acquire a read-guard.
    cleanup_lock: Arc<tokio::sync::RwLock<()>>,
}

/// All the information needed to commit a database
#[derive(Debug)]
pub(crate) struct DatabaseToCommit {
    pub(crate) server_id: ServerId,
    pub(crate) object_store: Arc<ObjectStore>,
    pub(crate) exec: Arc<Executor>,
    pub(crate) preserved_catalog: PreservedCatalog,
    pub(crate) catalog: Catalog,
    pub(crate) rules: DatabaseRules,
    pub(crate) write_buffer: Option<Arc<dyn WriteBuffer>>,
}

impl Db {
    pub(crate) fn new(database_to_commit: DatabaseToCommit, jobs: Arc<JobRegistry>) -> Self {
        let db_name = database_to_commit.rules.name.clone();

        let rules = RwLock::new(database_to_commit.rules);
        let server_id = database_to_commit.server_id;
        let store = Arc::clone(&database_to_commit.object_store);
        let metrics_registry = Arc::clone(&database_to_commit.catalog.metrics_registry);
        let metric_labels = database_to_commit.catalog.metric_labels.clone();
        let catalog = Arc::new(database_to_commit.catalog);

        let catalog_access = QueryCatalogAccess::new(
            &db_name,
            Arc::clone(&catalog),
            Arc::clone(&jobs),
            Arc::clone(&metrics_registry),
            metric_labels.clone(),
        );
        let catalog_access = Arc::new(catalog_access);

        let process_clock = process_clock::ProcessClock::new();

        Self {
            rules,
            server_id,
            store,
            exec: database_to_commit.exec,
            preserved_catalog: Arc::new(database_to_commit.preserved_catalog),
            catalog,
            jobs,
            metrics_registry,
            catalog_access,
            process_clock,
            worker_iterations_lifecycle: AtomicUsize::new(0),
            worker_iterations_cleanup: AtomicUsize::new(0),
            metric_labels,
            write_buffer: database_to_commit.write_buffer,
            cleanup_lock: Default::default(),
        }
    }

    /// Return a handle to the executor used to run queries
    pub fn executor(&self) -> Arc<Executor> {
        Arc::clone(&self.exec)
    }

    /// Rolls over the active chunk in the database's specified
    /// partition. Returns the previously open (now closed) Chunk if there was any.
    pub async fn rollover_partition(
        &self,
        table_name: &str,
        partition_key: &str,
    ) -> Result<Option<Arc<DbChunk>>> {
        let chunk = self
            .partition(table_name, partition_key)?
            .read()
            .open_chunk();

        if let Some(chunk) = chunk {
            let mut chunk = chunk.write();
            chunk.freeze().context(FreezingChunk)?;

            Ok(Some(DbChunk::snapshot(&chunk)))
        } else {
            Ok(None)
        }
    }

    fn partition(
        &self,
        table_name: &str,
        partition_key: &str,
    ) -> catalog::Result<Arc<tracker::RwLock<Partition>>> {
        let partition = self.catalog.partition(table_name, partition_key)?;
        Ok(Arc::clone(&partition))
    }

    fn chunk(
        &self,
        table_name: &str,
        partition_key: &str,
        chunk_id: u32,
    ) -> catalog::Result<Arc<tracker::RwLock<CatalogChunk>>> {
        self.catalog.chunk(table_name, partition_key, chunk_id)
    }

    pub fn lockable_chunk(
        &self,
        table_name: &str,
        partition_key: &str,
        chunk_id: u32,
    ) -> catalog::Result<LockableCatalogChunk<'_>> {
        let chunk = self.chunk(table_name, partition_key, chunk_id)?;
        Ok(LockableCatalogChunk { db: self, chunk })
    }

    /// Drops the specified chunk from the catalog and all storage systems
    pub fn drop_chunk(&self, table_name: &str, partition_key: &str, chunk_id: u32) -> Result<()> {
        debug!(%table_name, %partition_key, %chunk_id, "dropping chunk");
        let partition = self.partition(table_name, partition_key)?;
        partition.write().drop_chunk(chunk_id)?;
        Ok(())
    }

    /// Copies a chunk in the Closed state into the ReadBuffer from
    /// the mutable buffer and marks the chunk with `Moved` state
    ///
    /// This code does not do any checking of the read buffer against
    /// memory limits, etc
    ///
    /// This (async) function returns when this process is complete,
    /// but the process may take a long time
    ///
    /// Returns a handle to the newly loaded chunk in the read buffer
    pub async fn move_chunk_to_read_buffer(
        &self,
        table_name: &str,
        partition_key: &str,
        chunk_id: u32,
    ) -> Result<Arc<DbChunk>> {
        let chunk = self.lockable_chunk(table_name, partition_key, chunk_id)?;
        let (_, fut) =
            lifecycle::move_chunk_to_read_buffer(chunk.write()).context(LifecycleError)?;
        fut.await.context(TaskCancelled)?.context(LifecycleError)
    }

    /// Compacts all chunks in a partition to create a new chunk
    ///
    /// This code does not do any checking of the read buffer against
    /// memory limits, etc
    ///
    /// This (async) function returns when this process is complete,
    /// but the process may take a long time
    ///
    /// Returns a handle to the newly created chunk in the read buffer
    pub async fn compact_partition(
        &self,
        table_name: &str,
        partition_key: &str,
    ) -> Result<Arc<DbChunk>> {
        // Use explict scope to ensure the async generator doesn't
        // assume the locks have to possibly live across the `await`
        let fut = {
            let partition = self.partition(table_name, partition_key)?;
            let partition = LockableCatalogPartition {
                db: self,
                partition,
            };

            // Do lock dance to get a write lock on the partition as well
            // as on all of the chunks
            let partition = partition.read();

            // Get a list of all the chunks to compact
            let chunks = LockablePartition::chunks(&partition);
            let partition = partition.upgrade();
            let chunks = chunks.iter().map(|(_id, chunk)| chunk.write()).collect();

            let (_, fut) = lifecycle::compact_chunks(partition, chunks).context(LifecycleError)?;
            fut
        };

        fut.await.context(TaskCancelled)?.context(LifecycleError)
    }

    /// Write given table of a given chunk to object store.
    /// The writing only happen if that chunk already in read buffer
    pub async fn write_chunk_to_object_store(
        &self,
        table_name: &str,
        partition_key: &str,
        chunk_id: u32,
    ) -> Result<Arc<DbChunk>> {
        let chunk = self.lockable_chunk(table_name, partition_key, chunk_id)?;
        let (_, fut) =
            lifecycle::write_chunk_to_object_store(chunk.write()).context(LifecycleError)?;
        fut.await.context(TaskCancelled)?.context(LifecycleError)
    }

    /// Unload chunk from read buffer but keep it in object store
    pub fn unload_read_buffer(
        &self,
        table_name: &str,
        partition_key: &str,
        chunk_id: u32,
    ) -> Result<Arc<DbChunk>> {
        let chunk = self.lockable_chunk(table_name, partition_key, chunk_id)?;
        let chunk = chunk.write();
        lifecycle::unload_read_buffer_chunk(chunk).context(LifecycleError)
    }

    /// Return chunk summary information for all chunks in the specified
    /// partition across all storage systems
    pub fn partition_chunk_summaries(&self, partition_key: &str) -> Vec<ChunkSummary> {
        let partition_key = Some(partition_key);
        let table_names = TableNameFilter::AllTables;
        self.catalog
            .filtered_chunks(table_names, partition_key, CatalogChunk::summary)
    }

    /// Return Summary information for all columns in all chunks in the
    /// partition across all storage systems
    pub fn partition_summary(
        &self,
        table_name: &str,
        partition_key: &str,
    ) -> Option<PartitionSummary> {
        self.catalog
            .partition(table_name, partition_key)
            .map(|partition| partition.read().summary())
            .ok()
    }

    /// Return table summary information for the given chunk in the specified
    /// partition
    pub fn table_summary(
        &self,
        table_name: &str,
        partition_key: &str,
        chunk_id: u32,
    ) -> Option<Arc<TableSummary>> {
        Some(
            self.chunk(table_name, partition_key, chunk_id)
                .ok()?
                .read()
                .table_summary(),
        )
    }

    /// Returns the number of iterations of the background worker lifecycle loop
    pub fn worker_iterations_lifecycle(&self) -> usize {
        self.worker_iterations_lifecycle.load(Ordering::Relaxed)
    }

    /// Returns the number of iterations of the background worker lifecycle loop
    pub fn worker_iterations_cleanup(&self) -> usize {
        self.worker_iterations_cleanup.load(Ordering::Relaxed)
    }

    /// Background worker function
    pub async fn background_worker(
        self: &Arc<Self>,
        shutdown: tokio_util::sync::CancellationToken,
    ) {
        info!("started background worker");

        tokio::join!(
            // lifecycle policy loop
            async {
                let mut policy = ::lifecycle::LifecyclePolicy::new(&self);

                while !shutdown.is_cancelled() {
                    self.worker_iterations_lifecycle
                        .fetch_add(1, Ordering::Relaxed);
                    tokio::select! {
                        _ = policy.check_for_work(Utc::now(), std::time::Instant::now()) => {},
                        _ = shutdown.cancelled() => break,
                    }
                }
            },
            // object store cleanup loop
            async {
                while !shutdown.is_cancelled() {
                    self.worker_iterations_cleanup
                        .fetch_add(1, Ordering::Relaxed);
                    tokio::select! {
                        _ = async {
                            // Sleep for a duration drawn from a poisson distribution to de-correlate workers.
                            // Perform this sleep BEFORE the actual clean-up so that we don't immediately run a clean-up
                            // on startup.
                            let avg_sleep_secs = self.rules.read().worker_cleanup_avg_sleep.as_secs_f32().max(1.0);
                            let dist = Poisson::new(avg_sleep_secs).expect("parameter should be positive and finite");
                            let duration = Duration::from_secs_f32(dist.sample(&mut rand::thread_rng()));
                            debug!(?duration, "cleanup worker sleeps");
                            tokio::time::sleep(duration).await;

                            if let Err(e) = self.cleanup_unreferenced_parquet_files().await {
                                error!(%e, "error in background cleanup task");
                            }
                        } => {},
                        _ = shutdown.cancelled() => break,
                    }
                }
            },
        );

        info!("finished background worker");
    }

    async fn cleanup_unreferenced_parquet_files(
        self: &Arc<Self>,
    ) -> std::result::Result<(), parquet_file::cleanup::Error> {
        let guard = self.cleanup_lock.write().await;
        let files = get_unreferenced_parquet_files(&self.preserved_catalog, 1_000).await?;
        drop(guard);

        delete_parquet_files(&self.preserved_catalog, &files).await
    }

    /// Stores an entry based on the configuration.
    pub async fn store_entry(&self, entry: Entry) -> Result<()> {
        let immutable = {
            let rules = self.rules.read();
            rules.lifecycle_rules.immutable
        };

        match (self.write_buffer.as_ref(), immutable) {
            (Some(write_buffer), true) => {
                // If only the write buffer is configured, this is passing the data through to
                // the write buffer, and it's not an error. We ignore the returned metadata; it
                // will get picked up when data is read from the write buffer.
                let _ = write_buffer
                    .store_entry(&entry)
                    .await
                    .context(WriteBufferError)?;
                Ok(())
            }
            (Some(write_buffer), false) => {
                // If using both write buffer and mutable buffer, we want to wait for the write
                // buffer to return success before adding the entry to the mutable buffer.
                let sequence = write_buffer
                    .store_entry(&entry)
                    .await
                    .context(WriteBufferError)?;
                let sequenced_entry = Arc::new(
                    SequencedEntry::new_from_sequence(sequence, entry)
                        .context(SequencedEntryError)?,
                );

                self.store_sequenced_entry(sequenced_entry)
            }
            (None, true) => {
                // If no write buffer is configured and the database is immutable, trying to
                // store an entry is an error and we don't need to build a `SequencedEntry`.
                DatabaseNotWriteable {}.fail()
            }
            (None, false) => {
                // If no write buffer is configured, nothing is
                // sequencing entries so skip doing so here
                let sequenced_entry = Arc::new(SequencedEntry::new_unsequenced(entry));

                self.store_sequenced_entry(sequenced_entry)
            }
        }
    }

    /// Given a `SequencedEntry`, if the mutable buffer is configured, the `SequencedEntry` is then
    /// written into the mutable buffer.
    pub fn store_sequenced_entry(&self, sequenced_entry: Arc<SequencedEntry>) -> Result<()> {
        // Get all needed database rule values, then release the lock
        let rules = self.rules.read();
        let mutable_size_threshold = rules.lifecycle_rules.mutable_size_threshold;
        let immutable = rules.lifecycle_rules.immutable;
        let buffer_size_hard = rules.lifecycle_rules.buffer_size_hard;
        let late_arrival_window = rules.lifecycle_rules.late_arrive_window();
        std::mem::drop(rules);

        // We may have gotten here through `store_entry`, in which case this is checking the
        // configuration again unnecessarily, but we may have come here by consuming records from
        // the write buffer, so this check is necessary in that case.
        if immutable {
            return DatabaseNotWriteable {}.fail();
        }
        if let Some(hard_limit) = buffer_size_hard {
            if self.catalog.metrics().memory().total() > hard_limit.get() {
                return HardLimitReached {}.fail();
            }
        }

        if let Some(partitioned_writes) = sequenced_entry.partition_writes() {
            let sequence = sequenced_entry.as_ref().sequence();

            // Protect against DoS by limiting the number of errors we might collect
            const MAX_ERRORS_PER_SEQUENCED_ENTRY: usize = 10;

            let mut errors = Vec::with_capacity(MAX_ERRORS_PER_SEQUENCED_ENTRY);

            for write in partitioned_writes {
                let partition_key = write.key();
                for table_batch in write.table_batches() {
                    let row_count = table_batch.row_count();

                    if row_count == 0 {
                        continue;
                    }

                    let partition = self
                        .catalog
                        .get_or_create_partition(table_batch.name(), partition_key);

                    let mut partition = partition.write();
                    partition.update_last_write_at();

                    let (min_time, max_time) =
                        table_batch.min_max_time().context(TableBatchTimeError)?;

                    match partition.open_chunk() {
                        Some(chunk) => {
                            let mut chunk = chunk.write();
                            chunk.record_write();
                            let chunk_id = chunk.id();

                            let mb_chunk =
                                chunk.mutable_buffer().expect("cannot mutate open chunk");

                            if let Err(e) = mb_chunk
                                .write_table_batch(sequence, table_batch)
                                .context(WriteEntry {
                                    partition_key,
                                    chunk_id,
                                })
                            {
                                if errors.len() < MAX_ERRORS_PER_SEQUENCED_ENTRY {
                                    errors.push(e);
                                }
                                continue;
                            };

                            check_chunk_closed(&mut *chunk, mutable_size_threshold);
                        }
                        None => {
                            let metrics = self.metrics_registry.register_domain_with_labels(
                                "mutable_buffer",
                                self.metric_labels.clone(),
                            );
                            let mut mb_chunk = MBChunk::new(
                                table_batch.name(),
                                MutableBufferChunkMetrics::new(
                                    &metrics,
                                    self.catalog.metrics().memory().mutable_buffer(),
                                ),
                            );

                            if let Err(e) = mb_chunk
                                .write_table_batch(sequence, table_batch)
                                .context(WriteEntryInitial { partition_key })
                            {
                                if errors.len() < MAX_ERRORS_PER_SEQUENCED_ENTRY {
                                    errors.push(e);
                                }
                                continue;
                            }

                            let new_chunk = partition.create_open_chunk(mb_chunk);
                            check_chunk_closed(&mut *new_chunk.write(), mutable_size_threshold);
                        }
                    };

                    match partition.persistence_windows() {
                        Some(windows) => {
                            windows.add_range(
                                sequence,
                                row_count,
                                min_time,
                                max_time,
                                Instant::now(),
                            );
                        }
                        None => {
                            let mut windows = PersistenceWindows::new(late_arrival_window);
                            windows.add_range(
                                sequence,
                                row_count,
                                min_time,
                                max_time,
                                Instant::now(),
                            );
                            partition.set_persistence_windows(windows);
                        }
                    }
                }
            }

            ensure!(errors.is_empty(), StoreSequencedEntryFailures { errors });
        }

        Ok(())
    }
}

/// Check if the given chunk should be closed based on the the MutableBuffer size threshold.
fn check_chunk_closed(chunk: &mut CatalogChunk, mutable_size_threshold: Option<NonZeroUsize>) {
    if let Some(threshold) = mutable_size_threshold {
        if let Ok(mb_chunk) = chunk.mutable_buffer() {
            let size = mb_chunk.size();

            if size > threshold.get() {
                chunk.freeze().expect("cannot close open chunk");
            }
        }
    }
}

#[async_trait]
/// Convenience implementation of `Database` so the rest of the code
/// can just use Db as a `Database` even though the implementation
/// lives in `catalog_access`
impl QueryDatabase for Db {
    type Error = Error;
    type Chunk = DbChunk;

    fn chunks(&self, predicate: &Predicate) -> Vec<Arc<Self::Chunk>> {
        self.catalog_access.chunks(predicate)
    }

    fn partition_keys(&self) -> Result<Vec<String>, Self::Error> {
        self.catalog_access.partition_keys()
    }

    fn chunk_summaries(&self) -> Result<Vec<ChunkSummary>> {
        self.catalog_access.chunk_summaries()
    }
}

/// Convenience implementation of `CatalogProvider` so the rest of the
/// code can use Db as a `CatalogProvider` (e.g. for running
/// SQL). even though the implementation lives in `catalog_access`
impl CatalogProvider for Db {
    fn as_any(&self) -> &dyn Any {
        self as &dyn Any
    }

    fn schema_names(&self) -> Vec<String> {
        self.catalog_access.schema_names()
    }

    fn schema(&self, name: &str) -> Option<Arc<dyn SchemaProvider>> {
        self.catalog_access.schema(name)
    }
}

pub(crate) fn checkpoint_data_from_catalog(catalog: &Catalog) -> CheckpointData {
    let mut files = HashMap::new();

    for chunk in catalog.chunks() {
        let guard = chunk.read();
        if let ChunkStage::Persisted { parquet, .. } = guard.stage() {
            let path: DirsAndFileName = parquet.path().into();
            files.insert(path, parquet.parquet_metadata());
        }
    }

    CheckpointData { files }
}

pub mod test_helpers {
    use std::collections::HashSet;

    use entry::test_helpers::lp_to_entries;

    use super::*;

    /// Try to write lineprotocol data and return all tables that where written.
    pub async fn try_write_lp(db: &Db, lp: &str) -> Result<Vec<String>> {
        let entries = lp_to_entries(lp);

        let mut tables = HashSet::new();
        for entry in entries {
            if let Some(writes) = entry.partition_writes() {
                for write in writes {
                    for batch in write.table_batches() {
                        tables.insert(batch.name().to_string());
                    }
                }
                db.store_entry(entry).await?;
            }
        }

        let mut tables: Vec<_> = tables.into_iter().collect();
        tables.sort();
        Ok(tables)
    }

    /// Same was [`try_write_lp`](try_write_lp) but will panic on failure.
    pub async fn write_lp(db: &Db, lp: &str) -> Vec<String> {
        try_write_lp(db, lp).await.unwrap()
    }
}

#[cfg(test)]
mod tests {
    use std::{
        collections::HashSet,
        convert::TryFrom,
        iter::Iterator,
        num::{NonZeroU64, NonZeroUsize},
        str,
        time::{Duration, Instant},
    };

    use arrow::record_batch::RecordBatch;
    use bytes::Bytes;
    use futures::{stream, StreamExt, TryStreamExt};
    use internal_types::selection::Selection;
    use mutable_buffer::persistence_windows::MinMaxSequence;
    use tokio_util::sync::CancellationToken;

    use ::test_helpers::assert_contains;
    use arrow_util::{assert_batches_eq, assert_batches_sorted_eq};
    use data_types::{
        chunk_metadata::ChunkStorage,
        database_rules::{Order, Sort, SortOrder},
        partition_metadata::{ColumnSummary, InfluxDbType, StatValues, Statistics, TableSummary},
    };
    use entry::test_helpers::lp_to_entry;
    use object_store::{
        memory::InMemory,
        path::{parts::PathPart, ObjectStorePath, Path},
        ObjectStore, ObjectStoreApi,
    };
    use parquet_file::{
        catalog::test_helpers::TestCatalogState,
        metadata::IoxParquetMetaData,
        test_utils::{load_parquet_from_store_for_path, read_data_from_parquet_data},
    };
    use query::{frontend::sql::SqlQueryPlanner, QueryChunk, QueryDatabase};

    use crate::{
        db::{
            catalog::chunk::{ChunkStage, ChunkStageFrozenRepr},
            test_helpers::{try_write_lp, write_lp},
        },
        utils::{make_db, TestDb},
        write_buffer::test_helpers::MockBuffer,
    };

    use super::*;

    type Error = Box<dyn std::error::Error + Send + Sync + 'static>;
    type Result<T, E = Error> = std::result::Result<T, E>;

    #[tokio::test]
    async fn write_no_mutable_buffer() {
        // Validate that writes are rejected if there is no mutable buffer
        let db = make_db().await.db;
        db.rules.write().lifecycle_rules.immutable = true;
        let entry = lp_to_entry("cpu bar=1 10");
        let res = db.store_entry(entry).await;
        assert_contains!(
            res.unwrap_err().to_string(),
            "Cannot write to this database: no mutable buffer configured"
        );
    }

    #[tokio::test]
    async fn write_with_write_buffer_no_mutable_buffer() {
        // Writes should be forwarded to the write buffer and *not* rejected if the write buffer is
        // configured and the mutable buffer isn't
        let write_buffer = Arc::new(MockBuffer::default());
        let test_db = TestDb::builder()
            .write_buffer(Arc::clone(&write_buffer) as _)
            .build()
            .await
            .db;

        test_db.rules.write().lifecycle_rules.immutable = true;

        let entry = lp_to_entry("cpu bar=1 10");
        test_db.store_entry(entry).await.unwrap();

        assert_eq!(write_buffer.entries.lock().unwrap().len(), 1);
    }

    #[tokio::test]
    async fn write_buffer_and_mutable_buffer() {
        // Writes should be forwarded to the write buffer *and* the mutable buffer if both are
        // configured.
        let write_buffer = Arc::new(MockBuffer::default());
        let test_db = TestDb::builder()
            .write_buffer(Arc::clone(&write_buffer) as _)
            .build()
            .await
            .db;

        let entry = lp_to_entry("cpu bar=1 10");
        test_db.store_entry(entry).await.unwrap();

        assert_eq!(write_buffer.entries.lock().unwrap().len(), 1);

        let db = Arc::new(test_db);
        let batches = run_query(db, "select * from cpu").await;

        let expected = vec![
            "+-----+-------------------------------+",
            "| bar | time                          |",
            "+-----+-------------------------------+",
            "| 1   | 1970-01-01 00:00:00.000000010 |",
            "+-----+-------------------------------+",
        ];
        assert_batches_eq!(expected, &batches);
    }

    #[tokio::test]
    async fn read_write() {
        // This test also exercises the path without a write buffer.
        let db = Arc::new(make_db().await.db);
        write_lp(&db, "cpu bar=1 10").await;

        let batches = run_query(db, "select * from cpu").await;

        let expected = vec![
            "+-----+-------------------------------+",
            "| bar | time                          |",
            "+-----+-------------------------------+",
            "| 1   | 1970-01-01 00:00:00.000000010 |",
            "+-----+-------------------------------+",
        ];
        assert_batches_eq!(expected, &batches);
    }

    #[tokio::test]
    async fn try_all_partition_writes_when_some_fail() {
        let db = Arc::new(make_db().await.db);

        let nanoseconds_per_hour = 60 * 60 * 1_000_000_000u64;

        // 3 lines that will go into 3 hour partitions and start new chunks.
        let lp = format!(
            "foo,t1=alpha iv=1i {}
             foo,t1=bravo iv=1i {}
             foo,t1=charlie iv=1i {}",
            0,
            nanoseconds_per_hour,
            nanoseconds_per_hour * 2,
        );

        let entry = lp_to_entry(&lp);

        // This should succeed and start chunks in the MUB
        db.store_entry(entry).await.unwrap();

        // 3 more lines that should go in the 3 partitions/chunks.
        // Line 1 has the same schema and should end up in the MUB.
        // Line 2 has a different schema than line 1 and should error
        // Line 3 has the same schema as line 1 and should end up in the MUB.
        let lp = format!(
            "foo,t1=delta iv=1i {}
             foo t1=10i {}
             foo,t1=important iv=1i {}",
            1,
            nanoseconds_per_hour + 1,
            nanoseconds_per_hour * 2 + 1,
        );

        let entry = lp_to_entry(&lp);

        // This should return an error because there was at least one error in the loop
        let result = db.store_entry(entry).await;
        assert_contains!(
            result.unwrap_err().to_string(),
            "Storing sequenced entry failed with the following error(s), and possibly more:"
        );

        // But 5 points should be returned, most importantly the last one after the line with
        // the mismatched schema
        let batches = run_query(db, "select t1 from foo").await;

        let expected = vec![
            "+-----------+",
            "| t1        |",
            "+-----------+",
            "| alpha     |",
            "| bravo     |",
            "| charlie   |",
            "| delta     |",
            "| important |",
            "+-----------+",
        ];
        assert_batches_sorted_eq!(expected, &batches);
    }

    fn catalog_chunk_size_bytes_metric_eq(
        reg: &metrics::TestMetricRegistry,
        source: &'static str,
        v: u64,
    ) -> Result<(), metrics::Error> {
        reg.has_metric_family("catalog_chunks_mem_usage_bytes")
            .with_labels(&[
                ("db_name", "placeholder"),
                ("source", source),
                ("svr_id", "1"),
            ])
            .gauge()
            .eq(v as f64)
    }

    #[tokio::test]
    async fn metrics_during_rollover() {
        let test_db = make_db().await;
        let db = Arc::new(test_db.db);

        write_lp(db.as_ref(), "cpu bar=1 10").await;

        // A chunk has been opened
        test_db
            .metric_registry
            .has_metric_family("catalog_chunks_total")
            .with_labels(&[
                ("db_name", "placeholder"),
                ("state", "open"),
                ("svr_id", "1"),
            ])
            .counter()
            .eq(1.0)
            .unwrap();

        // verify chunk size updated
        catalog_chunk_size_bytes_metric_eq(&test_db.metric_registry, "mutable_buffer", 44).unwrap();

        // write into same chunk again.
        write_lp(db.as_ref(), "cpu bar=2 10").await;

        // verify chunk size updated
        catalog_chunk_size_bytes_metric_eq(&test_db.metric_registry, "mutable_buffer", 60).unwrap();

        // Still only one chunk open
        test_db
            .metric_registry
            .has_metric_family("catalog_chunks_total")
            .with_labels(&[
                ("db_name", "placeholder"),
                ("state", "open"),
                ("svr_id", "1"),
            ])
            .counter()
            .eq(1.0)
            .unwrap();

        db.rollover_partition("cpu", "1970-01-01T00").await.unwrap();

        // A chunk is now closed
        test_db
            .metric_registry
            .has_metric_family("catalog_chunks_total")
            .with_labels(&[
                ("db_name", "placeholder"),
                ("state", "closed"),
                ("svr_id", "1"),
            ])
            .counter()
            .eq(1.0)
            .unwrap();

        catalog_chunk_size_bytes_metric_eq(&test_db.metric_registry, "mutable_buffer", 1143)
            .unwrap();

        db.move_chunk_to_read_buffer("cpu", "1970-01-01T00", 0)
            .await
            .unwrap();

        // A chunk is now in the read buffer
        test_db
            .metric_registry
            .has_metric_family("catalog_chunks_total")
            .with_labels(&[
                ("db_name", "placeholder"),
                ("state", "moved"),
                ("svr_id", "1"),
            ])
            .counter()
            .eq(1.0)
            .unwrap();

        // verify chunk size updated (chunk moved from closing to moving to moved)
        catalog_chunk_size_bytes_metric_eq(&test_db.metric_registry, "mutable_buffer", 0).unwrap();
        catalog_chunk_size_bytes_metric_eq(&test_db.metric_registry, "read_buffer", 1486).unwrap();

        db.write_chunk_to_object_store("cpu", "1970-01-01T00", 0)
            .await
            .unwrap();

        // A chunk is now in the object store and still in read buffer
        test_db
            .metric_registry
            .has_metric_family("catalog_chunks_total")
            .with_labels(&[
                ("db_name", "placeholder"),
                ("state", "rub_and_os"),
                ("svr_id", "1"),
            ])
            .counter()
            .eq(1.0)
            .unwrap();

        let expected_parquet_size = 663;
        catalog_chunk_size_bytes_metric_eq(&test_db.metric_registry, "read_buffer", 1486).unwrap();
        // now also in OS
        catalog_chunk_size_bytes_metric_eq(
            &test_db.metric_registry,
            "parquet",
            expected_parquet_size,
        )
        .unwrap(); // TODO: #1311

        db.unload_read_buffer("cpu", "1970-01-01T00", 0).unwrap();

        // A chunk is now now in the "os-only" state.
        test_db
            .metric_registry
            .has_metric_family("catalog_chunks_total")
            .with_labels(&[("db_name", "placeholder"), ("state", "os"), ("svr_id", "1")])
            .counter()
            .eq(1.0)
            .unwrap();

        // verify chunk size not increased for OS (it was in OS before unload)
        catalog_chunk_size_bytes_metric_eq(
            &test_db.metric_registry,
            "parquet",
            expected_parquet_size,
        )
        .unwrap();
        // verify chunk size for RB has decreased
        catalog_chunk_size_bytes_metric_eq(&test_db.metric_registry, "read_buffer", 0).unwrap();
    }

    #[tokio::test]
    async fn write_with_rollover() {
        let db = Arc::new(make_db().await.db);
        write_lp(db.as_ref(), "cpu bar=1 10").await;
        assert_eq!(vec!["1970-01-01T00"], db.partition_keys().unwrap());

        let mb_chunk = db
            .rollover_partition("cpu", "1970-01-01T00")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(mb_chunk.id(), 0);

        let expected = vec![
            "+-----+-------------------------------+",
            "| bar | time                          |",
            "+-----+-------------------------------+",
            "| 1   | 1970-01-01 00:00:00.000000010 |",
            "+-----+-------------------------------+",
        ];
        let batches = run_query(Arc::clone(&db), "select * from cpu").await;
        assert_batches_sorted_eq!(expected, &batches);

        // add new data
        write_lp(db.as_ref(), "cpu bar=2 20").await;
        let expected = vec![
            "+-----+-------------------------------+",
            "| bar | time                          |",
            "+-----+-------------------------------+",
            "| 1   | 1970-01-01 00:00:00.000000010 |",
            "| 2   | 1970-01-01 00:00:00.000000020 |",
            "+-----+-------------------------------+",
        ];
        let batches = run_query(Arc::clone(&db), "select * from cpu").await;
        assert_batches_sorted_eq!(&expected, &batches);

        // And expect that we still get the same thing when data is rolled over again
        let chunk = db
            .rollover_partition("cpu", "1970-01-01T00")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(chunk.id(), 1);

        let batches = run_query(db, "select * from cpu").await;
        assert_batches_sorted_eq!(&expected, &batches);
    }

    #[tokio::test]
    async fn write_with_missing_tags_are_null() {
        let db = Arc::new(make_db().await.db);
        // Note the `region` tag is introduced in the second line, so
        // the values in prior rows for the region column are
        // null. Likewise the `core` tag is introduced in the third
        // line so the prior columns are null
        let lines = vec![
            "cpu,region=west user=23.2 10",
            "cpu, user=10.0 11",
            "cpu,core=one user=10.0 11",
        ];

        write_lp(db.as_ref(), &lines.join("\n")).await;
        assert_eq!(vec!["1970-01-01T00"], db.partition_keys().unwrap());

        let mb_chunk = db
            .rollover_partition("cpu", "1970-01-01T00")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(mb_chunk.id(), 0);

        let expected = vec![
            "+------+--------+-------------------------------+------+",
            "| core | region | time                          | user |",
            "+------+--------+-------------------------------+------+",
            "|      | west   | 1970-01-01 00:00:00.000000010 | 23.2 |",
            "|      |        | 1970-01-01 00:00:00.000000011 | 10   |",
            "| one  |        | 1970-01-01 00:00:00.000000011 | 10   |",
            "+------+--------+-------------------------------+------+",
        ];
        let batches = run_query(Arc::clone(&db), "select * from cpu").await;
        assert_batches_sorted_eq!(expected, &batches);
    }

    #[tokio::test]
    async fn read_from_read_buffer() {
        // Test that data can be loaded into the ReadBuffer
        let test_db = make_db().await;
        let db = Arc::new(test_db.db);

        write_lp(db.as_ref(), "cpu bar=1 10").await;
        write_lp(db.as_ref(), "cpu bar=2 20").await;

        let partition_key = "1970-01-01T00";
        let mb_chunk = db
            .rollover_partition("cpu", partition_key)
            .await
            .unwrap()
            .unwrap();
        let rb_chunk = db
            .move_chunk_to_read_buffer("cpu", partition_key, mb_chunk.id())
            .await
            .unwrap();

        // it should be the same chunk!
        assert_eq!(mb_chunk.id(), rb_chunk.id());

        // we should have chunks in both the read buffer only
        assert!(mutable_chunk_ids(&db, partition_key).is_empty());
        assert_eq!(read_buffer_chunk_ids(&db, partition_key), vec![0]);

        // data should be readable
        let expected = vec![
            "+-----+-------------------------------+",
            "| bar | time                          |",
            "+-----+-------------------------------+",
            "| 1   | 1970-01-01 00:00:00.000000010 |",
            "| 2   | 1970-01-01 00:00:00.000000020 |",
            "+-----+-------------------------------+",
        ];
        let batches = run_query(Arc::clone(&db), "select * from cpu").await;
        assert_batches_eq!(&expected, &batches);

        // A chunk is now in the object store
        test_db
            .metric_registry
            .has_metric_family("catalog_chunks_total")
            .with_labels(&[
                ("db_name", "placeholder"),
                ("state", "moved"),
                ("svr_id", "1"),
            ])
            .counter()
            .eq(1.0)
            .unwrap();

        // verify chunk size updated (chunk moved from moved to writing to written)
        catalog_chunk_size_bytes_metric_eq(&test_db.metric_registry, "read_buffer", 1486).unwrap();

        // drop, the chunk from the read buffer
        db.drop_chunk("cpu", partition_key, mb_chunk.id()).unwrap();
        assert_eq!(
            read_buffer_chunk_ids(db.as_ref(), partition_key),
            vec![] as Vec<u32>
        );

        // verify size is reported until chunk dropped
        catalog_chunk_size_bytes_metric_eq(&test_db.metric_registry, "read_buffer", 1486).unwrap();
        std::mem::drop(rb_chunk);

        // verify chunk size updated (chunk dropped from moved state)
        catalog_chunk_size_bytes_metric_eq(&test_db.metric_registry, "read_buffer", 0).unwrap();

        // Currently this doesn't work (as we need to teach the stores how to
        // purge tables after data bas been dropped println!("running
        // query after all data dropped!"); let expected = vec![] as
        // Vec<&str>; let batches = run_query(&db, "select * from
        // cpu").await; assert_batches_eq!(expected, &batches);
    }

    #[tokio::test]
    async fn compact() {
        // Test that data can be read after it is compacted
        let test_db = make_db().await;
        let db = Arc::new(test_db.db);

        write_lp(db.as_ref(), "cpu bar=1 10").await;

        let partition_key = "1970-01-01T00";
        let mb_chunk = db
            .rollover_partition("cpu", partition_key)
            .await
            .unwrap()
            .unwrap();
        let old_rb_chunk = db
            .move_chunk_to_read_buffer("cpu", partition_key, mb_chunk.id())
            .await
            .unwrap();

        // Put new data into the mutable buffer
        write_lp(db.as_ref(), "cpu bar=2 20").await;

        // now, compact it
        let compacted_rb_chunk = db.compact_partition("cpu", partition_key).await.unwrap();

        // no other read buffer data should be present
        assert_eq!(
            read_buffer_chunk_ids(&db, partition_key),
            vec![compacted_rb_chunk.id()]
        );
        assert_ne!(old_rb_chunk.id(), compacted_rb_chunk.id());

        // data should be readable
        let expected = vec![
            "+-----+-------------------------------+",
            "| bar | time                          |",
            "+-----+-------------------------------+",
            "| 1   | 1970-01-01 00:00:00.000000010 |",
            "| 2   | 1970-01-01 00:00:00.000000020 |",
            "+-----+-------------------------------+",
        ];
        let batches = run_query(Arc::clone(&db), "select * from cpu").await;
        assert_batches_eq!(&expected, &batches);
    }

    async fn collect_read_filter(chunk: &DbChunk) -> Vec<RecordBatch> {
        chunk
            .read_filter(&Default::default(), Selection::All)
            .unwrap()
            .collect::<Vec<_>>()
            .await
            .into_iter()
            .map(Result::unwrap)
            .collect()
    }

    #[tokio::test]
    async fn load_to_read_buffer_sorted() {
        let test_db = make_db().await;
        let db = Arc::new(test_db.db);

        write_lp(db.as_ref(), "cpu,tag1=cupcakes bar=1 10").await;
        write_lp(db.as_ref(), "cpu,tag1=asfd,tag2=foo bar=2 20").await;
        write_lp(db.as_ref(), "cpu,tag1=bingo,tag2=foo bar=2 10").await;
        write_lp(db.as_ref(), "cpu,tag1=bongo,tag2=a bar=2 20").await;
        write_lp(db.as_ref(), "cpu,tag1=bongo,tag2=a bar=2 10").await;
        write_lp(db.as_ref(), "cpu,tag2=a bar=3 5").await;

        let partition_key = "1970-01-01T00";
        let mb_chunk = db
            .rollover_partition("cpu", partition_key)
            .await
            .unwrap()
            .unwrap();

        let mb = collect_read_filter(&mb_chunk).await;

        let rb_chunk = db
            .move_chunk_to_read_buffer("cpu", partition_key, mb_chunk.id())
            .await
            .unwrap();

        // MUB chunk size
        test_db
            .metric_registry
            .has_metric_family("catalog_chunk_creation_size_bytes")
            .with_labels(&[
                ("db_name", "placeholder"),
                ("state", "closed"),
                ("svr_id", "1"),
            ])
            .histogram()
            .sample_sum_eq(280.0)
            .unwrap();

        // RB chunk size
        test_db
            .metric_registry
            .has_metric_family("catalog_chunk_creation_size_bytes")
            .with_labels(&[
                ("db_name", "placeholder"),
                ("state", "moved"),
                ("svr_id", "1"),
            ])
            .histogram()
            .sample_sum_eq(3035.0)
            .unwrap();

        let rb = collect_read_filter(&rb_chunk).await;

        // Test that data on load into the read buffer is sorted

        assert_batches_eq!(
            &[
                "+-----+----------+------+-------------------------------+",
                "| bar | tag1     | tag2 | time                          |",
                "+-----+----------+------+-------------------------------+",
                "| 1   | cupcakes |      | 1970-01-01 00:00:00.000000010 |",
                "| 2   | asfd     | foo  | 1970-01-01 00:00:00.000000020 |",
                "| 2   | bingo    | foo  | 1970-01-01 00:00:00.000000010 |",
                "| 2   | bongo    | a    | 1970-01-01 00:00:00.000000020 |",
                "| 2   | bongo    | a    | 1970-01-01 00:00:00.000000010 |",
                "| 3   |          | a    | 1970-01-01 00:00:00.000000005 |",
                "+-----+----------+------+-------------------------------+",
            ],
            &mb
        );

        assert_batches_eq!(
            &[
                "+-----+----------+------+-------------------------------+",
                "| bar | tag1     | tag2 | time                          |",
                "+-----+----------+------+-------------------------------+",
                "| 1   | cupcakes |      | 1970-01-01 00:00:00.000000010 |",
                "| 3   |          | a    | 1970-01-01 00:00:00.000000005 |",
                "| 2   | bongo    | a    | 1970-01-01 00:00:00.000000010 |",
                "| 2   | bongo    | a    | 1970-01-01 00:00:00.000000020 |",
                "| 2   | asfd     | foo  | 1970-01-01 00:00:00.000000020 |",
                "| 2   | bingo    | foo  | 1970-01-01 00:00:00.000000010 |",
                "+-----+----------+------+-------------------------------+",
            ],
            &rb
        );
    }

    async fn flatten_list_stream(
        storage: Arc<ObjectStore>,
        prefix: Option<&Path>,
    ) -> Result<Vec<Path>> {
        storage
            .list(prefix)
            .await?
            .map_ok(|v| stream::iter(v).map(Ok))
            .try_flatten()
            .try_collect()
            .await
    }

    #[tokio::test]
    async fn write_one_chunk_to_parquet_file() {
        // Test that data can be written into parquet files
        let object_store = Arc::new(ObjectStore::new_in_memory(InMemory::new()));

        // Create a DB given a server id, an object store and a db name
        let server_id = ServerId::try_from(10).unwrap();
        let db_name = "parquet_test_db";
        let test_db = TestDb::builder()
            .server_id(server_id)
            .object_store(Arc::clone(&object_store))
            .db_name(db_name)
            .build()
            .await;

        let db = Arc::new(test_db.db);

        // Write some line protocols in Mutable buffer of the DB
        write_lp(db.as_ref(), "cpu bar=1 10").await;
        write_lp(db.as_ref(), "cpu bar=2 20").await;

        //Now mark the MB chunk close
        let partition_key = "1970-01-01T00";
        let mb_chunk = db
            .rollover_partition("cpu", "1970-01-01T00")
            .await
            .unwrap()
            .unwrap();
        // Move that MB chunk to RB chunk and drop it from MB
        let rb_chunk = db
            .move_chunk_to_read_buffer("cpu", partition_key, mb_chunk.id())
            .await
            .unwrap();
        // Write the RB chunk to Object Store but keep it in RB
        let pq_chunk = db
            .write_chunk_to_object_store("cpu", partition_key, mb_chunk.id())
            .await
            .unwrap();

        // Read buffer + Parquet chunk size
        test_db
            .metric_registry
            .has_metric_family("catalog_chunk_creation_size_bytes")
            .with_labels(&[
                ("db_name", "parquet_test_db"),
                ("state", "rub_and_os"),
                ("svr_id", "10"),
            ])
            .histogram()
            .sample_sum_eq(2149.0)
            .unwrap();

        // it should be the same chunk!
        assert_eq!(mb_chunk.id(), rb_chunk.id());
        assert_eq!(mb_chunk.id(), pq_chunk.id());

        // we should have chunks in both the read buffer only
        assert!(mutable_chunk_ids(&db, partition_key).is_empty());
        assert_eq!(read_buffer_chunk_ids(&db, partition_key), vec![0]);
        assert_eq!(read_parquet_file_chunk_ids(&db, partition_key), vec![0]);

        // Verify data written to the parquet file in object store
        //
        // First, there must be one path of object store in the catalog
        let path = pq_chunk.object_store_path().unwrap();

        // Check that the path must exist in the object store
        let path_list = flatten_list_stream(Arc::clone(&object_store), Some(&path))
            .await
            .unwrap();
        assert_eq!(path_list.len(), 1);
        assert_eq!(path_list[0], path);

        // Now read data from that path
        let parquet_data = load_parquet_from_store_for_path(&path_list[0], object_store)
            .await
            .unwrap();
        let parquet_metadata = IoxParquetMetaData::from_file_bytes(parquet_data.clone()).unwrap();
        // Read metadata at file level
        let schema = parquet_metadata.read_schema().unwrap();
        // Read data
        let record_batches =
            read_data_from_parquet_data(Arc::clone(&schema.as_arrow()), parquet_data);

        let expected = vec![
            "+-----+-------------------------------+",
            "| bar | time                          |",
            "+-----+-------------------------------+",
            "| 1   | 1970-01-01 00:00:00.000000010 |",
            "| 2   | 1970-01-01 00:00:00.000000020 |",
            "+-----+-------------------------------+",
        ];
        assert_batches_eq!(expected, &record_batches);
    }

    #[tokio::test]
    async fn unload_chunk_from_read_buffer() {
        // Test that data can be written into parquet files and then
        // remove it from read buffer and make sure we are still
        // be able to read data from object store

        // Create an object store in memory
        let object_store = Arc::new(ObjectStore::new_in_memory(InMemory::new()));

        // Create a DB given a server id, an object store and a db name
        let server_id = ServerId::try_from(10).unwrap();
        let db_name = "unload_read_buffer_test_db";
        let test_db = TestDb::builder()
            .server_id(server_id)
            .object_store(Arc::clone(&object_store))
            .db_name(db_name)
            .build()
            .await;

        let db = Arc::new(test_db.db);

        // Write some line protocols in Mutable buffer of the DB
        write_lp(db.as_ref(), "cpu bar=1 10").await;
        write_lp(db.as_ref(), "cpu bar=2 20").await;

        // Now mark the MB chunk close
        let partition_key = "1970-01-01T00";
        let mb_chunk = db
            .rollover_partition("cpu", "1970-01-01T00")
            .await
            .unwrap()
            .unwrap();
        // Move that MB chunk to RB chunk and drop it from MB
        let rb_chunk = db
            .move_chunk_to_read_buffer("cpu", partition_key, mb_chunk.id())
            .await
            .unwrap();
        // Write the RB chunk to Object Store but keep it in RB
        let pq_chunk = db
            .write_chunk_to_object_store("cpu", partition_key, mb_chunk.id())
            .await
            .unwrap();

        // it should be the same chunk!
        assert_eq!(mb_chunk.id(), rb_chunk.id());
        assert_eq!(mb_chunk.id(), pq_chunk.id());

        // we should have chunks in both the read buffer only
        assert!(mutable_chunk_ids(&db, partition_key).is_empty());
        assert_eq!(read_buffer_chunk_ids(&db, partition_key), vec![0]);
        assert_eq!(read_parquet_file_chunk_ids(&db, partition_key), vec![0]);

        // Read buffer + Parquet chunk size
        test_db
            .metric_registry
            .has_metric_family("catalog_chunk_creation_size_bytes")
            .with_labels(&[
                ("db_name", "unload_read_buffer_test_db"),
                ("state", "rub_and_os"),
                ("svr_id", "10"),
            ])
            .histogram()
            .sample_sum_eq(2149.0)
            .unwrap();

        // Unload RB chunk but keep it in OS
        let pq_chunk = db
            .unload_read_buffer("cpu", partition_key, mb_chunk.id())
            .unwrap();

        // still should be the same chunk!
        assert_eq!(mb_chunk.id(), rb_chunk.id());
        assert_eq!(mb_chunk.id(), pq_chunk.id());

        // we should only have chunk in os
        assert!(mutable_chunk_ids(&db, partition_key).is_empty());
        assert!(read_buffer_chunk_ids(&db, partition_key).is_empty());
        assert_eq!(read_parquet_file_chunk_ids(&db, partition_key), vec![0]);

        // Parquet chunk size only
        test_db
            .metric_registry
            .has_metric_family("catalog_chunk_creation_size_bytes")
            .with_labels(&[
                ("db_name", "unload_read_buffer_test_db"),
                ("state", "os"),
                ("svr_id", "10"),
            ])
            .histogram()
            .sample_sum_eq(663.0)
            .unwrap();

        // Verify data written to the parquet file in object store
        //
        // First, there must be one path of object store in the catalog
        let path = pq_chunk.object_store_path().unwrap();

        // Check that the path must exist in the object store
        let path_list = flatten_list_stream(Arc::clone(&object_store), Some(&path))
            .await
            .unwrap();
        println!("path_list: {:#?}", path_list);
        assert_eq!(path_list.len(), 1);
        assert_eq!(path_list[0], path);

        // Now read data from that path
        let parquet_data = load_parquet_from_store_for_path(&path_list[0], object_store)
            .await
            .unwrap();
        let parquet_metadata = IoxParquetMetaData::from_file_bytes(parquet_data.clone()).unwrap();
        // Read metadata at file level
        let schema = parquet_metadata.read_schema().unwrap();
        // Read data
        let record_batches =
            read_data_from_parquet_data(Arc::clone(&schema.as_arrow()), parquet_data);

        let expected = vec![
            "+-----+-------------------------------+",
            "| bar | time                          |",
            "+-----+-------------------------------+",
            "| 1   | 1970-01-01 00:00:00.000000010 |",
            "| 2   | 1970-01-01 00:00:00.000000020 |",
            "+-----+-------------------------------+",
        ];
        assert_batches_eq!(expected, &record_batches);
    }

    #[tokio::test]
    async fn write_updates_last_write_at() {
        let db = Arc::new(make_db().await.db);
        let before_create = Utc::now();

        let partition_key = "1970-01-01T00";
        write_lp(&db, "cpu bar=1 10").await;
        let after_write = Utc::now();

        let last_write_prev = {
            let partition = db.catalog.partition("cpu", partition_key).unwrap();
            let partition = partition.read();

            assert_ne!(partition.created_at(), partition.last_write_at());
            assert!(before_create < partition.last_write_at());
            assert!(after_write > partition.last_write_at());
            partition.last_write_at()
        };

        write_lp(&db, "cpu bar=1 20").await;
        {
            let partition = db.catalog.partition("cpu", partition_key).unwrap();
            let partition = partition.read();
            assert!(last_write_prev < partition.last_write_at());
        }
    }

    #[tokio::test]
    async fn write_updates_persistence_windows() {
        // Writes should update the persistence windows when there
        // is a write buffer configured.
        let write_buffer = Arc::new(MockBuffer::default());
        let db = TestDb::builder()
            .write_buffer(Arc::clone(&write_buffer) as _)
            .build()
            .await
            .db;

        let partition_key = "1970-01-01T00";
        write_lp(&db, "cpu bar=1 10").await; // seq 0
        write_lp(&db, "cpu bar=1 20").await; // seq 1
        write_lp(&db, "cpu bar=1 30").await; // seq 2

        let partition = db.catalog.partition("cpu", partition_key).unwrap();
        let mut partition = partition.write();
        let windows = partition.persistence_windows().unwrap();
        let seq = windows.minimum_unpersisted_sequence().unwrap();

        let seq = seq.get(&0).unwrap();
        assert_eq!(seq, &MinMaxSequence::new(0, 2));
    }

    #[tokio::test]
    async fn write_with_no_write_buffer_updates_sequence() {
        let db = Arc::new(make_db().await.db);

        let partition_key = "1970-01-01T00";
        write_lp(&db, "cpu bar=1 10").await;
        write_lp(&db, "cpu bar=1 20").await;

        let partition = db.catalog.partition("cpu", partition_key).unwrap();
        let mut partition = partition.write();
        // validate it has data
        let table_summary = partition.summary().table;
        assert_eq!(&table_summary.name, "cpu");
        assert_eq!(table_summary.count(), 2);
        let windows = partition.persistence_windows().unwrap();
        let open_min = windows.open_min_time().unwrap();
        let open_max = windows.open_max_time().unwrap();
        assert_eq!(open_min.timestamp_nanos(), 10);
        assert_eq!(open_max.timestamp_nanos(), 20);
    }

    #[tokio::test]
    async fn test_chunk_timestamps() {
        let start = Utc::now();
        let db = Arc::new(make_db().await.db);

        // Given data loaded into two chunks
        write_lp(&db, "cpu bar=1 10").await;
        let after_data_load = Utc::now();

        // When the chunk is rolled over
        let partition_key = "1970-01-01T00";
        let chunk_id = db
            .rollover_partition("cpu", "1970-01-01T00")
            .await
            .unwrap()
            .unwrap()
            .id();
        let after_rollover = Utc::now();

        let partition = db.catalog.partition("cpu", partition_key).unwrap();
        let partition = partition.read();
        let chunk = partition.chunk(chunk_id).unwrap();
        let chunk = chunk.read();

        println!(
            "start: {:?}, after_data_load: {:?}, after_rollover: {:?}",
            start, after_data_load, after_rollover
        );
        println!("Chunk: {:#?}", chunk);

        // then the chunk creation and rollover times are as expected
        assert!(start < chunk.time_of_first_write().unwrap());
        assert!(chunk.time_of_first_write().unwrap() < after_data_load);
        assert!(chunk.time_of_first_write().unwrap() == chunk.time_of_last_write().unwrap());
        assert!(after_data_load < chunk.time_closed().unwrap());
        assert!(chunk.time_closed().unwrap() < after_rollover);
    }

    #[tokio::test]
    async fn test_chunk_closing() {
        let db = Arc::new(make_db().await.db);
        db.rules.write().lifecycle_rules.mutable_size_threshold =
            Some(NonZeroUsize::new(2).unwrap());

        write_lp(&db, "cpu bar=1 10").await;
        write_lp(&db, "cpu bar=1 20").await;

        let partitions = db.catalog.partition_keys();
        assert_eq!(partitions.len(), 1);
        let partition_key = partitions.into_iter().next().unwrap();

        let partition = db.catalog.partition("cpu", &partition_key).unwrap();
        let partition = partition.read();

        let chunks: Vec<_> = partition.chunks().collect();
        assert_eq!(chunks.len(), 2);
        assert!(matches!(
            chunks[0].read().stage(),
            ChunkStage::Frozen {
                representation: ChunkStageFrozenRepr::MutableBufferSnapshot(_),
                ..
            }
        ));
        assert!(matches!(
            chunks[1].read().stage(),
            ChunkStage::Frozen {
                representation: ChunkStageFrozenRepr::MutableBufferSnapshot(_),
                ..
            }
        ));
    }

    #[tokio::test]
    async fn chunks_sorted_by_times() {
        let db = Arc::new(make_db().await.db);
        write_lp(&db, "cpu val=1 1").await;
        write_lp(&db, "mem val=2 400000000000001").await;
        write_lp(&db, "cpu val=1 2").await;
        write_lp(&db, "mem val=2 400000000000002").await;

        let sort_rules = SortOrder {
            order: Order::Desc,
            sort: Sort::LastWriteTime,
        };
        let chunks = db.catalog.chunks_sorted_by(&sort_rules);
        let partitions: Vec<_> = chunks
            .into_iter()
            .map(|x| x.read().key().to_string())
            .collect();

        assert_eq!(partitions, vec!["1970-01-05T15", "1970-01-01T00"]);

        let sort_rules = SortOrder {
            order: Order::Asc,
            sort: Sort::CreatedAtTime,
        };
        let chunks = db.catalog.chunks_sorted_by(&sort_rules);
        let partitions: Vec<_> = chunks
            .into_iter()
            .map(|x| x.read().key().to_string())
            .collect();
        assert_eq!(partitions, vec!["1970-01-01T00", "1970-01-05T15"]);
    }

    #[tokio::test]
    async fn chunk_id_listing() {
        // Test that chunk id listing is hooked up
        let db = Arc::new(make_db().await.db);
        let partition_key = "1970-01-01T00";

        write_lp(&db, "cpu bar=1 10").await;
        write_lp(&db, "cpu bar=1 20").await;

        assert_eq!(mutable_chunk_ids(&db, partition_key), vec![0]);
        assert_eq!(
            read_buffer_chunk_ids(&db, partition_key),
            vec![] as Vec<u32>
        );

        let partition_key = "1970-01-01T00";
        let mb_chunk = db
            .rollover_partition("cpu", "1970-01-01T00")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(mb_chunk.id(), 0);

        // add a new chunk in mutable buffer, and move chunk1 (but
        // not chunk 0) to read buffer
        write_lp(&db, "cpu bar=1 30").await;
        let mb_chunk = db
            .rollover_partition("cpu", "1970-01-01T00")
            .await
            .unwrap()
            .unwrap();
        db.move_chunk_to_read_buffer("cpu", partition_key, mb_chunk.id())
            .await
            .unwrap();

        write_lp(&db, "cpu bar=1 40").await;

        assert_eq!(mutable_chunk_ids(&db, partition_key), vec![0, 2]);
        assert_eq!(read_buffer_chunk_ids(&db, partition_key), vec![1]);
    }

    /// Normalizes a set of ChunkSummaries for comparison by removing timestamps
    fn normalize_summaries(summaries: Vec<ChunkSummary>) -> Vec<ChunkSummary> {
        let mut summaries = summaries
            .into_iter()
            .map(|summary| {
                let ChunkSummary {
                    partition_key,
                    table_name,
                    id,
                    storage,
                    estimated_bytes,
                    row_count,
                    ..
                } = summary;
                ChunkSummary::new_without_timestamps(
                    partition_key,
                    table_name,
                    id,
                    storage,
                    estimated_bytes,
                    row_count,
                )
            })
            .collect::<Vec<_>>();
        summaries.sort_unstable();
        summaries
    }

    #[tokio::test]
    async fn partition_chunk_summaries() {
        // Test that chunk id listing is hooked up
        let db = Arc::new(make_db().await.db);

        write_lp(&db, "cpu bar=1 1").await;
        db.rollover_partition("cpu", "1970-01-01T00").await.unwrap();

        // write into a separate partitiion
        write_lp(&db, "cpu bar=1,baz2,frob=3 400000000000000").await;

        print!("Partitions: {:?}", db.partition_keys().unwrap());

        let chunk_summaries = db.partition_chunk_summaries("1970-01-05T15");
        let chunk_summaries = normalize_summaries(chunk_summaries);

        let expected = vec![ChunkSummary::new_without_timestamps(
            Arc::from("1970-01-05T15"),
            Arc::from("cpu"),
            0,
            ChunkStorage::OpenMutableBuffer,
            70,
            1,
        )];

        let size: usize = db
            .chunk_summaries()
            .unwrap()
            .into_iter()
            .map(|x| x.estimated_bytes)
            .sum();

        assert_eq!(
            db.catalog.metrics().memory().mutable_buffer().get_total(),
            size
        );

        assert_eq!(
            expected, chunk_summaries,
            "expected:\n{:#?}\n\nactual:{:#?}\n\n",
            expected, chunk_summaries
        );
    }

    #[tokio::test]
    async fn partition_chunk_summaries_timestamp() {
        let db = Arc::new(make_db().await.db);
        let start = Utc::now();
        write_lp(&db, "cpu bar=1 1").await;
        let after_first_write = Utc::now();
        write_lp(&db, "cpu bar=2 2").await;
        db.rollover_partition("cpu", "1970-01-01T00").await.unwrap();
        let after_close = Utc::now();

        let mut chunk_summaries = db.chunk_summaries().unwrap();

        chunk_summaries.sort_by_key(|s| s.id);

        let summary = &chunk_summaries[0];
        assert_eq!(summary.id, 0, "summary; {:#?}", summary);
        assert!(
            summary.time_of_first_write.unwrap() > start,
            "summary; {:#?}",
            summary
        );
        assert!(
            summary.time_of_first_write.unwrap() < after_close,
            "summary; {:#?}",
            summary
        );

        assert!(
            summary.time_of_last_write.unwrap() > after_first_write,
            "summary; {:#?}",
            summary
        );
        assert!(
            summary.time_of_last_write.unwrap() < after_close,
            "summary; {:#?}",
            summary
        );

        assert!(
            summary.time_closed.unwrap() > after_first_write,
            "summary; {:#?}",
            summary
        );
        assert!(
            summary.time_closed.unwrap() < after_close,
            "summary; {:#?}",
            summary
        );
    }

    #[tokio::test]
    async fn chunk_summaries() {
        // Test that chunk id listing is hooked up
        let db = Arc::new(make_db().await.db);

        // get three chunks: one open, one closed in mb and one close in rb
        write_lp(&db, "cpu bar=1 1").await;
        db.rollover_partition("cpu", "1970-01-01T00").await.unwrap();

        write_lp(&db, "cpu bar=1,baz=2 2").await;
        write_lp(&db, "cpu bar=1,baz=2,frob=3 400000000000000").await;

        print!("Partitions: {:?}", db.partition_keys().unwrap());

        db.move_chunk_to_read_buffer("cpu", "1970-01-01T00", 0)
            .await
            .unwrap();

        db.write_chunk_to_object_store("cpu", "1970-01-01T00", 0)
            .await
            .unwrap();

        print!("Partitions2: {:?}", db.partition_keys().unwrap());

        db.rollover_partition("cpu", "1970-01-05T15").await.unwrap();
        write_lp(&db, "cpu bar=1,baz=3,blargh=3 400000000000000").await;

        let chunk_summaries = db.chunk_summaries().expect("expected summary to return");
        let chunk_summaries = normalize_summaries(chunk_summaries);

        let expected = vec![
            ChunkSummary::new_without_timestamps(
                Arc::from("1970-01-01T00"),
                Arc::from("cpu"),
                0,
                ChunkStorage::ReadBufferAndObjectStore,
                2147, // size of RB and OS chunks
                1,
            ),
            ChunkSummary::new_without_timestamps(
                Arc::from("1970-01-01T00"),
                Arc::from("cpu"),
                1,
                ChunkStorage::OpenMutableBuffer,
                64,
                1,
            ),
            ChunkSummary::new_without_timestamps(
                Arc::from("1970-01-05T15"),
                Arc::from("cpu"),
                0,
                ChunkStorage::ClosedMutableBuffer,
                2190,
                1,
            ),
            ChunkSummary::new_without_timestamps(
                Arc::from("1970-01-05T15"),
                Arc::from("cpu"),
                1,
                ChunkStorage::OpenMutableBuffer,
                87,
                1,
            ),
        ];

        assert_eq!(
            expected, chunk_summaries,
            "expected:\n{:#?}\n\nactual:{:#?}\n\n",
            expected, chunk_summaries
        );

        assert_eq!(
            db.catalog.metrics().memory().mutable_buffer().get_total(),
            64 + 2190 + 87
        );
        assert_eq!(
            db.catalog.metrics().memory().read_buffer().get_total(),
            1484
        );
        assert_eq!(db.catalog.metrics().memory().parquet().get_total(), 663);
    }

    #[tokio::test]
    async fn partition_summaries() {
        // Test that chunk id listing is hooked up
        let db = Arc::new(make_db().await.db);

        write_lp(&db, "cpu bar=1 1").await;
        let chunk_id = db
            .rollover_partition("cpu", "1970-01-01T00")
            .await
            .unwrap()
            .unwrap()
            .id();
        write_lp(&db, "cpu bar=2,baz=3.0 2").await;
        write_lp(&db, "mem foo=1 1").await;

        // load a chunk to the read buffer
        db.move_chunk_to_read_buffer("cpu", "1970-01-01T00", chunk_id)
            .await
            .unwrap();

        // write the read buffer chunk to object store
        db.write_chunk_to_object_store("cpu", "1970-01-01T00", chunk_id)
            .await
            .unwrap();

        // write into a separate partition
        write_lp(&db, "cpu bar=1 400000000000000").await;
        write_lp(&db, "mem frob=3 400000000000001").await;

        print!("Partitions: {:?}", db.partition_keys().unwrap());

        let partition_summaries = vec![
            db.partition_summary("cpu", "1970-01-01T00").unwrap(),
            db.partition_summary("mem", "1970-01-01T00").unwrap(),
            db.partition_summary("cpu", "1970-01-05T15").unwrap(),
            db.partition_summary("mem", "1970-01-05T15").unwrap(),
        ];

        let expected = vec![
            PartitionSummary {
                key: "1970-01-01T00".into(),
                table: TableSummary {
                    name: "cpu".into(),
                    columns: vec![
                        ColumnSummary {
                            name: "bar".into(),
                            influxdb_type: Some(InfluxDbType::Field),
                            stats: Statistics::F64(StatValues::new(Some(1.0), Some(2.0), 2)),
                        },
                        ColumnSummary {
                            name: "time".into(),
                            influxdb_type: Some(InfluxDbType::Timestamp),
                            stats: Statistics::I64(StatValues::new(Some(1), Some(2), 2)),
                        },
                        ColumnSummary {
                            name: "baz".into(),
                            influxdb_type: Some(InfluxDbType::Field),
                            stats: Statistics::F64(StatValues::new(Some(3.0), Some(3.0), 1)),
                        },
                    ],
                },
            },
            PartitionSummary {
                key: "1970-01-01T00".into(),
                table: TableSummary {
                    name: "mem".into(),
                    columns: vec![
                        ColumnSummary {
                            name: "foo".into(),
                            influxdb_type: Some(InfluxDbType::Field),
                            stats: Statistics::F64(StatValues::new(Some(1.0), Some(1.0), 1)),
                        },
                        ColumnSummary {
                            name: "time".into(),
                            influxdb_type: Some(InfluxDbType::Timestamp),
                            stats: Statistics::I64(StatValues::new(Some(1), Some(1), 1)),
                        },
                    ],
                },
            },
            PartitionSummary {
                key: "1970-01-05T15".into(),
                table: TableSummary {
                    name: "cpu".into(),
                    columns: vec![
                        ColumnSummary {
                            name: "bar".into(),
                            influxdb_type: Some(InfluxDbType::Field),
                            stats: Statistics::F64(StatValues::new(Some(1.0), Some(1.0), 1)),
                        },
                        ColumnSummary {
                            name: "time".into(),
                            influxdb_type: Some(InfluxDbType::Timestamp),
                            stats: Statistics::I64(StatValues::new(
                                Some(400000000000000),
                                Some(400000000000000),
                                1,
                            )),
                        },
                    ],
                },
            },
            PartitionSummary {
                key: "1970-01-05T15".into(),
                table: TableSummary {
                    name: "mem".into(),
                    columns: vec![
                        ColumnSummary {
                            name: "frob".into(),
                            influxdb_type: Some(InfluxDbType::Field),
                            stats: Statistics::F64(StatValues::new(Some(3.0), Some(3.0), 1)),
                        },
                        ColumnSummary {
                            name: "time".into(),
                            influxdb_type: Some(InfluxDbType::Timestamp),
                            stats: Statistics::I64(StatValues::new(
                                Some(400000000000001),
                                Some(400000000000001),
                                1,
                            )),
                        },
                    ],
                },
            },
        ];

        assert_eq!(
            expected, partition_summaries,
            "expected:\n{:#?}\n\nactual:{:#?}\n\n",
            expected, partition_summaries
        );
    }

    // run a sql query against the database, returning the results as record batches
    async fn run_query(db: Arc<Db>, query: &str) -> Vec<RecordBatch> {
        let planner = SqlQueryPlanner::default();
        let executor = db.executor();

        let physical_plan = planner.query(db, query, &executor).unwrap();

        executor.collect(physical_plan).await.unwrap()
    }

    fn mutable_chunk_ids(db: &Db, partition_key: &str) -> Vec<u32> {
        let mut chunk_ids: Vec<u32> = db
            .partition_chunk_summaries(partition_key)
            .into_iter()
            .filter_map(|chunk| match chunk.storage {
                ChunkStorage::OpenMutableBuffer | ChunkStorage::ClosedMutableBuffer => {
                    Some(chunk.id)
                }
                _ => None,
            })
            .collect();
        chunk_ids.sort_unstable();
        chunk_ids
    }

    fn read_buffer_chunk_ids(db: &Db, partition_key: &str) -> Vec<u32> {
        let mut chunk_ids: Vec<u32> = db
            .partition_chunk_summaries(partition_key)
            .into_iter()
            .filter_map(|chunk| match chunk.storage {
                ChunkStorage::ReadBuffer => Some(chunk.id),
                ChunkStorage::ReadBufferAndObjectStore => Some(chunk.id),
                _ => None,
            })
            .collect();
        chunk_ids.sort_unstable();
        chunk_ids
    }

    fn read_parquet_file_chunk_ids(db: &Db, partition_key: &str) -> Vec<u32> {
        let mut chunk_ids: Vec<u32> = db
            .partition_chunk_summaries(partition_key)
            .into_iter()
            .filter_map(|chunk| match chunk.storage {
                ChunkStorage::ReadBufferAndObjectStore => Some(chunk.id),
                ChunkStorage::ObjectStoreOnly => Some(chunk.id),
                _ => None,
            })
            .collect();
        chunk_ids.sort_unstable();
        chunk_ids
    }

    #[tokio::test]
    async fn write_chunk_to_object_store_in_background() {
        // Test that data can be written to object store using a background task
        let db = Arc::new(make_db().await.db);

        // create MB partition
        write_lp(db.as_ref(), "cpu bar=1 10").await;
        write_lp(db.as_ref(), "cpu bar=2 20").await;

        // MB => RB
        let partition_key = "1970-01-01T00";
        let table_name = "cpu";
        let mb_chunk = db
            .rollover_partition(table_name, partition_key)
            .await
            .unwrap()
            .unwrap();
        let rb_chunk = db
            .move_chunk_to_read_buffer(table_name, partition_key, mb_chunk.id())
            .await
            .unwrap();
        assert_eq!(mb_chunk.id(), rb_chunk.id());

        // RB => OS
        db.write_chunk_to_object_store(table_name, partition_key, 0)
            .await
            .unwrap();

        // we should have chunks in both the read buffer only
        assert!(mutable_chunk_ids(&db, partition_key).is_empty());
        assert_eq!(read_buffer_chunk_ids(&db, partition_key), vec![0]);
        assert_eq!(read_parquet_file_chunk_ids(&db, partition_key), vec![0]);
    }

    #[tokio::test]
    async fn write_hard_limit() {
        let db = Arc::new(make_db().await.db);
        db.rules.write().lifecycle_rules.buffer_size_hard = Some(NonZeroUsize::new(10).unwrap());

        // inserting first line does not trigger hard buffer limit
        write_lp(db.as_ref(), "cpu bar=1 10").await;

        // but second line will
        assert!(matches!(
            try_write_lp(db.as_ref(), "cpu bar=2 20").await,
            Err(super::Error::HardLimitReached {})
        ));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn lock_tracker_metrics() {
        let object_store = Arc::new(ObjectStore::new_in_memory(InMemory::new()));

        // Create a DB given a server id, an object store and a db name
        let server_id = ServerId::try_from(10).unwrap();
        let db_name = "lock_tracker";
        let test_db = TestDb::builder()
            .server_id(server_id)
            .object_store(Arc::clone(&object_store))
            .db_name(db_name)
            // "dispable" clean-up by setting it to a very long time to avoid interference with this test
            .worker_cleanup_avg_sleep(Duration::from_secs(1_000))
            .build()
            .await;

        let db = Arc::new(test_db.db);

        write_lp(db.as_ref(), "cpu bar=1 10").await;

        test_db
            .metric_registry
            .has_metric_family("catalog_lock_total")
            .with_labels(&[
                ("db_name", "lock_tracker"),
                ("lock", "partition"),
                ("table", "cpu"),
                ("svr_id", "10"),
                ("access", "exclusive"),
            ])
            .counter()
            .eq(1.)
            .unwrap();

        test_db
            .metric_registry
            .has_metric_family("catalog_lock_total")
            .with_labels(&[
                ("db_name", "lock_tracker"),
                ("lock", "partition"),
                ("table", "cpu"),
                ("svr_id", "10"),
                ("access", "shared"),
            ])
            .counter()
            .eq(0.)
            .unwrap();

        let chunks = db.catalog.chunks();
        assert_eq!(chunks.len(), 1);

        let chunk_a = Arc::clone(&chunks[0]);
        let chunk_b = Arc::clone(&chunks[0]);

        let chunk_b = chunk_b.write();

        let task = tokio::spawn(async move {
            let _ = chunk_a.read();
        });

        // Hold lock for 100 milliseconds blocking background task
        std::thread::sleep(std::time::Duration::from_millis(100));

        std::mem::drop(chunk_b);
        task.await.unwrap();

        test_db
            .metric_registry
            .has_metric_family("catalog_lock_total")
            .with_labels(&[
                ("db_name", "lock_tracker"),
                ("lock", "partition"),
                ("table", "cpu"),
                ("svr_id", "10"),
                ("access", "exclusive"),
            ])
            .counter()
            .eq(1.)
            .unwrap();

        test_db
            .metric_registry
            .has_metric_family("catalog_lock_total")
            .with_labels(&[
                ("db_name", "lock_tracker"),
                ("lock", "partition"),
                ("table", "cpu"),
                ("svr_id", "10"),
                ("access", "shared"),
            ])
            .counter()
            .eq(1.)
            .unwrap();

        test_db
            .metric_registry
            .has_metric_family("catalog_lock_total")
            .with_labels(&[
                ("db_name", "lock_tracker"),
                ("lock", "chunk"),
                ("table", "cpu"),
                ("svr_id", "10"),
                ("access", "exclusive"),
            ])
            .counter()
            .eq(2.)
            .unwrap();

        test_db
            .metric_registry
            .has_metric_family("catalog_lock_total")
            .with_labels(&[
                ("db_name", "lock_tracker"),
                ("lock", "chunk"),
                ("table", "cpu"),
                ("svr_id", "10"),
                ("access", "shared"),
            ])
            .counter()
            .eq(1.)
            .unwrap();

        test_db
            .metric_registry
            .has_metric_family("catalog_lock_wait_seconds_total")
            .with_labels(&[
                ("db_name", "lock_tracker"),
                ("lock", "chunk"),
                ("svr_id", "10"),
                ("table", "cpu"),
                ("access", "shared"),
            ])
            .counter()
            .gt(0.07)
            .unwrap();
    }

    #[tokio::test]
    async fn write_one_chunk_to_preserved_catalog() {
        // Test that parquet data is committed to preserved catalog

        // ==================== setup ====================
        let object_store = Arc::new(ObjectStore::new_in_memory(InMemory::new()));
        let server_id = ServerId::try_from(1).unwrap();
        let db_name = "preserved_catalog_test";

        // ==================== do: create DB ====================
        // Create a DB given a server id, an object store and a db name
        let test_db = TestDb::builder()
            .object_store(Arc::clone(&object_store))
            .server_id(server_id)
            .db_name(db_name)
            .build()
            .await;
        let db = Arc::new(test_db.db);

        // ==================== check: empty catalog created ====================
        // at this point, an empty preserved catalog exists
        let maybe_preserved_catalog = PreservedCatalog::load::<TestCatalogState>(
            Arc::clone(&object_store),
            server_id,
            db_name.to_string(),
            (),
        )
        .await
        .unwrap();
        assert!(maybe_preserved_catalog.is_some());

        // ==================== do: write data to parquet ====================
        // create two chunks within the same table (to better test "new chunk ID" and "new table" during transaction
        // replay)
        let mut chunks = vec![];
        for _ in 0..2 {
            chunks.push(create_parquet_chunk(db.as_ref()).await);
        }

        // ==================== check: catalog state ====================
        // the preserved catalog should now register a single file
        let mut paths_expected = vec![];
        for (table_name, partition_key, chunk_id) in &chunks {
            let chunk = db.chunk(table_name, partition_key, *chunk_id).unwrap();
            let chunk = chunk.read();
            if let ChunkStage::Persisted { parquet, .. } = chunk.stage() {
                paths_expected.push(parquet.path().display());
            } else {
                panic!("Wrong chunk state.");
            }
        }
        paths_expected.sort();
        let (_preserved_catalog, catalog) = PreservedCatalog::load::<TestCatalogState>(
            Arc::clone(&object_store),
            server_id,
            db_name.to_string(),
            (),
        )
        .await
        .unwrap()
        .unwrap();
        let paths_actual = {
            let mut tmp: Vec<String> = catalog.parquet_files.keys().map(|p| p.display()).collect();
            tmp.sort();
            tmp
        };
        assert_eq!(paths_actual, paths_expected);

        // ==================== do: re-load DB ====================
        // Re-create database with same store, serverID, and DB name
        drop(db);
        let test_db = TestDb::builder()
            .object_store(Arc::clone(&object_store))
            .server_id(server_id)
            .db_name(db_name)
            .build()
            .await;
        let db = Arc::new(test_db.db);

        // ==================== check: DB state ====================
        // Re-created DB should have an "object store only"-chunk
        for (table_name, partition_key, chunk_id) in &chunks {
            let chunk = db.chunk(table_name, partition_key, *chunk_id).unwrap();
            let chunk = chunk.read();
            assert!(matches!(
                chunk.stage(),
                ChunkStage::Persisted {
                    read_buffer: None,
                    ..
                }
            ));
        }

        // ==================== check: DB still writable ====================
        write_lp(db.as_ref(), "cpu bar=1 10").await;
    }

    #[tokio::test]
    async fn object_store_cleanup() {
        // Test that stale parquet files are removed from object store

        // ==================== setup ====================
        let object_store = Arc::new(ObjectStore::new_in_memory(InMemory::new()));

        // ==================== do: create DB ====================
        // Create a DB given a server id, an object store and a db name
        let test_db = TestDb::builder()
            .object_store(Arc::clone(&object_store))
            .build()
            .await;
        let db = Arc::new(test_db.db);

        // ==================== do: write data to parquet ====================
        // create the following chunks:
        //   0: ReadBuffer + Parquet
        //   1: only Parquet
        //   2: dropped (not in current catalog but parquet file still present for time travel)
        let mut paths_keep = vec![];
        for i in 0..3i8 {
            let (table_name, partition_key, chunk_id) = create_parquet_chunk(db.as_ref()).await;
            let chunk = db.chunk(&table_name, &partition_key, chunk_id).unwrap();
            let chunk = chunk.read();
            if let ChunkStage::Persisted { parquet, .. } = chunk.stage() {
                paths_keep.push(parquet.path());
            } else {
                panic!("Wrong chunk state.");
            }

            // drop lock
            drop(chunk);

            if i == 1 {
                db.unload_read_buffer(&table_name, &partition_key, chunk_id)
                    .unwrap();
            }
            if i == 2 {
                db.drop_chunk(&table_name, &partition_key, chunk_id)
                    .unwrap();
            }
        }

        // ==================== do: create garbage ====================
        let mut path: DirsAndFileName = paths_keep[0].clone().into();
        path.file_name = Some(PathPart::from(
            format!("prefix_{}", path.file_name.unwrap().encoded()).as_ref(),
        ));
        let path_delete = object_store.path_from_dirs_and_filename(path);
        create_empty_file(&object_store, &path_delete).await;
        let path_delete = path_delete.display();

        // ==================== check: all files are there ====================
        let all_files = get_object_store_files(&object_store).await;
        for path in &paths_keep {
            assert!(all_files.contains(&path.display()));
        }

        // ==================== do: start background task loop ====================
        let shutdown: CancellationToken = Default::default();
        let shutdown_captured = shutdown.clone();
        let db_captured = Arc::clone(&db);
        let join_handle =
            tokio::spawn(async move { db_captured.background_worker(shutdown_captured).await });

        // ==================== check: after a while the dropped file should be gone ====================
        let t_0 = Instant::now();
        loop {
            let all_files = get_object_store_files(&object_store).await;
            if !all_files.contains(&path_delete) {
                break;
            }
            assert!(t_0.elapsed() < Duration::from_secs(10));
            tokio::time::sleep(Duration::from_millis(100)).await;
        }

        // ==================== do: stop background task loop ====================
        shutdown.cancel();
        join_handle.await.unwrap();

        // ==================== check: some files are there ====================
        let all_files = get_object_store_files(&object_store).await;
        assert!(!all_files.contains(&path_delete));
        for path in &paths_keep {
            assert!(all_files.contains(&path.display()));
        }
    }

    #[tokio::test]
    async fn checkpointing() {
        // Test that the preserved catalog creates checkpoints

        // ==================== setup ====================
        let object_store = Arc::new(ObjectStore::new_in_memory(InMemory::new()));
        let server_id = ServerId::try_from(1).unwrap();
        let db_name = "preserved_catalog_test";

        // ==================== do: create DB ====================
        // Create a DB given a server id, an object store and a db name
        let test_db = TestDb::builder()
            .object_store(Arc::clone(&object_store))
            .server_id(server_id)
            .db_name(db_name)
            .catalog_transactions_until_checkpoint(NonZeroU64::try_from(2).unwrap())
            .build()
            .await;
        let db = Arc::new(test_db.db);

        // ==================== do: write data to parquet ====================
        // create two chunks within the same table (to better test "new chunk ID" and "new table" during transaction
        // replay)
        let mut chunks = vec![];
        for _ in 0..2 {
            chunks.push(create_parquet_chunk(db.as_ref()).await);
        }

        // ==================== do: remove .txn files ====================
        drop(db);
        let files = object_store
            .list(None)
            .await
            .unwrap()
            .try_concat()
            .await
            .unwrap();
        let mut deleted_one = false;
        for file in files {
            let parsed: DirsAndFileName = file.clone().into();
            if parsed
                .file_name
                .map_or(false, |part| part.encoded().ends_with(".txn"))
            {
                object_store.delete(&file).await.unwrap();
                deleted_one = true;
            }
        }
        assert!(deleted_one);

        // ==================== do: re-load DB ====================
        // Re-create database with same store, serverID, and DB name
        let test_db = TestDb::builder()
            .object_store(Arc::clone(&object_store))
            .server_id(server_id)
            .db_name(db_name)
            .build()
            .await;
        let db = Arc::new(test_db.db);

        // ==================== check: DB state ====================
        // Re-created DB should have an "object store only"-chunk
        for (table_name, partition_key, chunk_id) in &chunks {
            let chunk = db.chunk(table_name, partition_key, *chunk_id).unwrap();
            let chunk = chunk.read();
            assert!(matches!(
                chunk.stage(),
                ChunkStage::Persisted {
                    read_buffer: None,
                    ..
                }
            ));
        }

        // ==================== check: DB still writable ====================
        write_lp(db.as_ref(), "cpu bar=1 10").await;
    }

    async fn create_parquet_chunk(db: &Db) -> (String, String, u32) {
        write_lp(db, "cpu bar=1 10").await;
        let partition_key = "1970-01-01T00";
        let table_name = "cpu";

        //Now mark the MB chunk close
        let chunk_id = {
            let mb_chunk = db
                .rollover_partition(table_name, partition_key)
                .await
                .unwrap()
                .unwrap();
            mb_chunk.id()
        };
        // Move that MB chunk to RB chunk and drop it from MB
        db.move_chunk_to_read_buffer(table_name, partition_key, chunk_id)
            .await
            .unwrap();

        // Write the RB chunk to Object Store but keep it in RB
        db.write_chunk_to_object_store(table_name, partition_key, chunk_id)
            .await
            .unwrap();

        (table_name.to_string(), partition_key.to_string(), chunk_id)
    }

    async fn get_object_store_files(object_store: &ObjectStore) -> HashSet<String> {
        object_store
            .list(None)
            .await
            .unwrap()
            .try_concat()
            .await
            .unwrap()
            .iter()
            .map(|p| p.display())
            .collect()
    }

    async fn create_empty_file(object_store: &ObjectStore, path: &Path) {
        let data = Bytes::default();
        let len = data.len();

        object_store
            .put(
                &path,
                futures::stream::once(async move { Ok(data) }),
                Some(len),
            )
            .await
            .unwrap();
    }
}
