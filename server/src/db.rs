//! This module contains the main IOx Database object which has the
//! instances of the mutable buffer, read buffer, and object store

use async_trait::async_trait;
use chrono::{TimeZone, Utc};
use futures::{stream::BoxStream, StreamExt};
use parking_lot::{Mutex, RwLock};
use rand_distr::{Distribution, Poisson};
use snafu::{ensure, OptionExt, ResultExt, Snafu};
use std::{
    any::Any,
    collections::HashMap,
    num::NonZeroUsize,
    sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        Arc,
    },
    time::{Duration, Instant},
};

use ::lifecycle::{LifecycleChunk, LockableChunk, LockablePartition};
use data_types::{
    chunk_metadata::{ChunkOrder, ChunkSummary},
    database_rules::DatabaseRules,
    partition_metadata::{PartitionSummary, TableSummary},
    server_id::ServerId,
};
use datafusion::catalog::{catalog::CatalogProvider, schema::SchemaProvider};
use entry::{Entry, Sequence, SequencedEntry, TableBatch};
use internal_types::schema::Schema;
use iox_object_store::IoxObjectStore;
use metrics::KeyValue;
use mutable_buffer::chunk::{ChunkMetrics as MutableBufferChunkMetrics, MBChunk};
use observability_deps::tracing::{debug, error, info};
use parquet_file::catalog::{
    api::{CatalogParquetInfo, CheckpointData, PreservedCatalog},
    cleanup::{delete_files as delete_parquet_files, get_unreferenced_parquet_files},
    prune::prune_history as prune_catalog_transaction_history,
};
use persistence_windows::{checkpoint::ReplayPlan, persistence_windows::PersistenceWindows};
use query::{
    exec::{ExecutionContextProvider, Executor, ExecutorType, IOxExecutionContext},
    predicate::Predicate,
    QueryDatabase,
};
use trace::ctx::SpanContext;
use write_buffer::{
    config::WriteBufferConfig,
    core::{FetchHighWatermark, WriteBufferError},
};

pub(crate) use crate::db::chunk::DbChunk;
use crate::{
    db::{
        self,
        access::QueryCatalogAccess,
        catalog::{
            chunk::{CatalogChunk, ChunkStage},
            partition::Partition,
            table::TableSchemaUpsertHandle,
            Catalog, Error as CatalogError, TableNameFilter,
        },
        lifecycle::{LockableCatalogChunk, LockableCatalogPartition, WeakDb},
    },
    JobRegistry,
};

pub mod access;
pub mod catalog;
mod chunk;
mod lifecycle;
pub mod load;
pub mod pred;
mod replay;
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
    WriteBufferWritingError {
        source: Box<dyn std::error::Error + Sync + Send>,
    },

    #[snafu(display("Cannot write to this database: no mutable buffer configured"))]
    DatabaseNotWriteable {},

    #[snafu(display(
        "Cannot write to this database, configured to only read from the write buffer"
    ))]
    WritingOnlyAllowedThroughWriteBuffer {},

    #[snafu(display("Hard buffer size limit reached"))]
    HardLimitReached {},

    #[snafu(display("Can not write entry {}:{} : {}", partition_key, chunk_id, source))]
    WriteEntry {
        partition_key: String,
        chunk_id: u32,
        source: mutable_buffer::chunk::Error,
    },

    #[snafu(display("Cannot write entry to new open chunk {}: {}", partition_key, source))]
    WriteEntryInitial {
        partition_key: String,
        source: mutable_buffer::chunk::Error,
    },

    #[snafu(display(
        "Cannot delete data from non-existing table, {}: {}",
        table_name,
        source
    ))]
    DeleteFromTable {
        table_name: String,
        source: CatalogError,
    },

    #[snafu(display("Internal error while adding predicate predicate to chunk: {}", source))]
    AddDeletePredicateError { source: db::catalog::chunk::Error },

    #[snafu(display(
        "Storing sequenced entry failed with the following error(s), and possibly more: {}",
        errors.iter().map(ToString::to_string).collect::<Vec<_>>().join(", ")
    ))]
    StoreSequencedEntryFailures { errors: Vec<Error> },

    #[snafu(display("background task cancelled: {}", source))]
    TaskCancelled { source: futures::future::Aborted },

    #[snafu(display("error computing time summary on table batch: {}", source))]
    TableBatchTimeError { source: entry::Error },

    #[snafu(display("error batch had null times"))]
    TableBatchMissingTimes {},

    #[snafu(display("Table batch has invalid schema: {}", source))]
    TableBatchSchemaExtractError {
        source: internal_types::schema::builder::Error,
    },

    #[snafu(display("Table batch has mismatching schema: {}", source))]
    TableBatchSchemaMergeError {
        source: internal_types::schema::merge::Error,
    },

    #[snafu(display(
        "Unable to flush partition at the moment {}:{}",
        table_name,
        partition_key,
    ))]
    CannotFlushPartition {
        table_name: String,
        partition_key: String,
    },

    #[snafu(display("Cannot create replay plan: {}", source))]
    ReplayPlanError {
        source: persistence_windows::checkpoint::Error,
    },

    #[snafu(display("Cannot replay: {}", source))]
    ReplayError { source: crate::db::replay::Error },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

/// Metrics for data ingest via write buffer.
#[derive(Debug)]
struct WriteBufferIngestMetrics {
    /// Metrics domain
    domain: Arc<metrics::Domain>,
}

impl WriteBufferIngestMetrics {
    fn new(domain: Arc<metrics::Domain>) -> Self {
        Self { domain }
    }

    fn new_sequencer_metrics(&self, sequencer_id: u32) -> SequencerMetrics {
        let attributes = vec![KeyValue::new("sequencer_id", sequencer_id.to_string())];

        let red = self
            .domain
            .register_red_metric_with_attributes(Some("ingest"), attributes.clone());
        let bytes_read = self.domain.register_counter_metric_with_attributes(
            "read",
            Some("bytes"),
            "Bytes read from sequencer",
            attributes.clone(),
        );
        let last_sequence_number = self.domain.register_gauge_metric_with_attributes(
            "last_sequence_number",
            None,
            "Last consumed sequence number (e.g. Kafka offset)",
            &attributes,
        );
        let sequence_number_lag = self.domain.register_gauge_metric_with_attributes(
            "sequence_number_lag",
            None,
            "The difference between the the last sequence number available (e.g. Kafka offset) and (= minus) last consumed sequence number",
            &attributes,
        );
        let last_min_ts = self.domain.register_gauge_metric_with_attributes(
            "last_min_ts",
            None,
            "Minimum timestamp of last write as unix timestamp in nanoseconds",
            &attributes,
        );
        let last_max_ts = self.domain.register_gauge_metric_with_attributes(
            "last_max_ts",
            None,
            "Maximum timestamp of last write as unix timestamp in nanoseconds",
            &attributes,
        );
        let last_ingest_ts = self.domain.register_gauge_metric_with_attributes(
            "last_ingest_ts",
            None,
            "Last seen ingest timestamp as unix timestamp in nanoseconds",
            &attributes,
        );

        SequencerMetrics {
            red,
            bytes_read,
            last_sequence_number,
            sequence_number_lag,
            last_min_ts,
            last_max_ts,
            last_ingest_ts,
        }
    }
}

/// Metrics for a single sequencer.
#[derive(Debug)]
struct SequencerMetrics {
    /// Metrics for tracking ingest.
    red: metrics::RedMetric,

    /// Bytes read from sequencer.
    ///
    /// This metrics is independent of the success / error state of the entries.
    bytes_read: metrics::Counter,

    /// Last consumed sequence number (e.g. Kafka offset).
    last_sequence_number: metrics::Gauge,

    // The difference between the the last sequence number available (e.g. Kafka offset) and (= minus) last consumed
    // sequence number.
    sequence_number_lag: metrics::Gauge,

    /// Minimum timestamp of last write as unix timestamp in nanoseconds.
    last_min_ts: metrics::Gauge,

    /// Maximum timestamp of last write as unix timestamp in nanoseconds.
    last_max_ts: metrics::Gauge,

    /// Last seen ingest timestamp as unix timestamp in nanoseconds.
    last_ingest_ts: metrics::Gauge,
}

/// `Db` is an instance-local, queryable, possibly persisted, and possibly mutable data store
///
/// It is responsible for:
///
/// * Receiving new writes for this IOx instance
/// * Exposing APIs for the lifecycle policy to compact/persist data
/// * Exposing APIs for the query engine to use to query data
///
/// The data in a `Db` is structured in this way:
///
/// ┌───────────────────────────────────────────────┐
/// │                                               │
/// │    ┌────────────────┐                         │
/// │    │    Database    │                         │
/// │    └────────────────┘                         │
/// │             │  multiple Tables (measurements) │
/// │             ▼                                 │
/// │    ┌────────────────┐                         │
/// │    │     Table      │                         │
/// │    └────────────────┘                         │
/// │             │ one partition per               │
/// │             │ partition_key                   │
/// │             ▼                                 │
/// │    ┌────────────────┐                         │
/// │    │   Partition    │                         │
/// │    └────────────────┘                         │
/// │             │  one open Chunk                 │
/// │             │  zero or more closed            │
/// │             ▼  Chunks                         │
/// │    ┌────────────────┐                         │
/// │    │     Chunk      │                         │
/// │    └────────────────┘                         │
/// │             │  multiple Columns               │
/// │             ▼                                 │
/// │    ┌────────────────┐                         │
/// │    │     Column     │                         │
/// │    └────────────────┘                         │
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
    rules: RwLock<Arc<DatabaseRules>>,

    server_id: ServerId, // this is also the Query Server ID

    /// Interface to use for persistence
    iox_object_store: Arc<IoxObjectStore>,

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

    /// The global metrics registry
    metrics_registry_v2: Arc<metric::Registry>,

    /// Catalog interface for query
    catalog_access: Arc<QueryCatalogAccess>,

    /// Number of iterations of the worker lifecycle loop for this Db
    worker_iterations_lifecycle: AtomicUsize,

    /// Number of iterations of the worker cleanup loop for this Db
    worker_iterations_cleanup: AtomicUsize,

    /// Metric attributes
    metric_attributes: Vec<KeyValue>,

    /// Ingest metrics
    ingest_metrics: WriteBufferIngestMetrics,

    /// Optionally connect to a write buffer for either buffering writes or reading buffered writes
    write_buffer: Option<WriteBufferConfig>,

    /// Lock that prevents the cleanup job from deleting files that are written but not yet added to the preserved
    /// catalog.
    ///
    /// The cleanup job needs exclusive access and hence will acquire a write-guard. Creating parquet files and creating
    /// catalog transaction only needs shared access and hence will acquire a read-guard.
    cleanup_lock: Arc<tokio::sync::RwLock<()>>,

    /// Lifecycle policy.
    ///
    /// Optional because it will be created after `Arc<Self>`.
    ///
    /// This is stored here for the following reasons:
    /// - to control the persistence suppression via a [`Db::unsuppress_persistence`]
    /// - to keep the lifecycle state (e.g. the number of running compactions) around
    lifecycle_policy: tokio::sync::Mutex<Option<::lifecycle::LifecyclePolicy<WeakDb>>>,

    /// Flag to stop background worker from reading from the write buffer.
    ///
    /// TODO: Move write buffer read loop out of Db.
    no_write_buffer_read: AtomicBool,

    /// TESTING ONLY: Mocked `Instant::now()` for the background worker
    background_worker_now_override: Mutex<Option<Instant>>,
}

/// All the information needed to commit a database
#[derive(Debug)]
pub(crate) struct DatabaseToCommit {
    pub(crate) server_id: ServerId,
    pub(crate) iox_object_store: Arc<IoxObjectStore>,
    pub(crate) exec: Arc<Executor>,
    pub(crate) preserved_catalog: PreservedCatalog,
    pub(crate) catalog: Catalog,
    pub(crate) rules: Arc<DatabaseRules>,
    pub(crate) write_buffer: Option<WriteBufferConfig>,
    pub(crate) metrics_registry_v2: Arc<metric::Registry>,
}

impl Db {
    pub(crate) fn new(database_to_commit: DatabaseToCommit, jobs: Arc<JobRegistry>) -> Arc<Self> {
        let db_name = database_to_commit.rules.name.clone();

        let rules = RwLock::new(database_to_commit.rules);
        let server_id = database_to_commit.server_id;
        let iox_object_store = Arc::clone(&database_to_commit.iox_object_store);
        let metrics_registry = Arc::clone(&database_to_commit.catalog.metrics_registry);
        let metric_attributes = database_to_commit.catalog.metric_attributes.clone();

        let ingest_domain = metrics_registry
            .register_domain_with_attributes("write_buffer", metric_attributes.clone());
        let ingest_metrics = WriteBufferIngestMetrics::new(Arc::new(ingest_domain));

        let catalog = Arc::new(database_to_commit.catalog);

        let catalog_access = QueryCatalogAccess::new(
            &db_name,
            Arc::clone(&catalog),
            Arc::clone(&jobs),
            database_to_commit.metrics_registry_v2.as_ref(),
        );
        let catalog_access = Arc::new(catalog_access);

        let this = Self {
            rules,
            server_id,
            iox_object_store,
            exec: database_to_commit.exec,
            preserved_catalog: Arc::new(database_to_commit.preserved_catalog),
            catalog,
            jobs,
            metrics_registry,
            metrics_registry_v2: database_to_commit.metrics_registry_v2,
            catalog_access,
            worker_iterations_lifecycle: AtomicUsize::new(0),
            worker_iterations_cleanup: AtomicUsize::new(0),
            metric_attributes,
            ingest_metrics,
            write_buffer: database_to_commit.write_buffer,
            cleanup_lock: Default::default(),
            lifecycle_policy: tokio::sync::Mutex::new(None),
            no_write_buffer_read: AtomicBool::new(true),
            background_worker_now_override: Default::default(),
        };
        let this = Arc::new(this);
        *this.lifecycle_policy.try_lock().expect("not used yet") = Some(
            ::lifecycle::LifecyclePolicy::new_suppress_persistence(WeakDb(Arc::downgrade(&this))),
        );
        this
    }

    /// Allow persistence if database rules all it.
    pub async fn unsuppress_persistence(&self) {
        let mut guard = self.lifecycle_policy.lock().await;
        let policy = guard
            .as_mut()
            .expect("lifecycle policy should be initialized");
        policy.unsuppress_persistence();
    }

    /// Allow continuous reads from the write buffer (if configured).
    ///
    /// TODO: Move write buffer read loop out of Db.
    pub fn allow_write_buffer_read(&self) {
        self.no_write_buffer_read.store(false, Ordering::SeqCst);
    }

    /// Return a handle to the executor used to run queries
    pub fn executor(&self) -> Arc<Executor> {
        Arc::clone(&self.exec)
    }

    /// Return the current database rules
    pub fn rules(&self) -> Arc<DatabaseRules> {
        Arc::clone(&*self.rules.read())
    }

    /// Updates the database rules
    pub fn update_rules(&self, new_rules: Arc<DatabaseRules>) {
        let late_arrive_window_updated = {
            let mut rules = self.rules.write();
            info!(db_name=%rules.name,  "updating rules for database");
            let late_arrive_window_updated = rules.lifecycle_rules.late_arrive_window_seconds
                != new_rules.lifecycle_rules.late_arrive_window_seconds;

            *rules = Arc::clone(&new_rules);
            late_arrive_window_updated
        };

        if late_arrive_window_updated {
            // Hold a read lock to prevent concurrent modification and
            // use values from re-acquired read guard
            let current = self.rules.read();

            // Update windows
            let partitions = self.catalog.partitions();
            for partition in &partitions {
                let mut partition = partition.write();
                let addr = partition.addr().clone();
                if let Some(windows) = partition.persistence_windows_mut() {
                    info!(partition=%addr, "updating persistence windows");
                    windows.set_late_arrival_period(Duration::from_secs(
                        current.lifecycle_rules.late_arrive_window_seconds.get() as u64,
                    ))
                }
            }
        }
    }

    /// Return the current database's object storage
    pub fn iox_object_store(&self) -> Arc<IoxObjectStore> {
        Arc::clone(&self.iox_object_store)
    }

    /// Rolls over the active chunk in the database's specified
    /// partition. Returns the previously open (now closed) Chunk if
    /// there was any.
    ///
    /// NOTE: this function is only used in tests and can be invoked
    /// by the management API. It is not called automatically by the
    /// lifecycle manager during normal operation.
    pub async fn rollover_partition(
        &self,
        table_name: &str,
        partition_key: &str,
    ) -> Result<Option<Arc<DbChunk>>> {
        let chunk = self
            .partition(table_name, partition_key)?
            .read()
            .open_chunk();

        info!(%table_name, %partition_key, found_chunk=chunk.is_some(), "rolling over a partition");
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
    ) -> catalog::Result<(Arc<tracker::RwLock<CatalogChunk>>, ChunkOrder)> {
        self.catalog.chunk(table_name, partition_key, chunk_id)
    }

    pub fn lockable_chunk(
        self: &Arc<Self>,
        table_name: &str,
        partition_key: &str,
        chunk_id: u32,
    ) -> catalog::Result<LockableCatalogChunk> {
        let (chunk, order) = self.chunk(table_name, partition_key, chunk_id)?;
        Ok(LockableCatalogChunk {
            db: Arc::clone(self),
            chunk,
            id: chunk_id,
            order,
        })
    }

    /// Drops the specified chunk from the catalog and all storage systems
    pub async fn drop_chunk(
        self: &Arc<Self>,
        table_name: &str,
        partition_key: &str,
        chunk_id: u32,
    ) -> Result<()> {
        // Use explicit scope to ensure the async generator doesn't
        // assume the locks have to possibly live across the `await`
        let fut = {
            let partition = self.partition(table_name, partition_key)?;
            let partition = LockableCatalogPartition::new(Arc::clone(self), partition);

            // Do lock dance to get a write lock on the partition as well
            // as on the to-be-dropped chunk.
            let partition = partition.read();

            let chunk = self.lockable_chunk(table_name, partition_key, chunk_id)?;
            let partition = partition.upgrade();

            let (_, fut) =
                lifecycle::drop_chunk(partition, chunk.write()).context(LifecycleError)?;
            fut
        };

        fut.await.context(TaskCancelled)?.context(LifecycleError)
    }

    /// Drops the specified partition from the catalog and all storage systems
    pub async fn drop_partition(
        self: &Arc<Self>,
        table_name: &str,
        partition_key: &str,
    ) -> Result<()> {
        // Use explicit scope to ensure the async generator doesn't
        // assume the locks have to possibly live across the `await`
        let fut = {
            let partition = self.partition(table_name, partition_key)?;
            let partition = LockableCatalogPartition::new(Arc::clone(self), partition);
            let partition = partition.write();
            let (_, fut) = lifecycle::drop_partition(partition).context(LifecycleError)?;
            fut
        };

        fut.await.context(TaskCancelled)?.context(LifecycleError)
    }

    /// Delete data from  a table on a specified predicate
    pub async fn delete(
        self: &Arc<Self>,
        table_name: &str,
        delete_predicate: &Predicate,
    ) -> Result<()> {
        // get all partitions of this table
        let table = self
            .catalog
            .table(table_name)
            .context(DeleteFromTable { table_name })?;
        let partitions = table.partitions();
        for partition in partitions {
            let partition = partition.write();
            let chunks = partition.chunks();
            for chunk in chunks {
                // save the delete predicate in the chunk
                let mut chunk = chunk.write();
                chunk
                    .add_delete_predicate(delete_predicate)
                    .context(AddDeletePredicateError)?;
            }
        }

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
        self: &Arc<Self>,
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
        self: &Arc<Self>,
        table_name: &str,
        partition_key: &str,
    ) -> Result<Arc<DbChunk>> {
        // Use explicit scope to ensure the async generator doesn't
        // assume the locks have to possibly live across the `await`
        let fut = {
            let partition = self.partition(table_name, partition_key)?;
            let partition = LockableCatalogPartition::new(Arc::clone(self), partition);

            // Do lock dance to get a write lock on the partition as well
            // as on all of the chunks
            let partition = partition.read();

            // Get a list of all the chunks to compact
            let chunks = LockablePartition::chunks(&partition);
            let partition = partition.upgrade();
            let chunks = chunks.iter().map(|chunk| chunk.write()).collect();

            let (_, fut) = lifecycle::compact_chunks(partition, chunks).context(LifecycleError)?;
            fut
        };

        fut.await.context(TaskCancelled)?.context(LifecycleError)
    }

    /// Persist given partition.
    ///
    /// Errors if there is nothing to persist at the moment as per the lifecycle rules. If successful it returns the
    /// chunk that contains the persisted data.
    ///
    /// The `now` timestamp should normally be `Instant::now()` but can be altered for testing.
    pub async fn persist_partition(
        self: &Arc<Self>,
        table_name: &str,
        partition_key: &str,
        now: Instant,
    ) -> Result<Arc<DbChunk>> {
        // Use explicit scope to ensure the async generator doesn't
        // assume the locks have to possibly live across the `await`
        let fut = {
            let partition = self.partition(table_name, partition_key)?;
            let partition = LockableCatalogPartition::new(Arc::clone(self), partition);
            let partition = partition.read();

            let chunks = LockablePartition::chunks(&partition);
            let mut partition = partition.upgrade();

            // get flush handle
            let flush_handle = partition
                .persistence_windows_mut()
                .map(|window| window.flush_handle(now))
                .flatten()
                .context(CannotFlushPartition {
                    table_name,
                    partition_key,
                })?;

            // get chunks for persistence, break after first chunk that cannot be persisted due to lifecycle reasons
            let chunks: Vec<_> = chunks
                .iter()
                .filter_map(|chunk| {
                    let chunk = chunk.read();
                    if matches!(chunk.stage(), ChunkStage::Persisted { .. })
                        || (chunk.min_timestamp() > flush_handle.timestamp())
                    {
                        None
                    } else {
                        Some(chunk)
                    }
                })
                .map(|chunk| {
                    if chunk.lifecycle_action().is_some() {
                        None
                    } else {
                        Some(chunk.upgrade())
                    }
                })
                .fuse()
                .flatten()
                .collect();

            ensure!(
                !chunks.is_empty(),
                CannotFlushPartition {
                    table_name,
                    partition_key
                }
            );

            let (_, fut) = lifecycle::persist_chunks(partition, chunks, flush_handle)
                .context(LifecycleError)?;
            fut
        };

        fut.await.context(TaskCancelled)?.context(LifecycleError)
    }

    /// Unload chunk from read buffer but keep it in object store
    pub fn unload_read_buffer(
        self: &Arc<Self>,
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
            .ok()
            .and_then(|partition| partition.read().summary())
    }

    /// Return table summary information for the given chunk in the specified
    /// partition
    pub fn table_summary(
        &self,
        table_name: &str,
        partition_key: &str,
        chunk_id: u32,
    ) -> Option<Arc<TableSummary>> {
        let (chunk, _order) = self.chunk(table_name, partition_key, chunk_id).ok()?;
        let chunk = chunk.read();
        Some(chunk.table_summary())
    }

    /// Returns the number of iterations of the background worker lifecycle loop
    pub fn worker_iterations_lifecycle(&self) -> usize {
        self.worker_iterations_lifecycle.load(Ordering::Relaxed)
    }

    /// Returns the number of iterations of the background worker lifecycle loop
    pub fn worker_iterations_cleanup(&self) -> usize {
        self.worker_iterations_cleanup.load(Ordering::Relaxed)
    }

    /// Perform sequencer-driven replay for this DB.
    ///
    /// When `replay_plan` is `None` then no real replay will be performed. Instead the write buffer streams will be set
    /// to the current high watermark and normal playback will continue from there.
    pub async fn perform_replay(&self, replay_plan: Option<&ReplayPlan>) -> Result<()> {
        use crate::db::replay::{perform_replay, seek_to_end};
        if let Some(replay_plan) = replay_plan {
            perform_replay(self, replay_plan).await.context(ReplayError)
        } else {
            seek_to_end(self).await.context(ReplayError)
        }
    }

    /// Background worker function
    pub async fn background_worker(
        self: &Arc<Self>,
        shutdown: tokio_util::sync::CancellationToken,
    ) {
        info!("started background worker");

        // Loop that drives the lifecycle for this database
        let lifecycle_loop = async {
            loop {
                self.worker_iterations_lifecycle
                    .fetch_add(1, Ordering::Relaxed);
                let mut guard = self.lifecycle_policy.lock().await;

                let policy = guard
                    .as_mut()
                    .expect("lifecycle policy should be initialized");

                policy
                    .check_for_work(Utc::now(), self.background_worker_now())
                    .await
            }
        };

        // object store cleanup loop
        let object_store_cleanup_loop = async {
            loop {
                self.worker_iterations_cleanup
                    .fetch_add(1, Ordering::Relaxed);

                // read relevant parts of the db rules
                let (avg_sleep_secs, catalog_transaction_prune_age) = {
                    let guard = self.rules.read();
                    let avg_sleep_secs = guard.worker_cleanup_avg_sleep.as_secs_f32().max(1.0);
                    let catalog_transaction_prune_age =
                        guard.lifecycle_rules.catalog_transaction_prune_age;
                    (avg_sleep_secs, catalog_transaction_prune_age)
                };

                // Sleep for a duration drawn from a poisson distribution to de-correlate workers.
                // Perform this sleep BEFORE the actual clean-up so that we don't immediately run a clean-up
                // on startup.
                let dist =
                    Poisson::new(avg_sleep_secs).expect("parameter should be positive and finite");
                let duration = Duration::from_secs_f32(dist.sample(&mut rand::thread_rng()));
                debug!(?duration, "cleanup worker sleeps");
                tokio::time::sleep(duration).await;

                match chrono::Duration::from_std(catalog_transaction_prune_age) {
                    Ok(catalog_transaction_prune_age) => {
                        if let Err(e) = prune_catalog_transaction_history(
                            self.iox_object_store(),
                            Utc::now() - catalog_transaction_prune_age,
                        )
                        .await
                        {
                            error!(%e, "error while pruning catalog transactions");
                        }
                    }
                    Err(e) => {
                        error!(%e, "cannot convert `catalog_transaction_prune_age`, skipping transaction pruning");
                    }
                }

                if let Err(e) = self.cleanup_unreferenced_parquet_files().await {
                    error!(%e, "error while cleaning unreferenced parquet files");
                }
            }
        };

        // streaming from the write buffer loop
        let write_buffer = async {
            if let Some(WriteBufferConfig::Reading(write_buffer)) = &self.write_buffer {
                // wait for permission
                while self.no_write_buffer_read.load(Ordering::SeqCst) {
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }

                let mut write_buffer = write_buffer
                    .try_lock()
                    .expect("no streams should exist at this point");
                let mut futures = vec![];
                for (sequencer_id, stream) in write_buffer.streams() {
                    let metrics = self.ingest_metrics.new_sequencer_metrics(sequencer_id);
                    let fut = self.stream_in_sequenced_entries(
                        sequencer_id,
                        stream.stream,
                        stream.fetch_high_watermark,
                        metrics,
                    );
                    futures.push(fut);
                }

                futures::future::join_all(futures).await;
            } else {
                futures::future::pending::<()>().await;
            }
        };

        // None of the futures need to perform drain logic on shutdown.
        // When the first one finishes, all of them are dropped
        tokio::select! {
            _ = lifecycle_loop => error!("lifecycle loop exited - database worker bailing out"),
            _ = write_buffer => error!("write buffer loop exited - database worker bailing out"),
            _ = object_store_cleanup_loop => error!("object store cleanup exited - database worker bailing out"),
            _ = shutdown.cancelled() => info!("database worker shutting down"),
        }

        info!("finished background worker");
    }

    /// This is used to take entries from a `Stream` and put them in the mutable buffer, such as
    /// streaming entries from a write buffer.
    async fn stream_in_sequenced_entries<'a>(
        &'a self,
        sequencer_id: u32,
        mut stream: BoxStream<'a, Result<SequencedEntry, WriteBufferError>>,
        f_mark: FetchHighWatermark<'a>,
        mut metrics: SequencerMetrics,
    ) {
        let db_name = self.rules.read().db_name().to_string();
        let mut watermark_last_updated: Option<Instant> = None;
        let mut watermark = 0;

        while let Some(sequenced_entry_result) = stream.next().await {
            let red_observation = metrics.red.observation();

            // get entry from sequencer
            let sequenced_entry = match sequenced_entry_result {
                Ok(sequenced_entry) => sequenced_entry,
                Err(e) => {
                    debug!(
                        %e,
                        %db_name,
                        sequencer_id,
                        "Error converting write buffer data to SequencedEntry",
                    );
                    red_observation.client_error();
                    continue;
                }
            };
            let sequenced_entry = Arc::new(sequenced_entry);

            // store entry
            let mut logged_hard_limit = false;
            loop {
                match self.store_sequenced_entry(
                    Arc::clone(&sequenced_entry),
                    filter_table_batch_keep_all,
                ) {
                    Ok(_) => {
                        red_observation.ok();
                        break;
                    }
                    Err(Error::HardLimitReached {}) => {
                        // wait a bit and retry
                        if !logged_hard_limit {
                            info!(
                                %db_name,
                                sequencer_id,
                                "Hard limit reached while reading from write buffer, waiting for compaction to catch up",
                            );
                            logged_hard_limit = true;
                        }
                        tokio::time::sleep(Duration::from_millis(100)).await;
                        continue;
                    }
                    Err(e) => {
                        debug!(
                            %e,
                            %db_name,
                            sequencer_id,
                            "Error storing SequencedEntry from write buffer in database"
                        );
                        red_observation.error();

                        // no retry
                        break;
                    }
                }
            }

            // maybe update sequencer watermark
            // We are not updating this watermark every round because asking the sequencer for that watermark can be
            // quite expensive.
            let now = self.background_worker_now();
            if watermark_last_updated
                .map(|ts| (now - ts) > Duration::from_secs(10))
                .unwrap_or(true)
            {
                match f_mark().await {
                    Ok(w) => {
                        watermark = w;
                    }
                    Err(e) => {
                        debug!(
                            %e,
                            %db_name,
                            sequencer_id,
                            "Error while reading sequencer watermark",
                        )
                    }
                }
                watermark_last_updated = Some(now);
            }

            // update:
            // - bytes read
            // - last sequence number
            // - lag
            // - min ts
            // - max ts
            // - ingest ts
            let sequence = sequenced_entry
                .sequence()
                .expect("entry from write buffer must be sequenced");
            let producer_wallclock_timestamp = sequenced_entry
                .producer_wallclock_timestamp()
                .expect("entry from write buffer must have a producer wallclock time");
            let entry = sequenced_entry.entry();
            metrics.bytes_read.add(entry.data().len() as u64);
            metrics
                .last_sequence_number
                .set(sequence.number as usize, &[]);
            metrics.sequence_number_lag.set(
                watermark.saturating_sub(sequence.number).saturating_sub(1) as usize,
                &[],
            );
            if let Some(min_ts) = entry
                .partition_writes()
                .map(|partition_writes| {
                    partition_writes
                        .iter()
                        .filter_map(|partition_write| {
                            partition_write
                                .table_batches()
                                .iter()
                                .filter_map(|table_batch| table_batch.min_max_time().ok())
                                .map(|(min, _max)| min)
                                .max()
                        })
                        .min()
                })
                .flatten()
            {
                metrics
                    .last_min_ts
                    .set(min_ts.timestamp_nanos() as usize, &[]);
            }
            if let Some(max_ts) = entry
                .partition_writes()
                .map(|partition_writes| {
                    partition_writes
                        .iter()
                        .filter_map(|partition_write| {
                            partition_write
                                .table_batches()
                                .iter()
                                .filter_map(|table_batch| table_batch.min_max_time().ok())
                                .map(|(_min, max)| max)
                                .max()
                        })
                        .max()
                })
                .flatten()
            {
                metrics
                    .last_max_ts
                    .set(max_ts.timestamp_nanos() as usize, &[]);
            }
            metrics
                .last_ingest_ts
                .set(producer_wallclock_timestamp.timestamp_nanos() as usize, &[]);
        }
    }

    /// `Instant::now()` that is used by the background worker. Can be mocked for testing.
    fn background_worker_now(&self) -> Instant {
        let mut guard = self.background_worker_now_override.lock();
        match *guard {
            Some(now) => {
                *guard = Some(now + Duration::from_nanos(1));
                now
            }
            None => Instant::now(),
        }
    }

    async fn cleanup_unreferenced_parquet_files(
        self: &Arc<Self>,
    ) -> std::result::Result<(), parquet_file::catalog::cleanup::Error> {
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
        debug!(%immutable, has_write_buffer=self.write_buffer.is_some(), "storing entry");

        match (self.write_buffer.as_ref(), immutable) {
            (Some(WriteBufferConfig::Writing(write_buffer)), true) => {
                // If only the write buffer is configured, this is passing the data through to
                // the write buffer, and it's not an error. We ignore the returned metadata; it
                // will get picked up when data is read from the write buffer.

                // TODO: be smarter than always using sequencer 0
                let _ = write_buffer
                    .store_entry(&entry, 0)
                    .await
                    .context(WriteBufferWritingError)?;
                Ok(())
            }
            (Some(WriteBufferConfig::Writing(write_buffer)), false) => {
                // If using both write buffer and mutable buffer, we want to wait for the write
                // buffer to return success before adding the entry to the mutable buffer.

                // TODO: be smarter than always using sequencer 0
                let (sequence, producer_wallclock_timestamp) = write_buffer
                    .store_entry(&entry, 0)
                    .await
                    .context(WriteBufferWritingError)?;
                let sequenced_entry = Arc::new(SequencedEntry::new_from_sequence(
                    sequence,
                    producer_wallclock_timestamp,
                    entry,
                ));

                self.store_sequenced_entry(sequenced_entry, filter_table_batch_keep_all)
            }
            (_, true) => {
                // If not configured to send entries to the write buffer and the database is
                // immutable, trying to store an entry is an error and we don't need to build a
                // `SequencedEntry`.
                DatabaseNotWriteable {}.fail()
            }
            (Some(WriteBufferConfig::Reading(_)), false) => {
                // If configured to read entries from the write buffer, we shouldn't be here
                WritingOnlyAllowedThroughWriteBuffer {}.fail()
            }
            (None, false) => {
                // If no write buffer is configured, nothing is
                // sequencing entries so skip doing so here
                let sequenced_entry = Arc::new(SequencedEntry::new_unsequenced(entry));

                self.store_sequenced_entry(sequenced_entry, filter_table_batch_keep_all)
            }
        }
    }

    /// Given a `SequencedEntry`, if the mutable buffer is configured, the `SequencedEntry` is then
    /// written into the mutable buffer.
    ///
    /// # Filtering
    /// `filter_table_batch` can be used to filter out table batches. It gets:
    ///
    /// 1. the current sequence
    /// 2. the partition key
    /// 3. the table batch (which also contains the table name)
    ///
    /// It shall return `(true, _)` if the batch should be stored and `(false, _)` otherwise. In the first case the
    /// second element in the tuple is a row-wise mask. If it is provided only rows marked with `true` are stored.
    pub fn store_sequenced_entry<F>(
        &self,
        sequenced_entry: Arc<SequencedEntry>,
        filter_table_batch: F,
    ) -> Result<()>
    where
        F: Fn(Option<&Sequence>, &str, &TableBatch<'_>) -> (bool, Option<Vec<bool>>),
    {
        // Get all needed database rule values, then release the lock
        let rules = self.rules.read();
        let immutable = rules.lifecycle_rules.immutable;
        let buffer_size_hard = rules.lifecycle_rules.buffer_size_hard;
        let late_arrival_window = rules.lifecycle_rules.late_arrive_window();
        let mub_row_threshold = rules.lifecycle_rules.mub_row_threshold;
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

        // Note: as this is taken before any synchronisation writes may arrive to a chunk
        // out of order w.r.t this timestamp. As DateTime<Utc> isn't monotonic anyway
        // this isn't an issue
        let time_of_write = Utc::now();

        if let Some(partitioned_writes) = sequenced_entry.partition_writes() {
            let sequence = sequenced_entry.as_ref().sequence();

            // Protect against DoS by limiting the number of errors we might collect
            const MAX_ERRORS_PER_SEQUENCED_ENTRY: usize = 10;

            let mut errors = Vec::with_capacity(MAX_ERRORS_PER_SEQUENCED_ENTRY);

            for write in partitioned_writes {
                let partition_key = write.key();
                for table_batch in write.table_batches() {
                    let row_count = match NonZeroUsize::new(table_batch.row_count()) {
                        Some(row_count) => row_count,
                        None => continue,
                    };

                    let (store_batch, mask) =
                        filter_table_batch(sequence, partition_key, &table_batch);
                    if !store_batch {
                        continue;
                    }

                    let (partition, table_schema) = self
                        .catalog
                        .get_or_create_partition(table_batch.name(), partition_key);

                    let batch_schema =
                        match table_batch.schema().context(TableBatchSchemaExtractError) {
                            Ok(batch_schema) => batch_schema,
                            Err(e) => {
                                if errors.len() < MAX_ERRORS_PER_SEQUENCED_ENTRY {
                                    errors.push(e);
                                }
                                continue;
                            }
                        };

                    let schema_handle =
                        match TableSchemaUpsertHandle::new(&table_schema, &batch_schema)
                            .context(TableBatchSchemaMergeError)
                        {
                            Ok(schema_handle) => schema_handle,
                            Err(e) => {
                                if errors.len() < MAX_ERRORS_PER_SEQUENCED_ENTRY {
                                    errors.push(e);
                                }
                                continue;
                            }
                        };

                    let timestamp_summary = table_batch
                        .timestamp_summary()
                        .context(TableBatchTimeError)?;

                    // At this point this should not be possible
                    ensure!(
                        timestamp_summary.stats.total_count == row_count.get() as u64,
                        TableBatchMissingTimes {}
                    );

                    let mut partition = partition.write();

                    let handle_chunk_write = |chunk: &mut CatalogChunk| {
                        chunk.record_write(time_of_write, &timestamp_summary);
                        if chunk.storage().0 >= mub_row_threshold.get() {
                            chunk.freeze().expect("freeze mub chunk");
                        }
                    };

                    match partition.open_chunk() {
                        Some(chunk) => {
                            let mut chunk = chunk.write();
                            let chunk_id = chunk.id();

                            let mb_chunk =
                                chunk.mutable_buffer().expect("cannot mutate open chunk");

                            if let Err(e) = mb_chunk
                                .write_table_batch(table_batch, mask.as_ref().map(|x| x.as_ref()))
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
                            handle_chunk_write(&mut *chunk)
                        }
                        None => {
                            let chunk_result = MBChunk::new(
                                MutableBufferChunkMetrics::new(self.metrics_registry_v2.as_ref()),
                                table_batch,
                                mask.as_ref().map(|x| x.as_ref()),
                            )
                            .context(WriteEntryInitial { partition_key });

                            match chunk_result {
                                Ok(mb_chunk) => {
                                    let chunk =
                                        partition.create_open_chunk(mb_chunk, time_of_write);
                                    let mut chunk = chunk
                                        .try_write()
                                        .expect("partition lock should prevent contention");
                                    handle_chunk_write(&mut *chunk)
                                }
                                Err(e) => {
                                    if errors.len() < MAX_ERRORS_PER_SEQUENCED_ENTRY {
                                        errors.push(e);
                                    }
                                    continue;
                                }
                            }
                        }
                    };

                    partition.update_last_write_at();

                    schema_handle.commit();

                    // TODO: PersistenceWindows use TimestampSummary
                    let min_time = Utc.timestamp_nanos(timestamp_summary.stats.min.unwrap());
                    let max_time = Utc.timestamp_nanos(timestamp_summary.stats.max.unwrap());

                    match partition.persistence_windows_mut() {
                        Some(windows) => {
                            windows.add_range(
                                sequence,
                                row_count,
                                min_time,
                                max_time,
                                self.background_worker_now(),
                            );
                        }
                        None => {
                            let mut windows = PersistenceWindows::new(
                                partition.addr().clone(),
                                late_arrival_window,
                                self.background_worker_now(),
                            );
                            windows.add_range(
                                sequence,
                                row_count,
                                min_time,
                                max_time,
                                self.background_worker_now(),
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

fn filter_table_batch_keep_all(
    _sequence: Option<&Sequence>,
    _partition_key: &str,
    _batch: &TableBatch<'_>,
) -> (bool, Option<Vec<bool>>) {
    (true, None)
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

    fn table_schema(&self, table_name: &str) -> Option<Arc<Schema>> {
        self.catalog_access.table_schema(table_name)
    }
}

impl ExecutionContextProvider for Db {
    fn new_query_context(self: &Arc<Self>, span_ctx: Option<SpanContext>) -> IOxExecutionContext {
        self.exec
            .new_execution_config(ExecutorType::Query)
            .with_default_catalog(Arc::<Self>::clone(self))
            .with_span_context(span_ctx)
            .build()
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
            let path = parquet.path().clone();

            let m = CatalogParquetInfo {
                path: path.clone(),
                file_size_bytes: parquet.file_size_bytes(),
                metadata: parquet.parquet_metadata(),
            };

            files.insert(path, m);
        }
    }

    CheckpointData { files }
}

pub mod test_helpers {
    use std::collections::HashSet;

    use arrow::record_batch::RecordBatch;

    use entry::test_helpers::lp_to_entries;
    use query::frontend::sql::SqlQueryPlanner;

    use super::*;

    /// Try to write lineprotocol data and return all tables that where written.
    pub async fn try_write_lp(db: &Db, lp: &str) -> Result<Vec<String>> {
        let entries = {
            let partitioner = &db.rules.read().partition_template;
            lp_to_entries(lp, partitioner)
        };

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

    /// Convenience macro to test if an [`db::Error`](crate::db::Error) is a
    /// [StoreSequencedEntryFailures](crate::db::Error::StoreSequencedEntryFailures) and then check for errors contained
    /// in it.
    #[macro_export]
    macro_rules! assert_store_sequenced_entry_failures {
        ($e:expr, [$($sub:pat),*]) => {
            {
                // bind $e to variable so we don't evaluate it twice
                let e = $e;

                if let $crate::db::Error::StoreSequencedEntryFailures{errors} = e {
                    assert!(matches!(&errors[..], [$($sub),*]));
                } else {
                    panic!("Expected StoreSequencedEntryFailures but got {}", e);
                }
            }
        };
    }

    /// Run a sql query against the database, returning the results as record batches.
    pub async fn run_query(db: Arc<Db>, query: &str) -> Vec<RecordBatch> {
        let planner = SqlQueryPlanner::default();
        let ctx = db.new_query_context(None);
        let physical_plan = planner.query(query, &ctx).unwrap();
        ctx.collect(physical_plan).await.unwrap()
    }
}

#[cfg(test)]
mod tests {
    use std::{
        collections::BTreeMap,
        convert::TryFrom,
        iter::Iterator,
        num::{NonZeroU32, NonZeroU64, NonZeroUsize},
        ops::Deref,
        str,
        time::{Duration, Instant},
    };

    use arrow::record_batch::RecordBatch;
    use bytes::Bytes;
    use chrono::{DateTime, TimeZone};
    use futures::{stream, StreamExt, TryStreamExt};
    use tokio_util::sync::CancellationToken;

    use ::test_helpers::{assert_contains, tracing::TracingCapture};
    use arrow_util::{assert_batches_eq, assert_batches_sorted_eq};
    use data_types::{
        chunk_metadata::{ChunkAddr, ChunkStorage},
        database_rules::{LifecycleRules, PartitionTemplate, TemplatePart},
        partition_metadata::{ColumnSummary, InfluxDbType, StatValues, Statistics, TableSummary},
        write_summary::TimestampSummary,
    };
    use entry::{test_helpers::lp_to_entry, Sequence};
    use internal_types::{schema::Schema, selection::Selection};
    use iox_object_store::ParquetFilePath;
    use metric::{Attributes, CumulativeGauge, Metric, Observation};
    use object_store::ObjectStore;
    use parquet_file::{
        catalog::test_helpers::TestCatalogState,
        metadata::IoxParquetMetaData,
        test_utils::{load_parquet_from_store_for_path, read_data_from_parquet_data},
    };
    use persistence_windows::min_max_sequence::MinMaxSequence;
    use query::{frontend::sql::SqlQueryPlanner, QueryChunk, QueryDatabase};
    use write_buffer::mock::{
        MockBufferForReading, MockBufferForWriting, MockBufferForWritingThatAlwaysErrors,
        MockBufferSharedState,
    };

    use crate::{
        assert_store_sequenced_entry_failures,
        db::{
            catalog::chunk::ChunkStage,
            test_helpers::{run_query, try_write_lp, write_lp},
        },
        utils::{make_db, TestDb},
    };

    use super::*;

    type TestError = Box<dyn std::error::Error + Send + Sync + 'static>;
    type Result<T, E = TestError> = std::result::Result<T, E>;

    async fn immutable_db() -> Arc<Db> {
        TestDb::builder()
            .lifecycle_rules(LifecycleRules {
                immutable: true,
                ..Default::default()
            })
            .build()
            .await
            .db
    }

    #[tokio::test]
    async fn write_no_mutable_buffer() {
        // Validate that writes are rejected if there is no mutable buffer
        let db = immutable_db().await;

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
        let write_buffer_state =
            MockBufferSharedState::empty_with_n_sequencers(NonZeroU32::try_from(1).unwrap());
        let write_buffer =
            Arc::new(MockBufferForWriting::new(write_buffer_state.clone(), None).unwrap());
        let test_db = TestDb::builder()
            .write_buffer(WriteBufferConfig::Writing(Arc::clone(&write_buffer) as _))
            .lifecycle_rules(LifecycleRules {
                immutable: true,
                ..Default::default()
            })
            .build()
            .await
            .db;

        let entry = lp_to_entry("cpu bar=1 10");
        test_db.store_entry(entry).await.unwrap();

        assert_eq!(write_buffer_state.get_messages(0).len(), 1);
    }

    #[tokio::test]
    async fn write_to_write_buffer_and_mutable_buffer() {
        // Writes should be forwarded to the write buffer *and* the mutable buffer if both are
        // configured.
        let write_buffer_state =
            MockBufferSharedState::empty_with_n_sequencers(NonZeroU32::try_from(1).unwrap());
        let write_buffer =
            Arc::new(MockBufferForWriting::new(write_buffer_state.clone(), None).unwrap());
        let db = TestDb::builder()
            .write_buffer(WriteBufferConfig::Writing(Arc::clone(&write_buffer) as _))
            .build()
            .await
            .db;

        let entry = lp_to_entry("cpu bar=1 10");
        db.store_entry(entry).await.unwrap();

        assert_eq!(write_buffer_state.get_messages(0).len(), 1);

        let batches = run_query(db, "select * from cpu").await;

        let expected = vec![
            "+-----+--------------------------------+",
            "| bar | time                           |",
            "+-----+--------------------------------+",
            "| 1   | 1970-01-01T00:00:00.000000010Z |",
            "+-----+--------------------------------+",
        ];
        assert_batches_eq!(expected, &batches);
    }

    #[tokio::test]
    async fn write_buffer_errors_propagated() {
        let write_buffer = Arc::new(MockBufferForWritingThatAlwaysErrors {});

        let db = TestDb::builder()
            .write_buffer(WriteBufferConfig::Writing(Arc::clone(&write_buffer) as _))
            .build()
            .await
            .db;

        let entry = lp_to_entry("cpu bar=1 10");

        let res = db.store_entry(entry).await;

        assert!(
            matches!(res, Err(Error::WriteBufferWritingError { .. })),
            "Expected Err(Error::WriteBufferWritingError {{ .. }}), got: {:?}",
            res
        );
    }

    #[tokio::test]
    async fn read_from_write_buffer_write_to_mutable_buffer() {
        let write_buffer_state =
            MockBufferSharedState::empty_with_n_sequencers(NonZeroU32::try_from(1).unwrap());
        let ingest_ts1 = Utc.timestamp_millis(42);
        let ingest_ts2 = Utc.timestamp_millis(1337);
        write_buffer_state.push_entry(SequencedEntry::new_from_sequence(
            Sequence::new(0, 0),
            ingest_ts1,
            lp_to_entry("mem foo=1 10"),
        ));
        write_buffer_state.push_entry(SequencedEntry::new_from_sequence(
            Sequence::new(0, 7),
            ingest_ts2,
            lp_to_entry("cpu bar=2 20\ncpu bar=3 30"),
        ));
        let write_buffer = MockBufferForReading::new(write_buffer_state, None).unwrap();

        let test_db = TestDb::builder()
            .write_buffer(WriteBufferConfig::Reading(Arc::new(
                tokio::sync::Mutex::new(Box::new(write_buffer) as _),
            )))
            .build()
            .await;
        let db = test_db.db;

        // do: start background task loop
        let shutdown: CancellationToken = Default::default();
        let shutdown_captured = shutdown.clone();
        let db_captured = Arc::clone(&db);
        let join_handle =
            tokio::spawn(async move { db_captured.background_worker(shutdown_captured).await });
        db.allow_write_buffer_read();

        let query = "select * from cpu";

        // check: after a while the table should exist and a query plan should succeed
        let t_0 = Instant::now();
        loop {
            let planner = SqlQueryPlanner::default();
            let ctx = db.new_query_context(None);
            let physical_plan = planner.query(query, &ctx);

            if physical_plan.is_ok() {
                break;
            }

            assert!(t_0.elapsed() < Duration::from_secs(10));
            tokio::time::sleep(Duration::from_millis(100)).await;
        }

        // check: metrics
        // We need to do that BEFORE shutting down the background loop because gauges would be dropped and resetted otherwise
        let metrics = test_db.metric_registry;
        metrics
            .has_metric_family("write_buffer_ingest_requests_total")
            .with_attributes(&[
                ("db_name", "placeholder"),
                ("svr_id", "1"),
                ("sequencer_id", "0"),
                ("status", "ok"),
            ])
            .counter()
            .eq(2.0)
            .unwrap();
        metrics
            .has_metric_family("write_buffer_read_bytes_total")
            .with_attributes(&[
                ("db_name", "placeholder"),
                ("svr_id", "1"),
                ("sequencer_id", "0"),
            ])
            .counter()
            .eq(528.0)
            .unwrap();
        metrics
            .has_metric_family("write_buffer_last_sequence_number")
            .with_attributes(&[
                ("db_name", "placeholder"),
                ("svr_id", "1"),
                ("sequencer_id", "0"),
            ])
            .gauge()
            .eq(7.0)
            .unwrap();
        metrics
            .has_metric_family("write_buffer_sequence_number_lag")
            .with_attributes(&[
                ("db_name", "placeholder"),
                ("svr_id", "1"),
                ("sequencer_id", "0"),
            ])
            .gauge()
            .eq(0.0)
            .unwrap();
        metrics
            .has_metric_family("write_buffer_last_min_ts")
            .with_attributes(&[
                ("db_name", "placeholder"),
                ("svr_id", "1"),
                ("sequencer_id", "0"),
            ])
            .gauge()
            .eq(20.0)
            .unwrap();
        metrics
            .has_metric_family("write_buffer_last_max_ts")
            .with_attributes(&[
                ("db_name", "placeholder"),
                ("svr_id", "1"),
                ("sequencer_id", "0"),
            ])
            .gauge()
            .eq(30.0)
            .unwrap();
        metrics
            .has_metric_family("write_buffer_last_ingest_ts")
            .with_attributes(&[
                ("db_name", "placeholder"),
                ("svr_id", "1"),
                ("sequencer_id", "0"),
            ])
            .gauge()
            .eq(ingest_ts2.timestamp_nanos() as f64)
            .unwrap();

        // do: stop background task loop
        shutdown.cancel();
        join_handle.await.unwrap();

        // check: the expected results should be there
        let batches = run_query(db, "select * from cpu order by time").await;

        let expected = vec![
            "+-----+--------------------------------+",
            "| bar | time                           |",
            "+-----+--------------------------------+",
            "| 2   | 1970-01-01T00:00:00.000000020Z |",
            "| 3   | 1970-01-01T00:00:00.000000030Z |",
            "+-----+--------------------------------+",
        ];
        assert_batches_eq!(expected, &batches);
    }

    #[tokio::test]
    async fn write_buffer_reads_wait_for_compaction() {
        let tracing_capture = TracingCapture::new();

        // setup write buffer
        // these numbers are handtuned to trigger hard buffer limits w/o making the test too big
        let n_entries = 50u64;
        let write_buffer_state =
            MockBufferSharedState::empty_with_n_sequencers(NonZeroU32::try_from(1).unwrap());
        for sequence_number in 0..n_entries {
            let lp = format!(
                "table_1,tag_partition_by=a foo=\"hello\",bar=1 {}",
                sequence_number / 2
            );
            write_buffer_state.push_entry(SequencedEntry::new_from_sequence(
                Sequence::new(0, sequence_number),
                Utc::now(),
                lp_to_entry(&lp),
            ));
        }
        write_buffer_state.push_entry(SequencedEntry::new_from_sequence(
            Sequence::new(0, n_entries),
            Utc::now(),
            lp_to_entry("table_2,partition_by=a foo=1 0"),
        ));
        let write_buffer = MockBufferForReading::new(write_buffer_state, None).unwrap();

        // create DB
        let partition_template = PartitionTemplate {
            parts: vec![TemplatePart::Column("tag_partition_by".to_string())],
        };
        let test_db = TestDb::builder()
            .write_buffer(WriteBufferConfig::Reading(Arc::new(
                tokio::sync::Mutex::new(Box::new(write_buffer) as _),
            )))
            .lifecycle_rules(data_types::database_rules::LifecycleRules {
                buffer_size_hard: Some(NonZeroUsize::new(10_000).unwrap()),
                mub_row_threshold: NonZeroUsize::new(10).unwrap(),
                ..Default::default()
            })
            .partition_template(partition_template)
            .build()
            .await;
        let db = test_db.db;

        // start background task loop
        let shutdown: CancellationToken = Default::default();
        let shutdown_captured = shutdown.clone();
        let db_captured = Arc::clone(&db);
        let join_handle =
            tokio::spawn(async move { db_captured.background_worker(shutdown_captured).await });
        db.allow_write_buffer_read();

        // after a while the table should exist
        let t_0 = Instant::now();
        loop {
            if db.table_schema("table_2").is_some() {
                break;
            }

            assert!(t_0.elapsed() < Duration::from_secs(10));
            tokio::time::sleep(Duration::from_millis(100)).await;
        }

        // do: stop background task loop
        shutdown.cancel();
        join_handle.await.unwrap();

        // no rows should be dropped
        let batches = run_query(db, "select sum(bar) as n from table_1").await;
        let expected = vec!["+----+", "| n  |", "+----+", "| 25 |", "+----+"];
        assert_batches_eq!(expected, &batches);

        // check that hard buffer limit was actually hit (otherwise this test is pointless/outdated)
        assert_contains!(tracing_capture.to_string(), "Hard limit reached while reading from write buffer, waiting for compaction to catch up");
    }

    #[tokio::test]
    async fn error_converting_data_from_write_buffer_to_sequenced_entry_is_reported() {
        let write_buffer_state =
            MockBufferSharedState::empty_with_n_sequencers(NonZeroU32::try_from(1).unwrap());
        write_buffer_state.push_error(
            String::from("Something bad happened on the way to creating a SequencedEntry").into(),
            0,
        );
        let write_buffer = MockBufferForReading::new(write_buffer_state, None).unwrap();

        let test_db = TestDb::builder()
            .write_buffer(WriteBufferConfig::Reading(Arc::new(
                tokio::sync::Mutex::new(Box::new(write_buffer) as _),
            )))
            .build()
            .await;

        let db = Arc::new(test_db.db);
        let metrics = test_db.metric_registry;

        // do: start background task loop
        let shutdown: CancellationToken = Default::default();
        let shutdown_captured = shutdown.clone();
        let db_captured = Arc::clone(&db);
        let join_handle =
            tokio::spawn(async move { db_captured.background_worker(shutdown_captured).await });
        db.allow_write_buffer_read();

        // check: after a while the error should be reported in the database's metrics
        let t_0 = Instant::now();
        loop {
            let family = metrics.try_has_metric_family("write_buffer_ingest_requests_total");

            if let Ok(metric) = family {
                if metric
                    .with_attributes(&[
                        ("db_name", "placeholder"),
                        ("svr_id", "1"),
                        ("sequencer_id", "0"),
                        ("status", "client_error"),
                    ])
                    .counter()
                    .eq(1.0)
                    .is_ok()
                {
                    break;
                }
            }

            assert!(t_0.elapsed() < Duration::from_secs(10));
            tokio::time::sleep(Duration::from_millis(100)).await;
        }

        // do: stop background task loop
        shutdown.cancel();
        join_handle.await.unwrap();
    }

    #[tokio::test]
    async fn cant_write_when_reading_from_write_buffer() {
        // Validate that writes are rejected if this database is reading from the write buffer
        let db = immutable_db().await;
        let entry = lp_to_entry("cpu bar=1 10");
        let res = db.store_entry(entry).await;
        assert_contains!(
            res.unwrap_err().to_string(),
            "Cannot write to this database: no mutable buffer configured"
        );
    }

    #[tokio::test]
    async fn read_write() {
        // This test also exercises the path without a write buffer.
        let db = make_db().await.db;
        write_lp(&db, "cpu bar=1 10").await;

        let batches = run_query(db, "select * from cpu").await;

        let expected = vec![
            "+-----+--------------------------------+",
            "| bar | time                           |",
            "+-----+--------------------------------+",
            "| 1   | 1970-01-01T00:00:00.000000010Z |",
            "+-----+--------------------------------+",
        ];
        assert_batches_eq!(expected, &batches);
    }

    #[tokio::test]
    async fn try_all_partition_writes_when_some_fail() {
        let db = make_db().await.db;

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
        registry: &metric::Registry,
        location: &'static str,
        expected: u64,
    ) {
        let actual = registry
            .get_instrument::<Metric<CumulativeGauge>>("catalog_chunks_mem_usage_bytes")
            .unwrap()
            .get_observer(&Attributes::from(&[
                ("db_name", "placeholder"),
                ("location", location),
            ]))
            .unwrap()
            .fetch();

        assert_eq!(actual, expected)
    }

    fn assert_storage_gauge(
        registry: &metric::Registry,
        name: &'static str,
        location: &'static str,
        expected: u64,
    ) {
        let actual = registry
            .get_instrument::<Metric<CumulativeGauge>>(name)
            .unwrap()
            .get_observer(&Attributes::from(&[
                ("db_name", "placeholder"),
                ("location", location),
                ("table", "cpu"),
            ]))
            .unwrap()
            .fetch();

        assert_eq!(actual, expected)
    }

    #[tokio::test]
    async fn metrics_during_rollover() {
        let test_db = make_db().await;

        let db = Arc::clone(&test_db.db);

        write_lp(db.as_ref(), "cpu bar=1 10").await;

        let registry = test_db.metrics_registry_v2.as_ref();

        // A chunk has been opened
        assert_storage_gauge(registry, "catalog_loaded_chunks", "mutable_buffer", 1);
        assert_storage_gauge(registry, "catalog_loaded_chunks", "read_buffer", 0);
        assert_storage_gauge(registry, "catalog_loaded_chunks", "object_store", 0);
        assert_storage_gauge(registry, "catalog_loaded_rows", "mutable_buffer", 1);
        assert_storage_gauge(registry, "catalog_loaded_rows", "read_buffer", 0);
        assert_storage_gauge(registry, "catalog_loaded_rows", "object_store", 0);

        // verify chunk size updated
        catalog_chunk_size_bytes_metric_eq(registry, "mutable_buffer", 700);

        // write into same chunk again.
        write_lp(db.as_ref(), "cpu bar=2 20").await;
        write_lp(db.as_ref(), "cpu bar=3 30").await;
        write_lp(db.as_ref(), "cpu bar=4 40").await;
        write_lp(db.as_ref(), "cpu bar=5 50").await;

        // verify chunk size updated
        catalog_chunk_size_bytes_metric_eq(registry, "mutable_buffer", 764);

        // Still only one chunk open
        assert_storage_gauge(registry, "catalog_loaded_chunks", "mutable_buffer", 1);
        assert_storage_gauge(registry, "catalog_loaded_chunks", "read_buffer", 0);
        assert_storage_gauge(registry, "catalog_loaded_chunks", "object_store", 0);
        assert_storage_gauge(registry, "catalog_loaded_rows", "mutable_buffer", 5);
        assert_storage_gauge(registry, "catalog_loaded_rows", "read_buffer", 0);
        assert_storage_gauge(registry, "catalog_loaded_rows", "object_store", 0);

        db.rollover_partition("cpu", "1970-01-01T00").await.unwrap();

        // A chunk is now closed
        assert_storage_gauge(registry, "catalog_loaded_chunks", "mutable_buffer", 1);
        assert_storage_gauge(registry, "catalog_loaded_chunks", "read_buffer", 0);
        assert_storage_gauge(registry, "catalog_loaded_chunks", "object_store", 0);
        assert_storage_gauge(registry, "catalog_loaded_rows", "mutable_buffer", 5);
        assert_storage_gauge(registry, "catalog_loaded_rows", "read_buffer", 0);
        assert_storage_gauge(registry, "catalog_loaded_rows", "object_store", 0);

        catalog_chunk_size_bytes_metric_eq(registry, "mutable_buffer", 1295);

        db.move_chunk_to_read_buffer("cpu", "1970-01-01T00", 0)
            .await
            .unwrap();

        // A chunk is now in the read buffer
        assert_storage_gauge(registry, "catalog_loaded_chunks", "mutable_buffer", 0);
        assert_storage_gauge(registry, "catalog_loaded_chunks", "read_buffer", 1);
        assert_storage_gauge(registry, "catalog_loaded_chunks", "object_store", 0);
        assert_storage_gauge(registry, "catalog_loaded_rows", "mutable_buffer", 0);
        assert_storage_gauge(registry, "catalog_loaded_rows", "read_buffer", 5);
        assert_storage_gauge(registry, "catalog_loaded_rows", "object_store", 0);

        // verify chunk size updated (chunk moved from closing to moving to moved)
        catalog_chunk_size_bytes_metric_eq(registry, "mutable_buffer", 0);
        let expected_read_buffer_size = 1706;
        catalog_chunk_size_bytes_metric_eq(registry, "read_buffer", expected_read_buffer_size);

        db.persist_partition(
            "cpu",
            "1970-01-01T00",
            Instant::now() + Duration::from_secs(1),
        )
        .await
        .unwrap();

        // A chunk is now in the object store and still in read buffer
        let expected_parquet_size = 1551;
        catalog_chunk_size_bytes_metric_eq(registry, "read_buffer", expected_read_buffer_size);
        // now also in OS
        catalog_chunk_size_bytes_metric_eq(registry, "object_store", expected_parquet_size);

        assert_storage_gauge(registry, "catalog_loaded_chunks", "mutable_buffer", 0);
        assert_storage_gauge(registry, "catalog_loaded_chunks", "read_buffer", 1);
        assert_storage_gauge(registry, "catalog_loaded_chunks", "object_store", 1);
        assert_storage_gauge(registry, "catalog_loaded_rows", "mutable_buffer", 0);
        assert_storage_gauge(registry, "catalog_loaded_rows", "read_buffer", 5);
        assert_storage_gauge(registry, "catalog_loaded_rows", "object_store", 5);

        db.unload_read_buffer("cpu", "1970-01-01T00", 1).unwrap();

        // A chunk is now now in the "os-only" state.
        assert_storage_gauge(registry, "catalog_loaded_chunks", "mutable_buffer", 0);
        assert_storage_gauge(registry, "catalog_loaded_chunks", "read_buffer", 0);
        assert_storage_gauge(registry, "catalog_loaded_chunks", "object_store", 1);
        assert_storage_gauge(registry, "catalog_loaded_rows", "mutable_buffer", 0);
        assert_storage_gauge(registry, "catalog_loaded_rows", "read_buffer", 0);
        assert_storage_gauge(registry, "catalog_loaded_rows", "object_store", 5);

        // verify chunk size not increased for OS (it was in OS before unload)
        catalog_chunk_size_bytes_metric_eq(registry, "object_store", expected_parquet_size);
        // verify chunk size for RB has decreased
        catalog_chunk_size_bytes_metric_eq(registry, "read_buffer", 0);
    }

    #[tokio::test]
    async fn write_metrics() {
        std::env::set_var("INFLUXDB_IOX_ROW_TIMESTAMP_METRICS", "write_metrics_test");
        let test_db = make_db().await;
        let db = Arc::clone(&test_db.db);

        write_lp(db.as_ref(), "write_metrics_test foo=1 100000000000").await;
        write_lp(db.as_ref(), "write_metrics_test foo=2 180000000000").await;
        write_lp(db.as_ref(), "write_metrics_test foo=3 650000000000").await;
        write_lp(db.as_ref(), "write_metrics_test foo=3 650000000010").await;

        let mut summary = TimestampSummary::default();
        summary.record(Utc.timestamp_nanos(100000000000));
        summary.record(Utc.timestamp_nanos(180000000000));
        summary.record(Utc.timestamp_nanos(650000000000));
        summary.record(Utc.timestamp_nanos(650000000010));

        let mut reporter = metric::RawReporter::default();
        test_db.metrics_registry_v2.report(&mut reporter);

        let observation = reporter
            .metric("catalog_row_time")
            .unwrap()
            .observation(&[("db_name", "placeholder"), ("table", "write_metrics_test")])
            .unwrap();

        let histogram = match observation {
            Observation::DurationHistogram(histogram) => histogram,
            _ => unreachable!(),
        };
        assert_eq!(histogram.buckets.len(), 60);

        for ((minute, count), observation) in
            summary.counts.iter().enumerate().zip(&histogram.buckets)
        {
            let minute = Duration::from_secs((minute * 60) as u64);
            assert_eq!(observation.le, minute);
            assert_eq!(*count as u64, observation.count)
        }
    }

    #[tokio::test]
    async fn write_with_rollover() {
        let db = make_db().await.db;
        write_lp(db.as_ref(), "cpu bar=1 10").await;
        assert_eq!(vec!["1970-01-01T00"], db.partition_keys().unwrap());

        let mb_chunk = db
            .rollover_partition("cpu", "1970-01-01T00")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(mb_chunk.id(), 0);

        let expected = vec![
            "+-----+--------------------------------+",
            "| bar | time                           |",
            "+-----+--------------------------------+",
            "| 1   | 1970-01-01T00:00:00.000000010Z |",
            "+-----+--------------------------------+",
        ];
        let batches = run_query(Arc::clone(&db), "select * from cpu").await;
        assert_batches_sorted_eq!(expected, &batches);

        // add new data
        write_lp(db.as_ref(), "cpu bar=2 20").await;
        let expected = vec![
            "+-----+--------------------------------+",
            "| bar | time                           |",
            "+-----+--------------------------------+",
            "| 1   | 1970-01-01T00:00:00.000000010Z |",
            "| 2   | 1970-01-01T00:00:00.000000020Z |",
            "+-----+--------------------------------+",
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
            "+------+--------+--------------------------------+------+",
            "| core | region | time                           | user |",
            "+------+--------+--------------------------------+------+",
            "|      |        | 1970-01-01T00:00:00.000000011Z | 10   |",
            "|      | west   | 1970-01-01T00:00:00.000000010Z | 23.2 |",
            "| one  |        | 1970-01-01T00:00:00.000000011Z | 10   |",
            "+------+--------+--------------------------------+------+",
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
            "+-----+--------------------------------+",
            "| bar | time                           |",
            "+-----+--------------------------------+",
            "| 1   | 1970-01-01T00:00:00.000000010Z |",
            "| 2   | 1970-01-01T00:00:00.000000020Z |",
            "+-----+--------------------------------+",
        ];
        let batches = run_query(Arc::clone(&db), "select * from cpu").await;
        assert_batches_eq!(&expected, &batches);

        let registry = test_db.metrics_registry_v2.as_ref();

        // A chunk is now in the read buffer
        assert_storage_gauge(registry, "catalog_loaded_chunks", "read_buffer", 1);

        // verify chunk size updated (chunk moved from moved to writing to written)
        catalog_chunk_size_bytes_metric_eq(registry, "read_buffer", 1700);

        // drop, the chunk from the read buffer
        db.drop_chunk("cpu", partition_key, mb_chunk.id())
            .await
            .unwrap();
        assert_eq!(
            read_buffer_chunk_ids(db.as_ref(), partition_key),
            vec![] as Vec<u32>
        );

        // verify size is not accounted even though a reference to the RubChunk still exists
        catalog_chunk_size_bytes_metric_eq(registry, "read_buffer", 0);
        std::mem::drop(rb_chunk);

        // verify chunk size updated (chunk dropped from moved state)
        catalog_chunk_size_bytes_metric_eq(registry, "read_buffer", 0);

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

        let time0 = Utc::now();
        write_lp(db.as_ref(), "cpu bar=1 10").await;
        let time1 = Utc::now();

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

        let first_old_rb_write = old_rb_chunk.time_of_first_write();
        let last_old_rb_write = old_rb_chunk.time_of_last_write();
        assert!(time0 < first_old_rb_write);
        assert_eq!(first_old_rb_write, last_old_rb_write);
        assert!(first_old_rb_write < time1);

        // Put new data into the mutable buffer
        let time2 = Utc::now();
        write_lp(db.as_ref(), "cpu bar=2 20").await;
        let time3 = Utc::now();

        // now, compact it
        let compacted_rb_chunk = db.compact_partition("cpu", partition_key).await.unwrap();

        // no other read buffer data should be present
        assert_eq!(
            read_buffer_chunk_ids(&db, partition_key),
            vec![compacted_rb_chunk.id()]
        );
        assert_ne!(old_rb_chunk.id(), compacted_rb_chunk.id());

        // Compacted first/last write times should be the min of the first writes and the max
        // of the last writes of the compacted chunks
        let first_compacted_write = compacted_rb_chunk.time_of_first_write();
        let last_compacted_write = compacted_rb_chunk.time_of_last_write();
        assert_eq!(first_old_rb_write, first_compacted_write);
        assert_ne!(last_old_rb_write, last_compacted_write);
        assert!(time2 < last_compacted_write);
        assert!(last_compacted_write < time3);

        // data should be readable
        let expected = vec![
            "+-----+--------------------------------+",
            "| bar | time                           |",
            "+-----+--------------------------------+",
            "| 1   | 1970-01-01T00:00:00.000000010Z |",
            "| 2   | 1970-01-01T00:00:00.000000020Z |",
            "+-----+--------------------------------+",
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

        let registry = test_db.metrics_registry_v2.as_ref();
        // MUB chunk size
        catalog_chunk_size_bytes_metric_eq(registry, "mutable_buffer", 3607);

        // With the above data, cardinality of tag2 is 2 and tag1 is 5. Hence, RUB is sorted on (tag2, tag1)
        let rb_chunk = db
            .move_chunk_to_read_buffer("cpu", partition_key, mb_chunk.id())
            .await
            .unwrap();

        // MUB chunk size
        catalog_chunk_size_bytes_metric_eq(registry, "mutable_buffer", 0);
        catalog_chunk_size_bytes_metric_eq(registry, "read_buffer", 3618);

        let rb = collect_read_filter(&rb_chunk).await;

        // Test that data on load into the read buffer is sorted

        assert_batches_eq!(
            &[
                "+-----+----------+------+--------------------------------+",
                "| bar | tag1     | tag2 | time                           |",
                "+-----+----------+------+--------------------------------+",
                "| 1   | cupcakes |      | 1970-01-01T00:00:00.000000010Z |",
                "| 2   | asfd     | foo  | 1970-01-01T00:00:00.000000020Z |",
                "| 2   | bingo    | foo  | 1970-01-01T00:00:00.000000010Z |",
                "| 2   | bongo    | a    | 1970-01-01T00:00:00.000000020Z |",
                "| 2   | bongo    | a    | 1970-01-01T00:00:00.000000010Z |",
                "| 3   |          | a    | 1970-01-01T00:00:00.000000005Z |",
                "+-----+----------+------+--------------------------------+",
            ],
            &mb
        );

        assert_batches_eq!(
            &[
                "+-----+----------+------+--------------------------------+",
                "| bar | tag1     | tag2 | time                           |",
                "+-----+----------+------+--------------------------------+",
                "| 1   | cupcakes |      | 1970-01-01T00:00:00.000000010Z |",
                "| 3   |          | a    | 1970-01-01T00:00:00.000000005Z |",
                "| 2   | bongo    | a    | 1970-01-01T00:00:00.000000010Z |",
                "| 2   | bongo    | a    | 1970-01-01T00:00:00.000000020Z |",
                "| 2   | asfd     | foo  | 1970-01-01T00:00:00.000000020Z |",
                "| 2   | bingo    | foo  | 1970-01-01T00:00:00.000000010Z |",
                "+-----+----------+------+--------------------------------+",
            ],
            &rb
        );
    }

    async fn parquet_files(iox_storage: &IoxObjectStore) -> Result<Vec<ParquetFilePath>> {
        iox_storage
            .parquet_files()
            .await?
            .map_ok(|v| stream::iter(v).map(Ok))
            .try_flatten()
            .try_collect()
            .await
    }

    #[tokio::test]
    async fn write_one_chunk_to_parquet_file() {
        // Test that data can be written into parquet files
        let object_store = Arc::new(ObjectStore::new_in_memory());

        // Create a DB given a server id, an object store and a db name
        let test_db = TestDb::builder()
            .lifecycle_rules(LifecycleRules {
                late_arrive_window_seconds: NonZeroU32::try_from(1).unwrap(),
                ..Default::default()
            })
            .object_store(Arc::clone(&object_store))
            .build()
            .await;

        let db = test_db.db;

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
            .persist_partition(
                "cpu",
                partition_key,
                Instant::now() + Duration::from_secs(1),
            )
            .await
            .unwrap();

        let registry = test_db.metrics_registry_v2.as_ref();

        // Read buffer + Parquet chunk size
        catalog_chunk_size_bytes_metric_eq(registry, "mutable_buffer", 0);
        catalog_chunk_size_bytes_metric_eq(registry, "read_buffer", 1700);
        catalog_chunk_size_bytes_metric_eq(registry, "object_store", 1551);

        // while MB and RB chunk are identical, the PQ chunk is a new one (split off)
        assert_eq!(mb_chunk.id(), rb_chunk.id());
        assert_ne!(mb_chunk.id(), pq_chunk.id());

        // we should have chunks in both the read buffer only
        assert!(mutable_chunk_ids(&db, partition_key).is_empty());
        assert_eq!(read_buffer_chunk_ids(&db, partition_key), vec![1]);
        assert_eq!(read_parquet_file_chunk_ids(&db, partition_key), vec![1]);

        // Verify data written to the parquet file in object store
        //
        // First, there must be one path of object store in the catalog
        let path = pq_chunk.object_store_path().unwrap();

        // Check that the path must exist in the object store
        let path_list = parquet_files(&db.iox_object_store).await.unwrap();
        assert_eq!(path_list.len(), 1);
        assert_eq!(&path_list[0], path);

        // Now read data from that path
        let parquet_data =
            load_parquet_from_store_for_path(&path_list[0], Arc::clone(&db.iox_object_store))
                .await
                .unwrap();
        let parquet_metadata = IoxParquetMetaData::from_file_bytes(parquet_data.clone()).unwrap();
        // Read metadata at file level
        let schema = parquet_metadata.decode().unwrap().read_schema().unwrap();
        // Read data
        let record_batches =
            read_data_from_parquet_data(Arc::clone(&schema.as_arrow()), parquet_data);

        let expected = vec![
            "+-----+--------------------------------+",
            "| bar | time                           |",
            "+-----+--------------------------------+",
            "| 1   | 1970-01-01T00:00:00.000000010Z |",
            "| 2   | 1970-01-01T00:00:00.000000020Z |",
            "+-----+--------------------------------+",
        ];
        assert_batches_eq!(expected, &record_batches);
    }

    #[tokio::test]
    async fn unload_chunk_from_read_buffer() {
        // Test that data can be written into parquet files and then
        // remove it from read buffer and make sure we are still
        // be able to read data from object store

        // Create an object store in memory
        let object_store = Arc::new(ObjectStore::new_in_memory());

        // Create a DB given a server id, an object store and a db name
        let test_db = TestDb::builder()
            .lifecycle_rules(LifecycleRules {
                late_arrive_window_seconds: NonZeroU32::try_from(1).unwrap(),
                ..Default::default()
            })
            .object_store(Arc::clone(&object_store))
            .build()
            .await;

        let db = test_db.db;

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
            .persist_partition(
                "cpu",
                partition_key,
                Instant::now() + Duration::from_secs(1),
            )
            .await
            .unwrap();

        // while MB and RB chunk are identical, the PQ chunk is a new one (split off)
        assert_eq!(mb_chunk.id(), rb_chunk.id());
        assert_ne!(mb_chunk.id(), pq_chunk.id());
        let pq_chunk_id = pq_chunk.id();

        // we should have chunks in both the read buffer only
        assert!(mutable_chunk_ids(&db, partition_key).is_empty());
        assert_eq!(read_buffer_chunk_ids(&db, partition_key), vec![pq_chunk_id]);
        assert_eq!(
            read_parquet_file_chunk_ids(&db, partition_key),
            vec![pq_chunk_id]
        );

        let registry = test_db.metrics_registry_v2.as_ref();

        // Read buffer + Parquet chunk size
        catalog_chunk_size_bytes_metric_eq(registry, "mutable_buffer", 0);
        catalog_chunk_size_bytes_metric_eq(registry, "read_buffer", 1700);
        catalog_chunk_size_bytes_metric_eq(registry, "object_store", 1551);

        // Unload RB chunk but keep it in OS
        let pq_chunk = db
            .unload_read_buffer("cpu", partition_key, pq_chunk_id)
            .unwrap();

        // still should be the same chunk!
        assert_eq!(mb_chunk.id(), rb_chunk.id());
        assert_eq!(pq_chunk_id, pq_chunk.id());

        // we should only have chunk in os
        assert!(mutable_chunk_ids(&db, partition_key).is_empty());
        assert!(read_buffer_chunk_ids(&db, partition_key).is_empty());
        assert_eq!(
            read_parquet_file_chunk_ids(&db, partition_key),
            vec![pq_chunk_id]
        );

        // Parquet chunk size only
        catalog_chunk_size_bytes_metric_eq(registry, "mutable_buffer", 0);
        catalog_chunk_size_bytes_metric_eq(registry, "read_buffer", 0);
        catalog_chunk_size_bytes_metric_eq(registry, "object_store", 1551);

        // Verify data written to the parquet file in object store
        //
        // First, there must be one path of object store in the catalog
        let path = pq_chunk.object_store_path().unwrap();

        // Check that the path must exist in the object store
        let path_list = parquet_files(&db.iox_object_store).await.unwrap();
        println!("path_list: {:#?}", path_list);
        assert_eq!(path_list.len(), 1);
        assert_eq!(&path_list[0], path);

        // Now read data from that path
        let parquet_data =
            load_parquet_from_store_for_path(&path_list[0], Arc::clone(&db.iox_object_store))
                .await
                .unwrap();
        let parquet_metadata = IoxParquetMetaData::from_file_bytes(parquet_data.clone()).unwrap();
        // Read metadata at file level
        let schema = parquet_metadata.decode().unwrap().read_schema().unwrap();
        // Read data
        let record_batches =
            read_data_from_parquet_data(Arc::clone(&schema.as_arrow()), parquet_data);

        let expected = vec![
            "+-----+--------------------------------+",
            "| bar | time                           |",
            "+-----+--------------------------------+",
            "| 1   | 1970-01-01T00:00:00.000000010Z |",
            "| 2   | 1970-01-01T00:00:00.000000020Z |",
            "+-----+--------------------------------+",
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
    async fn failed_write_doesnt_update_last_write_at() {
        let db = Arc::new(make_db().await.db);
        let before_create = Utc::now();

        let partition_key = "1970-01-01T00";
        write_lp(&db, "cpu bar=1 10").await;
        let after_write = Utc::now();

        let (last_write_prev, chunk_last_write_prev) = {
            let partition = db.catalog.partition("cpu", partition_key).unwrap();
            let partition = partition.read();

            assert_ne!(partition.created_at(), partition.last_write_at());
            assert!(before_create < partition.last_write_at());
            assert!(after_write > partition.last_write_at());
            let chunk = partition.open_chunk().unwrap();
            let chunk = chunk.read();

            (partition.last_write_at(), chunk.time_of_last_write())
        };

        let entry = lp_to_entry("cpu bar=true 10");
        let result = db.store_entry(entry).await;
        assert!(result.is_err());
        {
            let partition = db.catalog.partition("cpu", partition_key).unwrap();
            let partition = partition.read();
            assert_eq!(last_write_prev, partition.last_write_at());
            let chunk = partition.open_chunk().unwrap();
            let chunk = chunk.read();
            assert_eq!(chunk_last_write_prev, chunk.time_of_last_write());
        }
    }

    #[tokio::test]
    async fn write_updates_persistence_windows() {
        // Writes should update the persistence windows when there
        // is a write buffer configured.
        let write_buffer_state =
            MockBufferSharedState::empty_with_n_sequencers(NonZeroU32::try_from(1).unwrap());
        let write_buffer = Arc::new(MockBufferForWriting::new(write_buffer_state, None).unwrap());
        let db = TestDb::builder()
            .write_buffer(WriteBufferConfig::Writing(Arc::clone(&write_buffer) as _))
            .build()
            .await
            .db;

        let partition_key = "1970-01-01T00";
        write_lp(&db, "cpu bar=1 10").await; // seq 0
        write_lp(&db, "cpu bar=1 20").await; // seq 1
        write_lp(&db, "cpu bar=1 30").await; // seq 2

        let partition = db.catalog.partition("cpu", partition_key).unwrap();
        let partition = partition.write();
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
        let partition = partition.write();
        // validate it has data
        let table_summary = partition.summary().unwrap().table;
        assert_eq!(&table_summary.name, "cpu");
        assert_eq!(table_summary.total_count(), 2);
        let windows = partition.persistence_windows().unwrap();
        let open_min = windows.minimum_unpersisted_timestamp().unwrap();
        let open_max = windows.maximum_unpersisted_timestamp().unwrap();
        assert_eq!(open_min.timestamp_nanos(), 10);
        assert_eq!(open_max.timestamp_nanos(), 20);
    }

    #[tokio::test]
    async fn read_from_write_buffer_updates_persistence_windows() {
        let entry = lp_to_entry("cpu bar=1 10");
        let partition_key = "1970-01-01T00";

        let write_buffer_state =
            MockBufferSharedState::empty_with_n_sequencers(NonZeroU32::try_from(2).unwrap());
        write_buffer_state.push_entry(SequencedEntry::new_from_sequence(
            Sequence::new(0, 0),
            Utc::now(),
            entry.clone(),
        ));
        write_buffer_state.push_entry(SequencedEntry::new_from_sequence(
            Sequence::new(1, 0),
            Utc::now(),
            entry.clone(),
        ));
        write_buffer_state.push_entry(SequencedEntry::new_from_sequence(
            Sequence::new(1, 2),
            Utc::now(),
            entry.clone(),
        ));
        write_buffer_state.push_entry(SequencedEntry::new_from_sequence(
            Sequence::new(0, 1),
            Utc::now(),
            entry,
        ));
        let write_buffer = MockBufferForReading::new(write_buffer_state, None).unwrap();

        let db = TestDb::builder()
            .write_buffer(WriteBufferConfig::Reading(Arc::new(
                tokio::sync::Mutex::new(Box::new(write_buffer) as _),
            )))
            .build()
            .await
            .db;

        // do: start background task loop
        let shutdown: CancellationToken = Default::default();
        let shutdown_captured = shutdown.clone();
        let db_captured = Arc::clone(&db);
        let join_handle =
            tokio::spawn(async move { db_captured.background_worker(shutdown_captured).await });
        db.allow_write_buffer_read();

        // check: after a while the persistence windows should have the expected data
        let t_0 = Instant::now();
        let min_unpersisted = loop {
            if let Ok(partition) = db.catalog.partition("cpu", partition_key) {
                let partition = partition.write();
                let windows = partition.persistence_windows().unwrap();
                let min_unpersisted = windows.minimum_unpersisted_sequence();

                if let Some(min_unpersisted) = min_unpersisted {
                    break min_unpersisted;
                }
            }

            assert!(t_0.elapsed() < Duration::from_secs(10));
            tokio::time::sleep(Duration::from_millis(100)).await;
        };

        // do: stop background task loop
        shutdown.cancel();
        join_handle.await.unwrap();

        let mut expected_unpersisted = BTreeMap::new();
        expected_unpersisted.insert(0, MinMaxSequence::new(0, 1));
        expected_unpersisted.insert(1, MinMaxSequence::new(0, 2));

        assert_eq!(min_unpersisted, expected_unpersisted);
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
        let (chunk, _order) = partition.chunk(chunk_id).unwrap();
        let chunk = chunk.read();

        println!(
            "start: {:?}, after_data_load: {:?}, after_rollover: {:?}",
            start, after_data_load, after_rollover
        );
        println!("Chunk: {:#?}", chunk);

        // then the chunk creation and rollover times are as expected
        assert!(start < chunk.time_of_first_write());
        assert!(chunk.time_of_first_write() < after_data_load);
        assert!(chunk.time_of_first_write() == chunk.time_of_last_write());
        assert!(after_data_load < chunk.time_closed().unwrap());
        assert!(chunk.time_closed().unwrap() < after_rollover);
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

        let expected = vec![ChunkSummary {
            partition_key: Arc::from("1970-01-05T15"),
            table_name: Arc::from("cpu"),
            id: 0,
            storage: ChunkStorage::OpenMutableBuffer,
            lifecycle_action: None,
            memory_bytes: 1006,    // memory_size
            object_store_bytes: 0, // os_size
            row_count: 1,
            time_of_last_access: None,
            time_of_first_write: Utc.timestamp_nanos(1),
            time_of_last_write: Utc.timestamp_nanos(1),
            time_closed: None,
            order: ChunkOrder::new(5),
        }];

        let size: usize = db
            .chunk_summaries()
            .unwrap()
            .into_iter()
            .map(|x| x.memory_bytes)
            .sum();

        assert_eq!(db.catalog.metrics().memory().mutable_buffer(), size);

        for (expected_summary, actual_summary) in expected.iter().zip(chunk_summaries.iter()) {
            assert!(
                expected_summary.equal_without_timestamps(actual_summary),
                "expected:\n{:#?}\n\nactual:{:#?}\n\n",
                expected_summary,
                actual_summary
            );
        }
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
            summary.time_of_first_write > start,
            "summary; {:#?}",
            summary
        );
        assert!(
            summary.time_of_first_write < after_close,
            "summary; {:#?}",
            summary
        );

        assert!(
            summary.time_of_last_write > after_first_write,
            "summary; {:#?}",
            summary
        );
        assert!(
            summary.time_of_last_write < after_close,
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

    fn assert_first_last_times_eq(chunk_summary: &ChunkSummary) {
        let first_write = chunk_summary.time_of_first_write;
        let last_write = chunk_summary.time_of_last_write;

        assert_eq!(first_write, last_write);
    }

    fn assert_first_last_times_between(
        chunk_summary: &ChunkSummary,
        before: DateTime<Utc>,
        after: DateTime<Utc>,
    ) {
        let first_write = chunk_summary.time_of_first_write;
        let last_write = chunk_summary.time_of_last_write;

        assert!(before < first_write);
        assert!(before < last_write);
        assert!(first_write < after);
        assert!(last_write < after);
    }

    fn assert_chunks_times_ordered(before: &ChunkSummary, after: &ChunkSummary) {
        let before_last_write = before.time_of_last_write;
        let after_first_write = after.time_of_first_write;

        assert!(before_last_write < after_first_write);
    }

    fn assert_chunks_times_eq(a: &ChunkSummary, b: &ChunkSummary) {
        assert_chunks_first_times_eq(a, b);
        assert_chunks_last_times_eq(a, b);
    }

    fn assert_chunks_first_times_eq(a: &ChunkSummary, b: &ChunkSummary) {
        let a_first_write = a.time_of_first_write;
        let b_first_write = b.time_of_first_write;
        assert_eq!(a_first_write, b_first_write);
    }

    fn assert_chunks_last_times_eq(a: &ChunkSummary, b: &ChunkSummary) {
        let a_last_write = a.time_of_last_write;
        let b_last_write = b.time_of_last_write;
        assert_eq!(a_last_write, b_last_write);
    }

    #[tokio::test]
    async fn chunk_summaries() {
        // Test that chunk id listing is hooked up
        let db = make_db().await.db;

        // get three chunks: one open, one closed in mb and one close in rb
        // TIME 0 ---------------------------------------------------------------------------------
        let time0 = Utc::now();
        // In open chunk, will end up in rb/os
        write_lp(&db, "cpu bar=1 1").await;
        // TIME 1 ---------------------------------------------------------------------------------
        let time1 = Utc::now();
        // Move open chunk to closed
        db.rollover_partition("cpu", "1970-01-01T00").await.unwrap();
        // TIME 2 ---------------------------------------------------------------------------------
        let time2 = Utc::now();
        // New open chunk in mb
        // This point will end up in rb/os
        write_lp(&db, "cpu bar=1,baz=2 2").await;
        // TIME 3 ---------------------------------------------------------------------------------
        let time3 = Utc::now();

        // Check first/last write times on the chunks at this point
        let mut chunk_summaries = db.chunk_summaries().expect("expected summary to return");
        chunk_summaries.sort_unstable();
        assert_eq!(chunk_summaries.len(), 2);
        // Each chunk has one write, so both chunks should have first write == last write
        let closed_mb_t3 = chunk_summaries[0].clone();
        assert_eq!(closed_mb_t3.storage, ChunkStorage::ClosedMutableBuffer);
        assert_first_last_times_eq(&closed_mb_t3);
        assert_first_last_times_between(&closed_mb_t3, time0, time1);
        let open_mb_t3 = chunk_summaries[1].clone();
        assert_eq!(open_mb_t3.storage, ChunkStorage::OpenMutableBuffer);
        assert_first_last_times_eq(&open_mb_t3);
        assert_first_last_times_between(&open_mb_t3, time2, time3);
        assert_chunks_times_ordered(&closed_mb_t3, &open_mb_t3);

        // This point makes a new open mb chunk and will end up in the closed mb chunk
        write_lp(&db, "cpu bar=1,baz=2,frob=3 400000000000000").await;
        // TIME 4 ---------------------------------------------------------------------------------
        // we don't need to check this value with anything because no timestamps
        // should be between time3 and time4

        // Check first/last write times on the chunks at this point
        let mut chunk_summaries = db.chunk_summaries().expect("expected summary to return");
        chunk_summaries.sort_unstable();
        assert_eq!(chunk_summaries.len(), 3);
        // The closed chunk's times should be the same
        let closed_mb_t4 = chunk_summaries[0].clone();
        assert_eq!(closed_mb_t4.storage, ChunkStorage::ClosedMutableBuffer);
        assert_chunks_times_eq(&closed_mb_t4, &closed_mb_t3);
        // The first open chunk's times should be the same
        let open_mb_t4 = chunk_summaries[1].clone();
        assert_eq!(open_mb_t4.storage, ChunkStorage::OpenMutableBuffer);
        assert_chunks_times_eq(&open_mb_t4, &open_mb_t3);
        // The second open chunk's times should be later than the first open chunk's times
        let other_open_mb_t4 = chunk_summaries[2].clone();
        assert_eq!(other_open_mb_t4.storage, ChunkStorage::OpenMutableBuffer);
        assert_chunks_times_ordered(&open_mb_t4, &other_open_mb_t4);

        // Move closed mb chunk to rb
        db.move_chunk_to_read_buffer("cpu", "1970-01-01T00", 0)
            .await
            .unwrap();
        // TIME 5 ---------------------------------------------------------------------------------
        // we don't need to check this value with anything because no timestamps
        // should be between time4 and time5

        // Check first/last write times on the chunks at this point
        let mut chunk_summaries = db.chunk_summaries().expect("expected summary to return");
        chunk_summaries.sort_unstable();
        assert_eq!(chunk_summaries.len(), 3);
        // The rb chunk's times should be the same as they were when this was the closed mb chunk
        let rb_t5 = chunk_summaries[0].clone();
        assert_eq!(rb_t5.storage, ChunkStorage::ReadBuffer);
        assert_chunks_times_eq(&rb_t5, &closed_mb_t4);
        // The first open chunk's times should be the same
        let open_mb_t5 = chunk_summaries[1].clone();
        assert_eq!(open_mb_t5.storage, ChunkStorage::OpenMutableBuffer);
        assert_chunks_times_eq(&open_mb_t5, &open_mb_t4);
        // The second open chunk's times should be the same
        let other_open_mb_t5 = chunk_summaries[2].clone();
        assert_eq!(other_open_mb_t5.storage, ChunkStorage::OpenMutableBuffer);
        assert_chunks_times_eq(&other_open_mb_t5, &other_open_mb_t4);

        // Persist rb to parquet os
        db.persist_partition(
            "cpu",
            "1970-01-01T00",
            Instant::now() + Duration::from_secs(1),
        )
        .await
        .unwrap();
        // TIME 6 ---------------------------------------------------------------------------------
        // we don't need to check this value with anything because no timestamps
        // should be between time5 and time6

        // Check first/last write times on the chunks at this point
        let mut chunk_summaries = db.chunk_summaries().expect("expected summary to return");
        chunk_summaries.sort_unstable();
        // Persisting compacts chunks, so now there's only 2
        assert_eq!(chunk_summaries.len(), 2);
        // The rb chunk's times should be the first write of the rb chunk and the last write
        // of the first open chunk that got compacted together
        let rb_t6 = chunk_summaries[0].clone();
        assert_eq!(rb_t6.storage, ChunkStorage::ReadBufferAndObjectStore);
        assert_chunks_first_times_eq(&rb_t6, &rb_t5);
        assert_chunks_last_times_eq(&rb_t6, &open_mb_t5);
        // The first open chunk had all its points moved into the persisted chunk.
        // The remaining open chunk is the other open chunk that did not contain any points
        // for the first partition
        let open_mb_t6 = chunk_summaries[1].clone();
        assert_eq!(open_mb_t6.storage, ChunkStorage::OpenMutableBuffer);
        assert_chunks_times_eq(&open_mb_t6, &other_open_mb_t5);

        // Move open chunk to closed
        db.rollover_partition("cpu", "1970-01-05T15").await.unwrap();
        // TIME 7 ---------------------------------------------------------------------------------
        let time7 = Utc::now();

        // Check first/last write times on the chunks at this point
        let mut chunk_summaries = db.chunk_summaries().expect("expected summary to return");
        chunk_summaries.sort_unstable();
        assert_eq!(chunk_summaries.len(), 2);
        // The rb chunk's times should still be the same
        let rb_t7 = chunk_summaries[0].clone();
        assert_eq!(rb_t7.storage, ChunkStorage::ReadBufferAndObjectStore);
        assert_chunks_times_eq(&rb_t7, &rb_t6);
        // The open chunk should now be closed but the times should be the same
        let closed_mb_t7 = chunk_summaries[1].clone();
        assert_eq!(closed_mb_t7.storage, ChunkStorage::ClosedMutableBuffer);
        assert_chunks_times_eq(&closed_mb_t7, &open_mb_t6);

        // New open chunk in mb
        // This point will stay in this open mb chunk
        write_lp(&db, "cpu bar=1,baz=3,blargh=3 400000000000000").await;
        // TIME 8 ---------------------------------------------------------------------------------
        let time8 = Utc::now();

        // Check first/last write times on the chunks at this point
        let mut chunk_summaries = db.chunk_summaries().expect("expected summary to return");
        chunk_summaries.sort_unstable();
        assert_eq!(chunk_summaries.len(), 3);
        // The rb chunk's times should still be the same
        let rb_t8 = chunk_summaries[0].clone();
        assert_eq!(rb_t8.storage, ChunkStorage::ReadBufferAndObjectStore);
        assert_chunks_times_eq(&rb_t8, &rb_t7);
        // The closed chunk's times should still be the same
        let closed_mb_t8 = chunk_summaries[1].clone();
        assert_eq!(closed_mb_t8.storage, ChunkStorage::ClosedMutableBuffer);
        assert_chunks_times_eq(&closed_mb_t8, &closed_mb_t7);
        // The open chunk had one write, so its times should be between t7 and t8 and first/last
        // times should be the same
        let open_mb_t8 = chunk_summaries[2].clone();
        assert_eq!(open_mb_t8.storage, ChunkStorage::OpenMutableBuffer);
        assert_first_last_times_eq(&open_mb_t8);
        assert_first_last_times_between(&open_mb_t8, time7, time8);

        let lifecycle_action = None;

        let expected = vec![
            ChunkSummary {
                partition_key: Arc::from("1970-01-01T00"),
                table_name: Arc::from("cpu"),
                id: 2,
                storage: ChunkStorage::ReadBufferAndObjectStore,
                lifecycle_action,
                memory_bytes: 4557,       // size of RB and OS chunks
                object_store_bytes: 1577, // size of parquet file
                row_count: 2,
                time_of_last_access: None,
                time_of_first_write: Utc.timestamp_nanos(1),
                time_of_last_write: Utc.timestamp_nanos(1),
                time_closed: None,
                order: ChunkOrder::new(5),
            },
            ChunkSummary {
                partition_key: Arc::from("1970-01-05T15"),
                table_name: Arc::from("cpu"),
                id: 0,
                storage: ChunkStorage::ClosedMutableBuffer,
                lifecycle_action,
                memory_bytes: 2486,
                object_store_bytes: 0, // no OS chunks
                row_count: 1,
                time_of_last_access: None,
                time_of_first_write: Utc.timestamp_nanos(1),
                time_of_last_write: Utc.timestamp_nanos(1),
                time_closed: None,
                order: ChunkOrder::new(6),
            },
            ChunkSummary {
                partition_key: Arc::from("1970-01-05T15"),
                table_name: Arc::from("cpu"),
                id: 1,
                storage: ChunkStorage::OpenMutableBuffer,
                lifecycle_action,
                memory_bytes: 1303,
                object_store_bytes: 0, // no OS chunks
                row_count: 1,
                time_of_last_access: None,
                time_of_first_write: Utc.timestamp_nanos(1),
                time_of_last_write: Utc.timestamp_nanos(1),
                time_closed: None,
                order: ChunkOrder::new(5),
            },
        ];

        for (expected_summary, actual_summary) in expected.iter().zip(chunk_summaries.iter()) {
            assert!(
                expected_summary.equal_without_timestamps(actual_summary),
                "\n\nexpected item:\n{:#?}\n\nactual item:\n{:#?}\n\n\
                     all expected:\n{:#?}\n\nall actual:\n{:#?}",
                expected_summary,
                actual_summary,
                expected,
                chunk_summaries
            );
        }

        assert_eq!(db.catalog.metrics().memory().mutable_buffer(), 2486 + 1303);
        assert_eq!(db.catalog.metrics().memory().read_buffer(), 2550);
        assert_eq!(db.catalog.metrics().memory().object_store(), 2007);
    }

    #[tokio::test]
    async fn partition_summaries() {
        // Test that chunk id listing is hooked up
        let db = make_db().await.db;

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
        db.persist_partition(
            "cpu",
            "1970-01-01T00",
            Instant::now() + Duration::from_secs(1),
        )
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
                            stats: Statistics::F64(StatValues::new(Some(1.0), Some(2.0), 2, 0)),
                        },
                        ColumnSummary {
                            name: "baz".into(),
                            influxdb_type: Some(InfluxDbType::Field),
                            stats: Statistics::F64(StatValues::new(Some(3.0), Some(3.0), 2, 1)),
                        },
                        ColumnSummary {
                            name: "time".into(),
                            influxdb_type: Some(InfluxDbType::Timestamp),
                            stats: Statistics::I64(StatValues::new(Some(1), Some(2), 2, 0)),
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
                            stats: Statistics::F64(StatValues::new(Some(1.0), Some(1.0), 1, 0)),
                        },
                        ColumnSummary {
                            name: "time".into(),
                            influxdb_type: Some(InfluxDbType::Timestamp),
                            stats: Statistics::I64(StatValues::new(Some(1), Some(1), 1, 0)),
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
                            stats: Statistics::F64(StatValues::new(Some(1.0), Some(1.0), 1, 0)),
                        },
                        ColumnSummary {
                            name: "time".into(),
                            influxdb_type: Some(InfluxDbType::Timestamp),
                            stats: Statistics::I64(StatValues::new(
                                Some(400000000000000),
                                Some(400000000000000),
                                1,
                                0,
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
                            stats: Statistics::F64(StatValues::new(Some(3.0), Some(3.0), 1, 0)),
                        },
                        ColumnSummary {
                            name: "time".into(),
                            influxdb_type: Some(InfluxDbType::Timestamp),
                            stats: Statistics::I64(StatValues::new(
                                Some(400000000000001),
                                Some(400000000000001),
                                1,
                                0,
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
        let db = make_db().await.db;

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
        db.persist_partition(
            table_name,
            partition_key,
            Instant::now() + Duration::from_secs(1),
        )
        .await
        .unwrap();

        // we should have chunks in both the read buffer only
        assert!(mutable_chunk_ids(&db, partition_key).is_empty());
        assert_eq!(read_buffer_chunk_ids(&db, partition_key), vec![1]);
        assert_eq!(read_parquet_file_chunk_ids(&db, partition_key), vec![1]);
    }

    #[tokio::test]
    async fn write_hard_limit() {
        let db = TestDb::builder()
            .lifecycle_rules(LifecycleRules {
                buffer_size_hard: Some(NonZeroUsize::new(10).unwrap()),
                ..Default::default()
            })
            .build()
            .await
            .db;

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
        let object_store = Arc::new(ObjectStore::new_in_memory());

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

        let mut reporter = metric::RawReporter::default();
        test_db.metrics_registry_v2.report(&mut reporter);

        let exclusive = reporter
            .metric("catalog_lock")
            .unwrap()
            .observation(&[
                ("db_name", "lock_tracker"),
                ("lock", "partition"),
                ("table", "cpu"),
                ("access", "exclusive"),
            ])
            .unwrap();
        assert_eq!(exclusive, &Observation::U64Counter(1));

        let shared = reporter
            .metric("catalog_lock")
            .unwrap()
            .observation(&[
                ("db_name", "lock_tracker"),
                ("lock", "partition"),
                ("table", "cpu"),
                ("access", "shared"),
            ])
            .unwrap();
        assert_eq!(shared, &Observation::U64Counter(0));

        let chunks = db.catalog.chunks();
        assert_eq!(chunks.len(), 1);

        let (sender, receiver) = tokio::sync::oneshot::channel();

        let chunk_a = Arc::clone(&chunks[0]);
        let chunk_b = Arc::clone(&chunks[0]);

        let chunk_b = chunk_b.write();

        let task = tokio::spawn(async move {
            sender.send(()).unwrap();
            let _ = chunk_a.read();
        });

        // Wait for background task to reach lock
        let _ = receiver.await.unwrap();

        // Hold lock for 100 milliseconds blocking background task
        std::thread::sleep(std::time::Duration::from_millis(100));

        std::mem::drop(chunk_b);
        task.await.unwrap();

        let mut reporter = metric::RawReporter::default();
        test_db.metrics_registry_v2.report(&mut reporter);

        let exclusive = reporter
            .metric("catalog_lock")
            .unwrap()
            .observation(&[
                ("db_name", "lock_tracker"),
                ("lock", "partition"),
                ("table", "cpu"),
                ("access", "exclusive"),
            ])
            .unwrap();
        assert_eq!(exclusive, &Observation::U64Counter(1));

        let shared = reporter
            .metric("catalog_lock")
            .unwrap()
            .observation(&[
                ("db_name", "lock_tracker"),
                ("lock", "partition"),
                ("table", "cpu"),
                ("access", "shared"),
            ])
            .unwrap();
        assert_eq!(shared, &Observation::U64Counter(1));

        let exclusive_chunk = reporter
            .metric("catalog_lock")
            .unwrap()
            .observation(&[
                ("db_name", "lock_tracker"),
                ("lock", "chunk"),
                ("table", "cpu"),
                ("access", "exclusive"),
            ])
            .unwrap();
        assert_eq!(exclusive_chunk, &Observation::U64Counter(2));

        let shared_chunk = reporter
            .metric("catalog_lock")
            .unwrap()
            .observation(&[
                ("db_name", "lock_tracker"),
                ("lock", "chunk"),
                ("table", "cpu"),
                ("access", "shared"),
            ])
            .unwrap();
        assert_eq!(shared_chunk, &Observation::U64Counter(1));

        let shared_chunk_wait = reporter
            .metric("catalog_lock_wait")
            .unwrap()
            .observation(&[
                ("db_name", "lock_tracker"),
                ("lock", "chunk"),
                ("table", "cpu"),
                ("access", "shared"),
            ])
            .unwrap();
        assert!(
            matches!(shared_chunk_wait, Observation::DurationCounter(d) if d > &Duration::from_millis(70))
        )
    }

    #[tokio::test]
    async fn write_to_preserved_catalog() {
        // Test that parquet data is committed to preserved catalog

        // ==================== setup ====================
        let object_store = Arc::new(ObjectStore::new_in_memory());
        let server_id = ServerId::try_from(1).unwrap();
        let db_name = "preserved_catalog_test";

        // ==================== do: create DB ====================
        // Create a DB given a server id, an object store and a db name
        let test_db = TestDb::builder()
            .lifecycle_rules(LifecycleRules {
                late_arrive_window_seconds: NonZeroU32::try_from(1).unwrap(),
                ..Default::default()
            })
            .object_store(Arc::clone(&object_store))
            .server_id(server_id)
            .db_name(db_name)
            .build()
            .await;
        let db = Arc::new(test_db.db);

        // ==================== check: empty catalog created ====================
        // at this point, an empty preserved catalog exists
        let maybe_preserved_catalog =
            PreservedCatalog::load::<TestCatalogState>(Arc::clone(&db.iox_object_store), ())
                .await
                .unwrap();
        assert!(maybe_preserved_catalog.is_some());

        // ==================== do: write data to parquet ====================
        // create two chunks within the same table (to better test "new chunk ID" and "new table" during transaction
        // replay as well as dropping the chunk)
        let mut chunks = vec![];
        for _ in 0..4 {
            chunks.push(create_parquet_chunk(&db).await);
        }

        // ==================== do: drop last chunk ====================
        let (table_name, partition_key, chunk_id) = chunks.pop().unwrap();
        db.drop_chunk(&table_name, &partition_key, chunk_id)
            .await
            .unwrap();

        // ==================== check: catalog state ====================
        // the preserved catalog should now register a single file
        let mut paths_expected = vec![];
        for (table_name, partition_key, chunk_id) in &chunks {
            let (chunk, _order) = db.chunk(table_name, partition_key, *chunk_id).unwrap();
            let chunk = chunk.read();
            if let ChunkStage::Persisted { parquet, .. } = chunk.stage() {
                paths_expected.push(parquet.path().clone());
            } else {
                panic!("Wrong chunk state.");
            }
        }
        paths_expected.sort();
        let (_preserved_catalog, catalog) =
            PreservedCatalog::load::<TestCatalogState>(Arc::clone(&db.iox_object_store), ())
                .await
                .unwrap()
                .unwrap();
        let paths_actual = {
            let mut tmp: Vec<_> = catalog.files().map(|info| info.path.clone()).collect();
            tmp.sort();
            tmp
        };
        assert_eq!(paths_actual, paths_expected);

        // ==================== do: remember table schema ====================
        let mut table_schemas: HashMap<String, Arc<Schema>> = Default::default();
        for (table_name, _partition_key, _chunk_id) in &chunks {
            let schema = db.table_schema(table_name).unwrap();
            table_schemas.insert(table_name.clone(), schema);
        }

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
        assert_eq!(chunks.len(), db.chunks(&Default::default()).len());
        for (table_name, partition_key, chunk_id) in &chunks {
            let (chunk, _order) = db.chunk(table_name, partition_key, *chunk_id).unwrap();
            let chunk = chunk.read();
            assert!(matches!(
                chunk.stage(),
                ChunkStage::Persisted {
                    read_buffer: None,
                    ..
                }
            ));
        }
        for (table_name, schema) in &table_schemas {
            let schema2 = db.table_schema(table_name).unwrap();
            assert_eq!(schema2.deref(), schema.deref());
        }

        // ==================== check: DB still writable ====================
        write_lp(db.as_ref(), "cpu bar=1 10").await;
    }

    #[tokio::test]
    async fn object_store_cleanup() {
        // Test that stale parquet files are removed from object store

        // ==================== setup ====================
        let object_store = Arc::new(ObjectStore::new_in_memory());

        // ==================== do: create DB ====================
        // Create a DB given a server id, an object store and a db name
        let test_db = TestDb::builder()
            .lifecycle_rules(LifecycleRules {
                late_arrive_window_seconds: NonZeroU32::try_from(1).unwrap(),
                ..Default::default()
            })
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
            let (table_name, partition_key, chunk_id) = create_parquet_chunk(&db).await;
            let (chunk, _order) = db.chunk(&table_name, &partition_key, chunk_id).unwrap();
            let chunk = chunk.read();
            if let ChunkStage::Persisted { parquet, .. } = chunk.stage() {
                paths_keep.push(parquet.path().clone());
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
                    .await
                    .unwrap();
            }
        }

        // ==================== do: create garbage ====================
        let path_delete = ParquetFilePath::new(&ChunkAddr {
            table_name: "cpu".into(),
            partition_key: "123".into(),
            chunk_id: 3,
            db_name: "not used".into(),
        });
        create_empty_file(&db.iox_object_store, &path_delete).await;

        // ==================== check: all files are there ====================
        let all_files = parquet_files(&db.iox_object_store).await.unwrap();
        for path in &paths_keep {
            assert!(all_files.contains(path));
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
            let all_files = parquet_files(&db.iox_object_store).await.unwrap();
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
        let all_files = parquet_files(&db.iox_object_store).await.unwrap();
        assert!(!all_files.contains(&path_delete));
        for path in &paths_keep {
            assert!(all_files.contains(path));
        }
    }

    #[tokio::test]
    async fn checkpointing() {
        // Test that the preserved catalog creates checkpoints

        // ==================== setup ====================
        let object_store = Arc::new(ObjectStore::new_in_memory());
        let server_id = ServerId::try_from(1).unwrap();
        let db_name = "preserved_catalog_test";

        // ==================== do: create DB ====================
        // Create a DB given a server id, an object store and a db name
        let test_db = TestDb::builder()
            .object_store(Arc::clone(&object_store))
            .server_id(server_id)
            .db_name(db_name)
            .lifecycle_rules(LifecycleRules {
                catalog_transactions_until_checkpoint: NonZeroU64::try_from(2).unwrap(),
                late_arrive_window_seconds: NonZeroU32::try_from(1).unwrap(),
                ..Default::default()
            })
            .build()
            .await;
        let db = Arc::new(test_db.db);

        // ==================== do: write data to parquet ====================
        // create two chunks within the same table (to better test "new chunk ID" and "new table" during transaction
        // replay)
        let mut chunks = vec![];
        for _ in 0..2 {
            chunks.push(create_parquet_chunk(&db).await);
        }

        // ==================== do: remove .txn files ====================
        let files = db
            .iox_object_store
            .catalog_transaction_files()
            .await
            .unwrap()
            .try_concat()
            .await
            .unwrap();
        let mut deleted_one = false;
        for file in files {
            if !file.is_checkpoint() {
                db.iox_object_store
                    .delete_catalog_transaction_file(&file)
                    .await
                    .unwrap();
                deleted_one = true;
            }
        }
        assert!(deleted_one);
        drop(db);

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
            let (chunk, _order) = db.chunk(table_name, partition_key, *chunk_id).unwrap();
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
    async fn transaction_pruning() {
        // Test that the background worker prunes transactions

        // ==================== setup ====================
        let object_store = Arc::new(ObjectStore::new_in_memory());
        let server_id = ServerId::try_from(1).unwrap();
        let db_name = "transaction_pruning_test";

        // ==================== do: create DB ====================
        // Create a DB given a server id, an object store and a db name
        let test_db = TestDb::builder()
            .object_store(Arc::clone(&object_store))
            .server_id(server_id)
            .db_name(db_name)
            .lifecycle_rules(LifecycleRules {
                catalog_transactions_until_checkpoint: NonZeroU64::try_from(1).unwrap(),
                catalog_transaction_prune_age: Duration::from_millis(1),
                late_arrive_window_seconds: NonZeroU32::try_from(1).unwrap(),
                ..Default::default()
            })
            .build()
            .await;
        let db = Arc::new(test_db.db);

        // ==================== do: write data to parquet ====================
        create_parquet_chunk(&db).await;

        // ==================== do: start background task loop ====================
        let shutdown: CancellationToken = Default::default();
        let shutdown_captured = shutdown.clone();
        let db_captured = Arc::clone(&db);
        let join_handle =
            tokio::spawn(async move { db_captured.background_worker(shutdown_captured).await });

        // ==================== check: after a while the dropped file should be gone ====================
        let t_0 = Instant::now();
        loop {
            let all_revisions = db
                .iox_object_store()
                .catalog_transaction_files()
                .await
                .unwrap()
                .map_ok(|files| {
                    files
                        .into_iter()
                        .map(|f| f.revision_counter)
                        .collect::<Vec<u64>>()
                })
                .try_concat()
                .await
                .unwrap();
            if !all_revisions.contains(&0) {
                break;
            }
            assert!(t_0.elapsed() < Duration::from_secs(10));
            tokio::time::sleep(Duration::from_millis(100)).await;
        }

        // ==================== do: stop background task loop ====================
        shutdown.cancel();
        join_handle.await.unwrap();
    }

    #[tokio::test]
    async fn table_wide_schema_enforcement() {
        // need a table with a partition template that uses a tag column, so that we can easily write to different partitions
        let test_db = TestDb::builder()
            .partition_template(PartitionTemplate {
                parts: vec![TemplatePart::Column("tag_partition_by".to_string())],
            })
            .build()
            .await;
        let db = test_db.db;

        // first write should create schema
        try_write_lp(&db, "my_table,tag_partition_by=a field_integer=1 10")
            .await
            .unwrap();

        // other writes are allowed to evolve the schema
        try_write_lp(&db, "my_table,tag_partition_by=a field_string=\"foo\" 10")
            .await
            .unwrap();
        try_write_lp(&db, "my_table,tag_partition_by=b field_float=1.1 10")
            .await
            .unwrap();

        // check that we have the expected partitions
        let mut partition_keys = db.partition_keys().unwrap();
        partition_keys.sort();
        assert_eq!(
            partition_keys,
            vec![
                "tag_partition_by_a".to_string(),
                "tag_partition_by_b".to_string(),
            ]
        );

        // illegal changes
        let e = try_write_lp(&db, "my_table,tag_partition_by=a field_integer=\"foo\" 10")
            .await
            .unwrap_err();
        assert_store_sequenced_entry_failures!(
            e,
            [super::Error::TableBatchSchemaMergeError { .. }]
        );
        let e = try_write_lp(&db, "my_table,tag_partition_by=b field_integer=\"foo\" 10")
            .await
            .unwrap_err();
        assert_store_sequenced_entry_failures!(
            e,
            [super::Error::TableBatchSchemaMergeError { .. }]
        );
        let e = try_write_lp(&db, "my_table,tag_partition_by=c field_integer=\"foo\" 10")
            .await
            .unwrap_err();
        assert_store_sequenced_entry_failures!(
            e,
            [super::Error::TableBatchSchemaMergeError { .. }]
        );

        // drop all chunks
        for partition_key in db.partition_keys().unwrap() {
            let chunk_ids: Vec<_> = {
                let partition = db.partition("my_table", &partition_key).unwrap();
                let partition = partition.read();
                partition
                    .chunks()
                    .into_iter()
                    .map(|chunk| {
                        let chunk = chunk.read();
                        chunk.id()
                    })
                    .collect()
            };

            for chunk_id in chunk_ids {
                db.drop_chunk("my_table", &partition_key, chunk_id)
                    .await
                    .unwrap();
            }
        }

        // schema is still there
        let e = try_write_lp(&db, "my_table,tag_partition_by=a field_integer=\"foo\" 10")
            .await
            .unwrap_err();
        assert_store_sequenced_entry_failures!(
            e,
            [super::Error::TableBatchSchemaMergeError { .. }]
        );
    }

    #[tokio::test]
    async fn drop_unpersisted_chunk_on_persisted_db() {
        // We don't support dropping unpersisted chunks from a persisted DB because we would forget the write buffer
        // progress (partition checkpoints are only created when new parquet files are stored).
        // See https://github.com/influxdata/influxdb_iox/issues/2291
        let test_db = TestDb::builder()
            .lifecycle_rules(LifecycleRules {
                persist: true,
                ..Default::default()
            })
            .build()
            .await;
        let db = Arc::new(test_db.db);

        write_lp(db.as_ref(), "cpu bar=1 10").await;

        let partition_key = "1970-01-01T00";
        let chunks = db.partition_chunk_summaries(partition_key);
        assert_eq!(chunks.len(), 1);
        let chunk_id = chunks[0].id;

        let err = db
            .drop_chunk("cpu", partition_key, chunk_id)
            .await
            .unwrap_err();
        assert!(matches!(
            err,
            Error::LifecycleError {
                source: super::lifecycle::Error::CannotDropUnpersistedChunk { .. }
            }
        ));
    }

    #[tokio::test]
    async fn drop_unpersisted_partition_on_persisted_db() {
        let test_db = TestDb::builder()
            .lifecycle_rules(LifecycleRules {
                late_arrive_window_seconds: NonZeroU32::try_from(1).unwrap(),
                mub_row_threshold: NonZeroUsize::try_from(1).unwrap(),
                persist: true,
                ..Default::default()
            })
            .build()
            .await;
        let db = Arc::new(test_db.db);

        write_lp(db.as_ref(), "cpu bar=1 10").await;
        write_lp(db.as_ref(), "cpu bar=2 20").await;

        let partition_key = "1970-01-01T00";

        // two chunks created
        assert_eq!(db.partition_chunk_summaries(partition_key).len(), 2);

        // We don't support dropping unpersisted chunks from a persisted DB because we would forget the write buffer
        // progress (partition checkpoints are only created when new parquet files are stored).
        // See https://github.com/influxdata/influxdb_iox/issues/2291
        let err = db.drop_partition("cpu", partition_key).await.unwrap_err();
        assert!(matches!(
            err,
            Error::LifecycleError {
                source: super::lifecycle::Error::CannotDropUnpersistedChunk { .. }
            }
        ));

        // once persisted drop should work
        db.persist_partition(
            "cpu",
            partition_key,
            Instant::now() + Duration::from_secs(1),
        )
        .await
        .unwrap();
        db.drop_partition("cpu", partition_key).await.unwrap();

        // no chunks left
        assert_eq!(db.partition_chunk_summaries(partition_key), vec![]);
    }

    #[tokio::test]
    async fn query_after_drop_partition_on_persisted_db() {
        let test_db = TestDb::builder()
            .lifecycle_rules(LifecycleRules {
                late_arrive_window_seconds: NonZeroU32::try_from(1).unwrap(),
                mub_row_threshold: NonZeroUsize::try_from(1).unwrap(),
                persist: true,
                ..Default::default()
            })
            .build()
            .await;
        let db = Arc::new(test_db.db);

        write_lp(db.as_ref(), "cpu bar=1 10").await;
        write_lp(db.as_ref(), "cpu bar=2 20").await;

        let partition_key = "1970-01-01T00";
        db.persist_partition(
            "cpu",
            partition_key,
            Instant::now() + Duration::from_secs(1),
        )
        .await
        .unwrap();

        // query data before drop
        let expected = vec![
            "+-----------------+",
            "| COUNT(UInt8(1)) |",
            "+-----------------+",
            "| 2               |",
            "+-----------------+",
        ];
        let batches = run_query(Arc::clone(&db), "select count(*) from system.columns").await;
        assert_batches_sorted_eq!(&expected, &batches);

        // Drop the partition (avoid data)
        db.drop_partition("cpu", partition_key).await.unwrap();

        // query data after drop -- should have no rows and also no error
        let expected = vec![
            "+-----------------+",
            "| COUNT(UInt8(1)) |",
            "+-----------------+",
            "| 0               |",
            "+-----------------+",
        ];
        let batches = run_query(Arc::clone(&db), "select count(*) from system.columns").await;
        assert_batches_sorted_eq!(&expected, &batches);
    }

    async fn create_parquet_chunk(db: &Arc<Db>) -> (String, String, u32) {
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
        let chunk = db
            .persist_partition(
                table_name,
                partition_key,
                Instant::now() + Duration::from_secs(1),
            )
            .await
            .unwrap();

        // chunk ID changed during persistence
        let chunk_id = chunk.id();

        (table_name.to_string(), partition_key.to_string(), chunk_id)
    }

    async fn create_empty_file(iox_object_store: &IoxObjectStore, path: &ParquetFilePath) {
        let data = Bytes::default();
        let len = data.len();

        iox_object_store
            .put_parquet_file(
                path,
                futures::stream::once(async move { Ok(data) }),
                Some(len),
            )
            .await
            .unwrap();
    }
}
