crate::maybe_pub!(
    pub use super::wal_replay::*;
);

mod graceful_shutdown;
mod wal_replay;

use std::{path::PathBuf, sync::Arc, time::Duration};

use arrow_flight::flight_service_server::FlightService;
use backoff::BackoffConfig;
use futures::{future::Shared, Future, FutureExt};
use generated_types::influxdata::iox::{
    catalog::v1::catalog_service_server::CatalogService,
    ingester::v1::{persist_service_server::PersistService, write_service_server::WriteService},
};
use iox_catalog::interface::Catalog;
use iox_query::exec::Executor;
use observability_deps::tracing::*;
use parquet_file::storage::ParquetStorage;
use thiserror::Error;
use tokio::sync::oneshot;
use tokio_util::sync::CancellationToken;
use wal::Wal;

use crate::{
    buffer_tree::{
        namespace::name_resolver::{NamespaceNameProvider, NamespaceNameResolver},
        partition::resolver::{CatalogPartitionResolver, PartitionCache, PartitionProvider},
        table::name_resolver::{TableNameProvider, TableNameResolver},
        BufferTree,
    },
    dml_sink::{instrumentation::DmlSinkInstrumentation, tracing::DmlSinkTracing},
    ingest_state::IngestState,
    ingester_id::IngesterId,
    persist::{
        completion_observer::NopObserver, handle::PersistHandle,
        hot_partitions::HotPartitionPersister,
    },
    query::{instrumentation::QueryExecInstrumentation, tracing::QueryExecTracing},
    server::grpc::GrpcDelegate,
    timestamp_oracle::TimestampOracle,
    wal::{rotate_task::periodic_rotation, wal_sink::WalSink},
    TRANSITION_SHARD_INDEX,
};

use self::graceful_shutdown::graceful_shutdown_handler;

/// Acquire opaque handles to the Ingester RPC service implementations.
///
/// This trait serves as the public crate API boundary - callers external to the
/// Ingester crate utilise this abstraction to acquire type erased handles to
/// the RPC service implementations, hiding internal Ingester implementation
/// details & types.
///
/// Callers can mock out this trait or decorate the returned implementation in
/// order to simulate or modify the behaviour of an ingester in their own tests.
pub trait IngesterRpcInterface: Send + Sync + std::fmt::Debug {
    /// The type of the [`CatalogService`] implementation.
    type CatalogHandler: CatalogService;
    /// The type of the [`WriteService`] implementation.
    type WriteHandler: WriteService;
    /// The type of the [`PersistService`] implementation.
    type PersistHandler: PersistService;
    /// The type of the [`FlightService`] implementation.
    type FlightHandler: FlightService;

    /// Acquire an opaque handle to the Ingester's [`CatalogService`] RPC
    /// handler implementation.
    fn catalog_service(&self) -> Self::CatalogHandler;

    /// Acquire an opaque handle to the Ingester's [`WriteService`] RPC
    /// handler implementation.
    fn write_service(&self) -> Self::WriteHandler;

    /// Acquire an opaque handle to the Ingester's [`PersistService`] RPC
    /// handler implementation.
    fn persist_service(&self) -> Self::PersistHandler;

    /// Acquire an opaque handle to the Ingester's Arrow Flight
    /// [`FlightService`] RPC handler implementation, allowing at most
    /// `max_simultaneous_requests` queries to be running at any one time.
    fn query_service(&self, max_simultaneous_requests: usize) -> Self::FlightHandler;
}

/// A RAII guard to clean up `ingester2` instance resources when dropped.
#[must_use = "ingester stops when guard is dropped"]
#[derive(Debug)]
pub struct IngesterGuard<T> {
    rpc: T,

    /// The handle of the periodic WAL rotation task.
    ///
    /// Aborted on drop.
    rotation_task: tokio::task::JoinHandle<()>,

    /// The task handle executing the graceful shutdown once triggered.
    graceful_shutdown_handler: tokio::task::JoinHandle<()>,
    shutdown_complete: Shared<oneshot::Receiver<()>>,
}

impl<T> IngesterGuard<T>
where
    T: Send + Sync,
{
    /// Obtain a handle to the gRPC handlers.
    pub fn rpc(&self) -> &T {
        &self.rpc
    }

    /// Block and wait until the ingester has gracefully stopped.
    pub async fn join(&self) {
        self.shutdown_complete
            .clone()
            .await
            .expect("graceful shutdown task panicked")
    }
}

impl<T> Drop for IngesterGuard<T> {
    fn drop(&mut self) {
        self.rotation_task.abort();
        self.graceful_shutdown_handler.abort();
    }
}

/// Errors that occur during initialisation of an `ingester2` instance.
#[derive(Debug, Error)]
pub enum InitError {
    /// A catalog error occurred while fetching the most recent partitions for
    /// the internal cache.
    #[error("failed to pre-warm partition cache: {0}")]
    PreWarmPartitions(iox_catalog::interface::Error),

    /// An error initialising the WAL.
    #[error("failed to initialise write-ahead log: {0}")]
    WalInit(#[from] wal::Error),

    /// An error replaying the entries in the WAL.
    #[error(transparent)]
    WalReplay(Box<dyn std::error::Error>),
}

/// Initialise a new `ingester2` instance, returning the gRPC service handler
/// implementations to be bound by the caller.
///
/// ## WAL Replay
///
/// Writes through an `ingester2` instance commit to a durable write-ahead log.
///
/// During initialisation of an `ingester2` instance, any files in
/// `wal_directory` are read assuming they are redo log files from the
/// write-ahead log.
///
/// These files are read and replayed fully before this function returns.
///
/// Any error during replay is fatal.
///
/// ## Deferred Loading for Persist Operations
///
/// Several items within the ingester's internal state are loaded only when
/// needed at persist time; this includes string name identifiers of namespaces,
/// tables, etc that are embedded within the Parquet file metadata.
///
/// As an optimisation, these deferred loads occur in a background task before
/// the persist action actually needs them, in order to both eliminate the
/// latency of waiting for the value to be fetched, and to avoid persistence of
/// large numbers of partitions operations causing a large spike in catalog
/// requests / load.
///
/// These values are loaded a uniformly random duration of time between
/// initialisation, and at most, `persist_background_fetch_time` duration of
/// time later. By increasing this duration value the many loads are spread
/// approximately uniformly over a longer period of time, decreasing the peak
/// catalog load they cause.
///
/// If the `persist_background_fetch_time` duration is too large, they will not
/// have resolved in the background when a persist operation starts, and they
/// will require demand loading, causing an immediate catalog load spike. This
/// value should be tuned to be slightly less than the interval between persist
/// operations, but not so long that it causes catalog load spikes at persist
/// time (which can be observed by the catalog instrumentation metrics).
///
/// ## Graceful Shutdown
///
/// When `shutdown` completes, the ingester blocks ingest (returning an error to
/// all new write requests) while still executing query requests. The ingester
/// then persists all data currently buffered.
///
/// Callers can wait for this buffer persist to complete by awaiting
/// [`IngesterGuard::join()`], which will resolve once all data has been flushed
/// to object storage.
///
/// The ingester will continue answering queries until the gRPC server is
/// stopped by the caller (managed outside of this crate).
#[allow(clippy::too_many_arguments)]
pub async fn new<F>(
    catalog: Arc<dyn Catalog>,
    metrics: Arc<metric::Registry>,
    persist_background_fetch_time: Duration,
    wal_directory: PathBuf,
    wal_rotation_period: Duration,
    persist_executor: Arc<Executor>,
    persist_workers: usize,
    persist_queue_depth: usize,
    persist_hot_partition_cost: usize,
    object_store: ParquetStorage,
    shutdown: F,
) -> Result<IngesterGuard<impl IngesterRpcInterface>, InitError>
where
    F: Future<Output = CancellationToken> + Send + 'static,
{
    // Create the transition shard.
    let mut txn = catalog
        .start_transaction()
        .await
        .expect("start transaction");
    let topic = txn
        .topics()
        .create_or_get("iox-shared")
        .await
        .expect("get topic");
    let transition_shard = txn
        .shards()
        .create_or_get(&topic, TRANSITION_SHARD_INDEX)
        .await
        .expect("create transition shard");
    txn.commit().await.expect("commit transition shard");

    // Initialise a random ID for this ingester instance.
    let ingester_id = IngesterId::new();

    // Initialise the deferred namespace name resolver.
    let namespace_name_provider: Arc<dyn NamespaceNameProvider> =
        Arc::new(NamespaceNameResolver::new(
            persist_background_fetch_time,
            Arc::clone(&catalog),
            BackoffConfig::default(),
        ));

    // Initialise the deferred table name resolver.
    let table_name_provider: Arc<dyn TableNameProvider> = Arc::new(TableNameResolver::new(
        persist_background_fetch_time,
        Arc::clone(&catalog),
        BackoffConfig::default(),
    ));

    // Read the most recently created partitions for the shards this ingester
    // instance will be consuming from.
    //
    // By caching these hot partitions overall catalog load after an ingester
    // starts up is reduced, and the associated query latency is removed from
    // the (blocking) ingest hot path.
    let recent_partitions = catalog
        .repositories()
        .await
        .partitions()
        .most_recent_n(40_000, &[transition_shard.id])
        .await
        .map_err(InitError::PreWarmPartitions)?;

    // Build the partition provider, wrapped in the partition cache.
    let partition_provider = CatalogPartitionResolver::new(Arc::clone(&catalog));
    let partition_provider = PartitionCache::new(
        partition_provider,
        recent_partitions,
        persist_background_fetch_time,
        Arc::clone(&catalog),
        BackoffConfig::default(),
    );
    let partition_provider: Arc<dyn PartitionProvider> = Arc::new(partition_provider);

    // Initialise the ingest pause signal, used to propagate error conditions
    // between subsystems such that they cause an error to be returned in the
    // write path.
    let ingest_state = Arc::new(IngestState::default());

    // Spawn the persist workers to compact partition data, convert it into
    // Parquet files, and upload them to object storage.
    let persist_handle = PersistHandle::new(
        persist_workers,
        persist_queue_depth,
        Arc::clone(&ingest_state),
        persist_executor,
        object_store,
        Arc::clone(&catalog),
        NopObserver::default(),
        &metrics,
    );
    let persist_handle = Arc::new(persist_handle);

    // Instantiate a post-write observer for hot partition persistence.
    //
    // By enabling hot partition persistence before replaying the WAL, the
    // ingester can persist files during WAL replay.
    //
    // It is also important to respect potential configuration changes between
    // runs, such as if the configuration of the ingester was changed to persist
    // smaller partitions in-between executions because it was OOMing during WAL
    // replay (and the configuration was changed to mitigate it).
    let hot_partition_persister =
        HotPartitionPersister::new(Arc::clone(&persist_handle), persist_hot_partition_cost);

    let buffer = Arc::new(BufferTree::new(
        namespace_name_provider,
        table_name_provider,
        partition_provider,
        Arc::new(hot_partition_persister),
        Arc::clone(&metrics),
        transition_shard.id,
    ));

    // Initialise the WAL
    let wal = Wal::new(wal_directory).await.map_err(InitError::WalInit)?;

    // Replay the WAL log files, if any.
    let max_sequence_number =
        wal_replay::replay(&wal, &buffer, Arc::clone(&persist_handle), &metrics)
            .await
            .map_err(|e| InitError::WalReplay(e.into()))?;

    // Build the chain of DmlSink that forms the write path.
    let write_path = DmlSinkInstrumentation::new(
        "write_apply",
        DmlSinkTracing::new(
            DmlSinkTracing::new(
                WalSink::new(
                    DmlSinkInstrumentation::new(
                        "buffer",
                        DmlSinkTracing::new(Arc::clone(&buffer), "buffer"),
                        &metrics,
                    ),
                    Arc::clone(&wal),
                ),
                "wal",
            ),
            "write_apply",
        ),
        &metrics,
    );

    // And the chain of QueryExec that forms the read path.
    let read_path = QueryExecInstrumentation::new(
        "buffer",
        QueryExecTracing::new(Arc::clone(&buffer), "buffer"),
        &metrics,
    );

    // Spawn a background thread to periodically rotate the WAL segment file.
    let rotation_task = tokio::spawn(periodic_rotation(
        Arc::clone(&wal),
        wal_rotation_period,
        Arc::clone(&buffer),
        Arc::clone(&persist_handle),
    ));

    // Restore the highest sequence number from the WAL files, and default to 0
    // if there were no files to replay.
    //
    // This means sequence numbers are reused across different instances of an
    // ingester, but they are only used for internal ordering of operations at
    // runtime.
    let timestamp = Arc::new(TimestampOracle::new(
        max_sequence_number
            .map(|v| u64::try_from(v.get()).expect("sequence number overflow"))
            .unwrap_or(0),
    ));

    let (shutdown_tx, shutdown_rx) = oneshot::channel();
    let shutdown_task = tokio::spawn(graceful_shutdown_handler(
        shutdown,
        shutdown_tx,
        Arc::clone(&ingest_state),
        Arc::clone(&buffer),
        Arc::clone(&persist_handle),
        wal,
    ));

    Ok(IngesterGuard {
        rpc: GrpcDelegate::new(
            Arc::new(write_path),
            Arc::new(read_path),
            timestamp,
            ingest_state,
            ingester_id,
            catalog,
            metrics,
            buffer,
            persist_handle,
        ),
        rotation_task,
        graceful_shutdown_handler: shutdown_task,
        shutdown_complete: shutdown_rx.shared(),
    })
}
