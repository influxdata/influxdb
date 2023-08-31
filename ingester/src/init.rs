use data_types::ParquetFile;
use gossip::{NopDispatcher, TopicInterests};

use gossip_parquet_file::tx::ParquetFileTx;
/// This needs to be pub for the benchmarks but should not be used outside the crate.
#[cfg(feature = "benches")]
pub use wal_replay::*;

mod graceful_shutdown;
mod wal_replay;

use std::{net::SocketAddr, path::PathBuf, sync::Arc, time::Duration};

use arrow_flight::flight_service_server::FlightService;
use backoff::BackoffConfig;
use futures::{future::Shared, Future, FutureExt};
use generated_types::influxdata::iox::{
    catalog::v1::catalog_service_server::CatalogService,
    gossip::Topic,
    ingester::v1::{persist_service_server::PersistService, write_service_server::WriteService},
};
use iox_catalog::interface::Catalog;
use iox_query::exec::Executor;
use observability_deps::tracing::*;
use parquet_file::storage::ParquetStorage;
use thiserror::Error;
use tokio::sync::oneshot;
use tokio_util::sync::CancellationToken;
use tracker::DiskSpaceMetrics;
use wal::Wal;

use crate::{
    buffer_tree::{
        namespace::name_resolver::{NamespaceNameProvider, NamespaceNameResolver},
        partition::resolver::{
            CatalogPartitionResolver, CoalescePartitionResolver, OldPartitionBloomFilter,
            PartitionCache, PartitionProvider,
        },
        table::metadata_resolver::{TableProvider, TableResolver},
        BufferTree,
    },
    dml_sink::{instrumentation::DmlSinkInstrumentation, tracing::DmlSinkTracing},
    gossip::persist_parquet::ParquetFileNotification,
    ingest_state::IngestState,
    ingester_id::IngesterId,
    persist::{
        completion_observer::MaybeLayer, file_metrics::ParquetFileInstrumentation,
        handle::PersistHandle, hot_partitions::HotPartitionPersister,
    },
    query::{
        exec_instrumentation::QueryExecInstrumentation,
        result_instrumentation::QueryResultInstrumentation, tracing::QueryExecTracing,
    },
    server::grpc::GrpcDelegate,
    timestamp_oracle::TimestampOracle,
    wal::{
        disk_full_protection::{self, guard_disk_capacity},
        reference_tracker::WalReferenceHandle,
        rotate_task::periodic_rotation,
        wal_sink::WalSink,
    },
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

/// A RAII guard to clean up `ingester` instance resources when dropped.
#[must_use = "ingester stops when guard is dropped"]
#[derive(Debug)]
pub struct IngesterGuard<T> {
    rpc: T,

    /// The handle of the periodic WAL rotation task.
    ///
    /// Aborted on drop.
    rotation_task: tokio::task::JoinHandle<()>,

    /// The handle of the periodic disk metric task.
    ///
    /// Aborted on drop.
    disk_metric_task: tokio::task::JoinHandle<()>,

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
        self.disk_metric_task.abort();
        self.graceful_shutdown_handler.abort();
    }
}

/// Configuration parameters for the optional gossip sub-system.
#[derive(Debug, Default)]
pub enum GossipConfig {
    /// Disable the gossip sub-system.
    #[default]
    Disabled,

    /// Enable the gossip sub-system, listening on the specified `bind_addr` and
    /// using `peers` as the initial peer seed list.
    Enabled {
        /// UDP socket address to use for gossip communication.
        bind_addr: SocketAddr,
        /// Initial peer seed list in the form of either:
        ///
        ///   - "dns.address.example:port"
        ///   - "10.0.0.1:port"
        ///
        peers: Vec<String>,
    },
}

/// Errors that occur during initialisation of an `ingester` instance.
#[derive(Debug, Error)]
pub enum InitError {
    /// A catalog error occurred while fetching the most recent partitions for
    /// the internal cache.
    #[error("failed to pre-warm partition cache: {0}")]
    PreWarmPartitions(iox_catalog::interface::Error),

    /// A catalog error occurred while fetching the old-style partitions for the bloom filter.
    #[error("failed to fetch old-style partitions: {0}")]
    FetchOldStylePartitions(iox_catalog::interface::Error),

    /// An error initialising the WAL.
    #[error("failed to initialise write-ahead log: {0}")]
    WalInit(#[from] wal::Error),

    /// An error replaying the entries in the WAL.
    #[error(transparent)]
    WalReplay(Box<dyn std::error::Error>),

    /// An error binding the UDP socket for gossip communication.
    #[error("failed to bind udp gossip socket: {0}")]
    GossipBind(std::io::Error),
}

/// Initialise a new `ingester` instance, returning the gRPC service handler
/// implementations to be bound by the caller.
///
/// ## WAL Replay
///
/// Writes through an `ingester` instance commit to a durable write-ahead log.
///
/// During initialisation of an `ingester` instance, any files in
/// `wal_directory` are read assuming they are redo log files from the
/// write-ahead log.
///
/// These files are read and replayed fully before this function returns.
///
/// Any error during replay is fatal.
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
/// ## Hot Persistence
///
/// Partitions have a opaque estimate of the "cost" (in terms of time/space) to
/// persist the data within them. The cost calculation is made in
/// [`MutableBatch::size_data()`].
///
/// Once this cost estimation exceeds the `persist_hot_partition_cost` the
/// partition is immediately enqueued for persistence, and subsequent writes are
/// applied to a new buffer.
///
/// Increasing this value reduces the frequency of hot partition persistence,
/// but may also increase the total amount of data that needs persisting for a
/// single partition. In practice, this increases the memory utilisation of
/// datafusion during the persist compaction step.
///
/// Decreasing this value increases the frequency of persist operations, and
/// usually decreases the size of the resulting parquet files.
///
/// [`MutableBatch::size_data()`]: mutable_batch::MutableBatch::size_data
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
    gossip: GossipConfig,
    shutdown: F,
) -> Result<IngesterGuard<impl IngesterRpcInterface>, InitError>
where
    F: Future<Output = CancellationToken> + Send + 'static,
{
    // Initialise a random ID for this ingester instance.
    let ingester_id = IngesterId::new();

    // Initialise the deferred namespace name resolver.
    let namespace_name_provider: Arc<dyn NamespaceNameProvider> =
        Arc::new(NamespaceNameResolver::new(
            persist_background_fetch_time,
            Arc::clone(&catalog),
            BackoffConfig::default(),
            Arc::clone(&metrics),
        ));

    // Initialise the deferred table metadata resolver.
    let table_provider: Arc<dyn TableProvider> = Arc::new(TableResolver::new(
        persist_background_fetch_time,
        Arc::clone(&catalog),
        BackoffConfig::default(),
        Arc::clone(&metrics),
    ));

    // Read the most recently created partitions.
    //
    // By caching these hot partitions overall catalog load after an ingester
    // starts up is reduced, and the associated query latency is removed from
    // the (blocking) ingest hot path.
    let recent_partitions = catalog
        .repositories()
        .await
        .partitions()
        .most_recent_n(40_000)
        .await
        .map_err(InitError::PreWarmPartitions)?;

    // Fetch all the currently-existing old-style partitions to be put into a bloom filter that
    // determines if we need to resolve a partition (potentially making a catalog query) or not.
    let old_style = catalog
        .repositories()
        .await
        .partitions()
        .list_old_style()
        .await
        .map_err(InitError::FetchOldStylePartitions)?;

    // Build the partition provider, wrapped in the old partition bloom filter, partition cache,
    // and request coalescer.
    let partition_provider = CatalogPartitionResolver::new(Arc::clone(&catalog));
    let partition_provider = CoalescePartitionResolver::new(Arc::new(partition_provider));
    let partition_provider = OldPartitionBloomFilter::new(
        partition_provider,
        Arc::clone(&catalog),
        BackoffConfig::default(),
        persist_background_fetch_time,
        Arc::clone(&metrics),
        old_style,
    );
    let partition_provider = PartitionCache::new(
        partition_provider,
        recent_partitions,
        persist_background_fetch_time,
        Arc::clone(&catalog),
        BackoffConfig::default(),
        Arc::clone(&metrics),
    );
    let partition_provider: Arc<dyn PartitionProvider> = Arc::new(partition_provider);

    // Initialise the ingest pause signal, used to propagate error conditions
    // between subsystems such that they cause an error to be returned in the
    // write path.
    let ingest_state = Arc::new(IngestState::default());

    // Initialise the WAL
    let wal = Wal::new(wal_directory.clone())
        .await
        .map_err(InitError::WalInit)?;

    // Start defining the chain of persist completion observers so it can be
    // layered in gossip handlers if needed.
    //
    // Prepare the WAL segment reference tracker
    let (wal_reference_handle, wal_reference_actor) =
        WalReferenceHandle::new(Arc::clone(&wal), &metrics);
    // Add file metric instrumentation.
    let persist_observer = ParquetFileInstrumentation::new(wal_reference_handle.clone(), &metrics);

    // Optionally start the gossip subsystem and layer on the parquet file
    // gossip handler.
    let persist_observer = match gossip {
        GossipConfig::Disabled => {
            info!("gossip disabled");
            MaybeLayer::Without(persist_observer)
        }
        GossipConfig::Enabled { bind_addr, peers } => {
            // Start the gossip sub-system, which logs during init.
            let handle = gossip::Builder::<_, Topic>::new(
                peers,
                NopDispatcher::default(),
                Arc::clone(&metrics),
            )
            // Configure the ingester to ignore all user payloads, only acting
            // as a gossip peer exchange seed and sender of messages.
            .with_topic_filter(TopicInterests::default())
            .bind(bind_addr)
            .await
            .map_err(InitError::GossipBind)?;

            let persist_observer = ParquetFileNotification::new(
                persist_observer,
                ParquetFileTx::<ParquetFile>::new(handle),
            );

            MaybeLayer::With(persist_observer)
        }
    };

    // Spawn the persist workers to compact partition data, convert it into
    // Parquet files, and upload them to object storage.
    let persist_handle = PersistHandle::new(
        persist_workers,
        persist_queue_depth,
        Arc::clone(&ingest_state),
        persist_executor,
        object_store,
        Arc::clone(&catalog),
        persist_observer,
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
    let hot_partition_persister = HotPartitionPersister::new(
        Arc::clone(&persist_handle),
        persist_hot_partition_cost,
        &metrics,
    );

    let buffer = Arc::new(BufferTree::new(
        namespace_name_provider,
        table_provider,
        partition_provider,
        Arc::new(hot_partition_persister),
        Arc::clone(&metrics),
    ));

    // Start the WAL reference actor and then replay the WAL log files, if any.
    // The tokio handle does not need retained here as the actor handle is
    // responsible for aborting the actor's run loop when dropped.
    tokio::spawn(wal_reference_actor.run());

    // Initialize disk metrics to emit disk capacity / free statistics for the
    // WAL directory.
    let (disk_metric_task, snapshot_rx) = DiskSpaceMetrics::new(wal_directory, &metrics)
        .expect("failed to resolve WAL directory to disk");
    let disk_metric_task = tokio::task::spawn(disk_metric_task.run());
    // Spawn the disk full protection task, with values fed from the disk space
    // metric collection task. This importantly doesn't leak the task, as it will
    // shut itself down when the disk metric task's snapshot sender is dropped and
    // the receiver is disconnected.
    tokio::spawn(guard_disk_capacity(
        snapshot_rx,
        Arc::clone(&ingest_state),
        disk_full_protection::WalPersister::new(
            Arc::clone(&wal),
            wal_reference_handle.clone(),
            Arc::clone(&buffer),
            Arc::clone(&persist_handle),
        ),
    ));

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
                    wal_reference_handle.clone(),
                ),
                "wal",
            ),
            "write_apply",
        ),
        &metrics,
    );

    // And the chain of QueryExec that forms the read path.
    let read_path = QueryResultInstrumentation::new(Arc::clone(&buffer), &metrics);
    let read_path = QueryExecInstrumentation::new(
        "buffer",
        QueryExecTracing::new(read_path, "buffer"),
        &metrics,
    );

    // Spawn a background thread to periodically rotate the WAL segment file.
    let rotation_task = tokio::spawn(periodic_rotation(
        Arc::clone(&wal),
        wal_rotation_period,
        wal_reference_handle.clone(),
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
        max_sequence_number.map(|v| v.get()).unwrap_or(0),
    ));

    let (shutdown_tx, shutdown_rx) = oneshot::channel();
    let shutdown_task = tokio::spawn(graceful_shutdown_handler(
        shutdown,
        shutdown_tx,
        Arc::clone(&ingest_state),
        Arc::clone(&buffer),
        Arc::clone(&persist_handle),
        Arc::clone(&wal),
        wal_reference_handle,
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
        disk_metric_task,
        graceful_shutdown_handler: shutdown_task,
        shutdown_complete: shutdown_rx.shared(),
    })
}
