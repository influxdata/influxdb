//! Entrypoint for InfluxDB 3 Enterprise Server

use anyhow::{bail, Context};
use datafusion_util::config::register_iox_object_store;
use futures::future::join_all;
use futures::future::FutureExt;
use futures::TryFutureExt;
use influxdb3_cache::{
    distinct_cache::DistinctCacheProvider,
    last_cache::{self, LastCacheProvider},
    parquet_cache::create_cached_obj_store_and_oracle,
};
use influxdb3_clap_blocks::{
    datafusion::IoxQueryDatafusionConfig,
    memory_size::MemorySize,
    object_store::{ObjectStoreConfig, ObjectStoreType},
    socket_addr::SocketAddr,
    tokio::TokioDatafusionConfig,
};
use influxdb3_config::Config as ConfigTrait;
use influxdb3_config::EnterpriseConfig;
use influxdb3_enterprise_buffer::{
    modes::{read::CreateReadModeArgs, read_write::CreateReadWriteModeArgs},
    replica::ReplicationConfig,
    WriteBufferEnterprise,
};
use influxdb3_enterprise_clap_blocks::serve::BufferMode;
use influxdb3_enterprise_compactor::producer::CompactedDataProducer;
use influxdb3_enterprise_compactor::{
    compacted_data::{CompactedData, CompactedDataSystemTableView},
    sys_events::CompactionEventStore,
};
use influxdb3_enterprise_compactor::{
    consumer::CompactedDataConsumer, producer::CompactedDataProducerArgs,
};
use influxdb3_enterprise_data_layout::CompactionConfig;
use influxdb3_enterprise_parquet_cache::ParquetCachePreFetcher;
use influxdb3_internal_api::query_executor::QueryExecutor;
use influxdb3_process::{
    build_malloc_conf, setup_metric_registry, INFLUXDB3_GIT_HASH, INFLUXDB3_VERSION, PROCESS_UUID,
};
use influxdb3_server::{
    auth::AllOrNothingAuthorizer,
    builder::ServerBuilder,
    query_executor::{
        enterprise::{CompactionSysTableQueryExecutorArgs, CompactionSysTableQueryExecutorImpl},
        CreateQueryExecutorArgs, QueryExecutorImpl,
    },
    serve, CommonServerState,
};
use influxdb3_sys_events::SysEventStore;
use influxdb3_telemetry::store::TelemetryStore;
use influxdb3_wal::{Gen1Duration, WalConfig};
use influxdb3_write::{
    persister::Persister,
    write_buffer::{
        check_mem_and_force_snapshot_loop, persisted_files::PersistedFiles, WriteBufferImpl,
    },
    WriteBuffer,
};
use iox_query::exec::{DedicatedExecutor, Executor, ExecutorConfig};
use iox_time::SystemProvider;
use object_store::ObjectStore;
use observability_deps::tracing::*;
use panic_logging::SendPanicsToTracing;
use parquet_file::storage::{ParquetStorage, StorageId};
use std::{num::NonZeroUsize, sync::Arc, time::Duration};
use std::{
    path::{Path, PathBuf},
    str::FromStr,
};
use thiserror::Error;
use tokio::net::TcpListener;
use tokio::sync::RwLock;
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;
use trace_exporters::TracingConfig;
use trace_http::ctx::TraceHeaderParser;
use trogging::cli::LoggingConfig;
#[cfg(not(feature = "no_license"))]
use {
    influxdb3_server::EXPIRED_LICENSE,
    jsonwebtoken::{decode, decode_header, Algorithm, DecodingKey, Validation},
    object_store::path::Path as ObjPath,
    object_store::PutPayload,
    serde::{Deserialize, Serialize},
    std::io::IsTerminal,
    std::sync::atomic::Ordering,
    std::time::SystemTime,
    std::time::UNIX_EPOCH,
};

/// The default name of the influxdb data directory
#[allow(dead_code)]
pub const DEFAULT_DATA_DIRECTORY_NAME: &str = ".influxdb3";

/// The default bind address for the HTTP API.
pub const DEFAULT_HTTP_BIND_ADDR: &str = "0.0.0.0:8181";

pub const DEFAULT_TELMETRY_ENDPOINT: &str = "https://telemetry.v3.influxdata.com";

#[derive(Debug, Error)]
pub enum Error {
    #[error("Cannot parse object store config: {0}")]
    ObjectStoreParsing(#[from] influxdb3_clap_blocks::object_store::ParseError),

    #[error("Access of Object Store failed: {0}")]
    ObjectStore(#[from] object_store::Error),

    #[error("Tracing config error: {0}")]
    TracingConfig(#[from] trace_exporters::Error),

    #[error("Error initializing tokio runtime: {0}")]
    TokioRuntime(#[source] std::io::Error),

    #[error("Failed to bind address")]
    BindAddress(#[source] std::io::Error),

    #[error("Server error: {0}")]
    Server(#[from] influxdb3_server::Error),

    #[error("Write buffer error: {0}")]
    WriteBuffer(#[from] influxdb3_write::write_buffer::Error),

    #[error("invalid token: {0}")]
    InvalidToken(#[from] hex::FromHexError),

    #[error("failed to initialized write buffer: {0}")]
    WriteBufferInit(#[source] anyhow::Error),

    #[error("failed to initialize from persisted catalog: {0}")]
    InitializePersistedCatalog(#[source] influxdb3_write::persister::Error),

    #[error("Failed to execute job: {0}")]
    Job(#[source] executor::JobError),

    #[error("failed to initialize last cache: {0}")]
    InitializeLastCache(#[source] last_cache::Error),

    #[error("failed to initialize distinct cache: {0:#}")]
    InitializeDistinctCache(#[source] influxdb3_cache::distinct_cache::ProviderError),

    #[error("Error initializing compaction producer: {0}")]
    CompactionProducer(
        #[from] influxdb3_enterprise_compactor::producer::CompactedDataProducerError,
    ),

    #[error("Error initializing compaction consumer: {0}")]
    CompactionConsumer(#[from] anyhow::Error),

    #[error("Must have `compaction-hosts` specfied if running in compactor mode")]
    CompactorModeWithoutHosts,

    #[error("IO Error occurred: {0}")]
    Io(#[from] std::io::Error),

    #[error("Failed to make a request: {0}")]
    Reqwest(#[from] reqwest::Error),

    #[cfg(not(feature = "no_license"))]
    #[error("Failed to get a valid license. Please try again.")]
    LicenseTimeout,

    #[cfg(not(feature = "no_license"))]
    #[error("Failed to make a well formed request to get the license")]
    BadLicenseRequest,

    #[cfg(not(feature = "no_license"))]
    #[error("Failed to poll for license. Response code was {0}")]
    UnexpectedLicenseResponse(u16),

    #[cfg(not(feature = "no_license"))]
    #[error("No interactive TTY detected. Cannot prompt for email.")]
    NoTTY,

    #[cfg(not(feature = "no_license"))]
    #[error("Invalid email address")]
    InvalidEmail,
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

#[derive(Debug, clap::Parser)]
pub struct Config {
    /// object store options
    #[clap(flatten)]
    object_store_config: ObjectStoreConfig,

    /// logging options
    #[clap(flatten)]
    pub(crate) logging_config: LoggingConfig,

    /// tracing options
    #[clap(flatten)]
    pub(crate) tracing_config: TracingConfig,

    /// tokio datafusion config
    #[clap(flatten)]
    pub(crate) tokio_datafusion_config: TokioDatafusionConfig,

    /// iox_query extended DataFusion config
    #[clap(flatten)]
    pub(crate) iox_query_datafusion_config: IoxQueryDatafusionConfig,

    /// Maximum size of HTTP requests.
    #[clap(
    long = "max-http-request-size",
    env = "INFLUXDB3_MAX_HTTP_REQUEST_SIZE",
    default_value = "10485760", // 10 MiB
    action,
    )]
    pub max_http_request_size: usize,

    /// The address on which InfluxDB will serve HTTP API requests
    #[clap(
    long = "http-bind",
    env = "INFLUXDB3_HTTP_BIND_ADDR",
    default_value = DEFAULT_HTTP_BIND_ADDR,
    action,
    )]
    pub http_bind_address: SocketAddr,

    /// Size of the RAM cache used to store data in bytes.
    ///
    /// Can be given as absolute value or in percentage of the total available memory (e.g. `10%`).
    #[clap(
    long = "ram-pool-data-bytes",
    env = "INFLUXDB3_RAM_POOL_DATA_BYTES",
    default_value = "1073741824",  // 1GB
    action
    )]
    pub ram_pool_data_bytes: MemorySize,

    /// Size of memory pool used during query exec, in bytes.
    ///
    /// Can be given as absolute value or in percentage of the total available memory (e.g. `10%`).
    #[clap(
    long = "exec-mem-pool-bytes",
    env = "INFLUXDB3_EXEC_MEM_POOL_BYTES",
    default_value = "8589934592",  // 8GB
    action
    )]
    pub exec_mem_pool_bytes: MemorySize,

    /// bearer token to be set for requests
    #[clap(long = "bearer-token", env = "INFLUXDB3_BEARER_TOKEN", action)]
    pub bearer_token: Option<String>,

    /// Duration that the Parquet files get arranged into. The data timestamps will land each
    /// row into a file of this duration. 1m, 5m, and 10m are supported. These are known as
    /// "generation 1" files. The compactor in Pro can compact these into larger and longer
    /// generations.
    #[clap(
        long = "gen1-duration",
        env = "INFLUXDB3_GEN1_DURATION",
        default_value = "10m",
        action
    )]
    pub gen1_duration: Gen1Duration,

    /// Interval to flush buffered data to a wal file. Writes that wait for wal confirmation will
    /// take as long as this interval to complete.
    #[clap(
        long = "wal-flush-interval",
        env = "INFLUXDB3_WAL_FLUSH_INTERVAL",
        default_value = "1s",
        action
    )]
    pub wal_flush_interval: humantime::Duration,

    /// The number of WAL files to attempt to remove in a snapshot. This times the interval will
    /// determine how often snapshot is taken.
    #[clap(
        long = "wal-snapshot-size",
        env = "INFLUXDB3_WAL_SNAPSHOT_SIZE",
        default_value = "600",
        action
    )]
    pub wal_snapshot_size: usize,

    /// The maximum number of writes requests that can be buffered before a flush must be run
    /// and succeed.
    #[clap(
        long = "wal-max-write-buffer-size",
        env = "INFLUXDB3_WAL_MAX_WRITE_BUFFER_SIZE",
        default_value = "100000",
        action
    )]
    pub wal_max_write_buffer_size: usize,

    /// Number of snapshotted wal files to retain in object store, wal flush does not clear
    /// the wal files immediately instead they are only deleted when snapshotted and num wal files
    /// count exceeds this size
    #[clap(
        long = "snapshotted-wal-files-to-keep",
        env = "INFLUXDB3_NUM_WAL_FILES_TO_KEEP",
        default_value = "300",
        action
    )]
    pub snapshotted_wal_files_to_keep: u64,

    // TODO - tune this default:
    /// The size of the query log. Up to this many queries will remain in the log before
    /// old queries are evicted to make room for new ones.
    #[clap(
        long = "query-log-size",
        env = "INFLUXDB3_QUERY_LOG_SIZE",
        default_value = "1000",
        action
    )]
    pub query_log_size: usize,

    // TODO - make this default to 70% of available memory:
    /// The size limit of the buffered data. If this limit is passed a snapshot will be forced.
    #[clap(
        long = "buffer-mem-limit-mb",
        env = "INFLUXDB3_BUFFER_MEM_LIMIT_MB",
        default_value = "5000",
        action
    )]
    pub buffer_mem_limit_mb: usize,

    /// The host idendifier used as a prefix in all object store file paths. This should be unique
    /// for any hosts that share the same object store configuration, i.e., the same bucket.
    #[clap(long = "host-id", env = "INFLUXDB3_HOST_IDENTIFIER_PREFIX", action)]
    pub host_identifier_prefix: String,

    #[clap(flatten)]
    pub enterprise_config: influxdb3_enterprise_clap_blocks::serve::EnterpriseServeConfig,

    /// The size of the in-memory Parquet cache in megabytes (MB).
    #[clap(
        long = "parquet-mem-cache-size-mb",
        env = "INFLUXDB3_PARQUET_MEM_CACHE_SIZE_MB",
        default_value = "1000",
        action
    )]
    pub parquet_mem_cache_size: ParquetCacheSizeMb,

    /// The percentage of entries to prune during a prune operation on the in-memory Parquet cache.
    ///
    /// This must be a number between 0 and 1.
    #[clap(
        long = "parquet-mem-cache-prune-percentage",
        env = "INFLUXDB3_PARQUET_MEM_CACHE_PRUNE_PERCENTAGE",
        default_value = "0.1",
        action
    )]
    pub parquet_mem_cache_prune_percentage: ParquetCachePrunePercent,

    /// The interval on which to check if the in-memory Parquet cache needs to be pruned.
    ///
    /// Enter as a human-readable time, e.g., "1s", "100ms", "1m", etc.
    #[clap(
        long = "parquet-mem-cache-prune-interval",
        env = "INFLUXDB3_PARQUET_MEM_CACHE_PRUNE_INTERVAL",
        default_value = "1s",
        action
    )]
    pub parquet_mem_cache_prune_interval: humantime::Duration,

    /// Disable the in-memory Parquet cache. By default, the cache is enabled.
    #[clap(
        long = "disable-parquet-mem-cache",
        env = "INFLUXDB3_DISABLE_PARQUET_MEM_CACHE",
        default_value_t = false,
        action
    )]
    pub disable_parquet_mem_cache: bool,

    /// The interval on which to evict expired entries from the Last-N-Value cache, expressed as a
    /// human-readable time, e.g., "20s", "1m", "1h".
    #[clap(
        long = "last-cache-eviction-interval",
        env = "INFLUXDB3_LAST_CACHE_EVICTION_INTERVAL",
        default_value = "10s",
        action
    )]
    pub last_cache_eviction_interval: humantime::Duration,

    /// The interval on which to evict expired entries from the Distinct Value cache, expressed as a
    /// human-readable time, e.g., "20s", "1m", "1h".
    #[clap(
        long = "distinct-cache-eviction-interval",
        env = "INFLUXDB3_DISTINCT_CACHE_EVICTION_INTERVAL",
        default_value = "10s",
        action
    )]
    pub distinct_cache_eviction_interval: humantime::Duration,

    /// The local directory that has python plugins and their test files.
    #[clap(long = "plugin-dir", env = "INFLUXDB3_PLUGIN_DIR", action)]
    pub plugin_dir: Option<PathBuf>,

    /// Threshold for internal buffer, can be either percentage or absolute value.
    /// eg: 70% or 100000
    #[clap(
        long = "force-snapshot-mem-threshold",
        env = "INFLUXDB3_FORCE_SNAPSHOT_MEM_THRESHOLD",
        default_value = "70%",
        action
    )]
    pub force_snapshot_mem_threshold: MemorySize,
}

/// The interval to check for new snapshots from hosts to compact data from. This will do an S3
/// GET for every host in the replica list every interval.
const COMPACTION_CHECK_INTERVAL: Duration = Duration::from_secs(10);

/// Specified size of the Parquet cache in megabytes (MB)
#[derive(Debug, Clone, Copy)]
pub struct ParquetCacheSizeMb(usize);

impl ParquetCacheSizeMb {
    /// Express this cache size in terms of bytes (B)
    fn as_num_bytes(&self) -> usize {
        self.0 * 1_000 * 1_000
    }
}

impl FromStr for ParquetCacheSizeMb {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> std::prelude::v1::Result<Self, Self::Err> {
        s.parse()
            .context("failed to parse parquet cache size value as an unsigned integer")
            .map(Self)
    }
}

#[derive(Debug, Clone, Copy)]
pub struct ParquetCachePrunePercent(f64);

impl From<ParquetCachePrunePercent> for f64 {
    fn from(value: ParquetCachePrunePercent) -> Self {
        value.0
    }
}

impl FromStr for ParquetCachePrunePercent {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> std::prelude::v1::Result<Self, Self::Err> {
        let p = s
            .parse::<f64>()
            .context("failed to parse prune percent as f64")?;
        if p <= 0.0 || p >= 1.0 {
            bail!("prune percent must be between 0 and 1");
        }
        Ok(Self(p))
    }
}

/// If `p` does not exist, try to create it as a directory.
///
/// panic's if the directory does not exist and can not be created
#[allow(dead_code)]
fn ensure_directory_exists(p: &Path) {
    if !p.exists() {
        info!(
            p=%p.display(),
            "Creating directory",
        );
        std::fs::create_dir_all(p).expect("Could not create default directory");
    }
}

pub async fn command(config: Config) -> Result<()> {
    let startup_timer = Instant::now();
    let num_cpus = num_cpus::get();
    let build_malloc_conf = build_malloc_conf();
    info!(
        host_id = %config.host_identifier_prefix,
        mode = %config.enterprise_config.mode,
        git_hash = %INFLUXDB3_GIT_HASH as &str,
        version = %INFLUXDB3_VERSION.as_ref() as &str,
        uuid = %PROCESS_UUID.as_ref() as &str,
        num_cpus,
        "InfluxDB 3 Enterprise server starting",
    );
    debug!(%build_malloc_conf, "build configuration");

    let metrics = setup_metric_registry();

    // Install custom panic handler and forget about it.
    //
    // This leaks the handler and prevents it from ever being dropped during the
    // lifetime of the program - this is actually a good thing, as it prevents
    // the panic handler from being removed while unwinding a panic (which in
    // turn, causes a panic - see #548)
    let f = SendPanicsToTracing::new_with_metrics(&metrics);
    std::mem::forget(f);

    // Construct a token to trigger clean shutdown
    let frontend_shutdown = CancellationToken::new();

    let time_provider = Arc::new(SystemProvider::new());
    let sys_events_store = Arc::new(SysEventStore::new(Arc::clone(&time_provider) as _));
    let object_store: Arc<dyn ObjectStore> = config
        .object_store_config
        .make_object_store()
        .map_err(Error::ObjectStoreParsing)?;

    let (object_store, parquet_cache) = if !config.disable_parquet_mem_cache {
        let (object_store, parquet_cache) = create_cached_obj_store_and_oracle(
            object_store,
            Arc::clone(&time_provider) as _,
            Arc::clone(&metrics),
            config.parquet_mem_cache_size.as_num_bytes(),
            config.parquet_mem_cache_prune_percentage.into(),
            config.parquet_mem_cache_prune_interval.into(),
        );
        (object_store, Some(parquet_cache))
    } else {
        (object_store, None)
    };

    let trace_exporter = config.tracing_config.build()?;

    let parquet_store =
        ParquetStorage::new(Arc::clone(&object_store), StorageId::from("influxdb3"));

    let mut tokio_datafusion_config = config.tokio_datafusion_config;
    tokio_datafusion_config.num_threads = tokio_datafusion_config
        .num_threads
        .or_else(|| NonZeroUsize::new(num_cpus::get()))
        .or_else(|| NonZeroUsize::new(1));
    info!(
        num_threads = tokio_datafusion_config.num_threads.map(|n| n.get()),
        "Creating shared query executor"
    );

    let exec = Arc::new(Executor::new_with_config_and_executor(
        ExecutorConfig {
            target_query_partitions: tokio_datafusion_config.num_threads.unwrap(),
            object_stores: [&parquet_store]
                .into_iter()
                .map(|store| (store.id(), Arc::clone(store.object_store())))
                .collect(),
            metric_registry: Arc::clone(&metrics),
            mem_pool_size: config.exec_mem_pool_bytes.bytes(),
        },
        DedicatedExecutor::new(
            "datafusion",
            tokio_datafusion_config
                .builder()
                .map(|mut builder| {
                    builder.enable_all();
                    builder
                })
                .map_err(Error::TokioRuntime)?,
            Arc::clone(&metrics),
        ),
    ));
    let runtime_env = exec.new_context().inner().runtime_env();
    register_iox_object_store(runtime_env, parquet_store.id(), Arc::clone(&object_store));

    let trace_header_parser = TraceHeaderParser::new()
        .with_jaeger_trace_context_header_name(
            config
                .tracing_config
                .traces_jaeger_trace_context_header_name,
        )
        .with_jaeger_debug_name(config.tracing_config.traces_jaeger_debug_name);

    let persister = Arc::new(Persister::new(
        Arc::clone(&object_store),
        config.host_identifier_prefix.clone(),
    ));
    let wal_config = WalConfig {
        gen1_duration: config.gen1_duration,
        max_write_buffer_size: config.wal_max_write_buffer_size,
        flush_interval: config.wal_flush_interval.into(),
        snapshot_size: config.wal_snapshot_size,
    };

    let catalog = Arc::new(
        persister
            .load_or_create_catalog()
            .await
            .map_err(Error::InitializePersistedCatalog)?,
    );
    info!(instance_id = ?catalog.instance_id(), "catalog initialized");

    #[cfg(not(feature = "no_license"))]
    {
        load_and_validate_license(
            Arc::clone(&object_store),
            config.host_identifier_prefix.clone(),
            catalog.instance_id(),
        )
        .await?;
        info!("valid license found, happy data crunching");
    }

    let enterprise_config = match EnterpriseConfig::load(&object_store).await {
        Ok(config) => Arc::new(RwLock::new(config)),
        // If the config is not found we should create it
        Err(object_store::Error::NotFound { .. }) => {
            let config = EnterpriseConfig::default();
            config.persist(catalog.host_id(), &object_store).await?;
            Arc::new(RwLock::new(EnterpriseConfig::default()))
        }
        Err(err) => return Err(err.into()),
    };

    let parquet_cache_prefetcher = if let Some(parquet_cache) = parquet_cache.clone() {
        Some(ParquetCachePreFetcher::new(
            Arc::clone(&parquet_cache),
            config.enterprise_config.preemptive_cache_age,
            Arc::<SystemProvider>::clone(&time_provider),
        ))
    } else {
        None
    };

    let mut compacted_data: Option<Arc<CompactedData>> = None;
    let mut compaction_producer: Option<Arc<CompactedDataProducer>> = None;
    let compaction_event_store = Arc::clone(&sys_events_store) as Arc<dyn CompactionEventStore>;

    // The compactor disables cached parquet loader so it can stream row groups and does not
    // cache entire parquet files in memory at once. So, we override the inputted configuration
    // which defaults to using the cached loader:
    let mut compactor_datafusion_config = config.iox_query_datafusion_config.clone();
    compactor_datafusion_config.use_cached_parquet_loader = false;
    let compactor_datafusion_config = Arc::new(compactor_datafusion_config.build());

    if let Some(compactor_id) = config.enterprise_config.compactor_id {
        if config.enterprise_config.run_compactions {
            let compaction_config = CompactionConfig::new(
                &config.enterprise_config.compaction_multipliers.0,
                config.enterprise_config.compaction_gen2_duration.into(),
            )
            .with_per_file_row_limit(config.enterprise_config.compaction_row_limit)
            .with_max_num_files_per_compaction(
                config.enterprise_config.compaction_max_num_files_per_plan,
            );

            let hosts = if matches!(config.enterprise_config.mode, BufferMode::Compactor) {
                if let Some(compaction_hosts) = &config.enterprise_config.compaction_hosts {
                    compaction_hosts.to_vec()
                } else {
                    return Err(Error::CompactorModeWithoutHosts);
                }
            } else {
                let mut hosts = vec![config.host_identifier_prefix.clone()];
                if let Some(replicas) = &config.enterprise_config.replicas {
                    hosts.extend(replicas.iter().cloned());
                }
                hosts
            };

            let producer = CompactedDataProducer::new(CompactedDataProducerArgs {
                compactor_id,
                hosts,
                compaction_config,
                enterprise_config: Arc::clone(&enterprise_config),
                datafusion_config: compactor_datafusion_config,
                object_store: Arc::clone(&object_store),
                object_store_url: persister.object_store_url().clone(),
                executor: Arc::clone(&exec),
                parquet_cache_prefetcher: if config.enterprise_config.mode.is_compactor() {
                    None
                } else {
                    parquet_cache_prefetcher
                },
                sys_events_store: Arc::clone(&compaction_event_store),
            })
            .await?;

            compacted_data = Some(Arc::clone(&producer.compacted_data));
            compaction_producer = Some(Arc::new(producer));
        } else {
            let consumer = CompactedDataConsumer::new(
                compactor_id,
                Arc::clone(&object_store),
                parquet_cache_prefetcher,
                Arc::clone(&compaction_event_store),
            )
            .await
            .context("Error initializing compaction consumer")
            .map_err(Error::CompactionConsumer)?;

            compacted_data = Some(Arc::clone(&consumer.compacted_data));

            tokio::spawn(async move {
                consumer.poll_in_background(COMPACTION_CHECK_INTERVAL).await;
            });
        }
    }

    let datafusion_config = Arc::new(config.iox_query_datafusion_config.build());

    let time_provider = Arc::new(SystemProvider::new());

    let last_cache = LastCacheProvider::new_from_catalog_with_background_eviction(
        Arc::clone(&catalog),
        config.last_cache_eviction_interval.into(),
    )
    .map_err(Error::InitializeLastCache)?;

    let distinct_cache = DistinctCacheProvider::new_from_catalog_with_background_eviction(
        Arc::clone(&time_provider) as _,
        Arc::clone(&catalog),
        config.distinct_cache_eviction_interval.into(),
    )
    .map_err(Error::InitializeDistinctCache)?;

    let replica_config = config.enterprise_config.replicas.map(|replicas| {
        ReplicationConfig::new(
            config.enterprise_config.replication_interval.into(),
            replicas.into(),
        )
    });

    type CreateBufferModeResult = (
        Arc<dyn WriteBuffer>,
        Option<Arc<PersistedFiles>>,
        Option<Arc<WriteBufferImpl>>,
    );

    let (write_buffer, persisted_files, write_buffer_impl): CreateBufferModeResult =
        match config.enterprise_config.mode {
            BufferMode::Read => {
                let ReplicationConfig { interval, hosts } = replica_config
                    .context("must supply a replicas list when starting in read-only mode")
                    .map_err(Error::WriteBufferInit)?;
                (
                    Arc::new(
                        WriteBufferEnterprise::read(CreateReadModeArgs {
                            last_cache,
                            distinct_cache,
                            object_store: Arc::clone(&object_store),
                            catalog: Arc::clone(&catalog),
                            metric_registry: Arc::clone(&metrics),
                            replication_interval: interval,
                            hosts,
                            parquet_cache: parquet_cache.clone(),
                            compacted_data: compacted_data.clone(),
                            time_provider: Arc::<SystemProvider>::clone(&time_provider),
                        })
                        .await
                        .map_err(Error::WriteBufferInit)?,
                    ),
                    None,
                    None,
                )
            }
            BufferMode::ReadWrite => {
                let buf = Arc::new(
                    WriteBufferEnterprise::read_write(CreateReadWriteModeArgs {
                        host_id: persister.host_identifier_prefix().into(),
                        persister: Arc::clone(&persister),
                        catalog: Arc::clone(&catalog),
                        last_cache,
                        distinct_cache,
                        time_provider: Arc::<SystemProvider>::clone(&time_provider),
                        executor: Arc::clone(&exec),
                        wal_config,
                        metric_registry: Arc::clone(&metrics),
                        replication_config: replica_config,
                        parquet_cache: parquet_cache.clone(),
                        compacted_data: compacted_data.clone(),
                        snapshotted_wal_files_to_keep: config.snapshotted_wal_files_to_keep,
                    })
                    .await
                    .map_err(Error::WriteBufferInit)?,
                );
                let persisted_files = buf.persisted_files();
                let write_buffer_impl = buf.write_buffer_impl();
                (buf, Some(persisted_files), Some(write_buffer_impl))
            }
            BufferMode::Compactor => {
                let catalog = compacted_data
                    .as_ref()
                    .map(|cd| cd.compacted_catalog.catalog())
                    .expect("there was no compacted data initialized");
                let buf = Arc::new(WriteBufferEnterprise::compactor(catalog));
                (buf, None, None)
            }
        };

    if let Some(write_buffer_impl) = write_buffer_impl {
        info!("setting up background mem check for query buffer");
        background_buffer_checker(
            config.force_snapshot_mem_threshold.bytes(),
            &write_buffer_impl,
        )
        .await;
    }

    info!("setting up telemetry store");
    let telemetry_store = setup_telemetry_store(
        &config.object_store_config,
        catalog.instance_id(),
        num_cpus,
        persisted_files,
        DEFAULT_TELMETRY_ENDPOINT,
    )
    .await;

    let common_state = CommonServerState::new(
        Arc::clone(&metrics),
        trace_exporter,
        trace_header_parser,
        Arc::clone(&telemetry_store),
        Arc::clone(&enterprise_config),
        Arc::clone(&object_store),
        config.plugin_dir,
    )?;

    let sys_table_compacted_data: Option<Arc<dyn CompactedDataSystemTableView>> =
        if let Some(ref compacted_data) = compacted_data {
            Some(Arc::clone(compacted_data) as Arc<dyn CompactedDataSystemTableView>)
        } else {
            None
        };

    let query_executor: Arc<dyn QueryExecutor> = match config.enterprise_config.mode {
        BufferMode::Compactor => Arc::new(CompactionSysTableQueryExecutorImpl::new(
            CompactionSysTableQueryExecutorArgs {
                exec: Arc::clone(&exec),
                metrics: Arc::clone(&metrics),
                datafusion_config,
                query_log_size: config.query_log_size,
                telemetry_store: Arc::clone(&telemetry_store),
                sys_events_store: Arc::clone(&sys_events_store),
                compacted_data: sys_table_compacted_data,
            },
        )),
        _ => Arc::new(QueryExecutorImpl::new(CreateQueryExecutorArgs {
            catalog: write_buffer.catalog(),
            write_buffer: Arc::clone(&write_buffer),
            exec: Arc::clone(&exec),
            metrics: Arc::clone(&metrics),
            datafusion_config,
            query_log_size: config.query_log_size,
            telemetry_store: Arc::clone(&telemetry_store),
            compacted_data: sys_table_compacted_data,
            enterprise_config: Arc::clone(&enterprise_config),
            sys_events_store: Arc::clone(&sys_events_store),
        })),
    };

    let listener = TcpListener::bind(*config.http_bind_address)
        .await
        .map_err(Error::BindAddress)?;

    let builder = ServerBuilder::new(common_state)
        .max_request_size(config.max_http_request_size)
        .write_buffer(Arc::clone(&write_buffer))
        .query_executor(query_executor)
        .time_provider(Arc::clone(&time_provider))
        .persister(Arc::clone(&persister))
        .tcp_listener(listener);

    let server = if let Some(token) = config.bearer_token.map(hex::decode).transpose()? {
        builder
            .authorizer(Arc::new(AllOrNothingAuthorizer::new(token)))
            .build()
    } else {
        builder.build()
    };

    let mut futures = Vec::new();

    if let Some(compactor) = compaction_producer {
        // Run the compactor code on the DataFusion executor

        // Note that unlike tokio::spawn, if the handle to the task is
        // dropped, the task is cancelled, so it must be retained until the end
        debug!("Setting up the compaction loop");
        let t = exec
            .executor()
            .spawn(async move {
                compactor.run_compaction_loop(Duration::from_secs(10)).await;
            })
            .map_err(Error::Job);

        futures.push(t.boxed());
    }

    futures.push(
        serve(server, frontend_shutdown, startup_timer)
            .map_err(Error::from)
            .boxed(),
    );

    // Wait for all futures to complete, and if any failed return the first error
    let results = join_all(futures).await;
    for result in results {
        result?;
    }

    Ok(())
}

async fn setup_telemetry_store(
    object_store_config: &ObjectStoreConfig,
    instance_id: Arc<str>,
    num_cpus: usize,
    persisted_files: Option<Arc<PersistedFiles>>,
    telemetry_endpoint: &'static str,
) -> Arc<TelemetryStore> {
    let os = std::env::consts::OS;
    let influxdb_pkg_version = env!("CARGO_PKG_VERSION");
    let influxdb_pkg_name = env!("CARGO_PKG_NAME");
    // Following should show influxdb3-0.1.0
    let influx_version = format!("{}-{}", influxdb_pkg_name, influxdb_pkg_version);
    let obj_store_type = object_store_config
        .object_store
        .unwrap_or(ObjectStoreType::Memory);
    let storage_type = obj_store_type.as_str();

    TelemetryStore::new(
        instance_id,
        Arc::from(os),
        Arc::from(influx_version),
        Arc::from(storage_type),
        num_cpus,
        persisted_files.map(|p| p as _),
        telemetry_endpoint,
    )
    .await
}

async fn background_buffer_checker(
    mem_threshold_bytes: usize,
    write_buffer_impl: &Arc<WriteBufferImpl>,
) {
    debug!(mem_threshold_bytes, "setting up background buffer checker");
    check_mem_and_force_snapshot_loop(
        Arc::clone(write_buffer_impl),
        mem_threshold_bytes,
        Duration::from_secs(10),
    )
    .await;
}

#[cfg(not(feature = "no_license"))]
async fn load_and_validate_license(
    object_store: Arc<dyn ObjectStore>,
    host_id: String,
    instance_id: Arc<str>,
) -> Result<()> {
    let license_path: ObjPath = format!("{host_id}/license").into();
    let license = match object_store.get(&license_path).await {
        Ok(get_result) => get_result.bytes().await?,
        // The license does not exist so we need to create one
        Err(object_store::Error::NotFound { .. }) => {
            // Check for a TTY / stdin to prompt the user for their email
            if !std::io::stdin().is_terminal() {
                return Err(Error::NoTTY);
            }

            println!(
                "\nWelcome to InfluxDB 3 Enterprise\n\
                 No license file was detected. Please enter your email: \
            "
            );
            let mut email = String::new();
            let stdin = std::io::stdin();
            stdin.read_line(&mut email)?;
            let email = url::form_urlencoded::byte_serialize(email.trim().as_bytes())
                .map(ToString::to_string)
                .collect::<String>();

            if email.is_empty() {
                return Err(Error::InvalidEmail);
            }

            let client = reqwest::Client::new();
            let resp = client
                .post(format!("https://licenses.enterprise.influxdata.com/licenses?email={email}&instance-id={instance_id}&host-id={host_id}"))
            .send()
            .await?;

            if resp.status() == 400 {
                return Err(Error::BadLicenseRequest);
            }

            //TODO: Handle url for local vs actual production code
            let poll_url = format!(
                "https://licenses.enterprise.influxdata.com{}",
                resp.headers()
                    .get("Location")
                    .expect("Location header to be present")
                    .to_str()
                    .expect("Location header to be a valid utf-8 string")
            );

            println!("Email sent. Please check your inbox to verify your email address and proceed.\nWaiting for verification...");
            let start = Instant::now();
            loop {
                // If 20 minutes have passed timeout and return an error
                let duration = start.duration_since(Instant::now()).as_secs();
                if duration > (20 * 60) {
                    return Err(Error::LicenseTimeout);
                }

                let resp = client.get(&poll_url).send().await?;
                match resp.status().as_u16() {
                    404 => {
                        debug!("Polling license service again in 5 seconds");
                        tokio::time::sleep(Duration::from_secs(5)).await;
                        continue;
                    }
                    200 => {
                        let license = resp.bytes().await?;
                        object_store
                            .put(&license_path, PutPayload::from_bytes(license.clone()))
                            .await?;
                        break license;
                    }
                    400 => return Err(Error::BadLicenseRequest),
                    code => return Err(Error::UnexpectedLicenseResponse(code)),
                }
            }
        }
        Err(e) => return Err(e.into()),
    };

    let license = std::str::from_utf8(&license).expect("License file is valid utf-8");
    let kid = decode_header(license)
        .expect("License file is a valid JWT")
        .kid
        .expect("The kid field exists in the JWT Header");
    let key = SIGNING_KEYS[&kid];

    let claims = decode::<Claims>(
        license,
        &DecodingKey::from_ec_pem(key).expect("The signing keys are in ec pem format"),
        &Validation::new(Algorithm::ES256),
    )
    .expect("The license JWT could be decoded")
    .claims;

    if SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("Now is greater than the Epoch")
        .as_secs()
        > claims.license_exp
    {
        error!("License is expired please acquire a new one. Queries will be disabled");
        influxdb3_server::EXPIRED_LICENSE.store(true, std::sync::atomic::Ordering::Relaxed);
    } else if host_id != claims.host_id {
        eprintln!("Invalid host_id for license");
        std::process::exit(1);
    }

    async fn recurring_license_validation_check(
        mut claims: Claims,
        host_id: String,
        license_path: ObjPath,
        object_store: Arc<dyn ObjectStore>,
    ) {
        loop {
            // Check license once a day if not expired otherwise check every minute
            if EXPIRED_LICENSE.load(Ordering::Relaxed) {
                tokio::time::sleep(Duration::from_secs(60)).await;
                error!("License is expired please acquire a new one. Queries will be disabled. Will check for valid license every minute");
                let Ok(result) = object_store.get(&license_path).await else {
                    continue;
                };
                let Ok(license) = result.bytes().await else {
                    continue;
                };
                let license = std::str::from_utf8(&license).expect("License file is valid utf-8");
                let kid = decode_header(license)
                    .expect("License file is a valid JWT")
                    .kid
                    .expect("The kid field exists in the JWT Header");
                let key = SIGNING_KEYS[&kid];

                claims = decode::<Claims>(
                    license,
                    &DecodingKey::from_ec_pem(key).expect("The signing keys are in ec pem format"),
                    &Validation::new(Algorithm::ES256),
                )
                .expect("The license JWT could be decoded")
                .claims;
            } else {
                tokio::time::sleep(Duration::from_secs(60 * 60 * 24)).await;
            }

            if SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("Now is greater than the Epoch")
                .as_secs()
                > claims.license_exp
            {
                EXPIRED_LICENSE.store(true, Ordering::Relaxed);
            } else if host_id != claims.host_id {
                error!("Invalid host_id for license. Aborting process.");
                std::process::exit(1);
            } else {
                EXPIRED_LICENSE.store(false, Ordering::Relaxed);
            }
        }
    }
    tokio::task::spawn(recurring_license_validation_check(
        claims,
        host_id,
        license_path,
        object_store,
    ));

    Ok(())
}

#[cfg(not(feature = "no_license"))]
#[derive(Debug, Serialize, Deserialize)]
struct Claims {
    email: String,
    exp: u64,
    host_id: String,
    iat: u64,
    instance_id: String,
    iss: String,
    license_exp: u64,
}

#[cfg(not(feature = "no_license"))]
static SIGNING_KEYS: phf::Map<&'static str, &[u8]> = phf::phf_map! {
    "gcloud-kms_global_clustered-licensing_signing-key-2_v1.pem" => include_bytes!(
        "../../../influxdb3_license/service/keyring/keys/gcloud-kms_global_clustered-licensing_signing-key-2_v1.pem"
    ),
     "gcloud-kms_global_clustered-licensing_signing-key-3_v1.pem" => include_bytes!(
         "../../../influxdb3_license/service/keyring/keys/gcloud-kms_global_clustered-licensing_signing-key-3_v1.pem"
     ),
     "gcloud-kms_global_clustered-licensing_signing-key-4_v1.pem" => include_bytes!(
         "../../../influxdb3_license/service/keyring/keys/gcloud-kms_global_clustered-licensing_signing-key-4_v1.pem"
     ),
     "gcloud-kms_global_clustered-licensing_signing-key_v1.pem" => include_bytes!(
         "../../../influxdb3_license/service/keyring/keys/gcloud-kms_global_clustered-licensing_signing-key_v1.pem"
     ),
     "gcloud-kms_global_pro-licensing_signing-key-1_v1.pem" => include_bytes!(
         "../../../influxdb3_license/service/keyring/keys/gcloud-kms_global_pro-licensing_signing-key-1_v1.pem"
     ),
     "gcloud-kms_global_pro-licensing_signing-key-2_v1.pem" => include_bytes!(
         "../../../influxdb3_license/service/keyring/keys/gcloud-kms_global_pro-licensing_signing-key-2_v1.pem"
     ),
     "gcloud-kms_global_pro-licensing_signing-key-3_v1.pem" => include_bytes!(
         "../../../influxdb3_license/service/keyring/keys/gcloud-kms_global_pro-licensing_signing-key-3_v1.pem"
     ),
     "gcloud-kms_global_test-key_test-key_v1.pem" => include_bytes!(
         "../../../influxdb3_license/service/keyring/keys/gcloud-kms_global_test-key_test-key_v1.pem"
     ),
     "influxdb-clustered-license-server_self-managed_public_20240318_1.pem" => include_bytes!(
         "../../../influxdb3_license/service/keyring/keys/influxdb-clustered-license-server_self-managed_public_20240318_1.pem"
     ),
     "influxdb-clustered-license-server_self-managed_public_20240318_2.pem" => include_bytes!(
         "../../../influxdb3_license/service/keyring/keys/influxdb-clustered-license-server_self-managed_public_20240318_2.pem"
     ),
     "influxdb-clustered-license-server_self-managed_public_20240318_3.pem" => include_bytes!(
         "../../../influxdb3_license/service/keyring/keys/influxdb-clustered-license-server_self-managed_public_20240318_3.pem"
     ),
     "influxdb-clustered-license-server_self-managed_public_20240318_4.pem" => include_bytes!(
         "../../../influxdb3_license/service/keyring/keys/influxdb-clustered-license-server_self-managed_public_20240318_4.pem"
     ),
     "self-managed_test_private-key.pem" => include_bytes!(
         "../../../influxdb3_license/service/keyring/keys/self-managed_test_private-key.pem"
     ),
     "self-managed_test_public-key.pem" => include_bytes!(
         "../../../influxdb3_license/service/keyring/keys/self-managed_test_public-key.pem"
     ),
};
