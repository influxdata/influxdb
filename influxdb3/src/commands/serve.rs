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
use influxdb3_clap_blocks::plugins::{PackageManager, ProcessingEngineConfig};
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
use influxdb3_processing_engine::environment::{
    DisabledManager, PipManager, PythonEnvironmentManager, UVManager,
};
use influxdb3_processing_engine::plugins::ProcessingEngineEnvironmentManager;
use influxdb3_processing_engine::ProcessingEngineManagerImpl;
use influxdb3_server::query_executor::enterprise::QueryExecutorEnterprise;
use influxdb3_server::{
    auth::AllOrNothingAuthorizer,
    builder::ServerBuilder,
    query_executor::{
        enterprise::{CompactionSysTableQueryExecutorArgs, CompactionSysTableQueryExecutorImpl},
        CreateQueryExecutorArgs,
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
use iox_time::{SystemProvider, TimeProvider};
use object_store::ObjectStore;
use observability_deps::tracing::*;
use panic_logging::SendPanicsToTracing;
use parquet_file::storage::{ParquetStorage, StorageId};
use std::env;
use std::process::Command;
use std::{num::NonZeroUsize, sync::Arc, time::Duration};
use std::{
    path::{Path, PathBuf},
    str::FromStr,
};
use thiserror::Error;
use tokio::net::TcpListener;
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

use crate::commands::common::warn_use_of_deprecated_env_vars;

/// The default name of the influxdb data directory
#[allow(dead_code)]
pub const DEFAULT_DATA_DIRECTORY_NAME: &str = ".influxdb3";

/// The default bind address for the HTTP API.
pub const DEFAULT_HTTP_BIND_ADDR: &str = "0.0.0.0:8181";

pub const DEFAULT_TELEMETRY_ENDPOINT: &str = "https://telemetry.v3.influxdata.com";

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

    #[error("failed to initialized write buffer: {0:?}")]
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

    #[error("Error initializing compaction consumer: {0:?}")]
    CompactionConsumer(#[from] anyhow::Error),

    #[error("Must have `compact-from-node-ids` specfied if running in compactor mode")]
    CompactorModeWithoutNodeIds,

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
    #[error("Requested license not available.")]
    LicenseNotFound,

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

// variable name and migration message tuples
const DEPRECATED_ENV_VARS: &[(&str, &str)] = &[(
    "INFLUXDB3_PARQUET_MEM_CACHE_SIZE_MB",
    "use INFLUXDB3_PARQUET_MEM_CACHE_SIZE instead, it is in MB or %",
)];

/// Try to keep all the memory size in MB instead of raw bytes, also allow
/// them to be configured as a percentage of total memory using MemorySizeMb
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

    /// Size of memory pool used during query exec, in megabytes.
    ///
    /// Can be given as absolute value or in percentage of the total available memory (e.g. `10%`).
    #[clap(
        long = "exec-mem-pool-bytes",
        env = "INFLUXDB3_EXEC_MEM_POOL_BYTES",
        default_value = "20%",
        action
    )]
    pub exec_mem_pool_bytes: MemorySizeMb,

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

    /// The node idendifier used as a prefix in all object store file paths. This should be unique
    /// for any InfluxDB 3 Core servers that share the same object store configuration, i.e., the
    /// same bucket.
    #[clap(
        long = "node-id",
        // TODO: deprecate this alias in future version
        alias = "host-id",
        env = "INFLUXDB3_NODE_IDENTIFIER_PREFIX",
        action
    )]
    pub node_identifier_prefix: String,

    #[clap(flatten)]
    pub enterprise_config: influxdb3_enterprise_clap_blocks::serve::EnterpriseServeConfig,

    /// The size of the in-memory Parquet cache in megabytes or percentage of total available mem.
    /// breaking: removed parquet-mem-cache-size-mb and env var INFLUXDB3_PARQUET_MEM_CACHE_SIZE_MB
    #[clap(
        long = "parquet-mem-cache-size",
        env = "INFLUXDB3_PARQUET_MEM_CACHE_SIZE",
        default_value = "20%",
        action
    )]
    pub parquet_mem_cache_size: MemorySizeMb,

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

    /// The duration from `now` to check if parquet files pulled in query path requires caching
    /// Enter as a human-readable time, e.g., "5h", "3d"
    #[clap(
        long = "parquet-mem-cache-query-path-duration",
        env = "INFLUXDB3_PARQUET_MEM_CACHE_QUERY_PATH_DURATION",
        default_value = "5h",
        action
    )]
    pub parquet_mem_cache_query_path_duration: humantime::Duration,

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

    /// The processing engine config.
    #[clap(flatten)]
    pub processing_engine_config: ProcessingEngineConfig,

    /// Threshold for internal buffer, can be either percentage or absolute value in MB.
    /// eg: 70% or 1000 MB
    #[clap(
        long = "force-snapshot-mem-threshold",
        env = "INFLUXDB3_FORCE_SNAPSHOT_MEM_THRESHOLD",
        default_value = "50%",
        action
    )]
    pub force_snapshot_mem_threshold: MemorySizeMb,

    /// Disable sending telemetry data to telemetry.v3.influxdata.com.
    #[clap(
        long = "disable-telemetry-upload",
        env = "INFLUXDB3_TELEMETRY_DISABLE_UPLOAD",
        default_value_t = false,
        hide = true,
        action
    )]
    pub disable_telemetry_upload: bool,

    /// Send telemetry data to the specified endpoint.
    #[clap(
        long = "telemetry-endpoint",
        env = "INFLUXDB3_TELEMETRY_ENDPOINT",
        default_value = DEFAULT_TELEMETRY_ENDPOINT,
        hide = true,
        action
    )]
    pub telemetry_endpoint: String,

    /// Set the limit for number of parquet files allowed in a query. Defaults
    /// to 432 which is about 3 days worth of files using default settings.
    /// This number can be increased to allow more files to be queried, but
    /// query performance will likely suffer, RAM usage will spike, and the
    /// process might be OOM killed as a result. It would be better to specify
    /// smaller time ranges if possible in a query.
    #[clap(long = "query-file-limit", env = "INFLUXDB3_QUERY_FILE_LIMIT", action)]
    pub query_file_limit: Option<usize>,
}

/// The interval to check for new snapshots from writers to compact data from. This will do an S3
/// GET for every writer in the replica list every interval.
const COMPACTION_CHECK_INTERVAL: Duration = Duration::from_secs(10);

/// Specified size of the Parquet cache in megabytes (MB)
#[derive(Debug, Clone, Copy)]
pub struct MemorySizeMb(usize);

impl MemorySizeMb {
    /// Express this cache size in terms of bytes (B)
    fn as_num_bytes(&self) -> usize {
        self.0
    }
}

impl FromStr for MemorySizeMb {
    type Err = String;

    fn from_str(s: &str) -> std::prelude::v1::Result<Self, Self::Err> {
        let num_bytes = if s.contains("%") {
            let mem_size = MemorySize::from_str(s)?;
            mem_size.bytes()
        } else {
            let num_mb = usize::from_str(s)
                .map_err(|_| "failed to parse value as unsigned integer".to_string())?;
            num_mb * 1000 * 1000
        };
        Ok(Self(num_bytes))
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
        node_id = %config.node_identifier_prefix,
        mode = %config.enterprise_config.mode,
        git_hash = %INFLUXDB3_GIT_HASH as &str,
        version = %INFLUXDB3_VERSION.as_ref() as &str,
        uuid = %PROCESS_UUID.as_ref() as &str,
        num_cpus,
        "InfluxDB 3 Enterprise server starting",
    );
    debug!(%build_malloc_conf, "build configuration");

    // check if any env vars that are deprecated is still being passed around and warn
    warn_use_of_deprecated_env_vars(DEPRECATED_ENV_VARS);

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

    let time_provider: Arc<dyn TimeProvider> = Arc::new(SystemProvider::new());
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
            config.parquet_mem_cache_query_path_duration.into(),
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
            mem_pool_size: config.exec_mem_pool_bytes.as_num_bytes(),
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
        config.node_identifier_prefix.clone(),
        Arc::clone(&time_provider) as _,
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
            config.node_identifier_prefix.clone(),
            catalog.instance_id(),
            config.enterprise_config.license_email,
        )
        .await?;
        info!("valid license found, happy data crunching");
    }

    let enterprise_config = match EnterpriseConfig::load(&object_store).await {
        Ok(config) => Arc::new(config),
        // If the config is not found we should create it
        Err(object_store::Error::NotFound { .. }) => {
            let config = EnterpriseConfig::default();
            config.persist(catalog.node_id(), &object_store).await?;
            Arc::new(config)
        }
        Err(err) => return Err(err.into()),
    };

    let parquet_cache_prefetcher = if let Some(parquet_cache) = parquet_cache.clone() {
        Some(Arc::new(ParquetCachePreFetcher::new(
            Arc::clone(&parquet_cache),
            config.enterprise_config.preemptive_cache_age,
            Arc::clone(&time_provider),
        )))
    } else {
        None
    };

    let mut compacted_data: Option<Arc<CompactedData>> = None;
    let mut compaction_producer: Option<CompactedDataProducer> = None;
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

            let node_ids = if matches!(config.enterprise_config.mode, BufferMode::Compactor) {
                if let Some(compact_from_node_ids) = &config.enterprise_config.compact_from_node_ids
                {
                    compact_from_node_ids.to_vec()
                } else {
                    return Err(Error::CompactorModeWithoutNodeIds);
                }
            } else {
                let mut node_ids = vec![config.node_identifier_prefix.clone()];
                if let Some(read_from_node_ids) = &config.enterprise_config.read_from_node_ids {
                    node_ids.extend(read_from_node_ids.iter().cloned());
                }
                node_ids
            };

            let producer = CompactedDataProducer::new(CompactedDataProducerArgs {
                compactor_id,
                node_ids,
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
                time_provider: Arc::clone(&time_provider),
            })
            .await?;

            compacted_data = Some(Arc::clone(&producer.compacted_data));
            compaction_producer = Some(producer);
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

    let replica_config = config.enterprise_config.read_from_node_ids.map(|node_ids| {
        ReplicationConfig::new(
            config.enterprise_config.replication_interval.into(),
            node_ids.into(),
        )
    });

    type CreateBufferModeResult = (
        Arc<dyn WriteBuffer>,
        Option<Arc<PersistedFiles>>,
        Option<Arc<WriteBufferImpl>>,
    );

    let (write_buffer, persisted_files, write_buffer_impl): CreateBufferModeResult = match config
        .enterprise_config
        .mode
    {
        BufferMode::Read => {
            let ReplicationConfig { interval, node_ids } = replica_config
                .context("must supply a read-from-node-ids list when starting in read-only mode")
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
                        node_ids,
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
                    node_id: persister.node_identifier_prefix().into(),
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
            config.force_snapshot_mem_threshold.as_num_bytes(),
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
        config.telemetry_endpoint.as_str(),
        config.disable_telemetry_upload,
    )
    .await;

    let common_state = CommonServerState::new(
        Arc::clone(&metrics),
        trace_exporter,
        trace_header_parser,
        Arc::clone(&telemetry_store),
        Arc::clone(&enterprise_config),
        Arc::clone(&object_store),
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
        _ => Arc::new(QueryExecutorEnterprise::new(
            CreateQueryExecutorArgs {
                catalog: write_buffer.catalog(),
                write_buffer: Arc::clone(&write_buffer),
                exec: Arc::clone(&exec),
                metrics: Arc::clone(&metrics),
                datafusion_config,
                query_log_size: config.query_log_size,
                telemetry_store: Arc::clone(&telemetry_store),
                sys_events_store: Arc::clone(&sys_events_store),
            },
            sys_table_compacted_data,
            Arc::clone(&enterprise_config),
        )),
    };

    let listener = TcpListener::bind(*config.http_bind_address)
        .await
        .map_err(Error::BindAddress)?;

    let processing_engine = ProcessingEngineManagerImpl::new(
        setup_processing_engine_env_manager(&config.processing_engine_config),
        write_buffer.catalog(),
        Arc::clone(&write_buffer),
        Arc::clone(&query_executor) as _,
        Arc::clone(&time_provider) as _,
        write_buffer.wal(),
        sys_events_store,
    );

    let builder = ServerBuilder::new(common_state)
        .max_request_size(config.max_http_request_size)
        .write_buffer(Arc::clone(&write_buffer))
        .query_executor(query_executor)
        .time_provider(Arc::clone(&time_provider))
        .persister(Arc::clone(&persister))
        .tcp_listener(listener)
        .processing_engine(processing_engine);

    let server = if let Some(token) = config.bearer_token.map(hex::decode).transpose()? {
        builder
            .authorizer(Arc::new(AllOrNothingAuthorizer::new(token)))
            .build()
            .await
    } else {
        builder.build().await
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
                compactor
                    .run_compaction_loop(Duration::from_secs(10), Duration::from_secs(600))
                    .await;
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

pub(crate) fn setup_processing_engine_env_manager(
    config: &ProcessingEngineConfig,
) -> ProcessingEngineEnvironmentManager {
    let package_manager: Arc<dyn PythonEnvironmentManager> = match config.package_manager {
        PackageManager::Discover => determine_package_manager(),
        PackageManager::Pip => Arc::new(PipManager),
        PackageManager::UV => Arc::new(UVManager),
    };
    ProcessingEngineEnvironmentManager {
        plugin_dir: config.plugin_dir.clone(),
        virtual_env_location: config.virtual_env_location.clone(),
        package_manager,
    }
}

fn determine_package_manager() -> Arc<dyn PythonEnvironmentManager> {
    // Check for pip (highest preference)
    // XXX: put this somewhere common
    let python_exe_bn = if cfg!(windows) {
        "python.exe"
    } else {
        "python3"
    };
    let python_exe = if let Ok(v) = env::var("PYTHONHOME") {
        // honor PYTHONHOME (set earlier for python standalone). python build
        // standalone has bin/python3 on OSX/Linux and python.exe on Windows
        let mut path = PathBuf::from(v);
        if !cfg!(windows) {
            path.push("bin");
        }
        path.push(python_exe_bn);
        path
    } else {
        PathBuf::from(python_exe_bn)
    };

    if let Ok(output) = Command::new(python_exe)
        .args(["-m", "pip", "--version"])
        .output()
    {
        if output.status.success() {
            return Arc::new(PipManager);
        }
    }

    // Check for uv second (ie, prefer python standalone pip)
    if let Ok(output) = Command::new("uv").arg("--version").output() {
        if output.status.success() {
            return Arc::new(UVManager);
        }
    }

    // If neither is available, return DisabledManager
    Arc::new(DisabledManager)
}

async fn setup_telemetry_store(
    object_store_config: &ObjectStoreConfig,
    instance_id: Arc<str>,
    num_cpus: usize,
    persisted_files: Option<Arc<PersistedFiles>>,
    telemetry_endpoint: &str,
    disable_upload: bool,
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

    if disable_upload {
        debug!("Initializing TelemetryStore with upload disabled.");
        TelemetryStore::new_without_background_runners(persisted_files.map(|p| p as _))
    } else {
        debug!("Initializing TelemetryStore with upload enabled for {telemetry_endpoint}.");
        TelemetryStore::new(
            instance_id,
            Arc::from(os),
            Arc::from(influx_version),
            Arc::from(storage_type),
            num_cpus,
            persisted_files.map(|p| p as _),
            telemetry_endpoint.to_string(),
        )
        .await
    }
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
    node_id: String,
    instance_id: Arc<str>,
    license_email: Option<String>,
) -> Result<()> {
    let license_path: ObjPath = format!("{node_id}/license").into();
    let license = match object_store.get(&license_path).await {
        Ok(get_result) => get_result.bytes().await?,
        // The license does not exist so we need to create one
        Err(object_store::Error::NotFound { .. }) => {
            // Check for a TTY / stdin to prompt the user for their email

            if !std::io::stdin().is_terminal() && license_email.is_none() {
                return Err(Error::NoTTY);
            }

            println!("\nWelcome to InfluxDB 3 Enterprise\n");

            let (encoded_email, display_email) = license_email
                .clone()
                .map(Ok)
                .unwrap_or_else(|| -> Result<String> {
                    println!("No license file was detected. Please enter your email: ");
                    let mut email = String::new();
                    let stdin = std::io::stdin();
                    stdin.read_line(&mut email)?;
                    Ok(email)
                })
                .map(|s| {
                    (
                        url::form_urlencoded::byte_serialize(s.trim().as_bytes())
                            .collect::<String>(),
                        s,
                    )
                })?;

            if encoded_email.is_empty() {
                return Err(Error::InvalidEmail);
            }

            // first check if license exists on in the license server
            match get_license(&encoded_email, &instance_id).await {
                Ok(l) => l,
                // if license server reports 404 Not Found, then initiate onboarding process
                Err(Error::LicenseNotFound) => {
                    debug!("license not found on server, initiating onboarding process");
                    license_onboarding(
                        &object_store,
                        &node_id,
                        instance_id,
                        &display_email,
                        &encoded_email,
                        &license_path,
                    )
                    .await?
                }
                // anything else is an error (eg Bad Request, Internal Server Error, etc) that needs to be reported to the user
                Err(e) => return Err(e),
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
    } else if node_id != claims.node_id {
        eprintln!("Invalid node_id for license");
        std::process::exit(1);
    }

    async fn recurring_license_validation_check(
        mut claims: Claims,
        node_id: String,
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
            } else if node_id != claims.node_id {
                error!("Invalid node_id for license. Aborting process.");
                std::process::exit(1);
            } else {
                EXPIRED_LICENSE.store(false, Ordering::Relaxed);
            }
        }
    }
    tokio::task::spawn(recurring_license_validation_check(
        claims,
        node_id,
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
    #[serde(alias = "writer_id")]
    node_id: String,
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

#[cfg(not(feature = "no_license"))]
const LICENSE_SERVER_URL: &str = if cfg!(feature = "local_dev") {
    if let Some(url) = option_env!("LICENSE_SERVER_URL") {
        url
    } else {
        "http://localhost:8687"
    }
} else {
    "https://licenses.enterprise.influxdata.com"
};

#[cfg(not(feature = "no_license"))]
async fn license_onboarding(
    object_store: &Arc<dyn ObjectStore>,
    node_id: &str,
    instance_id: Arc<str>,
    display_email: &str,
    encoded_email: &str,
    license_path: &ObjPath,
) -> Result<bytes::Bytes> {
    let client = reqwest::Client::new();
    let resp = client
                .post(format!("{LICENSE_SERVER_URL}/licenses?email={encoded_email}&instance-id={instance_id}&node-id={node_id}"))
            .send()
            .await?;

    match resp.status().as_u16() {
        // * If this is the first time the user is attmpting to create a license without verification
        // we should see a 201 response.
        // * If the user has been verified and they haven't created a license for this
        // email/instance_id combo then they will probably successfully create a license and see a
        // 201 response.
        201 => {}
        // * If this is the second time without verification, they should should see a 202
        // response.
        202 => {}
        400 => return Err(Error::BadLicenseRequest),
        i => return Err(Error::UnexpectedLicenseResponse(i)),
    }

    //TODO: Handle url for local vs actual production code
    let poll_url = format!(
        "{LICENSE_SERVER_URL}{}",
        resp.headers()
            .get("Location")
            .expect("Location header to be present")
            .to_str()
            .expect("Location header to be a valid utf-8 string")
    );

    println!("Email sent to {display_email}. Please check your inbox to verify your email address and proceed.");
    println!("Waiting for verification...");
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
                    .put(license_path, PutPayload::from_bytes(license.clone()))
                    .await?;
                break Ok(license);
            }
            400 => return Err(Error::BadLicenseRequest),
            code => return Err(Error::UnexpectedLicenseResponse(code)),
        }
    }
}

#[cfg(not(feature = "no_license"))]
async fn get_license(encoded_email: &str, instance_id: &str) -> Result<bytes::Bytes> {
    //TODO: Handle url for local vs actual production code
    let get_url =
        format!("{LICENSE_SERVER_URL}/licenses?email={encoded_email}&instance-id={instance_id}");

    let client = reqwest::Client::new();
    let resp = client.get(get_url).send().await?;

    debug!("getting license from server");
    match resp.status().as_u16() {
        200 => Ok(resp.bytes().await?),
        404 => Err(Error::LicenseNotFound),
        400 => Err(Error::BadLicenseRequest),
        i => Err(Error::UnexpectedLicenseResponse(i)),
    }
}
