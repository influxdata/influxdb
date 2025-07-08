//! Entrypoint for InfluxDB 3 Core Server

use anyhow::{Context, bail};
use futures::{FutureExt, future::FusedFuture, pin_mut};
use influxdb3_authz::TokenAuthenticator;
use influxdb3_cache::{
    distinct_cache::DistinctCacheProvider,
    last_cache::{self, LastCacheProvider},
    parquet_cache::create_cached_obj_store_and_oracle,
};
use influxdb3_catalog::{CatalogError, catalog::Catalog};
use influxdb3_clap_blocks::plugins::{PackageManager, ProcessingEngineConfig};
use influxdb3_clap_blocks::{
    datafusion::IoxQueryDatafusionConfig, memory_size::MemorySize, object_store::ObjectStoreConfig,
    socket_addr::SocketAddr, tokio::TokioDatafusionConfig,
};
use influxdb3_process::{
    INFLUXDB3_GIT_HASH, INFLUXDB3_VERSION, PROCESS_START_TIME, PROCESS_UUID_STR, ProcessUuidGetter,
    ProcessUuidWrapper,
};
use influxdb3_processing_engine::ProcessingEngineManagerImpl;
use influxdb3_processing_engine::environment::{
    DisabledManager, PipManager, PythonEnvironmentManager, UVManager,
};
use influxdb3_processing_engine::plugins::ProcessingEngineEnvironmentManager;
use influxdb3_processing_engine::virtualenv::find_python;
use influxdb3_server::{
    CommonServerState, CreateServerArgs, Server,
    http::HttpApi,
    query_executor::{CreateQueryExecutorArgs, QueryExecutorImpl},
    serve, serve_admin_token_regen_endpoint,
};
use influxdb3_shutdown::{ShutdownManager, wait_for_signal};
use influxdb3_sys_events::SysEventStore;
use influxdb3_telemetry::{
    ProcessingEngineMetrics,
    store::{CreateTelemetryStoreArgs, TelemetryStore},
};
use influxdb3_wal::{Gen1Duration, WalConfig};
use influxdb3_write::{
    WriteBuffer, deleter,
    persister::Persister,
    write_buffer::{
        WriteBufferImpl, WriteBufferImplArgs, check_mem_and_force_snapshot_loop,
        persisted_files::PersistedFiles,
    },
};
use iox_query::exec::{DedicatedExecutor, Executor, ExecutorConfig, PerQueryMemoryPoolConfig};
use iox_time::{SystemProvider, TimeProvider};
use metric::U64Gauge;
use object_store::ObjectStore;
use object_store_metrics::ObjectStoreMetrics;
use observability_deps::tracing::*;
use panic_logging::SendPanicsToTracing;
use parquet_file::storage::{ParquetStorage, StorageId};
use rustls::{
    SupportedProtocolVersion,
    version::{TLS12, TLS13},
};
use std::{env, num::NonZeroUsize, sync::Arc, time::Duration};
use std::{path::Path, str::FromStr};
use std::{path::PathBuf, process::Command};
use thiserror::Error;
use tokio::net::TcpListener;
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;
use trace_exporters::TracingConfig;
use trace_http::ctx::TraceHeaderParser;
use trogging::cli::LoggingConfig;

use crate::commands::common::warn_use_of_deprecated_env_vars;

use super::helpers::DisableAuthzList;

#[cfg(all(feature = "jemalloc_replacing_malloc", not(target_env = "msvc")))]
mod jemalloc;

/// The default name of the influxdb data directory
#[allow(dead_code)]
pub const DEFAULT_DATA_DIRECTORY_NAME: &str = ".influxdb3";

/// The default bind address for the HTTP API.
pub const DEFAULT_HTTP_BIND_ADDR: &str = "0.0.0.0:8181";

/// The default bind address for admin token regeneration HTTP API.
pub const DEFAULT_ADMIN_TOKEN_REGENERATION_BIND_ADDR: &str = "127.0.0.1:8182";

pub const DEFAULT_TELEMETRY_ENDPOINT: &str = "https://telemetry.v3.influxdata.com";

#[derive(Debug, Error)]
pub enum Error {
    #[error("Cannot parse object store config: {0}")]
    ObjectStoreParsing(#[from] influxdb3_clap_blocks::object_store::ParseError),

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

    #[error("failed to initialize catalog: {0}")]
    InitializeCatalog(#[from] CatalogError),

    #[error("failed to initialize last cache: {0}")]
    InitializeLastCache(#[source] last_cache::Error),

    #[error("failed to initialize distinct cache: {0:#}")]
    InitializeDistinctCache(#[source] influxdb3_cache::distinct_cache::ProviderError),

    #[error("lost backend")]
    LostBackend,

    #[error("lost HTTP/gRPC service")]
    LostHttpGrpc,

    #[error("lost admin token regen service")]
    LostAdminTokenRegen,

    #[error("tls requires both a cert and a key file to be passed in to work")]
    NoCertOrKeyFile,
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

    /// The address on which admin token regeration will be allowed
    #[clap(
    long = "admin-token-regen-bind",
    env = "INFLUXDB3_ADMIN_TOKEN_REGEN_BIND_ADDR",
    default_value = DEFAULT_ADMIN_TOKEN_REGENERATION_BIND_ADDR,
    action,
    )]
    pub admin_token_regen_bind_address: SocketAddr,

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

    /// Flag to indicate that server should start without auth
    #[clap(long = "without-auth", env = "INFLUXDB3_START_WITHOUT_AUTH", action)]
    pub without_auth: bool,

    /// Disable authz for certain resources, allowed values are health,ping,metrics
    #[clap(long = "disable-authz", env = "INFLUXDB3_DISABLE_AUTHZ")]
    pub disable_authz: Option<DisableAuthzList>,

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

    /// The amount of time that the server looks back on startup when populating the in-memory
    /// index of gen1 files.
    ///
    /// This has two dimensions of impact on performance. The first is in terms of S3 API usage on
    /// startup; the second is in terms of initial memory usage on startup. To get a rough sense of
    /// both of these performance impacts, take the --gen1-duration value and divide that number
    /// into this parameter to get the total number of gen1 index snapshots loaded on startup. You
    /// can then take that number and multiply it by a rough approximation of the gen1 metadata
    /// stored in this index to obtain a rough estimate of memory usage.
    ///
    /// As an example, let's say we have the following values:
    ///
    /// * Estimated average of 128 bytes of parquet file metadata
    /// * --gen1-duration value of 10 minutes
    /// * --gen1-lookback-duration value of 1 month
    ///
    /// This leads to 144 files per day for 30 days for a total of 4320 index snapshots read in
    /// (via object store API calls) on startup and a (very rough) memory consumption estimate of
    /// ~533 KB.
    #[clap(
        long = "gen1-lookback-duration",
        env = "INFLUXDB3_GEN1_LOOKBACK_DURATION",
        default_value = "1month",
        action
    )]
    pub gen1_lookback_duration: humantime::Duration,

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

    /// Fail on error when replaying corrupt WAL files.
    ///
    /// When false (default), corrupt or truncated WAL files will be logged and skipped during startup.
    /// When true, the server will fail to start if any WAL files are corrupt.
    #[clap(
        long = "wal-replay-fail-on-error",
        env = "INFLUXDB3_WAL_REPLAY_FAIL_ON_ERROR",
        default_value_t = false,
        action
    )]
    pub wal_replay_fail_on_error: bool,

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

    #[clap(long = "tls-key", env = "INFLUXDB3_TLS_KEY")]
    pub key_file: Option<PathBuf>,

    #[clap(long = "tls-cert", env = "INFLUXDB3_TLS_CERT")]
    pub cert_file: Option<PathBuf>,

    #[clap(
        long = "tls-minimum-version",
        env = "INFLUXDB3_TLS_MINIMUM_VERSION",
        default_value = "tls-1.2"
    )]
    pub tls_minimum_version: TlsMinimumVersion,

    /// Provide a file path to write the address that the server is listening on to.
    ///
    /// This is mainly intended for testing purposes and is not considered stable.
    #[clap(
        long = "tcp-listener-file-path",
        env = "INFLUXDB3_TCP_LISTINER_FILE_PATH",
        hide = true
    )]
    pub tcp_listener_file_path: Option<PathBuf>,

    /// Provide a file path to write the address that the admin recovery endpoint mounted server is listening on to
    ///
    /// This is mainly intended for testing purposes and is not considered stable.
    #[clap(
        long = "admin-token-regen-tcp-listener-file-path",
        env = "INFLUXDB3_ADMIN_TOKEN_REGEN_TCP_LISTENER_FILE_PATH",
        hide = true
    )]
    pub admin_token_regen_tcp_listener_file_path: Option<PathBuf>,

    #[clap(
        long = "wal-replay-concurrency-limit",
        env = "INFLUXDB3_WAL_REPLAY_CONCURRENCY_LIMIT"
    )]
    pub wal_replay_concurrency_limit: Option<usize>,

    /// The duration from when a database or table is soft-deleted until the data is scheduled to
    /// be hard deleted.
    #[clap(
        long = "hard-delete-default-duration",
        env = "INFLUXDB3_HARD_DELETE_DEFAULT_DURATION",
        default_value_t = Catalog::DEFAULT_HARD_DELETE_DURATION.into(),
    )]
    pub hard_delete_default_duration: humantime::Duration,

    /// Grace period for hard deleted databases and tables before they are removed permanently from
    /// the catalog.
    #[clap(
        long = "delete-grace-period",
        env = "INFLUXDB3_DELETE_GRACE_PERIOD",
        default_value = "24h",
        action
    )]
    pub delete_grace_period: humantime::Duration,
}

/// The minimum version of TLS to use for InfluxDB
#[derive(Debug, Clone, Copy, Default)]
pub enum TlsMinimumVersion {
    #[default]
    Tls1_2,
    Tls1_3,
}

impl FromStr for TlsMinimumVersion {
    type Err = String;

    fn from_str(s: &str) -> std::prelude::v1::Result<Self, Self::Err> {
        match s {
            "tls-1.2" => Ok(Self::Tls1_2),
            "tls-1.3" => Ok(Self::Tls1_3),
            _ => Err("Valid minimum version strings are tls-1.2 and tls-1.3".into()),
        }
    }
}

impl From<TlsMinimumVersion> for &'static [&'static SupportedProtocolVersion] {
    fn from(val: TlsMinimumVersion) -> Self {
        static TLS1_2: &[&SupportedProtocolVersion] = &[&TLS12, &TLS13];
        static TLS1_3: &[&SupportedProtocolVersion] = &[&TLS13];
        match val {
            TlsMinimumVersion::Tls1_2 => TLS1_2,
            TlsMinimumVersion::Tls1_3 => TLS1_3,
        }
    }
}

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
        } else if let Some(suffix) = s.strip_suffix('b') {
            usize::from_str(suffix)
                .map_err(|_| "failed to parse value as unsigned integer".to_string())?
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
    // Check that both a cert file and key file are present if TLS is being set up
    match (&config.cert_file, &config.key_file) {
        (Some(_), None) | (None, Some(_)) => {
            return Err(Error::NoCertOrKeyFile);
        }
        (Some(_), Some(_)) | (None, None) => {}
    }

    let startup_timer = Instant::now();
    let num_cpus = num_cpus::get();
    let build_malloc_conf = build_malloc_conf();
    info!(
        node_id = %config.node_identifier_prefix,
        git_hash = %INFLUXDB3_GIT_HASH as &str,
        version = %INFLUXDB3_VERSION.as_ref() as &str,
        uuid = %PROCESS_UUID_STR.as_ref() as &str,
        num_cpus,
        "InfluxDB 3 Core server starting",
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

    // When you have extra executor, you need separate metrics registry! It is not clear what
    // the impact would be
    // TODO: confirm this is not going to mess up downstream metrics consumers
    let write_path_metrics = setup_metric_registry();

    // Install custom panic handler and forget about it.
    //
    // This leaks the handler and prevents it from ever being dropped during the
    // lifetime of the program - this is actually a good thing, as it prevents
    // the panic handler from being removed while unwinding a panic (which in
    // turn, causes a panic - see #548)
    let write_path_panic_handler_fn = SendPanicsToTracing::new_with_metrics(&write_path_metrics);
    std::mem::forget(write_path_panic_handler_fn);

    // Construct a token to trigger clean shutdown
    let frontend_shutdown = CancellationToken::new();
    let shutdown_manager = ShutdownManager::new(frontend_shutdown.clone());

    let time_provider: Arc<dyn TimeProvider> = Arc::new(SystemProvider::new());
    let sys_events_store = Arc::new(SysEventStore::new(Arc::clone(&time_provider) as _));
    // setup base object store:
    let object_store: Arc<dyn ObjectStore> = config
        .object_store_config
        .make_object_store()
        .map_err(Error::ObjectStoreParsing)?;

    // setup metrics'd object store:
    let object_store: Arc<dyn ObjectStore> = Arc::new(ObjectStoreMetrics::new(
        object_store,
        Arc::clone(&time_provider) as _,
        "main",
        &metrics,
        config.object_store_config.bucket.as_ref(),
    ));

    // setup cached object store:
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
            // TODO: need to make these configurable?
            per_query_mem_pool_config: PerQueryMemoryPoolConfig::Disabled,
            heap_memory_limit: None,
        },
        DedicatedExecutor::new(
            "datafusion",
            tokio_datafusion_config
                .builder()
                .map_err(Error::TokioRuntime)?,
            Arc::clone(&metrics),
        ),
    ));

    // Note: using same metrics registry causes runtime panic.
    let write_path_executor = Arc::new(Executor::new_with_config_and_executor(
        ExecutorConfig {
            // should this be divided? or should this contend for threads with executor that's
            // setup for querying only
            target_query_partitions: tokio_datafusion_config.num_threads.unwrap(),
            object_stores: [&parquet_store]
                .into_iter()
                .map(|store| (store.id(), Arc::clone(store.object_store())))
                .collect(),
            metric_registry: Arc::clone(&write_path_metrics),
            // use as much memory for persistence, can this be UnboundedMemoryPool?
            mem_pool_size: usize::MAX,
            // These are new additions, just skimming through the code it does not look like we can
            // achieve the same effect as having a separate executor. It looks like it's for "all"
            // queries, it'd be nice to have a filter to say when the query matches this pattern
            // apply these limits. If that's possible maybe we could avoid creating a separate
            // executor.
            per_query_mem_pool_config: PerQueryMemoryPoolConfig::Disabled,
            heap_memory_limit: None,
        },
        DedicatedExecutor::new(
            "datafusion_write_path",
            tokio_datafusion_config
                .builder()
                .map_err(Error::TokioRuntime)?,
            Arc::clone(&write_path_metrics),
        ),
    ));

    let trace_header_parser = TraceHeaderParser::new()
        .with_jaeger_trace_context_header_name(
            config
                .tracing_config
                .traces_jaeger_trace_context_header_name,
        )
        .with_jaeger_debug_name(config.tracing_config.traces_jaeger_debug_name);

    let persister = Arc::new(Persister::new(
        Arc::clone(&object_store),
        config.node_identifier_prefix.as_str(),
        Arc::clone(&time_provider) as _,
    ));

    let process_uuid_getter: Arc<dyn ProcessUuidGetter> = Arc::new(ProcessUuidWrapper::new());
    let catalog = Catalog::new_with_shutdown(
        config.node_identifier_prefix.as_str(),
        Arc::clone(&object_store),
        Arc::clone(&time_provider),
        Arc::clone(&metrics),
        shutdown_manager.register(),
        Arc::clone(&process_uuid_getter),
    )
    .await?;
    info!(catalog_uuid = ?catalog.catalog_uuid(), "catalog initialized");

    let _ = catalog
        .register_node(
            &config.node_identifier_prefix,
            num_cpus as u64,
            vec![influxdb3_catalog::log::NodeMode::Core],
            process_uuid_getter,
        )
        .await?;
    let node_def = catalog
        .node(&config.node_identifier_prefix)
        .expect("node should be registered in catalog");
    info!(instance_id = ?node_def.instance_id(), "catalog initialized");

    let last_cache = LastCacheProvider::new_from_catalog_with_background_eviction(
        Arc::clone(&catalog) as _,
        config.last_cache_eviction_interval.into(),
    )
    .await
    .map_err(Error::InitializeLastCache)?;

    let distinct_cache = DistinctCacheProvider::new_from_catalog_with_background_eviction(
        Arc::clone(&time_provider) as _,
        Arc::clone(&catalog),
        config.distinct_cache_eviction_interval.into(),
    )
    .await
    .map_err(Error::InitializeDistinctCache)?;

    // Set the gen1 duration in the catalog; if already set, nothing happens; if set to a different
    // value, we emit a WARN; if some other error occurs we exit.
    let gen1_duration = match catalog
        .set_gen1_duration(config.gen1_duration.as_duration())
        .await
    {
        Ok(_) | Err(CatalogError::AlreadyExists) => config.gen1_duration,
        Err(CatalogError::CannotChangeGenerationDuration { .. }) => {
            let existing: Gen1Duration = catalog
                .get_generation_duration(1)
                .unwrap()
                .try_into()
                .expect("catalog should contain valid gen1 duration");
            warn!(
                existing_secs = existing.as_duration().as_secs(),
                provided_secs = config.gen1_duration.as_duration().as_secs(),
                "cannot change the existing gen1 duration after it has been set"
            );
            existing
        }
        Err(error) => return Err(error.into()),
    };

    let n_snapshots_to_load_on_start =
        config.gen1_lookback_duration.as_secs() / gen1_duration.as_duration().as_secs();

    let wal_config = WalConfig {
        gen1_duration,
        max_write_buffer_size: config.wal_max_write_buffer_size,
        flush_interval: config.wal_flush_interval.into(),
        snapshot_size: config.wal_snapshot_size,
        wal_replay_fail_on_error: config.wal_replay_fail_on_error,
    };

    let write_buffer_impl = WriteBufferImpl::new(WriteBufferImplArgs {
        persister: Arc::clone(&persister),
        catalog: Arc::clone(&catalog),
        last_cache,
        distinct_cache,
        time_provider: Arc::clone(&time_provider),
        executor: Arc::clone(&write_path_executor),
        wal_config,
        parquet_cache,
        metric_registry: Arc::clone(&metrics),
        snapshotted_wal_files_to_keep: config.snapshotted_wal_files_to_keep,
        query_file_limit: config.query_file_limit,
        n_snapshots_to_load_on_start: n_snapshots_to_load_on_start as usize,
        shutdown: shutdown_manager.register(),
        wal_replay_concurrency_limit: config.wal_replay_concurrency_limit,
    })
    .await
    .map_err(|e| Error::WriteBufferInit(e.into()))?;

    let persisted_files = write_buffer_impl.persisted_files();

    let object_deleter = Some(Arc::clone(&persisted_files) as _);

    deleter::run(
        DeleteManagerArgs {
            catalog: Arc::clone(&catalog),
            time_provider: Arc::clone(&time_provider),
            object_deleter,
            delete_grace_period: *config.delete_grace_period,
        },
        shutdown_manager.register(),
    );

    info!("setting up background mem check for query buffer");
    background_buffer_checker(
        config.force_snapshot_mem_threshold.as_num_bytes(),
        &write_buffer_impl,
    )
    .await;

    info!("setting up telemetry store");
    let telemetry_store = setup_telemetry_store(TelemetryStoreSetupArgs {
        object_store_config: &config.object_store_config,
        instance_id: node_def.instance_id(),
        num_cpus,
        persisted_files: Some(persisted_files),
        telemetry_endpoint: &config.telemetry_endpoint,
        disable_upload: config.disable_telemetry_upload,
        catalog_uuid: catalog.catalog_uuid().to_string(),
        processing_engine_metrics: Arc::clone(&catalog) as Arc<dyn ProcessingEngineMetrics>,
    })
    .await;

    let write_buffer: Arc<dyn WriteBuffer> = write_buffer_impl;

    let common_state = CommonServerState::new(
        Arc::clone(&metrics),
        trace_exporter,
        trace_header_parser,
        Arc::clone(&telemetry_store),
    );

    if config.without_auth {
        warn!(
            "server started without auth (`--without-auth` switch), all token creation and regeneration of admin token endpoints are disabled"
        );
    }

    let query_executor = Arc::new(QueryExecutorImpl::new(CreateQueryExecutorArgs {
        catalog: write_buffer.catalog(),
        write_buffer: Arc::clone(&write_buffer),
        exec: Arc::clone(&exec),
        metrics: Arc::clone(&metrics),
        datafusion_config: Arc::new(config.iox_query_datafusion_config.build()),
        query_log_size: config.query_log_size,
        telemetry_store: Arc::clone(&telemetry_store),
        sys_events_store: Arc::clone(&sys_events_store),
        // convert to positive here so that we can avoid double negatives downstream
        started_with_auth: !config.without_auth,
        time_provider: Arc::clone(&time_provider) as _,
    }));

    let listener = TcpListener::bind(*config.http_bind_address)
        .await
        .map_err(Error::BindAddress)?;

    let admin_token_regen_listener = TcpListener::bind(*config.admin_token_regen_bind_address)
        .await
        .map_err(Error::BindAddress)?;

    let processing_engine = ProcessingEngineManagerImpl::new(
        setup_processing_engine_env_manager(&config.processing_engine_config),
        write_buffer.catalog(),
        config.node_identifier_prefix,
        Arc::clone(&write_buffer),
        Arc::clone(&query_executor) as _,
        Arc::clone(&time_provider) as _,
        sys_events_store,
    )
    .await;

    let cert_file = config.cert_file;
    let key_file = config.key_file;

    // Start processing engine triggers
    Arc::clone(&processing_engine)
        .start_triggers()
        .await
        .expect("failed to start processing engine triggers");

    write_buffer
        .wal()
        .add_file_notifier(Arc::clone(&processing_engine) as _);

    let authorizer: Arc<dyn influxdb3_authz::AuthProvider> = if config.without_auth {
        Arc::new(influxdb3_authz::NoAuthAuthenticator)
    } else {
        Arc::new(TokenAuthenticator::new(
            Arc::clone(&catalog) as _,
            Arc::clone(&time_provider) as _,
        ))
    };

    let http = Arc::new(HttpApi::new(
        common_state.clone(),
        Arc::clone(&time_provider) as _,
        Arc::clone(&write_buffer),
        Arc::clone(&query_executor) as _,
        Arc::clone(&processing_engine),
        config.max_http_request_size,
        Arc::clone(&authorizer),
    ));

    let admin_token_regen_server = Server::new(CreateServerArgs {
        common_state: common_state.clone(),
        http: Arc::clone(&http),
        authorizer: Arc::clone(&authorizer),
        listener: admin_token_regen_listener,
        cert_file: cert_file.clone(),
        key_file: key_file.clone(),
        tls_minimum_version: config.tls_minimum_version.into(),
    });

    let server = Server::new(CreateServerArgs {
        common_state,
        http,
        authorizer,
        listener,
        cert_file,
        key_file,
        tls_minimum_version: config.tls_minimum_version.into(),
    });

    // There are two different select! macros - tokio::select and futures::select
    //
    // tokio::select takes ownership of the passed future "moving" it into the
    // select block. This works well when not running select inside a loop, or
    // when using a future that can be dropped and recreated, often the case
    // with tokio's futures e.g. `channel.recv()`
    //
    // futures::select is more flexible as it doesn't take ownership of the provided
    // future. However, to safely provide this it imposes some additional
    // requirements
    //
    // All passed futures must implement FusedFuture - it is IB to poll a future
    // that has returned Poll::Ready(_). A FusedFuture has an is_terminated()
    // method that indicates if it is safe to poll - e.g. false if it has
    // returned Poll::Ready(_). futures::select uses this to implement its
    // functionality. futures::FutureExt adds a fuse() method that
    // wraps an arbitrary future and makes it a FusedFuture
    //
    // The additional requirement of futures::select is that if the future passed
    // outlives the select block, it must be Unpin or already Pinned

    // Create the FusedFutures that will be waited on before exiting the process
    let signal = wait_for_signal().fuse();
    let paths_without_authz: &'static Vec<&'static str> = config
        .disable_authz
        .unwrap_or_default()
        .get_mapped_endpoints();

    info!(
        ?paths_without_authz,
        "setting up server with authz disabled for paths"
    );

    let frontend = serve(
        server,
        frontend_shutdown.clone(),
        startup_timer,
        config.without_auth,
        paths_without_authz,
        config.tcp_listener_file_path,
    )
    .fuse();
    let backend = shutdown_manager.join().fuse();

    let regen_frontend = serve_admin_token_regen_endpoint(
        admin_token_regen_server,
        frontend_shutdown.clone(),
        config.admin_token_regen_tcp_listener_file_path,
    )
    .fuse();

    // pin_mut constructs a Pin<&mut T> from a T by preventing moving the T
    // from the current stack frame and constructing a Pin<&mut T> to it
    pin_mut!(signal);
    pin_mut!(frontend);
    pin_mut!(backend);
    pin_mut!(regen_frontend);

    let mut res = Ok(());

    // Graceful shutdown can be triggered by sending SIGINT or SIGTERM to the
    // process, or by a background task exiting - most likely with an error
    while !frontend.is_terminated() {
        futures::select! {
            // External shutdown signal, e.g., `ctrl+c`
            _ = signal => info!("shutdown requested"),
            // `join` on the `ShutdownManager` has completed
            _ = backend => {
                // If something stops the process on the backend the frontend shutdown should have
                // been signaled in which case we can break the loop here once checking that it
                // has been cancelled.
                //
                // The select! could also pick this branch in the event that the frontend and
                // backend stop at the same time. That shouldn't be an issue so long as the frontend
                // has indeed stopped, so we check on exiting the loop that the frontend has
                // terminated before checking and waiting on the backend.
                if frontend_shutdown.is_cancelled() {
                    break;
                }
                error!("backend shutdown before frontend");
                res = res.and(Err(Error::LostBackend));
            }
            // HTTP/gRPC frontend has stopped
            result = frontend => match result {
                Ok(_) if frontend_shutdown.is_cancelled() => info!("HTTP/gRPC service shutdown"),
                Ok(_) => {
                    error!("early HTTP/gRPC service exit");
                    res = res.and(Err(Error::LostHttpGrpc));
                },
                Err(error) => {
                    error!("HTTP/gRPC error");
                    res = res.and(Err(Error::Server(error)));
                }
            },
            regen_result = regen_frontend => match regen_result {
                Ok(_) if frontend_shutdown.is_cancelled() => info!("Admin token regeneration service shutdown"),
                Ok(_) => {
                    error!("early admin token regeneration service exit");
                    res = res.and(Err(Error::LostAdminTokenRegen));
                }
                Err(error) => {
                    error!("admin token regeneration service error");
                    res = res.and(Err(Error::Server(error)));
                }
            }
        }
        shutdown_manager.shutdown()
    }
    // ensure that the frontend has fully terminated so we dont close the connection on any clients
    if !frontend.is_terminated() {
        res = res.and(frontend.await.map_err(Into::into));
    }
    info!("frontend shutdown completed");

    if !backend.is_terminated() {
        backend.await;
    }
    info!("backend shutdown completed");

    res
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
    let python_exe = find_python();
    debug!("Running: {} -m pip --version", python_exe.display());

    if let Ok(output) = Command::new(&python_exe)
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

struct TelemetryStoreSetupArgs<'a> {
    object_store_config: &'a ObjectStoreConfig,
    instance_id: Arc<str>,
    num_cpus: usize,
    persisted_files: Option<Arc<PersistedFiles>>,
    telemetry_endpoint: &'a str,
    disable_upload: bool,
    catalog_uuid: String,
    processing_engine_metrics: Arc<dyn ProcessingEngineMetrics>,
}

async fn setup_telemetry_store(
    TelemetryStoreSetupArgs {
        object_store_config,
        instance_id,
        num_cpus,
        persisted_files,
        telemetry_endpoint,
        disable_upload,
        catalog_uuid,
        processing_engine_metrics,
    }: TelemetryStoreSetupArgs<'_>,
) -> Arc<TelemetryStore> {
    let os = std::env::consts::OS;
    let influxdb_pkg_version = env!("CARGO_PKG_VERSION");
    let influxdb_pkg_name = env!("CARGO_PKG_NAME");
    // Following should show influxdb3-0.1.0
    let influx_version = format!("{influxdb_pkg_name}-{influxdb_pkg_version}");
    let obj_store_type = object_store_config.object_store;
    let storage_type = obj_store_type.as_str();

    if disable_upload {
        debug!("Initializing TelemetryStore with upload disabled.");
        TelemetryStore::new_without_background_runners(
            persisted_files.map(|p| p as _),
            processing_engine_metrics,
        )
    } else {
        debug!("Initializing TelemetryStore with upload enabled for {telemetry_endpoint}.");
        TelemetryStore::new(CreateTelemetryStoreArgs {
            instance_id,
            os: Arc::from(os),
            influx_version: Arc::from(influx_version),
            storage_type: Arc::from(storage_type),
            cores: num_cpus,
            persisted_files: persisted_files.map(|p| p as _),
            telemetry_endpoint: telemetry_endpoint.to_string(),
            catalog_uuid,
            processing_engine_metrics,
        })
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

#[cfg(all(
    feature = "jemalloc_replacing_malloc",
    not(target_env = "msvc"),
    not(feature = "disable_custom_global_allocator")
))]
#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

use influxdb3_write::deleter::DeleteManagerArgs;
#[cfg(tokio_unstable)]
use tokio_metrics_bridge::setup_tokio_metrics;

#[cfg(any(not(feature = "jemalloc_replacing_malloc"), target_env = "msvc"))]
pub fn build_malloc_conf() -> String {
    "system".to_string()
}

#[cfg(all(feature = "jemalloc_replacing_malloc", not(target_env = "msvc")))]
pub fn build_malloc_conf() -> String {
    tikv_jemalloc_ctl::config::malloc_conf::mib()
        .unwrap()
        .read()
        .unwrap()
        .to_string()
}

pub fn setup_metric_registry() -> Arc<metric::Registry> {
    let registry = Arc::new(metric::Registry::default());

    // See https://prometheus.io/docs/instrumenting/writing_clientlibs/#process-metrics
    registry
        .register_metric::<U64Gauge>(
            "process_start_time_seconds",
            "Start time of the process since unix epoch in seconds.",
        )
        .recorder(&[
            ("version", INFLUXDB3_VERSION.as_ref()),
            ("git_hash", INFLUXDB3_GIT_HASH),
            ("uuid", PROCESS_UUID_STR.as_ref()),
        ])
        .set(PROCESS_START_TIME.timestamp() as u64);

    // Register jemalloc metrics
    #[cfg(all(feature = "jemalloc_replacing_malloc", not(target_env = "msvc")))]
    registry.register_instrument("jemalloc_metrics", jemalloc::JemallocMetrics::new);

    // Register tokio metric for main runtime
    #[cfg(tokio_unstable)]
    setup_tokio_metrics(
        tokio::runtime::Handle::current().metrics(),
        "main",
        Arc::clone(&registry),
    );

    registry
}
