//! Entrypoint for InfluxDB 3.0 Edge Server

use anyhow::{bail, Context};
use clap_blocks::{
    memory_size::MemorySize,
    object_store::{make_object_store, ObjectStoreConfig, ObjectStoreType},
    socket_addr::SocketAddr,
    tokio::TokioDatafusionConfig,
};
use datafusion_util::config::register_iox_object_store;
use futures::future::join_all;
use futures::future::FutureExt;
use futures::TryFutureExt;
use influxdb3_pro_buffer::{
    modes::read_write::ReadWriteArgs, replica::ReplicationConfig, WriteBufferPro,
};
use influxdb3_pro_clap_blocks::serve::BufferMode;
use influxdb3_pro_compactor::Compactor;
use influxdb3_pro_data_layout::compacted_data::CompactedData;
use influxdb3_pro_data_layout::CompactionConfig;
use influxdb3_process::{
    build_malloc_conf, setup_metric_registry, INFLUXDB3_GIT_HASH, INFLUXDB3_VERSION, PROCESS_UUID,
};
use influxdb3_server::{
    auth::AllOrNothingAuthorizer, builder::ServerBuilder, query_executor::QueryExecutorImpl, serve,
    CommonServerState,
};
use influxdb3_telemetry::store::TelemetryStore;
use influxdb3_wal::{Gen1Duration, WalConfig};
use influxdb3_write::{
    last_cache::LastCacheProvider, parquet_cache::create_cached_obj_store_and_oracle,
    persister::Persister, WriteBuffer,
};
use iox_query::exec::{DedicatedExecutor, Executor, ExecutorConfig};
use iox_time::SystemProvider;
use object_store::ObjectStore;
use observability_deps::tracing::*;
use panic_logging::SendPanicsToTracing;
use parquet_file::storage::{ParquetStorage, StorageId};
use std::{collections::HashMap, path::Path, str::FromStr};
use std::{num::NonZeroUsize, sync::Arc};
use thiserror::Error;
use tokio::net::TcpListener;
use tokio_util::sync::CancellationToken;
use trace_exporters::TracingConfig;
use trace_http::ctx::TraceHeaderParser;
use trogging::cli::LoggingConfig;

/// The default name of the influxdb_iox data directory
#[allow(dead_code)]
pub const DEFAULT_DATA_DIRECTORY_NAME: &str = ".influxdb3";

/// The default bind address for the HTTP API.
pub const DEFAULT_HTTP_BIND_ADDR: &str = "0.0.0.0:8181";

#[derive(Debug, Error)]
pub enum Error {
    #[error("Cannot parse object store config: {0}")]
    ObjectStoreParsing(#[from] clap_blocks::object_store::ParseError),

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

    #[error("Failed to load compactor: {0}")]
    Compactor(#[from] influxdb3_pro_data_layout::compacted_data::Error),

    #[error("failed to initialize last cache: {0}")]
    InitializeLastCache(#[source] influxdb3_write::last_cache::Error),
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

    /// DataFusion config.
    #[clap(
    long = "datafusion-config",
    env = "INFLUXDB_IOX_DATAFUSION_CONFIG",
    default_value = "",
    value_parser = parse_datafusion_config,
    action
    )]
    pub datafusion_config: HashMap<String, String>,

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
    pub pro_config: influxdb3_pro_clap_blocks::serve::ProServeConfig,

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
}

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
    let num_cpus = num_cpus::get();
    let build_malloc_conf = build_malloc_conf();
    info!(
        git_hash = %INFLUXDB3_GIT_HASH as &str,
        version = %INFLUXDB3_VERSION.as_ref() as &str,
        uuid = %PROCESS_UUID.as_ref() as &str,
        num_cpus,
        %build_malloc_conf,
        "InfluxDB3 Edge server starting",
    );

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

    let object_store: Arc<dyn ObjectStore> =
        make_object_store(&config.object_store_config).map_err(Error::ObjectStoreParsing)?;
    let time_provider = Arc::new(SystemProvider::new());

    let (object_store, parquet_cache) = if !config.disable_parquet_mem_cache {
        let (object_store, parquet_cache) = create_cached_obj_store_and_oracle(
            object_store,
            Arc::clone(&time_provider) as _,
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

    let compaction_hosts_and_data = if let Some(compactor_id) = config.pro_config.compactor_id {
        let mut compaction_hosts = config
            .pro_config
            .replicas
            .as_ref()
            .map(|replicas| replicas.to_vec())
            .unwrap_or_default();

        // if we're in ReadWrite mode, add this host to the list
        if let BufferMode::ReadWrite = config.pro_config.mode {
            compaction_hosts.push(config.host_identifier_prefix.clone());
        }

        let compaction_config = CompactionConfig::new(
            &config.pro_config.compaction_multipliers.0,
            config.pro_config.compaction_gen2_duration.into(),
            config.pro_config.compaction_row_limit,
        );

        let compacted_data = CompactedData::load_compacted_data(
            &compactor_id,
            compaction_config,
            Arc::clone(&object_store),
        )
        .await?;

        Some((compaction_hosts, compacted_data))
    } else {
        None
    };

    let time_provider = Arc::new(SystemProvider::new());

    let catalog = persister
        .load_or_create_catalog()
        .await
        .map_err(Error::InitializePersistedCatalog)?;

    let last_cache = LastCacheProvider::new_from_catalog(&catalog.clone_inner())
        .map_err(Error::InitializeLastCache)?;

    let replica_config = config.pro_config.replicas.map(|replicas| {
        ReplicationConfig::new(
            config.pro_config.replication_interval.into(),
            replicas.into(),
        )
    });

    let telemetry_store =
        setup_telemetry_store(&config.object_store_config, catalog.instance_id(), num_cpus).await;

    let common_state = CommonServerState::new(
        Arc::clone(&metrics),
        trace_exporter,
        trace_header_parser,
        Arc::clone(&telemetry_store),
    )?;
    let compacted_data = compaction_hosts_and_data
        .as_ref()
        .map(|(_, data)| Arc::clone(data));

    let write_buffer: Arc<dyn WriteBuffer> = match config.pro_config.mode {
        BufferMode::Read => {
            let replica_config = replica_config
                .context("must supply a replicas list when starting in read-only mode")
                .map_err(Error::WriteBufferInit)?;
            Arc::new(
                WriteBufferPro::read(
                    Arc::new(catalog),
                    Arc::new(last_cache),
                    Arc::clone(&object_store),
                    Arc::clone(&metrics),
                    replica_config,
                    parquet_cache,
                    compacted_data,
                )
                .await
                .map_err(Error::WriteBufferInit)?,
            )
        }
        BufferMode::ReadWrite => Arc::new(
            WriteBufferPro::read_write(ReadWriteArgs {
                host_id: persister.host_identifier_prefix().into(),
                persister: Arc::clone(&persister),
                catalog: Arc::new(catalog),
                last_cache: Arc::new(last_cache),
                time_provider: Arc::<SystemProvider>::clone(&time_provider),
                executor: Arc::clone(&exec),
                wal_config,
                metric_registry: Arc::clone(&metrics),
                replication_config: replica_config,
                parquet_cache,
                compacted_data,
            })
            .await
            .map_err(Error::WriteBufferInit)?,
        ),
    };
    let query_executor = Arc::new(QueryExecutorImpl::new(
        write_buffer.catalog(),
        Arc::clone(&write_buffer),
        Arc::clone(&exec),
        Arc::clone(&metrics),
        Arc::new(config.datafusion_config),
        10,
        config.query_log_size,
        Arc::clone(&telemetry_store),
    ));

    let listener = TcpListener::bind(*config.http_bind_address)
        .await
        .map_err(Error::BindAddress)?;

    let builder = ServerBuilder::new(common_state)
        .max_request_size(config.max_http_request_size)
        .write_buffer(Arc::clone(&write_buffer))
        .query_executor(query_executor)
        .time_provider(time_provider)
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

    if config.pro_config.run_compactions && compaction_hosts_and_data.is_some() {
        let (compaction_hosts, compacted_data) =
            compaction_hosts_and_data.expect("compacted data must have been initialized");
        let compactor = Compactor::new(
            compaction_hosts,
            compacted_data,
            Arc::clone(&write_buffer.catalog()),
            persister.object_store_url().clone(),
            Arc::clone(&exec),
            write_buffer.watch_persisted_snapshots(),
        )
        .await
        .map_err(Error::Compactor)?;

        // Run the compactor code on the DataFusion executor

        // Note that unlike tokio::spawn, if the handle to the task is
        // dropped, the task is cancelled, so it must be retained until the end
        let t = exec
            .executor()
            .spawn(compactor.compact())
            .map_err(Error::Job);

        futures.push(t.boxed());
    }

    futures.push(
        serve(server, frontend_shutdown)
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
    )
    .await
}

fn parse_datafusion_config(
    s: &str,
) -> Result<HashMap<String, String>, Box<dyn std::error::Error + Send + Sync + 'static>> {
    let s = s.trim();
    if s.is_empty() {
        return Ok(HashMap::with_capacity(0));
    }

    let mut out = HashMap::new();
    for part in s.split(',') {
        let kv = part.trim().splitn(2, ':').collect::<Vec<_>>();
        match kv.as_slice() {
            [key, value] => {
                let key_owned = key.trim().to_owned();
                let value_owned = value.trim().to_owned();
                let existed = out.insert(key_owned, value_owned).is_some();
                if existed {
                    return Err(format!("key '{key}' passed multiple times").into());
                }
            }
            _ => {
                return Err(
                    format!("Invalid key value pair - expected 'KEY:VALUE' got '{s}'").into(),
                );
            }
        }
    }

    Ok(out)
}
