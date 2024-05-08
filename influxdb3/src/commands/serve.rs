//! Entrypoint for InfluxDB 3.0 Edge Server

use clap_blocks::{
    memory_size::MemorySize,
    object_store::{make_object_store, ObjectStoreConfig},
    socket_addr::SocketAddr,
};
use datafusion_util::config::register_iox_object_store;
use influxdb3_compactor::Compactor;
use influxdb3_process::{
    build_malloc_conf, setup_metric_registry, INFLUXDB3_GIT_HASH, INFLUXDB3_VERSION, PROCESS_UUID,
};
use influxdb3_server::{
    auth::AllOrNothingAuthorizer, builder::ServerBuilder, query_executor::QueryExecutorImpl, serve,
    CommonServerState,
};
use influxdb3_write::persister::PersisterImpl;
use influxdb3_write::wal::WalImpl;
use influxdb3_write::write_buffer::WriteBufferImpl;
use influxdb3_write::SegmentDuration;
use iox_query::exec::{Executor, ExecutorConfig};
use iox_time::SystemProvider;
use ioxd_common::reexport::trace_http::ctx::TraceHeaderParser;
use object_store::DynObjectStore;
use observability_deps::tracing::*;
use panic_logging::SendPanicsToTracing;
use parquet_file::storage::{ParquetStorage, StorageId};
use std::collections::HashMap;
use std::{
    num::NonZeroUsize,
    path::{Path, PathBuf},
    sync::Arc,
};
use thiserror::Error;
use tokio::task;
use tokio_util::sync::CancellationToken;
use trace_exporters::TracingConfig;
use trogging::cli::LoggingConfig;

/// The default name of the influxdb_iox data directory
#[allow(dead_code)]
pub const DEFAULT_DATA_DIRECTORY_NAME: &str = ".influxdb3";

/// The default bind address for the HTTP API.
pub const DEFAULT_HTTP_BIND_ADDR: &str = "127.0.0.1:8181";

#[derive(Debug, Error)]
pub enum Error {
    #[error("Cannot parse object store config: {0}")]
    ObjectStoreParsing(#[from] clap_blocks::object_store::ParseError),

    #[error("Tracing config error: {0}")]
    TracingConfig(#[from] trace_exporters::Error),

    #[error("Server error: {0}")]
    Server(#[from] influxdb3_server::Error),

    #[error("Wal error: {0}")]
    Wal(#[from] influxdb3_write::wal::Error),

    #[error("Write buffer error: {0}")]
    WriteBuffer(#[from] influxdb3_write::write_buffer::Error),

    #[error("invalid token: {0}")]
    InvalidToken(#[from] hex::FromHexError),
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

#[derive(Debug, clap::Parser)]
pub struct Config {
    /// Maximum size of HTTP requests.
    #[clap(
    long = "max-http-request-size",
    env = "INFLUXDB3_MAX_HTTP_REQUEST_SIZE",
    default_value = "10485760", // 10 MiB
    action,
    )]
    pub max_http_request_size: usize,

    #[clap(flatten)]
    object_store_config: ObjectStoreConfig,

    /// The directory to store the write ahead log
    ///
    /// If not specified, defaults to INFLUXDB3_DB_DIR/wal
    #[clap(long = "wal-directory", env = "INFLUXDB3_WAL_DIRECTORY", action)]
    pub wal_directory: Option<PathBuf>,

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

    /// logging options
    #[clap(flatten)]
    pub(crate) logging_config: LoggingConfig,

    /// tracing options
    #[clap(flatten)]
    pub(crate) tracing_config: TracingConfig,

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

    /// Duration of wal segments that are persisted to object storage. Valid values: 1m, 5m, 10m,
    /// 15m, 30m, 1h, 2h, 4h.
    #[clap(
        long = "segment-duration",
        env = "INFLUXDB3_SEGMENT_DURATION",
        default_value = "1h",
        action
    )]
    pub segment_duration: SegmentDuration,
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

    let object_store: Arc<DynObjectStore> =
        make_object_store(&config.object_store_config).map_err(Error::ObjectStoreParsing)?;

    let trace_exporter = config.tracing_config.build()?;

    // TODO: make this a parameter
    let num_threads =
        NonZeroUsize::new(num_cpus::get()).unwrap_or_else(|| NonZeroUsize::new(1).unwrap());

    info!(%num_threads, "Creating shared query executor");
    let parquet_store =
        ParquetStorage::new(Arc::clone(&object_store), StorageId::from("influxdb3"));
    let exec = Arc::new(Executor::new_with_config(
        "datafusion",
        ExecutorConfig {
            num_threads,
            target_query_partitions: num_threads,
            object_stores: [&parquet_store]
                .into_iter()
                .map(|store| (store.id(), Arc::clone(store.object_store())))
                .collect(),
            metric_registry: Arc::clone(&metrics),
            mem_pool_size: config.exec_mem_pool_bytes.bytes(),
        },
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

    let common_state = CommonServerState::new(
        Arc::clone(&metrics),
        trace_exporter,
        trace_header_parser,
        *config.http_bind_address,
    )?;
    let persister = Arc::new(PersisterImpl::new(Arc::clone(&object_store)));
    let wal: Option<Arc<WalImpl>> = config
        .wal_directory
        .map(|dir| WalImpl::new(dir).map(Arc::new))
        .transpose()?;

    let time_provider = Arc::new(SystemProvider::new());
    let write_buffer = Arc::new(
        WriteBufferImpl::new(
            Arc::clone(&persister),
            wal,
            Arc::clone(&time_provider),
            config.segment_duration,
            Arc::clone(&exec),
        )
        .await?,
    );
    let query_executor = Arc::new(QueryExecutorImpl::new(
        write_buffer.catalog(),
        Arc::clone(&write_buffer),
        Arc::clone(&exec),
        Arc::clone(&metrics),
        Arc::new(config.datafusion_config),
        10,
    ));
    let compactor = Compactor::new(Arc::clone(&write_buffer), Arc::clone(&persister));

    let builder = ServerBuilder::new(common_state)
        .max_request_size(config.max_http_request_size)
        .write_buffer(write_buffer)
        .query_executor(query_executor)
        .time_provider(time_provider)
        .persister(persister);

    let server = if let Some(token) = config.bearer_token.map(hex::decode).transpose()? {
        builder
            .authorizer(Arc::new(AllOrNothingAuthorizer::new(token)))
            .build()
    } else {
        builder.build()
    };
    task::spawn(compactor.compact());
    serve(server, frontend_shutdown).await?;

    Ok(())
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
