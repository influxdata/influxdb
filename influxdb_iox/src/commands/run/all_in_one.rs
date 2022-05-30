//! Implementation of command line option for running all in one mode

use super::main;
use clap_blocks::object_store::make_object_store;
use clap_blocks::querier::QuerierConfig;
use clap_blocks::{
    catalog_dsn::CatalogDsnConfig, compactor::CompactorConfig, ingester::IngesterConfig,
    object_store::ObjectStoreConfig, run_config::RunConfig, socket_addr::SocketAddr,
    write_buffer::WriteBufferConfig,
};
use iox_query::exec::Executor;
use iox_time::{SystemProvider, TimeProvider};
use ioxd_common::{
    server_type::{CommonServerState, CommonServerStateError},
    Service,
};
use ioxd_compactor::create_compactor_server_type;
use ioxd_ingester::create_ingester_server_type;
use ioxd_querier::{create_querier_server_type, QuerierServerTypeArgs};
use ioxd_router::create_router_server_type;
use object_store::DynObjectStore;
use observability_deps::tracing::*;
use std::{path::PathBuf, sync::Arc};
use thiserror::Error;
use trace_exporters::TracingConfig;
use trogging::cli::LoggingConfig;

/// The default bind address for the Router HTTP API.
pub const DEFAULT_ROUTER_HTTP_BIND_ADDR: &str = "127.0.0.1:8080";

/// The default bind address for the Router gRPC.
pub const DEFAULT_ROUTER_GRPC_BIND_ADDR: &str = "127.0.0.1:8081";

/// The default bind address for the Querier gRPC (chosen to match default gRPC addr)
pub const DEFAULT_QUERIER_GRPC_BIND_ADDR: &str = "127.0.0.1:8082";

/// The default bind address for the Ingester gRPC
pub const DEFAULT_INGESTER_GRPC_BIND_ADDR: &str = "127.0.0.1:8083";

/// The default bind address for the Compactor gRPC
pub const DEFAULT_COMPACTOR_GRPC_BIND_ADDR: &str = "127.0.0.1:8084";

// If you want this level of control, should be instantiating the services individually
const QUERY_POOL_NAME: &str = "iox-shared";

#[derive(Debug, Error)]
pub enum Error {
    #[error("Run: {0}")]
    Run(#[from] main::Error),

    #[error("Catalog DSN error: {0}")]
    CatalogDsn(#[from] clap_blocks::catalog_dsn::Error),

    #[error("Catalog error: {0}")]
    Catalog(#[from] iox_catalog::interface::Error),

    #[error("Cannot parse object store config: {0}")]
    ObjectStoreParsing(#[from] clap_blocks::object_store::ParseError),

    #[error("Router error: {0}")]
    Router(#[from] ioxd_router::Error),

    #[error("Ingester error: {0}")]
    Ingester(#[from] ioxd_ingester::Error),

    #[error("error initializing compactor: {0}")]
    Compactor(#[from] ioxd_compactor::Error),

    #[error("Invalid config: {0}")]
    InvalidConfig(#[from] CommonServerStateError),
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

/// The intention is to keep the number of options on this Config
/// object as small as possible. For more complex configurations and
/// deployments the individual services (e.g. Router and Compactor)
/// should be instantiated and configured individually.
///
/// This creates the following four services, configured to talk to a
/// common catalog, objectstore and write buffer.
///
/// ┌─────────────┐  ┌─────────────┐   ┌─────────────┐   ┌─────────────┐
/// │   Router    │  │  Ingester   │   │   Querier   │   │  Compactor  │
/// └─────────────┘  └──────┬──────┘   └──────┬──────┘   └──────┬──────┘
///        │                │                 │                 │
///        │                │                 │                 │
///        │                │                 │                 │
///        └────┬───────────┴─────┬───────────┴─────────┬───────┘
///             │                 │                     │
///             │                 │                     │
///          .──▼──.           .──▼──.               .──▼──.
///         (       )         (       )             (       )
///         │`─────'│         │`─────'│             │`─────'│
///         │       │         │       │             │       │
///         │.─────.│         │.─────.│             │.─────.│
///         (       )         (       )             (       )
///          `─────'           `─────'               `─────'
///
///         Existing         Object Store        kafka / redpanda
///         Postgres        (file, mem, or         Object Store
///        (started by      pre-existing)         (file, mem, or
///           user)        configured with        pre-existing)
///                         --object-store       configured with
///
/// Ideally all the gRPC services would listen on the same port, but
/// due to challenges with tonic this effort was postponed. See
/// <https://github.com/influxdata/influxdb_iox/issues/3926#issuecomment-1063231779>
/// for more details
///
/// Currently, the starts services on 5 ports, designed so that the
/// ports used to interact with the old architecture are the same as
/// the new architecture (8081 write endpoint and query on 8082).
///
/// Router;
///  8080 http  (overrides INFLUXDB_IOX_BIND_ADDR)
///  8081 grpc  (overrides INFLUXDB_IOX_GRPC_BIND_ADDR)
///
/// Querier:
///  8082 grpc
///
/// Ingester
///  8083 grpc
///
/// Compactor
///  8084 grpc
///
/// The reason it would be nicer to run all 4 sets of gRPC services
/// (router, compactor, querier, ingester) on the same ports is:
///
/// 1. It reduces the number of listening ports required
/// 2. It probably enforces good hygiene around API design
///
/// Some downsides are that different services would have to implement
/// distinct APIs (aka there can't be multiple 'write' apis) and some
/// general-purpose services (like the google job API or the health
/// endpoint) that are provided by multiple server modes need to be
/// specially handled.
#[derive(Debug, clap::Parser)]
#[clap(
    name = "all-in-one",
    about = "Runs in IOx All in One mode, containing router, ingester, compactor and querier."
)]
pub struct Config {
    /// logging options
    #[clap(flatten)]
    pub(crate) logging_config: LoggingConfig,

    /// tracing options
    #[clap(flatten)]
    pub(crate) tracing_config: TracingConfig,

    /// Maximum size of HTTP requests.
    #[clap(
        long = "--max-http-request-size",
        env = "INFLUXDB_IOX_MAX_HTTP_REQUEST_SIZE",
        default_value = "10485760" // 10 MiB
    )]
    pub max_http_request_size: usize,

    /// The location InfluxDB IOx will use to store files locally. If not specified, will run in
    /// ephemeral mode.
    #[clap(long = "--data-dir", env = "INFLUXDB_IOX_DB_DIR")]
    pub database_directory: Option<PathBuf>,

    /// Postgres connection string. If not specified, will use an in-memory catalog.
    #[clap(long = "--catalog-dsn", env = "INFLUXDB_IOX_CATALOG_DSN")]
    pub dsn: Option<String>,

    /// Schema name for PostgreSQL-based catalogs.
    #[clap(
        long = "--catalog-postgres-schema-name",
        env = "INFLUXDB_IOX_CATALOG_POSTGRES_SCHEMA_NAME",
        default_value = iox_catalog::postgres::PostgresConnectionOptions::DEFAULT_SCHEMA_NAME,
    )]
    pub postgres_schema_name: String,

    /// The ingester will continue to pull data and buffer it from the write buffer
    /// as long as it is below this size. If it hits this size it will pause
    /// ingest from the write buffer until persistence goes below this threshold.
    /// The default value is 100 GB (in bytes).
    #[clap(
        long = "--pause-ingest-size-bytes",
        env = "INFLUXDB_IOX_PAUSE_INGEST_SIZE_BYTES",
        default_value = "107374182400"
    )]
    pub pause_ingest_size_bytes: usize,

    /// Once the ingester crosses this threshold of data buffered across
    /// all sequencers, it will pick the largest partitions and persist
    /// them until it falls below this threshold. An ingester running in
    /// a steady state is expected to take up this much memory.
    /// The default value is 1 GB (in bytes).
    #[clap(
        long = "--persist-memory-threshold-bytes",
        env = "INFLUXDB_IOX_PERSIST_MEMORY_THRESHOLD_BYTES",
        default_value = "1073741824"
    )]
    pub persist_memory_threshold_bytes: usize,

    /// If an individual partition crosses this size threshold, it will be persisted.
    /// The default value is 300MB (in bytes).
    #[clap(
        long = "--persist-partition-size-threshold-bytes",
        env = "INFLUXDB_IOX_PERSIST_PARTITION_SIZE_THRESHOLD_BYTES",
        default_value = "314572800"
    )]
    pub persist_partition_size_threshold_bytes: usize,

    /// If a partition has had data buffered for longer than this period of time
    /// it will be persisted. This puts an upper bound on how far back the
    /// ingester may need to read in Kafka on restart or recovery. The default value
    /// is 30 minutes (in seconds).
    #[clap(
        long = "--persist-partition-age-threshold-seconds",
        env = "INFLUXDB_IOX_PERSIST_PARTITION_AGE_THRESHOLD_SECONDS",
        default_value = "1800"
    )]
    pub persist_partition_age_threshold_seconds: u64,

    /// If a partition has had data buffered and hasn't received a write for this
    /// period of time, it will be persisted. The default value is 300 seconds (5 minutes).
    #[clap(
        long = "--persist-partition-cold-threshold-seconds",
        env = "INFLUXDB_IOX_PERSIST_PARTITION_COLD_THRESHOLD_SECONDS",
        default_value = "300"
    )]
    pub persist_partition_cold_threshold_seconds: u64,

    /// The address on which IOx will serve Router HTTP API requests
    #[clap(
    long = "--router-http-bind",
    // by default, write API requests go to router
    alias = "api-bind",
    env = "INFLUXDB_IOX_ROUTER_HTTP_BIND_ADDR",
    default_value = DEFAULT_ROUTER_HTTP_BIND_ADDR,
    )]
    pub router_http_bind_address: SocketAddr,

    /// The address on which IOx will serve Router gRPC API requests
    #[clap(
    long = "--router-grpc-bind",
    env = "INFLUXDB_IOX_ROUTER_GRPC_BIND_ADDR",
    default_value = DEFAULT_ROUTER_GRPC_BIND_ADDR,
    )]
    pub router_grpc_bind_address: SocketAddr,

    /// The address on which IOx will serve Querier gRPC API requests
    #[clap(
    long = "--querier-grpc-bind",
    // by default, grpc requests go to querier
    alias = "grpc-bind",
    env = "INFLUXDB_IOX_QUERIER_GRPC_BIND_ADDR",
    default_value = DEFAULT_QUERIER_GRPC_BIND_ADDR,
    )]
    pub querier_grpc_bind_address: SocketAddr,

    /// The address on which IOx will serve Router Ingester API requests
    #[clap(
    long = "--ingester-grpc-bind",
    env = "INFLUXDB_IOX_INGESTER_GRPC_BIND_ADDR",
    default_value = DEFAULT_INGESTER_GRPC_BIND_ADDR,
    )]
    pub ingester_grpc_bind_address: SocketAddr,

    /// The address on which IOx will serve Router Compactor API requests
    #[clap(
    long = "--compactor-grpc-bind",
    env = "INFLUXDB_IOX_COMPACTOR_GRPC_BIND_ADDR",
    default_value = DEFAULT_COMPACTOR_GRPC_BIND_ADDR,
    )]
    pub compactor_grpc_bind_address: SocketAddr,

    /// Size of the querier RAM cache pool in bytes.
    #[clap(
        long = "--querier-ram-pool-bytes",
        env = "INFLUXDB_IOX_QUERIER_RAM_POOL_BYTES",
        default_value = "1073741824"
    )]
    pub querier_ram_pool_bytes: usize,
}

impl Config {
    /// Get a specialized run config to use for each service
    fn specialize(self) -> SpecializedConfig {
        let Self {
            logging_config,
            tracing_config,
            max_http_request_size,
            database_directory,
            dsn,
            postgres_schema_name,
            pause_ingest_size_bytes,
            persist_memory_threshold_bytes,
            persist_partition_size_threshold_bytes,
            persist_partition_age_threshold_seconds,
            persist_partition_cold_threshold_seconds,
            router_http_bind_address,
            router_grpc_bind_address,
            querier_grpc_bind_address,
            ingester_grpc_bind_address,
            compactor_grpc_bind_address,
            querier_ram_pool_bytes,
        } = self;

        let object_store_config = ObjectStoreConfig::new(database_directory.clone());
        let write_buffer_config = WriteBufferConfig::new(QUERY_POOL_NAME, database_directory);
        let catalog_dsn = dsn
            .map(|postgres_url| CatalogDsnConfig::new_postgres(postgres_url, postgres_schema_name))
            .unwrap_or_else(CatalogDsnConfig::new_memory);

        let router_run_config = RunConfig::new(
            logging_config,
            tracing_config,
            router_http_bind_address,
            router_grpc_bind_address,
            max_http_request_size,
            object_store_config,
        );

        let querier_run_config = router_run_config
            .clone()
            .with_grpc_bind_address(querier_grpc_bind_address);

        let ingester_run_config = router_run_config
            .clone()
            .with_grpc_bind_address(ingester_grpc_bind_address);

        let compactor_run_config = router_run_config
            .clone()
            .with_grpc_bind_address(compactor_grpc_bind_address);

        // All-in-one mode only supports one write buffer partition.
        let write_buffer_partition_range_start = 0;
        let write_buffer_partition_range_end = 0;

        // Use whatever data is available in the write buffer rather than erroring if the sequence
        // number has not been retained in the write buffer.
        let skip_to_oldest_available = true;

        let ingester_config = IngesterConfig {
            write_buffer_partition_range_start,
            write_buffer_partition_range_end,
            pause_ingest_size_bytes,
            persist_memory_threshold_bytes,
            persist_partition_size_threshold_bytes,
            persist_partition_age_threshold_seconds,
            persist_partition_cold_threshold_seconds,
            skip_to_oldest_available,
            test_flight_do_get_panic: 0,
            concurrent_request_limit: 10,
        };

        // create a CompactorConfig for the all in one server based on
        // settings from other configs. Can't use `#clap(flatten)` as the
        // parameters are redundant with ingester's
        let compactor_config = CompactorConfig {
            topic: QUERY_POOL_NAME.to_string(),
            write_buffer_partition_range_start,
            write_buffer_partition_range_end,
            split_percentage: 90,
            max_concurrent_compaction_size_bytes: 100000,
            compaction_max_size_bytes: 100000,
            compaction_max_file_count: 10,
        };

        let querier_config = QuerierConfig {
            num_query_threads: None,    // will be ignored
            ingester_addresses: vec![], // will be ignored
            ram_pool_bytes: querier_ram_pool_bytes,
        };

        SpecializedConfig {
            router_run_config,
            querier_run_config,

            ingester_run_config,
            compactor_run_config,

            catalog_dsn,
            write_buffer_config,
            ingester_config,
            compactor_config,
            querier_config,
        }
    }
}

/// Different run configs for the different services (needed as they
/// listen on different ports)
struct SpecializedConfig {
    router_run_config: RunConfig,
    querier_run_config: RunConfig,
    ingester_run_config: RunConfig,
    compactor_run_config: RunConfig,

    catalog_dsn: CatalogDsnConfig,
    write_buffer_config: WriteBufferConfig,
    ingester_config: IngesterConfig,
    compactor_config: CompactorConfig,
    querier_config: QuerierConfig,
}

pub async fn command(config: Config) -> Result<()> {
    let SpecializedConfig {
        router_run_config,
        querier_run_config,
        ingester_run_config,
        compactor_run_config,
        catalog_dsn,
        write_buffer_config,
        ingester_config,
        compactor_config,
        querier_config,
    } = config.specialize();

    let metrics = Arc::new(metric::Registry::default());

    let catalog = catalog_dsn
        .get_catalog("iox-all-in-one", Arc::clone(&metrics))
        .await?;

    // In the name of ease of use, automatically run db migrations in
    // all in one mode to ensure the database is ready.
    info!("running db migrations");
    catalog.setup().await?;

    // Create a topic
    catalog
        .repositories()
        .await
        .kafka_topics()
        .create_or_get(QUERY_POOL_NAME)
        .await?;

    let object_store: Arc<DynObjectStore> =
        make_object_store(router_run_config.object_store_config())
            .map_err(Error::ObjectStoreParsing)?;

    let time_provider: Arc<dyn TimeProvider> = Arc::new(SystemProvider::new());

    // create common state from the router and use it below
    let common_state = CommonServerState::from_config(router_run_config.clone())?;

    // TODO: make num_threads a parameter (other modes have it
    // configured by a command line)
    let num_threads = num_cpus::get();
    info!(%num_threads, "Creating shared query executor");
    let exec = Arc::new(Executor::new(num_threads));

    info!("starting router");
    let router = create_router_server_type(
        &common_state,
        Arc::clone(&metrics),
        Arc::clone(&catalog),
        Arc::clone(&object_store),
        &write_buffer_config,
        QUERY_POOL_NAME,
        1_000, // max 1,000 concurrent HTTP requests
    )
    .await?;

    info!("starting ingester");
    let ingester = create_ingester_server_type(
        &common_state,
        Arc::clone(&metrics),
        Arc::clone(&catalog),
        Arc::clone(&object_store),
        Arc::clone(&exec),
        &write_buffer_config,
        ingester_config,
    )
    .await?;

    info!("starting compactor");
    let compactor = create_compactor_server_type(
        &common_state,
        Arc::clone(&metrics),
        Arc::clone(&catalog),
        Arc::clone(&object_store),
        Arc::clone(&exec),
        Arc::clone(&time_provider),
        compactor_config,
    )
    .await?;

    let ingester_addresses = vec![format!("http://{}", ingester_run_config.grpc_bind_address)];
    info!(?ingester_addresses, "starting querier");
    let querier = create_querier_server_type(QuerierServerTypeArgs {
        common_state: &common_state,
        metric_registry: Arc::clone(&metrics),
        catalog,
        object_store,
        time_provider,
        exec,
        ingester_addresses,
        ram_pool_bytes: querier_config.ram_pool_bytes(),
    })
    .await;

    info!("starting all in one server");

    let services = vec![
        Service::create(router, &router_run_config),
        Service::create_grpc_only(ingester, &ingester_run_config),
        Service::create_grpc_only(compactor, &compactor_run_config),
        Service::create_grpc_only(querier, &querier_run_config),
    ];

    Ok(main::main(common_state, services, metrics).await?)
}
