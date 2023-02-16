//! Implementation of command line option for running all in one mode

use crate::process_info::setup_metric_registry;

use super::main;
use clap_blocks::{
    catalog_dsn::CatalogDsnConfig,
    compactor2::{Compactor2Config, CompactorAlgoVersion},
    ingester2::Ingester2Config,
    ingester_address::IngesterAddress,
    object_store::{make_object_store, ObjectStoreConfig},
    querier::{IngesterAddresses, QuerierConfig},
    router2::Router2Config,
    run_config::RunConfig,
    socket_addr::SocketAddr,
};
use compactor2::object_store::metrics::MetricsStore;
use iox_query::exec::{Executor, ExecutorConfig};
use iox_time::{SystemProvider, TimeProvider};
use ioxd_common::{
    server_type::{CommonServerState, CommonServerStateError},
    Service,
};
use ioxd_compactor2::create_compactor2_server_type as create_compactor_server_type;
use ioxd_ingester2::create_ingester_server_type;
use ioxd_querier::{create_querier_server_type, QuerierServerTypeArgs};
use ioxd_router::create_router2_server_type;
use object_store::DynObjectStore;
use observability_deps::tracing::*;
use once_cell::sync::Lazy;
use parquet_file::storage::{ParquetStorage, StorageId};
use std::{
    collections::HashMap, num::NonZeroUsize, path::PathBuf, str::FromStr, sync::Arc, time::Duration,
};
use tempfile::TempDir;
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

    #[error("Querier error: {0}")]
    Querier(#[from] ioxd_querier::Error),

    #[error("Invalid config: {0}")]
    InvalidConfig(#[from] CommonServerStateError),
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

/// Use a static so the directory lasts the entire process
static DEFAULT_WAL_DIRECTORY: Lazy<TempDir> =
    Lazy::new(|| TempDir::new().expect("Could not create a temporary directory for WAL"));

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
#[group(skip)]
pub struct Config {
    /// logging options
    #[clap(flatten)]
    pub(crate) logging_config: LoggingConfig,

    /// tracing options
    #[clap(flatten)]
    pub(crate) tracing_config: TracingConfig,

    /// Maximum size of HTTP requests.
    #[clap(
        long = "max-http-request-size",
        env = "INFLUXDB_IOX_MAX_HTTP_REQUEST_SIZE",
        default_value = "10485760", // 10 MiB
        action,
    )]
    pub max_http_request_size: usize,

    #[clap(flatten)]
    object_store_config: ObjectStoreConfig,

    #[clap(flatten)]
    catalog_dsn: CatalogDsnConfig,

    /// The number of seconds between WAL file rotations.
    #[clap(
        long = "wal-rotation-period-seconds",
        env = "INFLUXDB_IOX_WAL_ROTATION_PERIOD_SECONDS",
        default_value = "300",
        action
    )]
    pub wal_rotation_period_seconds: u64,

    /// Sets how many queries the ingester will handle simultaneously before
    /// rejecting further incoming requests.
    #[clap(
        long = "concurrent-query-limit",
        env = "INFLUXDB_IOX_CONCURRENT_QUERY_LIMIT",
        default_value = "20",
        action
    )]
    pub concurrent_query_limit: usize,

    /// The maximum number of persist tasks that can run simultaneously.
    #[clap(
        long = "persist-max-parallelism",
        env = "INFLUXDB_IOX_PERSIST_MAX_PARALLELISM",
        default_value = "5",
        action
    )]
    pub persist_max_parallelism: usize,

    /// The maximum number of persist tasks that can be queued at any one time.
    ///
    /// Once this limit is reached, ingest is blocked until the persist backlog
    /// is reduced.
    #[clap(
        long = "persist-queue-depth",
        env = "INFLUXDB_IOX_PERSIST_QUEUE_DEPTH",
        default_value = "250",
        action
    )]
    pub persist_queue_depth: usize,

    /// The limit at which a partition's estimated persistence cost causes it to
    /// be queued for persistence.
    #[clap(
        long = "persist-hot-partition-cost",
        env = "INFLUXDB_IOX_PERSIST_HOT_PARTITION_COST",
        default_value = "20000000", // 20,000,000
        action
    )]
    pub persist_hot_partition_cost: usize,

    /// The address on which IOx will serve Router HTTP API requests
    #[clap(
        long = "router-http-bind",
        // by default, write API requests go to router
        alias = "api-bind",
        env = "INFLUXDB_IOX_ROUTER_HTTP_BIND_ADDR",
        default_value = DEFAULT_ROUTER_HTTP_BIND_ADDR,
        action,
    )]
    pub router_http_bind_address: SocketAddr,

    /// The address on which IOx will serve Router gRPC API requests
    #[clap(
        long = "router-grpc-bind",
        env = "INFLUXDB_IOX_ROUTER_GRPC_BIND_ADDR",
        default_value = DEFAULT_ROUTER_GRPC_BIND_ADDR,
        action,
    )]
    pub router_grpc_bind_address: SocketAddr,

    /// The address on which IOx will serve Querier gRPC API requests
    #[clap(
        long = "querier-grpc-bind",
        // by default, grpc requests go to querier
        alias = "grpc-bind",
        env = "INFLUXDB_IOX_QUERIER_GRPC_BIND_ADDR",
        default_value = DEFAULT_QUERIER_GRPC_BIND_ADDR,
        action,
    )]
    pub querier_grpc_bind_address: SocketAddr,

    /// The address on which IOx will serve Router Ingester API requests
    #[clap(
        long = "ingester-grpc-bind",
        env = "INFLUXDB_IOX_INGESTER_GRPC_BIND_ADDR",
        default_value = DEFAULT_INGESTER_GRPC_BIND_ADDR,
        action,
    )]
    pub ingester_grpc_bind_address: SocketAddr,

    /// The address on which IOx will serve Router Compactor API requests
    #[clap(
        long = "compactor-grpc-bind",
        env = "INFLUXDB_IOX_COMPACTOR_GRPC_BIND_ADDR",
        default_value = DEFAULT_COMPACTOR_GRPC_BIND_ADDR,
        action,
    )]
    pub compactor_grpc_bind_address: SocketAddr,

    /// Size of the querier RAM cache used to store catalog metadata information in bytes.
    #[clap(
        long = "querier-ram-pool-metadata-bytes",
        env = "INFLUXDB_IOX_QUERIER_RAM_POOL_METADATA_BYTES",
        default_value = "134217728",  // 128MB
        action
    )]
    pub querier_ram_pool_metadata_bytes: usize,

    /// Size of the querier RAM cache used to store data in bytes.
    #[clap(
        long = "querier-ram-pool-data-bytes",
        env = "INFLUXDB_IOX_QUERIER_RAM_POOL_DATA_BYTES",
        default_value = "1073741824",  // 1GB
        action
    )]
    pub querier_ram_pool_data_bytes: usize,

    /// Limit the number of concurrent queries.
    #[clap(
        long = "querier-max-concurrent-queries",
        env = "INFLUXDB_IOX_QUERIER_MAX_CONCURRENT_QUERIES",
        default_value = "10",
        action
    )]
    pub querier_max_concurrent_queries: usize,

    /// Size of memory pool used during query exec, in bytes.
    #[clap(
        long = "exec-mem-pool-bytes",
        env = "INFLUXDB_IOX_EXEC_MEM_POOL_BYTES",
        default_value = "8589934592",  // 8GB
        action
    )]
    pub exec_mem_pool_bytes: usize,
}

impl Config {
    /// Get a specialized run config to use for each service
    fn specialize(self) -> SpecializedConfig {
        let Self {
            logging_config,
            tracing_config,
            max_http_request_size,
            object_store_config,
            catalog_dsn,
            wal_rotation_period_seconds,
            concurrent_query_limit,
            persist_max_parallelism,
            persist_queue_depth,
            persist_hot_partition_cost,
            router_http_bind_address,
            router_grpc_bind_address,
            querier_grpc_bind_address,
            ingester_grpc_bind_address,
            compactor_grpc_bind_address,
            querier_ram_pool_metadata_bytes,
            querier_ram_pool_data_bytes,
            querier_max_concurrent_queries,
            exec_mem_pool_bytes,
        } = self;

        let database_directory = object_store_config.database_directory.clone();
        // If dir location is provided and no object store type is provided, use it also as file object store.
        let object_store_config = {
            if object_store_config.object_store.is_none() && database_directory.is_some() {
                ObjectStoreConfig::new(database_directory.clone())
            } else {
                object_store_config
            }
        };

        // if data directory is specified, default to data-dir/wal
        // otherwise use a per-process temporary directory
        let wal_directory = database_directory
            .map(|database_directory| database_directory.join("wal"))
            .unwrap_or_else(|| PathBuf::from(DEFAULT_WAL_DIRECTORY.path()));

        let catalog_dsn = if catalog_dsn.dsn.is_none() {
            CatalogDsnConfig::new_memory()
        } else {
            catalog_dsn
        };

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

        let ingester_config = Ingester2Config {
            wal_directory,
            wal_rotation_period_seconds,
            concurrent_query_limit,
            persist_max_parallelism,
            persist_queue_depth,
            persist_hot_partition_cost,
        };

        let router_config = Router2Config {
            query_pool_name: QUERY_POOL_NAME.to_string(),
            http_request_limit: 1_000,
            ingester_addresses: vec![IngesterAddress::from_str(
                &ingester_grpc_bind_address.to_string(),
            )
            .unwrap()],
            new_namespace_retention_hours: None, // infinite retention
            namespace_autocreation_enabled: true,
            partition_key_pattern: "%Y-%m-%d".to_string(),
            topic: QUERY_POOL_NAME.to_string(),
            rpc_write_timeout_seconds: Duration::new(3, 0),
        };

        // create a CompactorConfig for the all in one server based on
        // settings from other configs. Can't use `#clap(flatten)` as the
        // parameters are redundant with ingester's
        let compactor_config = Compactor2Config {
            compaction_partition_concurrency: NonZeroUsize::new(1).unwrap(),
            compaction_job_concurrency: NonZeroUsize::new(1).unwrap(),
            compaction_partition_scratchpad_concurrency: NonZeroUsize::new(1).unwrap(),
            compaction_partition_minute_threshold: 10,
            query_exec_thread_count: 1,
            exec_mem_pool_bytes,
            max_desired_file_size_bytes: 30_000,
            percentage_max_file_size: 30,
            split_percentage: 80,
            partition_timeout_secs: 0,
            partition_filter: None,
            shadow_mode: false,
            ignore_partition_skip_marker: false,
            max_input_parquet_bytes_per_partition: 268_435_456, // 256 MB
            shard_count: None,
            shard_id: None,
            compact_version: CompactorAlgoVersion::AllAtOnce,
            min_num_l1_files_to_compact: 1,
            process_once: false,
            process_all_partitions: false,
            max_num_columns_per_table: 200,
            max_num_files_per_plan: 200,
        };

        let querier_config = QuerierConfig {
            num_query_threads: None,       // will be ignored
            shard_to_ingesters_file: None, // will be ignored
            shard_to_ingesters: None,      // will be ignored
            ingester_addresses: vec![ingester_grpc_bind_address.to_string()], // will be ignored
            ram_pool_metadata_bytes: querier_ram_pool_metadata_bytes,
            ram_pool_data_bytes: querier_ram_pool_data_bytes,
            max_concurrent_queries: querier_max_concurrent_queries,
            exec_mem_pool_bytes,
            ingester_circuit_breaker_threshold: u64::MAX, // never for all-in-one-mode
        };

        SpecializedConfig {
            router_run_config,
            querier_run_config,

            ingester_run_config,
            compactor_run_config,

            catalog_dsn,
            ingester_config,
            router_config,
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
    ingester_config: Ingester2Config,
    router_config: Router2Config,
    compactor_config: Compactor2Config,
    querier_config: QuerierConfig,
}

pub async fn command(config: Config) -> Result<()> {
    let SpecializedConfig {
        router_run_config,
        querier_run_config,
        ingester_run_config,
        compactor_run_config,
        catalog_dsn,
        ingester_config,
        router_config,
        compactor_config,
        querier_config,
    } = config.specialize();

    let metrics = setup_metric_registry();

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
        .topics()
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

    let parquet_store_real = ParquetStorage::new(Arc::clone(&object_store), StorageId::from("iox"));
    let exec = Arc::new(Executor::new_with_config(ExecutorConfig {
        num_threads,
        target_query_partitions: num_threads,
        object_stores: HashMap::from([(
            parquet_store_real.id(),
            Arc::clone(parquet_store_real.object_store()),
        )]),
        mem_pool_size: querier_config.exec_mem_pool_bytes,
    }));

    info!("starting router");
    let router = create_router2_server_type(
        &common_state,
        Arc::clone(&metrics),
        Arc::clone(&catalog),
        Arc::clone(&object_store),
        &router_config,
    )
    .await?;

    info!("starting ingester");
    let ingester = create_ingester_server_type(
        &common_state,
        Arc::clone(&catalog),
        Arc::clone(&metrics),
        &ingester_config,
        Arc::clone(&exec),
        parquet_store_real.clone(),
    )
    .await
    .expect("failed to start ingester");

    info!("starting compactor");
    let parquet_store_scratchpad = ParquetStorage::new(
        Arc::new(MetricsStore::new(
            Arc::new(object_store::memory::InMemory::new()),
            &metrics,
            "scratchpad",
        )),
        StorageId::from("iox_scratchpad"),
    );

    let compactor = create_compactor_server_type(
        &common_state,
        Arc::clone(&metrics),
        Arc::clone(&catalog),
        parquet_store_real,
        parquet_store_scratchpad,
        Arc::clone(&exec),
        Arc::clone(&time_provider),
        compactor_config,
    )
    .await;

    let ingester_addresses = IngesterAddresses::List(vec![IngesterAddress::from_str(
        &ingester_run_config.grpc_bind_address.to_string(),
    )
    .unwrap()]);

    info!(?ingester_addresses, "starting querier");
    let querier = create_querier_server_type(QuerierServerTypeArgs {
        common_state: &common_state,
        metric_registry: Arc::clone(&metrics),
        catalog,
        object_store,
        exec,
        time_provider,
        ingester_addresses,
        querier_config,
        rpc_write: true,
    })
    .await?;

    info!("starting all in one server");

    let services = vec![
        Service::create(router, &router_run_config),
        Service::create_grpc_only(ingester, &ingester_run_config),
        Service::create_grpc_only(compactor, &compactor_run_config),
        Service::create_grpc_only(querier, &querier_run_config),
    ];

    Ok(main::main(common_state, services, metrics).await?)
}
