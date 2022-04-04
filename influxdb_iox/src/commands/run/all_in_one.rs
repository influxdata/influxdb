//! Implementation of command line option for running all in one mode
use std::{num::NonZeroU32, sync::Arc};

use clap_blocks::compactor::CompactorConfig;
use clap_blocks::{
    catalog_dsn::CatalogDsnConfig,
    ingester::IngesterConfig,
    run_config::{RunConfig, DEFAULT_API_BIND_ADDR, DEFAULT_GRPC_BIND_ADDR},
    socket_addr::SocketAddr,
    write_buffer::WriteBufferConfig,
};
use ioxd_common::server_type::{CommonServerState, CommonServerStateError};
use ioxd_common::Service;
use ioxd_compactor::create_compactor_server_type;
use ioxd_ingester::create_ingester_server_type;
use ioxd_querier::create_querier_server_type;
use ioxd_router2::create_router2_server_type;
use object_store::{DynObjectStore, ObjectStoreImpl};
use observability_deps::tracing::*;
use query::exec::Executor;
use thiserror::Error;
use time::{SystemProvider, TimeProvider};

use super::main;

/// The default bind address for the Router HTTP API.
pub const DEFAULT_ROUTER_HTTP_BIND_ADDR: &str = "127.0.0.1:8080";

/// The default bind address for the Router gRPC.
pub const DEFAULT_ROUTER_GRPC_BIND_ADDR: &str = "127.0.0.1:8081";

/// The default bind address for the Querier gRPC (chosen to match default gRPC addr)
pub const DEFAULT_QUERIER_GRPC_BIND_ADDR: &str = "127.0.0.1:8082";

/// The default bind address for the Ingester gRPC (chosen to match default gRPC addr)
pub const DEFAULT_INGESTER_GRPC_BIND_ADDR: &str = "127.0.0.1:8083";

/// The default bind address for the Compactor gRPC (chosen to match default gRPC addr)
pub const DEFAULT_COMPACTOR_GRPC_BIND_ADDR: &str = "127.0.0.1:8084";

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

    #[error("Router2 error: {0}")]
    Router2(#[from] ioxd_router2::Error),

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
/// the new architecture (8082 write endpoint and query on 8082).
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
    #[clap(flatten)]
    pub(crate) run_config: RunConfig,

    #[clap(flatten)]
    pub(crate) catalog_dsn: CatalogDsnConfig,

    #[clap(flatten)]
    pub(crate) write_buffer_config: WriteBufferConfig,

    #[clap(flatten)]
    pub(crate) ingester_config: IngesterConfig,

    /// The address on which IOx will serve Router HTTP API requests
    #[clap(
    long = "--router-http-bind",
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
}

impl Config {
    /// Get a specialized run config to use for each service
    fn specialize(self) -> SpecializedConfig {
        let Self {
            run_config,
            catalog_dsn,
            write_buffer_config,
            ingester_config,
            router_http_bind_address,
            router_grpc_bind_address,
            querier_grpc_bind_address,
            ingester_grpc_bind_address,
            compactor_grpc_bind_address,
        } = self;

        if run_config.http_bind_address != DEFAULT_API_BIND_ADDR.parse().unwrap() {
            eprintln!("Warning: --http-bind-addr ignored in all in one mode");
        }
        if run_config.grpc_bind_address != DEFAULT_GRPC_BIND_ADDR.parse().unwrap() {
            eprintln!("Warning: --grpc-bind-addr ignored in all in one mode");
        }

        let router_run_config = run_config
            .clone()
            .with_http_bind_address(router_http_bind_address)
            .with_grpc_bind_address(router_grpc_bind_address);

        let querier_run_config = run_config
            .clone()
            .with_grpc_bind_address(querier_grpc_bind_address);

        let ingester_run_config = run_config
            .clone()
            .with_grpc_bind_address(ingester_grpc_bind_address);

        let compactor_run_config = run_config.with_grpc_bind_address(compactor_grpc_bind_address);

        // create a CompactorConfig for the all in one server based on
        // settings from other configs. Cant use `#clap(flatten)` as the
        // parameters are redundant with ingesters
        let compactor_config = CompactorConfig {
            topic: write_buffer_config.topic().to_string(),
            write_buffer_partition_range_start: ingester_config.write_buffer_partition_range_start,
            write_buffer_partition_range_end: ingester_config.write_buffer_partition_range_end,
            split_percentage: 90,
            max_concurrent_compaction_size_bytes: 100000,
            compaction_max_size_bytes: 100000,
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
}

pub async fn command(config: Config) -> Result<()> {
    let SpecializedConfig {
        router_run_config,
        querier_run_config,
        ingester_run_config,
        compactor_run_config,
        catalog_dsn,
        mut write_buffer_config,
        ingester_config,
        compactor_config,
    } = config.specialize();

    // Ensure at least one topic is automatically created in all in one mode
    write_buffer_config.set_auto_create_topics(Some(
        write_buffer_config.auto_create_topics().unwrap_or_else(|| {
            let default_config = NonZeroU32::new(1).unwrap();
            info!(
                ?default_config,
                "Automatically configuring creation of a single topic"
            );
            default_config
        }),
    ));

    // If you want this level of control, should be instatiating the
    // services individually
    let query_pool_name = "iox-shared";

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
        .create_or_get(query_pool_name)
        .await?;

    let object_store: Arc<DynObjectStore> = Arc::new(
        ObjectStoreImpl::try_from(router_run_config.object_store_config())
            .map_err(Error::ObjectStoreParsing)?,
    );

    let time_provider: Arc<dyn TimeProvider> = Arc::new(SystemProvider::new());

    // create common state from the router and use it below
    let common_state = CommonServerState::from_config(router_run_config.clone())?;

    // TODO: make num_threads a parameter (other modes have it
    // configured by a command line)
    let num_threads = num_cpus::get();
    info!(%num_threads, "Creating shared query executor");
    let exec = Arc::new(Executor::new(num_threads));

    info!("starting router2");
    let router2 = create_router2_server_type(
        &common_state,
        Arc::clone(&metrics),
        Arc::clone(&catalog),
        &write_buffer_config,
        query_pool_name,
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

    info!("starting querier");
    let querier = create_querier_server_type(
        &common_state,
        metrics,
        catalog,
        object_store,
        time_provider,
        exec,
    )
    .await;

    info!("starting all in one server");

    let services = vec![
        Service::create(router2, &router_run_config),
        Service::create_grpc_only(ingester, &ingester_run_config),
        Service::create_grpc_only(compactor, &compactor_run_config),
        Service::create_grpc_only(querier, &querier_run_config),
    ];

    Ok(main::main(common_state, services).await?)
}
