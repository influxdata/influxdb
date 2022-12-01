//! Command line options for running a router using the RPC write path.
use super::main;
use crate::process_info::setup_metric_registry;
use clap_blocks::{
    catalog_dsn::CatalogDsnConfig, object_store::make_object_store, run_config::RunConfig,
};
use iox_time::{SystemProvider, TimeProvider};
use ioxd_common::{
    server_type::{CommonServerState, CommonServerStateError},
    Service,
};
use ioxd_router::create_router_grpc_write_server_type;
use object_store::DynObjectStore;
use object_store_metrics::ObjectStoreMetrics;
use observability_deps::tracing::*;
use std::sync::Arc;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum Error {
    #[error("Run: {0}")]
    Run(#[from] main::Error),

    #[error("Invalid config: {0}")]
    InvalidConfig(#[from] CommonServerStateError),

    #[error("Cannot parse object store config: {0}")]
    ObjectStoreParsing(#[from] clap_blocks::object_store::ParseError),

    #[error("Creating router: {0}")]
    Router(#[from] ioxd_router::Error),

    #[error("Catalog DSN error: {0}")]
    CatalogDsn(#[from] clap_blocks::catalog_dsn::Error),
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

#[derive(Debug, clap::Parser)]
#[clap(
    name = "run",
    about = "Runs in router mode using the RPC write path",
    long_about = "Run the IOx router server.\n\nThe configuration options below can be \
    set either with the command line flags or with the specified environment \
    variable. If there is a file named '.env' in the current working directory, \
    it is sourced before loading the configuration.

Configuration is loaded from the following sources (highest precedence first):
        - command line arguments
        - user set environment variables
        - .env file contents
        - pre-configured default values"
)]
pub struct Config {
    #[clap(flatten)]
    pub(crate) run_config: RunConfig,

    #[clap(flatten)]
    pub(crate) catalog_dsn: CatalogDsnConfig,

    /// The maximum number of simultaneous requests the HTTP server is
    /// configured to accept.
    ///
    /// This number of requests, multiplied by the maximum request body size the
    /// HTTP server is configured with gives the rough amount of memory a HTTP
    /// server will use to buffer request bodies in memory.
    ///
    /// A default maximum of 200 requests, multiplied by the default 10MiB
    /// maximum for HTTP request bodies == ~2GiB.
    #[clap(
        long = "max-http-requests",
        env = "INFLUXDB_IOX_MAX_HTTP_REQUESTS",
        default_value = "200",
        action
    )]
    pub(crate) http_request_limit: usize,

    /// gRPC address for the router to talk with the ingesters. For
    /// example:
    ///
    /// "http://127.0.0.1:8083"
    ///
    /// or
    ///
    /// "http://10.10.10.1:8083,http://10.10.10.2:8083"
    ///
    /// for multiple addresses.
    #[clap(
        long = "ingester-addresses",
        env = "INFLUXDB_IOX_INGESTER_ADDRESSES",
        required = true
    )]
    pub(crate) ingester_addresses: Vec<String>,

    /// Write buffer topic/database that should be used.
    // This isn't really relevant to the RPC write path and will be removed eventually.
    #[clap(
        long = "write-buffer-topic",
        env = "INFLUXDB_IOX_WRITE_BUFFER_TOPIC",
        default_value = "iox-shared",
        action
    )]
    pub(crate) topic: String,

    /// Query pool name to dispatch writes to.
    // This isn't really relevant to the RPC write path and will be removed eventually.
    #[clap(
        long = "query-pool",
        env = "INFLUXDB_IOX_QUERY_POOL_NAME",
        default_value = "iox-shared",
        action
    )]
    pub(crate) query_pool_name: String,

    /// Retention period to use when auto-creating namespaces.
    /// For infinite retention, leave this unset and it will default to `None`.
    /// Setting it to zero will not make it infinite.
    #[clap(
        long = "new-namespace-retention-hours",
        env = "INFLUXDB_IOX_NEW_NAMESPACE_RETENTION_HOURS",
        action
    )]
    pub(crate) new_namespace_retention_hours: Option<u64>,
}

pub async fn command(config: Config) -> Result<()> {
    let common_state = CommonServerState::from_config(config.run_config.clone())?;
    let time_provider = Arc::new(SystemProvider::new()) as Arc<dyn TimeProvider>;
    let metrics = setup_metric_registry();

    let catalog = config
        .catalog_dsn
        .get_catalog("router_rpc_write", Arc::clone(&metrics))
        .await?;

    let object_store = make_object_store(config.run_config.object_store_config())
        .map_err(Error::ObjectStoreParsing)?;
    // Decorate the object store with a metric recorder.
    let object_store: Arc<DynObjectStore> = Arc::new(ObjectStoreMetrics::new(
        object_store,
        time_provider,
        &metrics,
    ));

    let server_type = create_router_grpc_write_server_type(
        &common_state,
        Arc::clone(&metrics),
        catalog,
        object_store,
        config.http_request_limit,
        &config.ingester_addresses,
        config.topic.as_ref(),
        config.query_pool_name.as_ref(),
        config.new_namespace_retention_hours,
    )
    .await?;

    info!("starting router_rpc_write");
    let services = vec![Service::create(server_type, common_state.run_config())];
    Ok(main::main(common_state, services, metrics).await?)
}
