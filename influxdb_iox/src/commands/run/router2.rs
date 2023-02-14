//! Command line options for running a router2 that uses the RPC write path.
use super::main;
use crate::process_info::setup_metric_registry;
use clap_blocks::{
    catalog_dsn::CatalogDsnConfig, object_store::make_object_store, router2::Router2Config,
    run_config::RunConfig,
};
use iox_time::{SystemProvider, TimeProvider};
use ioxd_common::{
    server_type::{CommonServerState, CommonServerStateError},
    Service,
};
use ioxd_router::create_router2_server_type;
use object_store::DynObjectStore;
use object_store_metrics::ObjectStoreMetrics;
use observability_deps::tracing::*;
use panic_logging::make_panics_fatal;
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

    #[clap(flatten)]
    pub(crate) router_config: Router2Config,
}

pub async fn command(config: Config) -> Result<()> {
    if std::env::var("INFLUXDB_IOX_RPC_MODE").is_err() {
        panic!(
            "`INFLUXDB_IOX_RPC_MODE` was not specified but `router2` was the command run. Either set
             `INFLUXDB_IOX_RPC_MODE` or run the `router` command."
        );
    }

    // Ensure panics (even in threads or tokio tasks) are fatal when
    // running in this server mode.  This is done to avoid potential
    // data corruption because there is no foolproof way to recover
    // state after a panic.
    make_panics_fatal();

    let common_state = CommonServerState::from_config(config.run_config.clone())?;
    let time_provider = Arc::new(SystemProvider::new()) as Arc<dyn TimeProvider>;
    let metrics = setup_metric_registry();

    let catalog = config
        .catalog_dsn
        .get_catalog("router2", Arc::clone(&metrics))
        .await?;

    let object_store = make_object_store(config.run_config.object_store_config())
        .map_err(Error::ObjectStoreParsing)?;
    // Decorate the object store with a metric recorder.
    let object_store: Arc<DynObjectStore> = Arc::new(ObjectStoreMetrics::new(
        object_store,
        time_provider,
        &metrics,
    ));

    let server_type = create_router2_server_type(
        &common_state,
        Arc::clone(&metrics),
        catalog,
        object_store,
        &config.router_config,
    )
    .await?;

    info!("starting router2");
    let services = vec![Service::create(server_type, common_state.run_config())];
    Ok(main::main(common_state, services, metrics).await?)
}
