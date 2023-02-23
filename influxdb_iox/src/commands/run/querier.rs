//! Implementation of command line option for running the querier

use crate::process_info::setup_metric_registry;

use super::main;
use clap_blocks::{
    catalog_dsn::CatalogDsnConfig, object_store::make_object_store, querier::QuerierConfig,
    run_config::RunConfig,
};
use iox_query::exec::Executor;
use iox_time::{SystemProvider, TimeProvider};
use ioxd_common::{
    server_type::{CommonServerState, CommonServerStateError},
    Service,
};
use ioxd_querier::{create_querier_server_type, QuerierServerTypeArgs};
use object_store::DynObjectStore;
use object_store_metrics::ObjectStoreMetrics;
use observability_deps::tracing::*;
use std::{num::NonZeroUsize, sync::Arc};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum Error {
    #[error("Run: {0}")]
    Run(#[from] main::Error),

    #[error("Invalid config: {0}")]
    InvalidConfigCommon(#[from] CommonServerStateError),

    #[error("Invalid config: {0}")]
    InvalidConfigIngester(#[from] clap_blocks::querier::Error),

    #[error("Catalog error: {0}")]
    Catalog(#[from] iox_catalog::interface::Error),

    #[error("Catalog DSN error: {0}")]
    CatalogDsn(#[from] clap_blocks::catalog_dsn::Error),

    #[error("Cannot parse object store config: {0}")]
    ObjectStoreParsing(#[from] clap_blocks::object_store::ParseError),

    #[error("Querier error: {0}")]
    Querier(#[from] ioxd_querier::Error),
}

#[derive(Debug, clap::Parser)]
#[clap(
    name = "run",
    about = "Runs in querier mode",
    long_about = "Run the IOx querier server.\n\nThe configuration options below can be \
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
    pub(crate) querier_config: QuerierConfig,
}

pub async fn command(config: Config) -> Result<(), Error> {
    let common_state = CommonServerState::from_config(config.run_config.clone())?;

    let time_provider = Arc::new(SystemProvider::new()) as Arc<dyn TimeProvider>;
    let metric_registry = setup_metric_registry();

    let catalog = config
        .catalog_dsn
        .get_catalog("querier", Arc::clone(&metric_registry))
        .await?;

    let object_store = make_object_store(config.run_config.object_store_config())
        .map_err(Error::ObjectStoreParsing)?;
    // Decorate the object store with a metric recorder.
    let object_store: Arc<DynObjectStore> = Arc::new(ObjectStoreMetrics::new(
        object_store,
        Arc::clone(&time_provider),
        &metric_registry,
    ));

    let time_provider = Arc::new(SystemProvider::new());

    let num_query_threads = config.querier_config.num_query_threads();
    let num_threads = num_query_threads.unwrap_or_else(|| {
        NonZeroUsize::new(num_cpus::get()).unwrap_or_else(|| NonZeroUsize::new(1).unwrap())
    });
    info!(%num_threads, "using specified number of threads per thread pool");

    let rpc_write = std::env::var("INFLUXDB_IOX_RPC_MODE").is_ok();
    if rpc_write {
        info!("using the RPC write path");
    } else {
        info!("using the write buffer path");
    }

    let ingester_addresses = config.querier_config.ingester_addresses()?;
    info!(?ingester_addresses, "using ingester addresses");

    let exec = Arc::new(Executor::new(
        num_threads,
        config.querier_config.exec_mem_pool_bytes,
    ));

    let server_type = create_querier_server_type(QuerierServerTypeArgs {
        common_state: &common_state,
        metric_registry: Arc::clone(&metric_registry),
        catalog,
        object_store,
        exec,
        time_provider,
        ingester_addresses,
        querier_config: config.querier_config,
        rpc_write,
    })
    .await?;

    info!("starting querier");

    let services = vec![Service::create(server_type, common_state.run_config())];
    Ok(main::main(common_state, services, metric_registry).await?)
}
