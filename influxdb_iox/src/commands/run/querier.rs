//! Implementation of command line option for running the querier

use object_store::ObjectStore;
use observability_deps::tracing::*;
use querier::{database::QuerierDatabase, handler::QuerierHandlerImpl, server::QuerierServer};
use query::exec::Executor;
use std::sync::Arc;
use thiserror::Error;
use time::SystemProvider;

use clap_blocks::{catalog_dsn::CatalogDsnConfig, run_config::RunConfig};
use influxdb_ioxd::{
    self,
    server_type::common_state::{CommonServerState, CommonServerStateError},
    server_type::querier::QuerierServerType,
};

#[derive(Debug, Error)]
pub enum Error {
    #[error("Run: {0}")]
    Run(#[from] influxdb_ioxd::Error),

    #[error("Invalid config: {0}")]
    InvalidConfig(#[from] CommonServerStateError),

    #[error("Catalog error: {0}")]
    Catalog(#[from] iox_catalog::interface::Error),

    #[error("Catalog DSN error: {0}")]
    CatalogDsn(#[from] clap_blocks::catalog_dsn::Error),

    #[error("Cannot parse object store config: {0}")]
    ObjectStoreParsing(#[from] clap_blocks::object_store::ParseError),
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

    /// The number of threads to use for queries.
    ///
    /// If not specified, defaults to the number of cores on the system
    #[clap(long = "--num-query-threads", env = "INFLUXDB_IOX_NUM_QUERY_THREADS")]
    pub num_query_threads: Option<usize>,
}

pub async fn command(config: Config) -> Result<(), Error> {
    let common_state = CommonServerState::from_config(config.run_config.clone())?;

    let metric_registry: Arc<metric::Registry> = Default::default();
    let catalog = config
        .catalog_dsn
        .get_catalog("querier", Arc::clone(&metric_registry))
        .await?;

    let object_store = Arc::new(
        ObjectStore::try_from(config.run_config.object_store_config())
            .map_err(Error::ObjectStoreParsing)?,
    );

    let time_provider = Arc::new(SystemProvider::new());

    let num_threads = config.num_query_threads.unwrap_or_else(num_cpus::get);
    info!(%num_threads, "using specified number of threads per thread pool");

    let exec = Arc::new(Executor::new(num_threads));
    let database = Arc::new(QuerierDatabase::new(
        catalog,
        Arc::clone(&metric_registry),
        object_store,
        time_provider,
        exec,
    ));
    let querier_handler = Arc::new(QuerierHandlerImpl::new(Arc::clone(&database)));

    let querier = QuerierServer::new(metric_registry, querier_handler);
    let server_type = Arc::new(QuerierServerType::new(querier, database, &common_state));

    info!("starting querier");

    Ok(influxdb_ioxd::main(common_state, server_type).await?)
}
