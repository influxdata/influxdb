//! Implementation of command line option for running the compactor

use data_types2::SequencerId;
use object_store::ObjectStore;
use observability_deps::tracing::*;
use query::exec::Executor;
use std::sync::Arc;
use thiserror::Error;
use time::SystemProvider;

use clap_blocks::{catalog_dsn::CatalogDsnConfig, run_config::RunConfig};
use influxdb_ioxd::{
    self,
    server_type::{
        common_state::{CommonServerState, CommonServerStateError},
        compactor::create_compactor_server_type,
    },
    Service,
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
    about = "Runs in compactor mode",
    long_about = "Run the IOx compactor server.\n\nThe configuration options below can be \
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

    /// Number of threads to use for the compactor query execution, compaction and persistence.
    #[clap(
        long = "--query-exec-thread-count",
        env = "INFLUXDB_IOX_QUERY_EXEC_THREAD_COUNT",
        default_value = "4"
    )]
    pub query_exect_thread_count: usize,
}

impl Config {
    /// Get a reference to the config's run config.
    pub fn run_config(&self) -> &RunConfig {
        &self.run_config
    }
}

pub async fn command(config: Config) -> Result<(), Error> {
    let common_state = CommonServerState::from_config(config.run_config.clone())?;

    let metric_registry: Arc<metric::Registry> = Default::default();
    let catalog = config
        .catalog_dsn
        .get_catalog("compactor", Arc::clone(&metric_registry))
        .await?;

    let object_store = Arc::new(
        ObjectStore::try_from(config.run_config().object_store_config())
            .map_err(Error::ObjectStoreParsing)?,
    );

    let exec = Arc::new(Executor::new(config.query_exect_thread_count));
    let time_provider = Arc::new(SystemProvider::new());

    // TODO: modify config to let us get assigned sequence numbers
    let sequencers: Vec<SequencerId> = vec![];

    let server_type = create_compactor_server_type(
        &common_state,
        metric_registry,
        catalog,
        object_store,
        exec,
        time_provider,
        sequencers,
    )
    .await;

    info!("starting compactor");

    let services = vec![Service::create(server_type, common_state.run_config())];
    Ok(influxdb_ioxd::main(common_state, services).await?)
}
