//! Implementation of command line option for running router2

use std::sync::Arc;

use clap_blocks::{
    catalog_dsn::CatalogDsnConfig, run_config::RunConfig, write_buffer::WriteBufferConfig,
};
use influxdb_ioxd::{
    self,
    server_type::{
        common_state::{CommonServerState, CommonServerStateError},
        router2::create_router2_server_type,
    },
};
use observability_deps::tracing::*;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum Error {
    #[error("Run: {0}")]
    Run(#[from] influxdb_ioxd::Error),

    #[error("Invalid config: {0}")]
    InvalidConfig(#[from] CommonServerStateError),

    #[error("Creating router: {0}")]
    Router(#[from] influxdb_ioxd::server_type::router2::Error),

    #[error("Catalog DSN error: {0}")]
    CatalogDsn(#[from] clap_blocks::catalog_dsn::Error),
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

#[derive(Debug, clap::Parser)]
#[clap(
    name = "run",
    about = "Runs in router2 mode",
    long_about = "Run the IOx router2 server.\n\nThe configuration options below can be \
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
    pub(crate) write_buffer_config: WriteBufferConfig,

    /// Query pool name to dispatch writes to.
    #[clap(
        long = "--query-pool",
        env = "INFLUXDB_IOX_QUERY_POOL_NAME",
        default_value = "iox-shared"
    )]
    pub(crate) query_pool_name: String,
}

pub async fn command(config: Config) -> Result<()> {
    let common_state = CommonServerState::from_config(config.run_config.clone())?;
    let metrics = Arc::new(metric::Registry::default());

    let catalog = config
        .catalog_dsn
        .get_catalog("router2", Arc::clone(&metrics))
        .await?;

    let server_type = create_router2_server_type(
        &common_state,
        Arc::clone(&metrics),
        catalog,
        &config.write_buffer_config,
        &config.query_pool_name,
    )
    .await?;

    info!("starting router2");
    Ok(influxdb_ioxd::main(common_state, server_type).await?)
}
