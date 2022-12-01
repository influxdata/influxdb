//! Command line options for running an ingester for a router using the RPC write path to talk to.

use super::main;
use crate::process_info::setup_metric_registry;
use clap_blocks::{
    catalog_dsn::CatalogDsnConfig, ingester2::Ingester2Config, run_config::RunConfig,
};
use ioxd_common::{
    server_type::{CommonServerState, CommonServerStateError},
    Service,
};
use ioxd_ingester2::create_ingester_server_type;
use observability_deps::tracing::*;
use std::sync::Arc;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum Error {
    #[error("Run: {0}")]
    Run(#[from] main::Error),

    #[error("Invalid config: {0}")]
    InvalidConfig(#[from] CommonServerStateError),

    #[error("error initializing ingester2: {0}")]
    Ingester(#[from] ioxd_ingester2::Error),

    #[error("Catalog DSN error: {0}")]
    CatalogDsn(#[from] clap_blocks::catalog_dsn::Error),
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

#[derive(Debug, clap::Parser)]
#[clap(
    name = "run",
    about = "Runs in ingester mode",
    long_about = "Run the IOx ingester server.\n\nThe configuration options below can be \
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
    pub(crate) ingester_config: Ingester2Config,
}

pub async fn command(config: Config) -> Result<()> {
    let common_state = CommonServerState::from_config(config.run_config.clone())?;
    let metric_registry = setup_metric_registry();

    let catalog = config
        .catalog_dsn
        .get_catalog("ingester", Arc::clone(&metric_registry))
        .await?;

    let server_type = create_ingester_server_type(
        &common_state,
        catalog,
        Arc::clone(&metric_registry),
        &config.ingester_config,
    )
    .await?;

    info!("starting ingester2");

    let services = vec![Service::create(server_type, common_state.run_config())];
    Ok(main::main(common_state, services, metric_registry).await?)
}
