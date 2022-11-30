//! Command line options for running an ingester for a router using the RPC write path to talk to.

use clap_blocks::run_config::RunConfig;
use observability_deps::tracing::*;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum Error {}

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
}

pub async fn command(_config: Config) -> Result<()> {
    info!("starting ingester2");

    Ok(())
}
