//! Implementation of command line option for running router2

use std::sync::Arc;

use crate::{
    clap_blocks::run_config::RunConfig,
    influxdb_ioxd::{
        self,
        server_type::{
            common_state::{CommonServerState, CommonServerStateError},
            router2::RouterServerType,
        },
    },
};
use observability_deps::tracing::*;
use router2::{
    dml_handler::nop::NopDmlHandler,
    server::{http::HttpDelegate, RouterServer},
};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum Error {
    #[error("Run: {0}")]
    Run(#[from] influxdb_ioxd::Error),

    #[error("Cannot setup server: {0}")]
    Setup(#[from] crate::influxdb_ioxd::server_type::database::setup::Error),

    #[error("Invalid config: {0}")]
    InvalidConfig(#[from] CommonServerStateError),
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
}

pub async fn command(config: Config) -> Result<()> {
    let common_state = CommonServerState::from_config(config.run_config.clone())?;

    let http = HttpDelegate::new(
        config.run_config.max_http_request_size,
        NopDmlHandler::default(),
    );
    let router_server = RouterServer::new(http, Default::default());
    let server_type = Arc::new(RouterServerType::new(router_server, &common_state));

    info!("starting router2");

    Ok(influxdb_ioxd::main(common_state, server_type).await?)
}
