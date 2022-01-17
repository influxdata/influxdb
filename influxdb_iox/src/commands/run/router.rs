//! Implementation of command line option for running server

use std::sync::Arc;

use crate::{
    clap_blocks::run_config::RunConfig,
    influxdb_ioxd::{
        self,
        server_type::{
            common_state::{CommonServerState, CommonServerStateError},
            router::RouterServerType,
        },
    },
};
use observability_deps::tracing::warn;
use router::{resolver::RemoteTemplate, server::RouterServer};
use thiserror::Error;
use time::SystemProvider;

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
    about = "Runs in router mode",
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

    /// When IOx nodes need to talk to remote peers they consult an internal remote address
    /// mapping. This mapping is populated via API calls. If the mapping doesn't produce
    /// a result, this config entry allows to generate a hostname from at template:
    /// occurrences of the "{id}" substring will be replaced with the remote Server ID.
    ///
    /// Example: http://node-{id}.ioxmydomain.com:8082
    #[clap(long = "--remote-template", env = "INFLUXDB_IOX_REMOTE_TEMPLATE")]
    pub remote_template: Option<String>,
}

pub async fn command(config: Config) -> Result<()> {
    let common_state = CommonServerState::from_config(config.run_config.clone())?;

    let remote_template = config.remote_template.map(RemoteTemplate::new);
    let time_provider = Arc::new(SystemProvider::new());
    let router_server = Arc::new(
        RouterServer::new(
            remote_template,
            common_state.trace_collector(),
            time_provider,
        )
        .await,
    );
    if let Some(id) = config.run_config.server_id_config.server_id {
        router_server
            .set_server_id(id)
            .expect("server id already set");
    } else {
        warn!("server ID not set. ID must be set via the INFLUXDB_IOX_ID config or API before writing or querying data.");
    }
    let server_type = Arc::new(RouterServerType::new(router_server, &common_state));

    Ok(influxdb_ioxd::main(common_state, server_type).await?)
}
