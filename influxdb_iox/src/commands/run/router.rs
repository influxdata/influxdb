//! Implementation of command line option for running server

use hashbrown::HashMap;
use std::sync::Arc;

use clap_blocks::run_config::RunConfig;

use data_types::router::Router as RouterConfig;
use generated_types::{google::FieldViolation, influxdata::iox::router::v1::RouterConfigFile};
use ioxd_common::server_type::{CommonServerState, CommonServerStateError};
use ioxd_common::Service;
use ioxd_router::RouterServerType;
use observability_deps::tracing::warn;
use router::{resolver::RemoteTemplate, server::RouterServer};
use thiserror::Error;
use time::SystemProvider;

use super::main;

#[derive(Debug, Error)]
pub enum Error {
    #[error("Run: {0}")]
    Run(#[from] main::Error),

    #[error("Cannot setup server: {0}")]
    Setup(#[from] ioxd_database::setup::Error),

    #[error("Invalid config: {0}")]
    InvalidConfig(#[from] CommonServerStateError),

    #[error("error reading config file: {0}")]
    ReadConfig(#[from] std::io::Error),

    #[error("error decoding config file: {0}")]
    DecodeConfig(#[from] serde_json::Error),

    #[error("invalid config for router \"{0}\" in config file: {1}")]
    InvalidRouterConfig(String, FieldViolation),

    #[error("invalid router template \"{0}\" in config file: {1}")]
    InvalidRouterTemplate(String, FieldViolation),

    #[error("router template \"{template}\" not found for router \"{name}\"")]
    TemplateNotFound { name: String, template: String },
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

    /// Path to a configuration file to use for routing configuration, this will
    /// disable dynamic configuration via `influxdata.iox.router.v1.RouterService`
    ///
    /// The config file should contain a JSON encoded `influxdata.iox.router.v1.RouterConfigFile`
    #[clap(long = "--config-file", env = "INFLUXDB_IOX_CONFIG_FILE")]
    pub config_file: Option<String>,
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

    let config_immutable = match config.config_file {
        Some(file) => {
            let data = tokio::fs::read(file).await?;
            let config: RouterConfigFile = serde_json::from_slice(data.as_slice())?;

            for router in config.routers {
                let name = router.name.clone();
                let config = router
                    .try_into()
                    .map_err(|e| Error::InvalidRouterConfig(name, e))?;

                router_server.update_router(config);
            }

            let templates = config
                .templates
                .into_iter()
                .map(|router| {
                    let name = router.name.clone();
                    match router.try_into() {
                        Ok(router) => Ok((name, router)),
                        Err(e) => Err(Error::InvalidRouterTemplate(name, e)),
                    }
                })
                .collect::<Result<HashMap<String, RouterConfig>>>()?;

            for instance in config.instances {
                match templates.get(&instance.template) {
                    Some(template) => {
                        router_server.update_router(RouterConfig {
                            name: instance.name,
                            ..template.clone()
                        });
                    }
                    None => {
                        return Err(Error::TemplateNotFound {
                            name: instance.name,
                            template: instance.template,
                        })
                    }
                }
            }

            true
        }
        None => false,
    };

    if let Some(id) = config.run_config.server_id_config().server_id {
        router_server
            .set_server_id(id)
            .expect("server id already set");
    } else {
        warn!("server ID not set. ID must be set via the INFLUXDB_IOX_ID config or API before writing or querying data.");
    }

    let server_type = Arc::new(RouterServerType::new(
        router_server,
        &common_state,
        config_immutable,
    ));

    let services = vec![Service::create(server_type, common_state.run_config())];
    Ok(main::main(common_state, services).await?)
}
