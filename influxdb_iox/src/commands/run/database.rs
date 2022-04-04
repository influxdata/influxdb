//! Implementation of command line option for running server

use std::sync::Arc;

use clap_blocks::run_config::RunConfig;
use data_types::boolean_flag::BooleanFlag;
use ioxd_common::server_type::{CommonServerState, CommonServerStateError};
use ioxd_common::Service;
use ioxd_database::{
    setup::{make_application, make_server},
    DatabaseServerType,
};

use thiserror::Error;

use super::main;

#[derive(Debug, Error)]
pub enum Error {
    #[error("Run: {0}")]
    Run(#[from] main::Error),

    #[error("Cannot setup server: {0}")]
    Setup(#[from] ioxd_database::setup::Error),

    #[error("Invalid config: {0}")]
    InvalidConfig(#[from] CommonServerStateError),
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

#[derive(Debug, clap::Parser)]
#[clap(
    name = "run",
    about = "Runs in database mode",
    long_about = "Run the IOx database server.\n\nThe configuration options below can be \
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

    /// The number of threads to use for all worker pools.
    ///
    /// IOx uses a pool with `--num-threads` threads *each* for
    /// 1. Handling API requests
    /// 2. Running queries.
    /// 3. Reorganizing data (e.g. compacting chunks)
    ///
    /// If not specified, defaults to the number of cores on the system
    #[clap(long = "--num-worker-threads", env = "INFLUXDB_IOX_NUM_WORKER_THREADS")]
    pub(crate) num_worker_threads: Option<usize>,

    // TODO(marco): Remove once the database-run-mode (aka the `server` crate) cannot handle routing anymore and we're
    //              fully migrated to the new router code.
    /// When IOx nodes need to talk to remote peers they consult an internal remote address
    /// mapping. This mapping is populated via API calls. If the mapping doesn't produce
    /// a result, this config entry allows to generate a hostname from at template:
    /// occurrences of the "{id}" substring will be replaced with the remote Server ID.
    ///
    /// Example: http://node-{id}.ioxmydomain.com:8082
    #[clap(long = "--remote-template", env = "INFLUXDB_IOX_REMOTE_TEMPLATE")]
    pub remote_template: Option<String>,

    /// Automatically wipe the preserved catalog on error
    #[clap(
        long = "--wipe-catalog-on-error",
        env = "INFLUXDB_IOX_WIPE_CATALOG_ON_ERROR",
        default_value = "no"
    )]
    pub wipe_catalog_on_error: BooleanFlag,

    /// Skip replaying the write buffer and seek to high watermark instead.
    #[clap(
        long = "--skip-replay",
        env = "INFLUXDB_IOX_SKIP_REPLAY",
        default_value = "no"
    )]
    pub skip_replay_and_seek_instead: BooleanFlag,

    /// Path to a configuration file to use for routing configuration, this will
    /// disable dynamic configuration via `influxdata.iox.management.v1.ManagementService`
    ///
    /// The config file should contain a JSON encoded `influxdata.iox.management.v1.ServerConfigFile`
    #[clap(long = "--config-file", env = "INFLUXDB_IOX_CONFIG_FILE")]
    pub config_file: Option<String>,
}

impl Config {
    /// Get a reference to the config's run config.
    pub fn run_config(&self) -> &RunConfig {
        &self.run_config
    }

    /// Get a reference to the config's config file.
    pub fn config_file(&self) -> Option<&String> {
        self.config_file.as_ref()
    }
}

pub async fn command(config: Config) -> Result<()> {
    let common_state = CommonServerState::from_config(config.run_config.clone())?;

    let application = make_application(
        config.run_config(),
        config.config_file().cloned(),
        config.num_worker_threads,
        common_state.trace_collector(),
    )
    .await?;
    let app_server = make_server(
        Arc::clone(&application),
        config.wipe_catalog_on_error.into(),
        config.skip_replay_and_seek_instead.into(),
        config.run_config(),
    )?;
    let server_type = Arc::new(DatabaseServerType::new(
        Arc::clone(&application),
        Arc::clone(&app_server),
        &common_state,
        config.config_file.is_some(),
    ));

    let services = vec![Service::create(server_type, common_state.run_config())];
    Ok(main::main(common_state, services).await?)
}
