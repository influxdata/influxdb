//! Implementation of command line option for running server

use std::sync::Arc;

use crate::{
    clap_blocks::{boolean_flag::BooleanFlag, run_config::RunConfig},
    influxdb_ioxd::{
        self,
        server_type::{
            common_state::{CommonServerState, CommonServerStateError},
            database::{
                setup::{make_application, make_server},
                DatabaseServerType,
            },
        },
    },
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
    pub num_worker_threads: Option<usize>,

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
        // TODO: Don't automatically wipe on error (#1522)
        default_value = "yes"
    )]
    pub wipe_catalog_on_error: BooleanFlag,

    /// Skip replaying the write buffer and seek to high watermark instead.
    #[clap(
        long = "--skip-replay",
        env = "INFLUXDB_IOX_SKIP_REPLAY",
        default_value = "no"
    )]
    pub skip_replay_and_seek_instead: BooleanFlag,
}

pub async fn command(config: Config) -> Result<()> {
    let common_state = CommonServerState::from_config(config.run_config.clone())?;

    let application = make_application(&config, common_state.trace_collector()).await?;
    let app_server = make_server(Arc::clone(&application), &config);
    let server_type = Arc::new(DatabaseServerType::new(
        Arc::clone(&application),
        Arc::clone(&app_server),
        &common_state,
    ));

    Ok(influxdb_ioxd::main(common_state, server_type).await?)
}
