//! Implementation of command line option for running router2

use std::{collections::BTreeSet, sync::Arc};

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
use data_types::write_buffer::WriteBufferConnection;
use observability_deps::tracing::*;
use router2::{
    dml_handler::nop::NopDmlHandler,
    sequencer::Sequencer,
    server::{http::HttpDelegate, RouterServer},
    sharded_write_buffer::ShardedWriteBuffer,
    sharder::TableNamespaceSharder,
};
use thiserror::Error;
use time::SystemProvider;
use write_buffer::{config::WriteBufferConfigFactory, core::WriteBufferError};

#[derive(Debug, Error)]
pub enum Error {
    #[error("Run: {0}")]
    Run(#[from] influxdb_ioxd::Error),

    #[error("Invalid config: {0}")]
    InvalidConfig(#[from] CommonServerStateError),

    #[error("failed to initialise write buffer connection: {0}")]
    WriteBuffer(#[from] WriteBufferError),
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

    /// The type of write buffer to use.
    ///
    /// Valid options are: file, kafka, rskafka
    #[clap(long = "--write-buffer", env = "INFLUXDB_IOX_WRITE_BUFFER_TYPE")]
    pub(crate) write_buffer_type: String,

    /// The address to the write buffer.
    #[clap(long = "--write-buffer-addr", env = "INFLUXDB_IOX_WRITE_BUFFER_ADDR")]
    pub(crate) write_buffer_connection_string: String,
}

pub async fn command(config: Config) -> Result<()> {
    let common_state = CommonServerState::from_config(config.run_config.clone())?;
    let metrics = Arc::new(metric::Registry::default());

    // This will be used by the proper DML handler.
    let _write_buffer = init_write_buffer(&config, "iox_shared", Arc::clone(&metrics)).await?;

    let http = HttpDelegate::new(
        config.run_config.max_http_request_size,
        NopDmlHandler::default(),
    );
    let router_server = RouterServer::new(http, Default::default(), metrics);
    let server_type = Arc::new(RouterServerType::new(router_server, &common_state));

    info!("starting router2");

    Ok(influxdb_ioxd::main(common_state, server_type).await?)
}

/// Initialise the [`ShardedWriteBuffer`] with one shard per Kafka partition,
/// using the [`TableNamespaceSharder`] to shard operations by their destination
/// namespace & table name.
async fn init_write_buffer(
    config: &Config,
    topic: &str,
    metrics: Arc<metric::Registry>,
) -> Result<ShardedWriteBuffer<TableNamespaceSharder<Sequencer>>> {
    let write_buffer_config = WriteBufferConnection {
        type_: config.write_buffer_type.clone(),
        connection: config.write_buffer_connection_string.clone(),
        connection_config: Default::default(),
        creation_config: None,
    };

    let write_buffer = WriteBufferConfigFactory::new(Arc::new(SystemProvider::default()), metrics);
    let write_buffer = Arc::new(
        write_buffer
            .new_config_write(topic, &write_buffer_config)
            .await?,
    );

    // Construct the (ordered) set of sequencers.
    //
    // The sort order must be deterministic in order for all nodes to shard to
    // the same sequencers, therefore we type assert the returned set is of the
    // ordered variety.
    let shards: BTreeSet<_> = write_buffer.sequencer_ids();
    //          ^ don't change this to an unordered set

    info!(%topic, shards=shards.len(), "connected to write buffer topic");

    Ok(ShardedWriteBuffer::new(
        shards
            .into_iter()
            .map(|id| Sequencer::new(id as _, Arc::clone(&write_buffer)))
            .collect::<TableNamespaceSharder<_>>(),
    ))
}
