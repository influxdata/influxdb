//! Implementation of command line option for running router2

use std::{collections::BTreeSet, sync::Arc};

use crate::{
    clap_blocks::{run_config::RunConfig, write_buffer::WriteBufferConfig},
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
    dml_handlers::ShardedWriteBuffer,
    sequencer::Sequencer,
    server::{http::HttpDelegate, RouterServer},
    sharder::TableNamespaceSharder,
};
use thiserror::Error;
use trace::TraceCollector;
use write_buffer::core::WriteBufferError;

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

    #[clap(flatten)]
    pub(crate) write_buffer_config: WriteBufferConfig,
}

pub async fn command(config: Config) -> Result<()> {
    let common_state = CommonServerState::from_config(config.run_config.clone())?;
    let metrics = Arc::new(metric::Registry::default());

    let write_buffer = init_write_buffer(
        &config,
        Arc::clone(&metrics),
        common_state.trace_collector(),
    )
    .await?;

    let http = HttpDelegate::new(config.run_config.max_http_request_size, write_buffer);
    let router_server = RouterServer::new(
        http,
        Default::default(),
        metrics,
        common_state.trace_collector(),
    );
    let server_type = Arc::new(RouterServerType::new(router_server, &common_state));

    info!("starting router2");

    Ok(influxdb_ioxd::main(common_state, server_type).await?)
}

/// Initialise the [`ShardedWriteBuffer`] with one shard per Kafka partition,
/// using the [`TableNamespaceSharder`] to shard operations by their destination
/// namespace & table name.
async fn init_write_buffer(
    config: &Config,
    metrics: Arc<metric::Registry>,
    trace_collector: Option<Arc<dyn TraceCollector>>,
) -> Result<ShardedWriteBuffer<TableNamespaceSharder<Arc<Sequencer>>>> {
    let write_buffer = Arc::new(
        config
            .write_buffer_config
            .init_write_buffer(metrics, trace_collector)
            .await?,
    );

    // Construct the (ordered) set of sequencers.
    //
    // The sort order must be deterministic in order for all nodes to shard to
    // the same sequencers, therefore we type assert the returned set is of the
    // ordered variety.
    let shards: BTreeSet<_> = write_buffer.sequencer_ids();
    //          ^ don't change this to an unordered set

    info!(
        topic = config.write_buffer_config.topic.as_str(),
        shards = shards.len(),
        "connected to write buffer topic",
    );

    Ok(ShardedWriteBuffer::new(
        shards
            .into_iter()
            .map(|id| Sequencer::new(id as _, Arc::clone(&write_buffer)))
            .map(Arc::new)
            .collect::<TableNamespaceSharder<_>>(),
    ))
}
