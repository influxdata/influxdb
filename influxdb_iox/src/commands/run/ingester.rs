//! Implementation of command line option for running ingester

use std::sync::Arc;

use crate::{
    clap_blocks::run_config::RunConfig,
    influxdb_ioxd::{
        self,
        server_type::{
            common_state::{CommonServerState, CommonServerStateError},
            ingester::IngesterServerType,
        },
    },
};
use ingester::handler::IngestHandlerImpl;
use ingester::server::grpc::GrpcDelegate;
use ingester::server::{http::HttpDelegate, IngesterServer};
use iox_catalog::interface::{Catalog, KafkaPartition};
use iox_catalog::postgres::PostgresCatalog;
use observability_deps::tracing::*;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum Error {
    #[error("Run: {0}")]
    Run(#[from] influxdb_ioxd::Error),

    #[error("Cannot setup server: {0}")]
    Setup(#[from] crate::influxdb_ioxd::server_type::database::setup::Error),

    #[error("Invalid config: {0}")]
    InvalidConfig(#[from] CommonServerStateError),

    #[error("Catalog error: {0}")]
    Catalog(#[from] iox_catalog::interface::Error),

    #[error("Kafka topic {0} not found in the catalog")]
    KafkaTopicNotFound(String),
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

    /// Postgres connection string
    #[clap(env = "INFLUXDB_IOX_CATALOG_DNS")]
    pub catalog_dsn: String,

    /// Kafka connection string
    #[clap(env = "INFLUXDB_IOX_KAFKA_CONNECTION")]
    pub kafka_connection: String,

    /// Kafka topic name
    #[clap(env = "INFLUXDB_IOX_KAFKA_TOPIC")]
    pub kafka_topic: String,

    /// Kafka partition number to start (inclusive) range with
    #[clap(env = "INFLUXDB_IOX_KAFKA_PARTITION_RANGE_START")]
    pub kafka_partition_range_start: i32,

    /// Kafka partition number to end (inclusive) range with
    #[clap(env = "INFLUXDB_IOX_KAFKA_PARTITION_RANGE_END")]
    pub kafka_partition_range_end: i32,
}

pub async fn command(config: Config) -> Result<()> {
    let common_state = CommonServerState::from_config(config.run_config.clone())?;

    let catalog: Arc<dyn Catalog> = Arc::new(
        PostgresCatalog::connect(
            "ingester",
            iox_catalog::postgres::SCHEMA_NAME,
            &config.catalog_dsn,
        )
        .await?,
    );

    let kafka_topic = match catalog
        .kafka_topics()
        .get_by_name(&config.kafka_topic)
        .await?
    {
        Some(k) => k,
        None => return Err(Error::KafkaTopicNotFound(config.kafka_topic)),
    };
    let kafka_partitions: Vec<_> = (config.kafka_partition_range_start
        ..config.kafka_partition_range_end)
        .map(KafkaPartition::new)
        .collect();

    let ingest_handler = Arc::new(IngestHandlerImpl::new(
        kafka_topic,
        kafka_partitions,
        catalog,
    ));
    let http = HttpDelegate::new(Arc::clone(&ingest_handler));
    let grpc = GrpcDelegate::new(ingest_handler);

    let ingester = IngesterServer::new(http, grpc);
    let server_type = Arc::new(IngesterServerType::new(ingester, &common_state));

    info!("starting ingester");

    Ok(influxdb_ioxd::main(common_state, server_type).await?)
}
