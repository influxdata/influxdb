//! Implementation of command line option for running ingester

use crate::{
    clap_blocks::{
        catalog_dsn::CatalogDsnConfig, run_config::RunConfig, write_buffer::WriteBufferConfig,
    },
    influxdb_ioxd::{
        self,
        server_type::{
            common_state::{CommonServerState, CommonServerStateError},
            ingester::IngesterServerType,
        },
    },
};
use ingester::{
    handler::IngestHandlerImpl,
    lifecycle::LifecycleConfig,
    server::{grpc::GrpcDelegate, http::HttpDelegate, IngesterServer},
};
use iox_catalog::interface::KafkaPartition;
use object_store::ObjectStore;
use observability_deps::tracing::*;
use query::exec::Executor;
use std::{collections::BTreeMap, convert::TryFrom, sync::Arc, time::Duration};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum Error {
    #[error("Run: {0}")]
    Run(#[from] influxdb_ioxd::Error),

    #[error("Invalid config: {0}")]
    InvalidConfig(#[from] CommonServerStateError),

    #[error("Catalog error: {0}")]
    Catalog(#[from] iox_catalog::interface::Error),

    #[error("Kafka topic {0} not found in the catalog")]
    KafkaTopicNotFound(String),

    #[error("Cannot parse object store config: {0}")]
    ObjectStoreParsing(#[from] crate::clap_blocks::object_store::ParseError),

    #[error("kafka_partition_range_start must be <= kafka_partition_range_end")]
    KafkaRange,

    #[error("error initializing ingester: {0}")]
    Ingester(#[from] ingester::handler::Error),

    #[error("error initializing write buffer {0}")]
    WriteBuffer(#[from] write_buffer::core::WriteBufferError),

    #[error("Invalid number of sequencers: {0}")]
    NumSequencers(#[from] std::num::TryFromIntError),

    #[error("Catalog DSN error: {0}")]
    CatalogDsn(#[from] crate::clap_blocks::catalog_dsn::Error),
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
    pub(crate) write_buffer_config: WriteBufferConfig,

    /// Write buffer partition number to start (inclusive) range with
    #[clap(
        long = "--write-buffer-partition-range-start",
        env = "INFLUXDB_IOX_WRITE_BUFFER_PARTITION_RANGE_START"
    )]
    pub write_buffer_partition_range_start: i32,

    /// Write buffer partition number to end (inclusive) range with
    #[clap(
        long = "--write-buffer-partition-range-end",
        env = "INFLUXDB_IOX_WRITE_BUFFER_PARTITION_RANGE_END"
    )]
    pub write_buffer_partition_range_end: i32,

    /// The ingester will continue to pull data and buffer it from Kafka
    /// as long as it is below this size. If it hits this size it will pause
    /// ingest from Kafka until persistence goes below this threshold.
    #[clap(
        long = "--pause-ingest-size-bytes",
        env = "INFLUXDB_IOX_PAUSE_INGEST_SIZE_BYTES"
    )]
    pub pause_ingest_size_bytes: usize,

    /// Once the ingester crosses this threshold of data buffered across
    /// all sequencers, it will pick the largest partitions and persist
    /// them until it falls below this threshold. An ingester running in
    /// a steady state is expected to take up this much memory.
    #[clap(
        long = "--persist-memory-threshold-bytes",
        env = "INFLUXDB_IOX_PERSIST_MEMORY_THRESHOLD_BYTES"
    )]
    pub persist_memory_threshold_bytes: usize,

    /// If an individual partition crosses this size threshold, it will be persisted.
    /// The default value is 300MB (in bytes).
    #[clap(
        long = "--persist-partition-size-threshold-bytes",
        env = "INFLUXDB_IOX_PERSIST_PARTITION_SIZE_THRESHOLD_BYTES",
        default_value = "314572800"
    )]
    pub persist_partition_size_threshold_bytes: usize,

    /// If a partition has had data buffered for longer than this period of time
    /// it will be persisted. This puts an upper bound on how far back the
    /// ingester may need to read in Kafka on restart or recovery. The default value
    /// is 30 minutes (in seconds).
    #[clap(
        long = "--persist-partition-age-threshold-seconds",
        env = "INFLUXDB_IOX_PERSIST_PARTITION_AGE_THRESHOLD_SECONDS",
        default_value = "1800"
    )]
    pub persist_partition_age_threshold_seconds: u64,

    /// Number of threads to use for the ingester query execution, compaction and persistence.
    #[clap(
        long = "--query-exec-thread-count",
        env = "INFLUXDB_IOX_QUERY_EXEC_THREAD_COUNT",
        default_value = "4"
    )]
    pub query_exect_thread_count: usize,
}

pub async fn command(config: Config) -> Result<()> {
    let common_state = CommonServerState::from_config(config.run_config.clone())?;

    let catalog = config.catalog_dsn.get_catalog("ingester").await?;

    let mut txn = catalog.start_transaction().await?;
    let kafka_topic = txn
        .kafka_topics()
        .get_by_name(&config.write_buffer_config.topic)
        .await?
        .ok_or_else(|| Error::KafkaTopicNotFound(config.write_buffer_config.topic.clone()))?;

    if config.write_buffer_partition_range_start > config.write_buffer_partition_range_end {
        return Err(Error::KafkaRange);
    }

    let kafka_partitions: Vec<_> = (config.write_buffer_partition_range_start
        ..=config.write_buffer_partition_range_end)
        .map(KafkaPartition::new)
        .collect();

    let object_store = Arc::new(
        ObjectStore::try_from(&config.run_config.object_store_config)
            .map_err(Error::ObjectStoreParsing)?,
    );

    let mut sequencers = BTreeMap::new();
    for k in kafka_partitions {
        let s = txn.sequencers().create_or_get(&kafka_topic, k).await?;
        sequencers.insert(k, s);
    }
    txn.commit().await?;

    let metric_registry: Arc<metric::Registry> = Default::default();
    let trace_collector = common_state.trace_collector();

    let write_buffer = config
        .write_buffer_config
        .reading(Arc::clone(&metric_registry), trace_collector.clone())
        .await?;

    let lifecycle_config = LifecycleConfig::new(
        config.pause_ingest_size_bytes,
        config.persist_memory_threshold_bytes,
        config.persist_partition_size_threshold_bytes,
        Duration::from_secs(config.persist_partition_age_threshold_seconds),
    );
    let ingest_handler = Arc::new(
        IngestHandlerImpl::new(
            lifecycle_config,
            kafka_topic,
            sequencers,
            catalog,
            object_store,
            write_buffer,
            Executor::new(config.query_exect_thread_count),
            &metric_registry,
        )
        .await?,
    );
    let http = HttpDelegate::new(Arc::clone(&ingest_handler));
    let grpc = GrpcDelegate::new(Arc::clone(&ingest_handler));

    let ingester = IngesterServer::new(metric_registry, http, grpc, ingest_handler);
    let server_type = Arc::new(IngesterServerType::new(ingester, &common_state));

    info!("starting ingester");

    Ok(influxdb_ioxd::main(common_state, server_type).await?)
}
