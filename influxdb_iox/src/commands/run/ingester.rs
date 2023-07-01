//! Command line options for running an ingester for a router using the RPC write path to talk to.

use super::main;
use crate::process_info::{setup_metric_registry, USIZE_MAX};
use clap_blocks::{
    catalog_dsn::CatalogDsnConfig, ingester::IngesterConfig, object_store::make_object_store,
    run_config::RunConfig,
};
use iox_query::exec::Executor;
use iox_time::{SystemProvider, TimeProvider};
use ioxd_common::{
    server_type::{CommonServerState, CommonServerStateError},
    Service,
};
use ioxd_ingester::create_ingester_server_type;
use object_store::DynObjectStore;
use object_store_metrics::ObjectStoreMetrics;
use observability_deps::tracing::*;
use panic_logging::make_panics_fatal;
use parquet_file::storage::{ParquetStorage, StorageId};
use std::{num::NonZeroUsize, sync::Arc};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum Error {
    #[error("run: {0}")]
    Run(#[from] main::Error),

    #[error("invalid config: {0}")]
    InvalidConfig(#[from] CommonServerStateError),

    #[error("cannot parse object store config: {0}")]
    ObjectStoreParsing(#[from] clap_blocks::object_store::ParseError),

    #[error("error initializing ingester: {0}")]
    Ingester(#[from] ioxd_ingester::Error),

    #[error("catalog DSN error: {0}")]
    CatalogDsn(#[from] clap_blocks::catalog_dsn::Error),
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
    pub(crate) ingester_config: IngesterConfig,

    /// Specify the size of the thread-pool for query execution, and the
    /// separate compaction thread-pool.
    #[clap(
        long = "exec-thread-count",
        env = "INFLUXDB_IOX_EXEC_THREAD_COUNT",
        default_value = "4",
        action
    )]
    pub exec_thread_count: NonZeroUsize,

    /// Size of memory pool used during query exec, in bytes.
    #[clap(
        long = "exec-mem-pool-bytes",
        env = "INFLUXDB_IOX_EXEC_MEM_POOL_BYTES",
        default_value = &USIZE_MAX[..],
        action
    )]
    exec_mem_pool_bytes: usize,
}

pub async fn command(config: Config) -> Result<()> {
    // Ensure panics (even in threads or tokio tasks) are fatal when
    // running in this server mode.  This is done to avoid potential
    // data corruption because there is no foolproof way to recover
    // state after a panic.
    make_panics_fatal();

    let common_state = CommonServerState::from_config(config.run_config.clone())?;
    let time_provider = Arc::new(SystemProvider::new()) as Arc<dyn TimeProvider>;
    let metric_registry = setup_metric_registry();

    let catalog = config
        .catalog_dsn
        .get_catalog("ingester", Arc::clone(&metric_registry))
        .await?;

    let exec = Arc::new(Executor::new(
        config.exec_thread_count,
        config.exec_mem_pool_bytes,
        Arc::clone(&metric_registry),
    ));
    let object_store = make_object_store(config.run_config.object_store_config())
        .map_err(Error::ObjectStoreParsing)?;

    // Decorate the object store with a metric recorder.
    let object_store: Arc<DynObjectStore> = Arc::new(ObjectStoreMetrics::new(
        object_store,
        Arc::clone(&time_provider),
        &metric_registry,
    ));

    let server_type = create_ingester_server_type(
        &common_state,
        catalog,
        Arc::clone(&metric_registry),
        &config.ingester_config,
        exec,
        ParquetStorage::new(object_store, StorageId::from("iox")),
    )
    .await?;

    info!("starting ingester");

    let services = vec![Service::create(server_type, common_state.run_config())];
    Ok(main::main(common_state, services, metric_registry).await?)
}
