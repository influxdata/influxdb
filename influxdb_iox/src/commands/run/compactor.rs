//! Command line options for running compactor

use super::main;
use crate::process_info::setup_metric_registry;
use clap_blocks::{
    catalog_dsn::CatalogDsnConfig, compactor::CompactorConfig, object_store::make_object_store,
    run_config::RunConfig,
};
use compactor::object_store::metrics::MetricsStore;
use iox_query::exec::{Executor, ExecutorConfig};
use iox_time::{SystemProvider, TimeProvider};
use ioxd_common::{
    server_type::{CommonServerState, CommonServerStateError},
    Service,
};
use ioxd_compactor::create_compactor_server_type;
use object_store::DynObjectStore;
use object_store_metrics::ObjectStoreMetrics;
use observability_deps::tracing::*;
use parquet_file::storage::{ParquetStorage, StorageId};
use std::num::NonZeroUsize;
use std::sync::Arc;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum Error {
    #[error("Run: {0}")]
    Run(#[from] main::Error),

    #[error("Invalid config: {0}")]
    InvalidConfig(#[from] CommonServerStateError),

    #[error("Catalog error: {0}")]
    Catalog(#[from] iox_catalog::interface::Error),

    #[error("Catalog DSN error: {0}")]
    CatalogDsn(#[from] clap_blocks::catalog_dsn::Error),

    #[error("Cannot parse object store config: {0}")]
    ObjectStoreParsing(#[from] clap_blocks::object_store::ParseError),
}

#[derive(Debug, clap::Parser)]
#[clap(
    name = "run",
    about = "Runs in compactor mode using the RPC write path",
    long_about = "Run the IOx compactor server.\n\nThe configuration options below can be \
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
    pub(crate) compactor_config: CompactorConfig,
}

pub async fn command(config: Config) -> Result<(), Error> {
    let common_state = CommonServerState::from_config(config.run_config.clone())?;

    let time_provider = Arc::new(SystemProvider::new()) as Arc<dyn TimeProvider>;
    let metric_registry = setup_metric_registry();
    let catalog = config
        .catalog_dsn
        .get_catalog("compactor", Arc::clone(&metric_registry))
        .await?;

    let object_store = make_object_store(config.run_config.object_store_config())
        .map_err(Error::ObjectStoreParsing)?;

    // Decorate the object store with a metric recorder.
    let object_store: Arc<DynObjectStore> = Arc::new(ObjectStoreMetrics::new(
        object_store,
        Arc::clone(&time_provider),
        &metric_registry,
    ));

    let parquet_store_real = ParquetStorage::new(object_store, StorageId::from("iox"));
    let parquet_store_scratchpad = ParquetStorage::new(
        Arc::new(MetricsStore::new(
            Arc::new(object_store::memory::InMemory::new()),
            &metric_registry,
            "scratchpad",
        )),
        StorageId::from("iox_scratchpad"),
    );

    let num_threads = config
        .compactor_config
        .query_exec_thread_count
        .unwrap_or_else(|| {
            NonZeroUsize::new(num_cpus::get().saturating_sub(1))
                .unwrap_or_else(|| NonZeroUsize::new(1).unwrap())
        });
    info!(%num_threads, "using specified number of threads");

    let exec = Arc::new(Executor::new_with_config(ExecutorConfig {
        num_threads,
        target_query_partitions: num_threads,
        object_stores: [&parquet_store_real, &parquet_store_scratchpad]
            .into_iter()
            .map(|store| (store.id(), Arc::clone(store.object_store())))
            .collect(),
        metric_registry: Arc::clone(&metric_registry),
        mem_pool_size: config.compactor_config.exec_mem_pool_bytes,
    }));
    let time_provider = Arc::new(SystemProvider::new());

    let process_once = config.compactor_config.process_once;
    let server_type = create_compactor_server_type(
        &common_state,
        Arc::clone(&metric_registry),
        catalog,
        parquet_store_real,
        parquet_store_scratchpad,
        exec,
        time_provider,
        config.compactor_config,
    )
    .await;

    info!("starting compactor");

    let services = vec![Service::create(server_type, common_state.run_config())];

    let res = main::main(common_state, services, metric_registry).await;
    match res {
        Ok(()) => Ok(()),
        // compactor is allowed to shut itself down
        Err(main::Error::Wrapper {
            source: _source @ ioxd_common::Error::LostServer,
        }) if process_once => Ok(()),
        Err(e) => Err(e.into()),
    }
}
