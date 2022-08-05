use clap_blocks::{
    catalog_dsn::CatalogDsnConfig,
    compactor::CompactorConfig,
    object_store::{make_object_store, ObjectStoreConfig},
};
use iox_query::exec::Executor;
use iox_time::{SystemProvider, TimeProvider};
use ioxd_compactor::build_compactor_from_config;
use object_store::DynObjectStore;
use object_store_metrics::ObjectStoreMetrics;
use snafu::prelude::*;
use std::sync::Arc;

#[derive(Debug, clap::Parser)]
pub struct Config {
    #[clap(subcommand)]
    command: Command,
}

#[derive(Debug, clap::Parser)]
pub enum Command {
    /// Run the compactor for one cycle
    RunOnce {
        #[clap(flatten)]
        object_store_config: ObjectStoreConfig,

        #[clap(flatten)]
        catalog_dsn: CatalogDsnConfig,

        #[clap(flatten)]
        compactor_config: CompactorConfig,

        /// Number of threads to use for the compactor query execution, compaction and persistence.
        #[clap(
            long = "--query-exec-thread-count",
            env = "INFLUXDB_IOX_QUERY_EXEC_THREAD_COUNT",
            default_value = "4",
            action
        )]
        query_exec_thread_count: usize,
    },
}

pub async fn command(config: Config) -> Result<()> {
    match config.command {
        Command::RunOnce {
            object_store_config,
            catalog_dsn,
            compactor_config,
            query_exec_thread_count,
        } => {
            let time_provider = Arc::new(SystemProvider::new()) as Arc<dyn TimeProvider>;
            let metric_registry: Arc<metric::Registry> = Default::default();
            let catalog = catalog_dsn
                .get_catalog("compactor", Arc::clone(&metric_registry))
                .await?;

            let object_store = make_object_store(&object_store_config)?;

            // Decorate the object store with a metric recorder.
            let object_store: Arc<DynObjectStore> = Arc::new(ObjectStoreMetrics::new(
                object_store,
                Arc::clone(&time_provider),
                &*metric_registry,
            ));

            let exec = Arc::new(Executor::new(query_exec_thread_count));
            let time_provider = Arc::new(SystemProvider::new());

            let compactor = build_compactor_from_config(
                compactor_config,
                catalog,
                object_store,
                exec,
                time_provider,
                metric_registry,
            )
            .await?;
            let compactor = Arc::new(compactor);

            compactor::handler::run_compactor_once(compactor).await;
        }
    }

    Ok(())
}

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(context(false))]
    CatalogParsing {
        source: clap_blocks::catalog_dsn::Error,
    },

    #[snafu(context(false))]
    ObjectStoreParsing {
        source: clap_blocks::object_store::ParseError,
    },

    #[snafu(context(false))]
    Compacting { source: ioxd_compactor::Error },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;
