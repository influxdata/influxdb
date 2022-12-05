use clap_blocks::{
    catalog_dsn::CatalogDsnConfig,
    compactor::CompactorOnceConfig,
    object_store::{make_object_store, ObjectStoreConfig},
};
use iox_query::exec::{Executor, ExecutorConfig};
use iox_time::{SystemProvider, TimeProvider};
use ioxd_compactor::build_compactor_from_config;
use object_store::DynObjectStore;
use object_store_metrics::ObjectStoreMetrics;
use parquet_file::storage::{ParquetStorage, StorageId};
use snafu::prelude::*;
use std::{collections::HashMap, sync::Arc};

use crate::process_info::{setup_metric_registry, USIZE_MAX};

mod generate;

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
        compactor_config: CompactorOnceConfig,

        /// Number of threads to use for the compactor query execution, compaction and persistence.
        #[clap(
            long = "query-exec-thread-count",
            env = "INFLUXDB_IOX_QUERY_EXEC_THREAD_COUNT",
            default_value = "4",
            action
        )]
        query_exec_thread_count: usize,

        /// Size of memory pool used during query exec, in bytes.
        #[clap(
            long = "exec-mem-pool-bytes",
            env = "INFLUXDB_IOX_EXEC_MEM_POOL_BYTES",
            default_value = &USIZE_MAX[..],
            action
        )]
        exec_mem_pool_bytes: usize,
    },

    /// Generate Parquet files and catalog entries with different characteristics for the purposes
    /// of investigating how the compactor handles them.
    ///
    /// Only works with `--object-store file` because this is for generating local development
    /// data.
    ///
    /// Within the directory specified by `--data-dir`, will generate a
    /// `compactor_data/line_protocol` subdirectory to avoid interfering with other existing IOx
    /// files that may be in the `--data-dir`.
    ///
    /// WARNING: On every run of this tool, the `compactor_data/line_protocol` subdirectory will be
    /// removed. If you want to keep any previously generated files, move or copy them before
    /// running this tool again.
    Generate(generate::Config),
}

pub async fn command(config: Config) -> Result<()> {
    match config.command {
        Command::RunOnce {
            object_store_config,
            catalog_dsn,
            compactor_config,
            query_exec_thread_count,
            exec_mem_pool_bytes,
        } => {
            let compactor_config = compactor_config.into_compactor_config();

            let time_provider = Arc::new(SystemProvider::new()) as Arc<dyn TimeProvider>;
            let metric_registry = setup_metric_registry();
            let catalog = catalog_dsn
                .get_catalog("compactor", Arc::clone(&metric_registry))
                .await?;

            let object_store = make_object_store(&object_store_config)?;

            // Decorate the object store with a metric recorder.
            let object_store: Arc<DynObjectStore> = Arc::new(ObjectStoreMetrics::new(
                object_store,
                Arc::clone(&time_provider),
                &metric_registry,
            ));
            let parquet_store = ParquetStorage::new(object_store, StorageId::from("iox"));

            let exec = Arc::new(Executor::new_with_config(ExecutorConfig {
                num_threads: query_exec_thread_count,
                target_query_partitions: query_exec_thread_count,
                object_stores: HashMap::from([(
                    parquet_store.id(),
                    Arc::clone(parquet_store.object_store()),
                )]),
                mem_pool_size: exec_mem_pool_bytes,
            }));
            let time_provider = Arc::new(SystemProvider::new());

            let compactor = build_compactor_from_config(
                compactor_config,
                catalog,
                parquet_store,
                exec,
                time_provider,
                metric_registry,
            )
            .await?;
            let compactor = Arc::new(compactor);

            compactor::handler::run_compactor_once(compactor).await;
        }
        Command::Generate(config) => {
            generate::run(config).await?;
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

    #[snafu(context(false))]
    Generating { source: generate::Error },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;
