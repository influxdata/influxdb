use clap::ValueEnum;
use clap_blocks::{
    catalog_dsn::CatalogDsnConfig,
    compactor::CompactorOnceConfig,
    object_store::{make_object_store, ObjectStoreConfig},
};
use iox_query::exec::Executor;
use iox_time::{SystemProvider, TimeProvider};
use ioxd_compactor::build_compactor_from_config;
use object_store::DynObjectStore;
use object_store_metrics::ObjectStoreMetrics;
use snafu::prelude::*;
use std::{num::NonZeroUsize, sync::Arc};

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
            long = "--query-exec-thread-count",
            env = "INFLUXDB_IOX_QUERY_EXEC_THREAD_COUNT",
            default_value = "4",
            action
        )]
        query_exec_thread_count: usize,
    },

    /// Generate Parquet files and catalog entries with different characteristics for the purposes
    /// of investigating how the compactor handles them.
    Generate {
        #[clap(flatten)]
        object_store_config: ObjectStoreConfig,

        #[clap(flatten)]
        catalog_dsn: CatalogDsnConfig,

        /// The type of compaction to be done on the files. If `hot` is specified, the generated
        /// files will have compaction level 0. If `cold` is specified, the generated files will
        /// have compaction level 1 and will be marked that they were created at least 8 hours ago.
        #[clap(
            arg_enum,
            value_parser,
            long = "--compaction-type",
            env = "INFLUXDB_IOX_COMPACTOR_GENERATE_TYPE",
            default_value = "hot",
            action
        )]
        compaction_type: CompactionType,

        /// The number of IOx partitions to generate files for. Each partition will have the number
        /// of files specified by `--num-files` generated.
        #[clap(
            long = "--num-partitions",
            env = "INFLUXDB_IOX_COMPACTOR_GENERATE_NUM_PARTITIONS",
            default_value = "1",
            action
        )]
        num_partitions: NonZeroUsize,

        /// The number of parquet files to generate per partition.
        #[clap(
            long = "--num-files",
            env = "INFLUXDB_IOX_COMPACTOR_GENERATE_NUM_FILES",
            default_value = "1",
            action
        )]
        num_files: NonZeroUsize,

        /// The number of columns to generate in each file. One column will always be the
        /// timestamp. Additional columns will be given a type in I64, F64, U64, String, Bool, and
        /// Tag in equal proportion.
        #[clap(
            long = "--num-cols",
            env = "INFLUXDB_IOX_COMPACTOR_GENERATE_NUM_COLS",
            default_value = "7",
            action
        )]
        num_columns: NonZeroUsize,

        /// The number of rows to generate in each file.
        #[clap(
            long = "--num-rows",
            env = "INFLUXDB_IOX_COMPACTOR_GENERATE_NUM_ROWS",
            default_value = "1",
            action
        )]
        num_rows: NonZeroUsize,
    },
}

#[derive(Debug, Copy, Clone, PartialEq, Eq, PartialOrd, Ord, ValueEnum)]
pub enum CompactionType {
    Hot,
    Cold,
}

pub async fn command(config: Config) -> Result<()> {
    match config.command {
        Command::RunOnce {
            object_store_config,
            catalog_dsn,
            compactor_config,
            query_exec_thread_count,
        } => {
            let compactor_config = compactor_config.into_compactor_config();

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
        Command::Generate { .. } => {}
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
