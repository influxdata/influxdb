//! Implements the `compactor generate` command.

use bytes::Bytes;
use clap::ValueEnum;
use clap_blocks::{
    catalog_dsn::CatalogDsnConfig,
    object_store::{make_object_store, ObjectStoreConfig},
};
use object_store::{path::Path, DynObjectStore};
use snafu::prelude::*;
use std::{num::NonZeroUsize, sync::Arc};

#[derive(Debug, clap::Parser)]
pub struct Config {
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
}

#[derive(Debug, Copy, Clone, PartialEq, Eq, PartialOrd, Ord, ValueEnum)]
pub enum CompactionType {
    Hot,
    Cold,
}

pub async fn run(config: Config) -> Result<()> {
    let object_store = make_object_store(&config.object_store_config)?;

    write_data_generation_spec(object_store, &config).await?;

    Ok(())
}

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Could not parse the object store configuration"))]
    #[snafu(context(false))]
    ObjectStoreConfigParsing {
        source: clap_blocks::object_store::ParseError,
    },

    #[snafu(display("Could not write file to object storage"))]
    ObjectStoreWriting { source: object_store::Error },

    #[snafu(display("Could not parse object store path"))]
    ObjectStorePathParsing { source: object_store::path::Error },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

async fn write_data_generation_spec(
    object_store: Arc<DynObjectStore>,
    config: &Config,
) -> Result<()> {
    let spec = String::new();

    let path = Path::parse("compactor_data/line_protocol/spec.toml")
        .context(ObjectStorePathParsingSnafu)?;

    let data = Bytes::from(spec);

    object_store
        .put(&path, data)
        .await
        .context(ObjectStoreWritingSnafu)?;

    Ok(())
}
