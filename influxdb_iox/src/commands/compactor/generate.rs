//! Implements the `compactor generate` command.

use clap::ValueEnum;
use clap_blocks::{catalog_dsn::CatalogDsnConfig, object_store::ObjectStoreConfig};
use snafu::prelude::*;
use std::num::NonZeroUsize;

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

pub fn run(_config: Config) -> Result<()> {
    Ok(())
}

#[derive(Debug, Snafu)]
pub enum Error {
    Unknown,
}

pub type Result<T, E = Error> = std::result::Result<T, E>;
