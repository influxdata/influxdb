//! This module contains code to report compression statistics for storage files

use snafu::{ResultExt, Snafu};
use structopt::StructOpt;
use tracing::info;

use ingest::parquet::{error::IOxParquetError, stats as parquet_stats};
use packers::{
    stats::{FileSetStatsBuilder, FileStats},
    Name,
};

use crate::commands::input::{FileType, InputPath, InputReader};

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Not implemented: {}", operation_name))]
    NotImplemented { operation_name: String },

    #[snafu(display("Error opening input {}", source))]
    OpenInput { source: super::input::Error },

    #[snafu(display("Unable to dump parquet file metadata: {}", source))]
    UnableDumpToParquetMetadata { source: IOxParquetError },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

/// Print out storage statistics information to stdout.
///
/// If a directory is specified, checks all files recursively
#[derive(Debug, StructOpt)]
pub struct Config {
    /// The input filename or directory to read from
    input: String,

    /// Include detailed information per column
    #[structopt(long)]
    per_column: bool,

    /// Include detailed information per file
    #[structopt(long)]
    per_file: bool,
}

/// Print statistics about all the files rooted at input_path
pub async fn stats(config: &Config) -> Result<()> {
    info!("stats starting for {:?}", config);
    let input_path = InputPath::new(&config.input, |p| {
        p.extension() == Some(std::ffi::OsStr::new("parquet"))
    })
    .context(OpenInput)?;

    println!("Storage statistics:");

    let mut builder = FileSetStatsBuilder::default();

    for input_reader in input_path.input_readers() {
        let input_reader = input_reader.context(OpenInput)?;

        let file_stats = stats_for_file(config, input_reader).await?;

        if config.per_file {
            println!("{}", file_stats);
        }
        builder = builder.accumulate(&file_stats);
    }

    let overall_stats = builder.build();
    if config.per_column {
        println!("-------------------------------");
        println!("Overall Per Column Stats");
        println!("-------------------------------");
        for c in &overall_stats.col_stats {
            println!("{}", c);
            println!();
        }
    }
    println!("{}", overall_stats);

    Ok(())
}

/// Print statistics about the file name in input_filename to stdout
pub async fn stats_for_file(config: &Config, input_reader: InputReader) -> Result<FileStats> {
    let input_name = String::from(input_reader.name());
    let file_stats = match input_reader.file_type() {
        FileType::LineProtocol => {
            return NotImplemented {
                operation_name: "Line protocol storage statistics",
            }
            .fail()
        }
        FileType::TSM => {
            return NotImplemented {
                operation_name: "TSM storage statistics",
            }
            .fail()
        }
        FileType::Parquet => {
            parquet_stats::file_stats(input_reader).context(UnableDumpToParquetMetadata)?
        }
    };

    if config.per_column && config.per_file {
        println!("-------------------------------");
        println!("Column Stats for {}", input_name);
        println!("-------------------------------");
        for c in &file_stats.col_stats {
            println!("{}", c);
            println!();
        }
    }

    Ok(file_stats)
}
