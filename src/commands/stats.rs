//! This module contains code to report compression statistics for storage files

use delorean_parquet::{error::Error as DeloreanParquetError, stats as parquet_stats};
use delorean_table::{
    stats::{FileSetStatsBuilder, FileStats},
    Name,
};
use log::info;
use snafu::{ResultExt, Snafu};

use crate::commands::input::{FileType, InputPath, InputReader};

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Not implemented: {}", operation_name))]
    NotImplemented { operation_name: String },

    #[snafu(display("Error opening input {}", source))]
    OpenInput { source: super::input::Error },

    #[snafu(display("Unable to dump parquet file metadata: {}", source))]
    UnableDumpToParquetMetadata { source: DeloreanParquetError },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

/// Describes what statistics are desired
#[derive(Debug)]
pub struct StatsConfig {
    /// The path to start searching from
    pub input_path: String,

    /// Are detailed file-by-file statistics desired?
    pub per_file: bool,

    /// Are detailed column-by-column statistics desired?
    pub per_column: bool,
}

/// Print statistics about all the files rooted at input_path
pub async fn stats(config: &StatsConfig) -> Result<()> {
    info!("stats starting for {:?}", config);
    let input_path = InputPath::new(&config.input_path, |p| {
        p.extension() == Some(std::ffi::OsStr::new("parquet"))
    })
    .context(OpenInput)?;

    println!("Storage statistics:");

    let mut builder = FileSetStatsBuilder::default();

    for input_reader in input_path
        .files()
        .iter()
        .rev()
        .map(|p| InputReader::new(&p.to_string_lossy()))
    {
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
pub async fn stats_for_file(config: &StatsConfig, input_reader: InputReader) -> Result<FileStats> {
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
