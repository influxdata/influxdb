//! This module implements the `parquet_to_lp` CLI command
use std::{io::BufWriter, path::PathBuf};

use observability_deps::tracing::info;
use snafu::{ResultExt, Snafu};

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Cannot {} output file '{:?}': {}", operation, path, source))]
    File {
        operation: String,
        path: PathBuf,
        source: std::io::Error,
    },
    #[snafu(display("Error converting: {}", source))]
    Conversion {
        source: parquet_to_line_protocol::Error,
    },
    #[snafu(display("Cannot flush output: {}", message))]
    Flush {
        // flush error has the W writer in it, all we care about is the error
        message: String,
    },
}

/// Convert IOx Parquet files into InfluxDB line protocol format
#[derive(Debug, clap::Parser)]
pub struct Config {
    /// Input file name
    #[clap(value_parser)]
    input: PathBuf,

    #[clap(long, short)]
    /// The path to which to write. If not specified writes to stdout
    output: Option<PathBuf>,
}

pub async fn command(config: Config) -> Result<(), Error> {
    let Config { input, output } = config;
    info!(?input, ?output, "Exporting parquet as line protocol");

    if let Some(output) = output {
        let path = &output;
        let file = std::fs::File::create(path).context(FileSnafu {
            operation: "open",
            path,
        })?;

        let file = convert(input, file).await?;

        file.sync_all().context(FileSnafu {
            operation: "close",
            path,
        })?;
    } else {
        convert(input, std::io::stdout()).await?;
    }

    Ok(())
}

/// Does the actual conversion, returning the writer when done
async fn convert<W: std::io::Write + Send>(input: PathBuf, writer: W) -> Result<W, Error> {
    // use a buffered writer and ensure it is flushed
    parquet_to_line_protocol::convert_file(input, BufWriter::new(writer))
        .await
        .context(ConversionSnafu)?
        // flush the buffered writer
        .into_inner()
        .map_err(|e| Error::Flush {
            message: e.to_string(),
        })
}
