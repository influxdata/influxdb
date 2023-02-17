//! This module implements the `parquet_to_lp` CLI command
use std::{path::PathBuf, pin::Pin};

use observability_deps::tracing::info;
use snafu::{ResultExt, Snafu};
use tokio::io::{AsyncWrite, AsyncWriteExt, BufWriter};
use tokio_stream::StreamExt;

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
    #[snafu(display("IO error: {}", source))]
    IO { source: std::io::Error },
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
        let file = tokio::fs::File::create(path).await.context(FileSnafu {
            operation: "open",
            path,
        })?;

        let file = convert(input, file).await?;

        file.sync_all().await.context(FileSnafu {
            operation: "close",
            path,
        })?;
    } else {
        convert(input, tokio::io::stdout()).await?;
    }

    Ok(())
}

/// Does the actual conversion, returning the writer when done
async fn convert<W: AsyncWrite + Send + Unpin>(input: PathBuf, writer: W) -> Result<W, Error> {
    let buf_writer = BufWriter::new(writer);
    let mut writer = Box::pin(buf_writer);

    // prepare the conversion
    let mut input_stream = parquet_to_line_protocol::convert_file(input)
        .await
        .context(ConversionSnafu)?;

    // now read a batch and write it to the output as fast as we can
    while let Some(bytes) = input_stream.next().await {
        let bytes = bytes.context(ConversionSnafu)?;
        writer.write_all(&bytes).await.context(IOSnafu)?;
    }

    // flush any remaining buffered data
    writer.flush().await.map_err(|e| Error::Flush {
        message: e.to_string(),
    })?;
    let buf_writer = *(Pin::into_inner(writer));

    // flush the inner writer
    let mut writer = buf_writer.into_inner();
    writer.flush().await.map_err(|e| Error::Flush {
        message: e.to_string(),
    })?;

    Ok(writer)
}
