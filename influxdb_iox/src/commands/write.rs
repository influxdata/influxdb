use futures::{stream::BoxStream, StreamExt};
use influxdb_iox_client::{connection::Connection, write};
use observability_deps::tracing::{debug, info};
use snafu::{ensure, OptionExt, ResultExt, Snafu};
use std::{
    fs::File,
    io::{BufReader, Read},
    num::NonZeroUsize,
    path::{Path, PathBuf},
    time::Instant,
};

#[allow(clippy::enum_variant_names)]
#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Error reading file {:?}: {}", file_name, source))]
    ReadingFile {
        file_name: PathBuf,
        source: std::io::Error,
    },

    #[snafu(display("Error reading files: {:#?}", sources))]
    ReadingFiles { sources: Vec<Error> },

    #[snafu(display("Client error: {source}"))]
    ClientError {
        source: influxdb_iox_client::error::Error,
    },

    #[snafu(display("Error converting parquet: {}", source))]
    Conversion {
        source: parquet_to_line_protocol::Error,
    },

    #[snafu(display("Line protocol was not valid utf8: {}", source))]
    InvalidUtf8 { source: std::string::FromUtf8Error },

    #[snafu(display("Error decoding gzip {:?}:  {}", file_name, source))]
    Gz {
        file_name: PathBuf,
        source: std::io::Error,
    },

    #[snafu(display("Max concurrent uploads must be greater than zero"))]
    MaxConcurrentUploadsVerfication,
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

/// Write data into the specified namespace
#[derive(Debug, clap::Parser)]
pub struct Config {
    /// If specified, restricts the maximum amount of line protocol
    /// sent per request to this many bytes. Defaults to 1MB
    #[clap(action, long, short = 'b', default_value = "1048576")]
    max_request_payload_size_bytes: usize,

    /// Uploads up to this many http requests at a time. Defaults to 10
    #[clap(action, long, short = 'c', default_value = "10")]
    max_concurrent_uploads: usize,

    /// The namespace into which to write, in the form <org_id>_<bucket_id>
    #[clap(action)]
    namespace: String,

    /// File(s) with data to load. Currently supported formats are .lp (line protocol),
    /// .parquet (IOx created parquet files), and .gz (gzipped line protocol)
    #[clap(action)]
    file_names: Vec<PathBuf>,
}

pub async fn command(connection: Connection, config: Config) -> Result<()> {
    let start = Instant::now();

    let Config {
        namespace,
        file_names,
        max_request_payload_size_bytes,
        max_concurrent_uploads,
    } = config;

    let max_concurrent_uploads =
        NonZeroUsize::new(max_concurrent_uploads).context(MaxConcurrentUploadsVerficationSnafu)?;

    info!(
        num_files = file_names.len(),
        max_request_payload_size_bytes, max_concurrent_uploads, "Beginning upload"
    );

    // first pass is to check that all the files exist and can be
    // opened and if not fail fast.
    let file_open_errors: Vec<_> = file_names
        .iter()
        .filter_map(|file_name| {
            File::open(file_name)
                .context(ReadingFileSnafu { file_name })
                .err()
        })
        .collect();

    ensure!(
        file_open_errors.is_empty(),
        ReadingFilesSnafu {
            sources: file_open_errors
        }
    );

    // if everything looked good, go through and read the files out
    // them in parallel.
    let lp_stream = futures_util::stream::iter(file_names)
        .map(|file_name| tokio::task::spawn(slurp_file(file_name)))
        // Since the contents of each file are buffered into a string,
        // limit the number that are open at once to the maximum
        // possible uploads
        .buffered(max_concurrent_uploads.into())
        // warn and skip any errors starting to read the files
        .filter_map(|res| async move {
            match res {
                Ok(Ok(stream)) => Some(stream),
                Ok(Err(e)) => {
                    eprintln!("WARNING: ignoring error : {e}");
                    None
                }
                Err(e) => {
                    eprintln!("WARNING: ignoring task fail: {e}");
                    None
                }
            }
        })
        // now flatten out the stream of streams and ignore any errors actually reading the files
        .flat_map(|stream| stream)
        .filter_map(|res| async move {
            match res {
                Ok(lp_data) => {
                    debug!(
                        len = lp_data.len(),
                        lines = lp_data.lines().count(),
                        "Sending block of lines"
                    );
                    Some(lp_data)
                }
                Err(e) => {
                    eprintln!("WARNING: ignoring read error: {e}");
                    None
                }
            }
        });

    let mut client = write::Client::new(connection)
        .with_max_concurrent_uploads(max_concurrent_uploads)
        .with_max_request_payload_size_bytes(Some(max_request_payload_size_bytes));

    let total_bytes = client
        .write_lp_stream(namespace, lp_stream)
        .await
        .context(ClientSnafu)?;

    let elapsed = Instant::now() - start;
    let mb = (total_bytes as f64) / (1024.0 * 1024.0);
    let mb_per_sec = (mb / (elapsed.as_millis() as f64)) * (1000.0);
    println!("{total_bytes} Bytes OK in {elapsed:?}. {mb_per_sec:.2} MB/sec");

    Ok(())
}

/// Reads the contents of `file_name` into one or more Strings, each
/// containing a whole number of line protcol (lines do not span results)
///
/// .parquet files --> iox parquet files (convert to parquet)
/// .gz  --> treated as gzipped line protocol
/// .lp (or anything else) --> treated as raw line protocol
///
async fn slurp_file(file_name: PathBuf) -> Result<BoxStream<'static, Result<String>>> {
    let file_name = &file_name;

    let extension = file_name
        .extension()
        .map(|extension| extension.to_ascii_lowercase());

    match extension {
        // Transform parquet to line protocol prior to upload
        // Not the most efficient process, but it is expedient
        Some(extension) if extension.to_string_lossy() == "parquet" => {
            info!(
                ?file_name,
                file_size_bytes = file_size(file_name),
                "Streaming line protocol from parquet file"
            );
            let stream = parquet_to_line_protocol::convert_file(file_name)
                .await
                .context(ConversionSnafu)?
                .map(|lp_data| {
                    lp_data
                        .context(ConversionSnafu)
                        .and_then(|lp_data| String::from_utf8(lp_data).context(InvalidUtf8Snafu))
                })
                .boxed();
            Ok(stream)
        }
        // decompress as gz
        Some(extension) if extension.to_string_lossy() == "gz" => {
            let mut lp_data = String::new();
            let reader =
                BufReader::new(File::open(file_name).context(ReadingFileSnafu { file_name })?);

            // could be fanicer and read out chunks of line protocol,
            // but we would have to figure out where the newlines were
            // so for now just buffer the whole thing
            flate2::read::GzDecoder::new(reader)
                .read_to_string(&mut lp_data)
                .context(GzSnafu { file_name })?;

            info!(
                ?file_name,
                file_size_bytes = lp_data.len(),
                "Buffered line protocol from gzipped line protocol file"
            );
            let stream = futures::stream::iter(vec![Ok(lp_data)]).boxed();
            Ok(stream)
        }
        // anything else, treat as line protocol
        Some(_) | None => {
            // could be fanicer and read out chunks of line protocol,
            // but we would have to figure out where the newlines were
            // so for now just buffer the whole thing
            let lp_data =
                std::fs::read_to_string(file_name).context(ReadingFileSnafu { file_name })?;

            info!(
                ?file_name,
                file_size_bytes = lp_data.len(),
                "Buffered line protocol file"
            );
            let stream = futures::stream::iter(vec![Ok(lp_data)]).boxed();
            Ok(stream)
        }
    }
}

fn file_size(path: &Path) -> u64 {
    std::fs::metadata(path)
        .map(|meta| meta.len())
        // ignore errors fetching metadta
        .unwrap_or_default()
}

#[cfg(test)]
mod test {
    use clap::Parser;
    use influxdb_iox_client::write::DEFAULT_MAX_REQUEST_PAYLOAD_SIZE_BYTES;

    use super::*;

    #[test]
    fn command_default_is_same_as_client_default() {
        let config = Config::try_parse_from(vec!["my_db", "file1"]).unwrap();
        assert_eq!(
            Some(config.max_request_payload_size_bytes),
            DEFAULT_MAX_REQUEST_PAYLOAD_SIZE_BYTES
        );
    }
}
