use super::common::InfluxDb3Config;
use clap::Parser;
use influxdb3_clap_blocks::memory_size::format_bytes;
use influxdb3_client::Precision;
use std::ops::AddAssign;
use std::{
    fs,
    io::{BufReader, IsTerminal, Read, stdin},
    path::PathBuf,
    time::{Duration, Instant},
};
use tokio::io;
use url::Url;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error(transparent)]
    Client(#[from] influxdb3_client::Error),

    #[error("error reading file: {0}")]
    Io(#[from] io::Error),

    #[error("No input from stdin detected, no string was passed in, and no file path was given")]
    NoInput,

    #[error("no line protocol string provided")]
    NoLine,

    #[error(
        "ensure that a single protocol line string is provided as the final \
        argument, enclosed in quotes"
    )]
    MoreThanOne,
}

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, Parser)]
#[clap(visible_alias = "w", trailing_var_arg = true)]
pub struct Config {
    /// Common InfluxDB 3 server config
    #[clap(flatten)]
    influxdb3_config: InfluxDb3Config,

    /// Override host URL for write operations
    #[clap(long = "write-host", env = "INFLUXDB3_WRITE_HOST_URL")]
    write_host_url: Option<Url>,

    /// File path to load the write data from
    ///
    /// Currently, only files containing line protocol are supported.
    #[clap(short = 'f', long = "file")]
    file_path: Option<String>,

    /// Flag to request the server accept partial writes
    ///
    /// Invalid lines in the input data will be ignored by the server.
    #[clap(long = "accept-partial")]
    accept_partial_writes: bool,

    /// Flag to request the server not wait for sync before ACK'ing
    ///
    /// This option returns a success before a write is durable.
    #[clap(long = "no-sync")]
    no_sync_writes: bool,

    /// Gzip-compress the request body before sending.
    ///
    /// Reduces network bytes for large line-protocol payloads (e.g. when using
    /// `--file`). The server accepts gzipped writes.
    #[clap(long = "gzip")]
    gzip: bool,

    /// Quiet mode: suppress status printing to stdout
    #[clap(long = "quiet", short = 'q')]
    quiet: bool,

    /// Give a quoted line protocol line via the command line
    line_protocol: Option<Vec<String>>,

    /// Specify a supported precision (eg: auto, ns, us, ms, s).
    #[clap(short = 'p', long = "precision")]
    precision: Option<Precision>,

    /// An optional arg to use a custom ca for useful for testing with self signed certs
    #[clap(long = "tls-ca", env = "INFLUXDB3_TLS_CA")]
    ca_cert: Option<PathBuf>,

    /// Disable TLS certificate verification
    #[clap(long = "tls-no-verify", env = "INFLUXDB3_TLS_NO_VERIFY")]
    tls_no_verify: bool,
}

pub async fn command(config: Config) -> Result<()> {
    let InfluxDb3Config {
        host_url,
        database_name,
        auth_token,
    } = config.influxdb3_config;
    let host_url = config.write_host_url.unwrap_or(host_url);
    let client = influxdb3_client::Client::new(host_url, config.ca_cert, config.tls_no_verify)?
        .with_resolved_auth_token(auth_token.as_ref())
        .await?;

    let writes = if let Some(line) = config.line_protocol {
        parse_line(line)?
    } else if let Some(file_path) = config.file_path {
        fs::read_to_string(file_path)?
    } else {
        let stdin = stdin();
        // Checks if stdin has had data passed to it via a pipe
        if stdin.is_terminal() {
            return Err(Error::NoInput);
        }
        let mut reader = BufReader::new(stdin);
        let mut buffer = String::new();
        reader.read_to_string(&mut buffer)?;
        buffer
    };

    let mut total_metrics = Metrics::new();

    let start = Instant::now();
    let mut req = client.api_v3_write_lp(database_name);
    if let Some(precision) = config.precision {
        req = req.precision(precision);
    }
    if config.accept_partial_writes {
        req = req.accept_partial(true);
    }
    if config.no_sync_writes {
        req = req.no_sync(true);
    }
    if config.gzip {
        req = req.with_gzip(true);
    }
    let write_metrics = Metrics::from_write(&writes);
    req.body(writes).send().await?;
    total_metrics += write_metrics;

    let elapsed = start.elapsed();
    if !config.quiet {
        println!("{}", total_metrics.report(elapsed));
    }

    Ok(())
}

/// Track data sent during write.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
struct Metrics {
    chunk_count: usize,
    bytes_sent: usize,
    lines_sent: usize,
}

impl AddAssign for Metrics {
    fn add_assign(&mut self, other: Self) {
        self.chunk_count += other.chunk_count;
        self.bytes_sent += other.bytes_sent;
        self.lines_sent += other.lines_sent;
    }
}

impl Metrics {
    /// Create a new empty metrics object.
    fn new() -> Self {
        Self::default()
    }

    /// Return metrics describing a single write of the given line-protocol
    /// bytes.
    fn from_write(write: impl AsRef<[u8]>) -> Self {
        let write = write.as_ref();
        Self {
            chunk_count: 1,
            bytes_sent: write.len(),
            lines_sent: count_lines(write),
        }
    }

    /// View this `Metrics` paired with an elapsed duration for `Display`
    /// rendering: counts + throughput.
    fn report(&self, elapsed: Duration) -> MetricsReport<'_> {
        MetricsReport {
            metrics: self,
            elapsed,
        }
    }
}

/// Render `elapsed` as a humantime string with at most millisecond
/// precision (drop microseconds / nanoseconds).
///
/// For example: 1s 230ms
fn fmt_elapsed_ms(elapsed: Duration) -> humantime::FormattedDuration {
    let ms = u64::try_from(elapsed.as_millis()).unwrap_or(u64::MAX);
    humantime::format_duration(Duration::from_millis(ms))
}

/// `Display` adapter that pairs a `Metrics` snapshot with an elapsed time.
struct MetricsReport<'a> {
    metrics: &'a Metrics,
    elapsed: Duration,
}

impl std::fmt::Display for MetricsReport<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let m = self.metrics;
        if m.chunk_count == 0 {
            write!(f, "no data written")?;
            return Ok(());
        }
        let request_word = if m.chunk_count == 1 {
            "request"
        } else {
            "requests"
        };
        let secs = self.elapsed.as_secs_f64().max(f64::EPSILON);
        let rps = m.chunk_count as f64 / secs;
        let lps = (m.lines_sent as f64 / secs).round() as u64;
        let bps = (m.bytes_sent as f64 / secs) as usize;
        write!(
            f,
            "{}: \
             {} {request_word} ({rps:.2} requests/sec), \
             {} lines ({lps} lines/s), \
             {} ({}/s)",
            fmt_elapsed_ms(self.elapsed),
            m.chunk_count,
            m.lines_sent,
            format_bytes(m.bytes_sent),
            format_bytes(bps),
        )
    }
}

/// Count lines in a portion of line protocol data: number of `\n` bytes, plus
/// one for any trailing partial line.
fn count_lines(write: &[u8]) -> usize {
    if write.is_empty() {
        return 0;
    }
    let newlines = write.iter().filter(|&&b| b == b'\n').count();
    if write.last() == Some(&b'\n') {
        newlines
    } else {
        newlines + 1
    }
}

/// Parse the user-inputted line protocol string
///
/// NOTE: This is only necessary because clap will not accept a single string for a trailing arg
fn parse_line(mut input: Vec<String>) -> Result<String> {
    if input.is_empty() {
        Err(Error::NoLine)?
    }
    if input.len() > 1 {
        Err(Error::MoreThanOne)?
    } else {
        Ok(input.remove(0))
    }
}

#[cfg(test)]
mod tests;
