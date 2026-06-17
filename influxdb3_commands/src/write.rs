use super::common::InfluxDb3Config;
use bytes::{Bytes, BytesMut};
use clap::Parser;
use futures::{
    Stream, StreamExt,
    stream::{self, BoxStream},
};
use influxdb3_clap_blocks::memory_size::format_bytes;
use influxdb3_client::Precision;
use std::ops::AddAssign;
use std::{
    fs,
    io::{IsTerminal, Read, stdin},
    path::PathBuf,
    pin::Pin,
    task::{Context, Poll, ready},
    time::{Duration, Instant},
};
use tokio::io;
use url::Url;

/// Default per-request body size cap. Matches the server's default
/// `--max-http-request-size` so a `write` command piped from a generator
/// that produces a stream larger than 10 MiB still succeeds out of the box.
const DEFAULT_MAX_REQUEST_SIZE: usize = 10 * 1024 * 1024;

/// Size of each blocking `read()` from the input source (file / stdin).
const READ_SIZE: usize = 4 * 1024 * 1024;

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

    /// Maximum size (in bytes) of a single write request body.
    ///
    /// Inputs larger than this are split at newline boundaries and sent as
    /// multiple requests. The default is chosen to match the default
    /// `--max-http-request-size` (10 MiB).
    #[clap(
        long = "max-request-size",
        default_value_t = DEFAULT_MAX_REQUEST_SIZE,
        value_parser = parse_nonzero_usize,
    )]
    max_request_size: usize,

    /// Maximum number of write requests in flight concurrently.
    ///
    /// Larger values increase throughput when the server can keep up, at the
    /// cost of more memory in the client and more in-flight load on the
    /// server. Set to 1 for strictly sequential writes.
    #[clap(
        long = "max-concurrent-requests",
        default_value_t = 32,
        value_parser = parse_nonzero_usize,
    )]
    max_concurrent_requests: usize,

    /// Quiet mode: suppress status printing to stderr
    #[clap(long = "quiet", short = 'q')]
    quiet: bool,

    /// Give a quoted line protocol line via the command line
    line_protocol: Option<Vec<String>>,

    /// Specify a supported precision (eg: auto, ns, us, ms, s).
    #[clap(short = 'p', long = "precision")]
    precision: Option<Precision>,

    /// An optional arg to use a custom CA, useful for testing with self-signed certs
    #[clap(long = "tls-ca", env = "INFLUXDB3_TLS_CA")]
    ca_cert: Option<PathBuf>,

    /// Disable TLS certificate verification
    #[clap(long = "tls-no-verify", env = "INFLUXDB3_TLS_NO_VERIFY")]
    tls_no_verify: bool,
}

pub async fn command(mut config: Config) -> Result<()> {
    // Prepare to read data into aligned line protocol chunks
    let chunks = WriteChunkStream::new(config.stream(), config.max_request_size);

    let InfluxDb3Config {
        host_url,
        database_name,
        auth_token,
    } = config.influxdb3_config;
    let host_url = config.write_host_url.unwrap_or(host_url);
    let client = influxdb3_client::Client::new(host_url, config.ca_cert, config.tls_no_verify)?
        .with_resolved_auth_token(auth_token.as_ref())
        .await?;

    // Send each chunk and generate metrics using a stream so we can run them in
    // parallel via `buffer_unordered`.
    let max_concurrent_requests = config.max_concurrent_requests;
    let mut result_stream = chunks
        .map(|chunk| async {
            let chunk = chunk?;
            let mut req = client.api_v3_write_lp(&database_name);
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
            let chunk_metrics = Metrics::from_write(&chunk);
            req.body(chunk).send().await?;
            Ok::<_, Error>(chunk_metrics)
        })
        // Note: Uses concurrent I/O, not multiple tasks (cores): once an HTTP
        // request is made, the next is prepared while waiting for the first to
        // finish. Don't even try to use multiple cores in parallel, as
        // preparing requests is mostly copying bytes around, and wasn't a
        // bottleneck in local testing.
        //
        // Note 2: *unordered* client writes as writes are not ordered on server
        .buffer_unordered(max_concurrent_requests);

    // Drain the stream, checking for errors and reporting metrics
    let start = Instant::now();
    let mut total_metrics = Metrics::new();
    while let Some(metrics) = result_stream.next().await.transpose()? {
        total_metrics += metrics;
        if !config.quiet {
            eprintln!("  {}", total_metrics.report(start.elapsed()));
        }
    }

    Ok(())
}

impl Config {
    /// Return the input line protocol as an async stream bytes chunks.
    ///
    /// Note that the chunks are arbitrarily aligned.
    /// Splitting at line boundaries is handled downstream by
    /// [`WriteChunkStream`].
    fn stream(&mut self) -> BoxStream<'static, Result<Bytes>> {
        if let Some(line) = self.line_protocol.take() {
            let line = match parse_line(line) {
                Ok(line) => line,
                Err(e) => return stream::iter(vec![Err(e)]).boxed(),
            };
            stream::iter(vec![Ok(Bytes::from(line))]).boxed()
        } else if let Some(file_path) = self.file_path.as_ref() {
            let file = match fs::File::open(file_path) {
                Ok(file) => file,
                Err(e) => return stream::iter(vec![Err(Error::Io(e))]).boxed(),
            };
            read_as_stream(file)
        } else {
            let stdin = stdin();
            // Checks if stdin has had data passed to it via a pipe
            if stdin.is_terminal() {
                return stream::iter(vec![Err(Error::NoInput)]).boxed();
            }
            read_as_stream(stdin)
        }
    }
}

/// Adapt a blocking `Read` source into an async `Stream<Item = Result<Bytes>>`.
///
/// A single dedicated `spawn_blocking` task owns the reader and feeds buffers
/// through a bounded mpsc channel;
fn read_as_stream<R: Read + Send + 'static>(mut r: R) -> BoxStream<'static, Result<Bytes>> {
    // use 2 so we have a small buffer
    let (tx, rx) = tokio::sync::mpsc::channel::<Result<Bytes>>(2);
    tokio::task::spawn_blocking(move || {
        loop {
            let mut buffer = vec![0u8; READ_SIZE];
            match r.read(&mut buffer) {
                Ok(0) => break, // EOF
                Ok(n) => {
                    buffer.truncate(n);
                    if tx.blocking_send(Ok(Bytes::from(buffer))).is_err() {
                        // Receiver dropped: consumer cancelled, stop reading.
                        break;
                    }
                }
                Err(e) => {
                    // try to send the error back to consumer. Ignore error: if
                    // can't send consumer cancelled / no one is around to get
                    // the error.
                    let _ = tx.blocking_send(Err(Error::Io(e)));
                    break;
                }
            }
        }
    });

    stream::unfold(rx, |mut rx| async move {
        rx.recv().await.map(|item| (item, rx))
    })
    .boxed()
}

/// Stream that coalesces incoming byte chunks and re-emits them as
/// line-protocol chunks of at most `max_size` bytes, always split at `\n`
/// boundaries.
///
/// A single line longer than `max_size` is emitted as one oversize chunk
/// containing that whole line; the server will return a clean error rather
/// than the chunk being silently split mid-line.
struct WriteChunkStream {
    /// Bytes that have been read from the input but not yet emitted.
    buffer: Vec<Bytes>,
    /// Maximum chunk size in bytes.
    max_size: usize,
    /// Underlying input stream.
    input: BoxStream<'static, Result<Bytes>>,
    /// True once the input stream has reported end-of-stream (or an error).
    finished: bool,
}

impl WriteChunkStream {
    fn new(input: BoxStream<'static, Result<Bytes>>, max_size: usize) -> Self {
        Self {
            buffer: vec![],
            max_size,
            input,
            finished: false,
        }
    }

    fn buffered_len(&self) -> usize {
        self.buffer.iter().map(|b| b.len()).sum()
    }

    /// Collapse the buffer into a single contiguous `Bytes`. Cheap when the
    /// buffer already has at most one element.
    fn coalesce(&mut self) -> Bytes {
        match self.buffer.len() {
            0 => Bytes::new(),
            1 => self.buffer.pop().unwrap(),
            _ => {
                let total = self.buffered_len();
                let mut out = BytesMut::with_capacity(total);
                for b in self.buffer.drain(..) {
                    out.extend_from_slice(&b);
                }
                out.freeze()
            }
        }
    }
}

impl Stream for WriteChunkStream {
    type Item = Result<Bytes>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        loop {
            let total = self.buffered_len();

            // Try to emit a chunk when either have at least max_size bytes
            // buffered, or input has ended and there is buffered data
            if total > 0 && (total >= self.max_size || self.finished) {
                let coalesced = self.coalesce();
                match next_chunk_end(&coalesced, self.max_size, self.finished) {
                    Some(end) => {
                        let chunk = coalesced.slice(..end);
                        if end < coalesced.len() {
                            self.buffer.push(coalesced.slice(end..));
                        }
                        return Poll::Ready(Some(Ok(chunk)));
                    }
                    None => {
                        // We have ≥ max_size data but no newline anywhere yet
                        // — keep reading until we find one or hit EOF.
                        self.buffer.push(coalesced);
                    }
                }
            }

            if self.finished {
                debug_assert!(self.buffer.is_empty());
                return Poll::Ready(None);
            }

            match ready!(self.input.poll_next_unpin(cx)) {
                None => self.finished = true,
                Some(Ok(b)) => {
                    if !b.is_empty() {
                        self.buffer.push(b);
                    }
                }
                Some(Err(e)) => {
                    // Abort: discard any buffered data and return the error
                    // immediately
                    self.buffer.clear();
                    self.finished = true;
                    return Poll::Ready(Some(Err(e)));
                }
            }
        }
    }
}

/// Determine how many bytes of `buf` to emit as the next chunk, or `None` if
/// more input is needed (only possible when `!finished`).
///
/// The result always falls on a `\n` boundary unless the input ends without
/// a trailing newline (in which case the final partial line is emitted).
fn next_chunk_end(buf: &[u8], max_size: usize, finished: bool) -> Option<usize> {
    if buf.is_empty() {
        return None;
    }
    let window = max_size.min(buf.len());
    // Prefer the largest line-aligned prefix <= max_size.
    if window > 0
        && let Some(nl) = buf[..window].iter().rposition(|&b| b == b'\n')
    {
        return Some(nl + 1);
    }
    // No newline within the window. Look beyond it for an oversize whole line.
    if let Some(nl) = buf.iter().position(|&b| b == b'\n') {
        return Some(nl + 1);
    }
    // No newline anywhere. If the input is finished, emit whatever's left
    // (the final line had no trailing newline). Otherwise wait for more.
    if finished { Some(buf.len()) } else { None }
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

/// Clap value parser for flags that require a positive (non-zero) `usize`.
///
/// `--max-request-size 0` would defeat the chunking logic, and
/// `--max-concurrent-requests 0` would stall `buffer_unordered`, so both must
/// be rejected at the CLI layer with a clear message.
fn parse_nonzero_usize(s: &str) -> std::result::Result<usize, String> {
    let v: usize = s
        .parse()
        .map_err(|e: std::num::ParseIntError| e.to_string())?;
    if v == 0 {
        Err("value must be at least 1".to_string())
    } else {
        Ok(v)
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
