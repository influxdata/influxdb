//! InfluxDB 3.0 Edge server implementation
//!
//! The server is responsible for handling the HTTP API
mod http;
mod write_buffer;
mod query_executor;

use std::borrow::Cow;
use arrow::record_batch::RecordBatch;
use std::fmt::Debug;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::Arc;
use observability_deps::tracing::info;
use thiserror::Error;
use tokio_util::sync::CancellationToken;
use trace::TraceCollector;
use crate::http::HttpApi;
use async_trait::async_trait;
use futures::Stream;
use trace_http::{ctx::TraceHeaderParser, tower::TraceLayer};

#[derive(Debug, Error)]
pub enum Error {
    #[error("http error: {0}")]
    Http(#[from] http::Error),
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

#[derive(Debug, Clone)]
pub struct CommonServerState {
    metrics: Arc<metric::Registry>,
    trace_exporter: Option<Arc<trace_exporters::export::AsyncExporter>>,
    trace_header_parser: TraceHeaderParser,
    http_addr: SocketAddr,
}

impl CommonServerState {
    pub fn new(metrics: Arc<metric::Registry>, trace_exporter: Option<Arc<trace_exporters::export::AsyncExporter>>, trace_header_parser: TraceHeaderParser, http_addr: SocketAddr) -> Self {
        Self{
            metrics,
            trace_exporter,
            trace_header_parser,
            http_addr,
        }
    }

    pub fn trace_exporter(&self) -> Option<Arc<trace_exporters::export::AsyncExporter>> {
        self.trace_exporter.clone()
    }

    pub fn trace_collector(&self) -> Option<Arc<dyn TraceCollector>> {
        self.trace_exporter
            .clone()
            .map(|x| -> Arc<dyn TraceCollector> { x })
    }

    pub fn trace_header_parser(&self) -> TraceHeaderParser {
        self.trace_header_parser.clone()
    }

    pub fn metric_registry(&self) -> Arc<metric::Registry> {
        self.metrics.clone()
    }
}

#[derive(Debug)]
pub struct Server<W, Q> {
    common_state: CommonServerState,
    http: Arc<HttpApi<W, Q>>,
    write_buffer: Arc<W>,
    query_executor: Arc<Q>,
}

/// A correctly formed database name.
///
/// Using this wrapper type allows the consuming code to enforce the invariant
/// that only valid names are provided.
///
/// This type derefs to a `str` and therefore can be used in place of anything
/// that is expecting a `str`:
///
/// ```rust
/// # use influxdb3_server::DatabaseName;
/// fn print_database(s: &str) {
///     println!("database name: {}", s);
/// }
///
/// let db = DatabaseName::new("data").unwrap();
/// print_database(&db);
/// ```
///
/// But this is not reciprocal - functions that wish to accept only
/// pre-validated names can use `DatabaseName` as a parameter.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct DatabaseName<'a>(Cow<'a, str>);

#[async_trait]
pub trait WriteBuffer: Debug + Send + Sync + 'static {
    async fn write_lp(&self, database: DatabaseName<'static>, lp: &str) -> Result<()>;
}

#[async_trait]
pub trait QueryExecutor: Debug + Send + Sync + 'static {
    async fn query(&self, database: DatabaseName<'static>, q: &str) -> Result<QueryResult>;
}

pub type QueryResult = Pin<Box<dyn Stream<Item = Vec<RecordBatch>> + Send>>;

impl<W, Q> Server<W, Q> {
    pub fn new(common_state: CommonServerState, write_buffer: Arc<W>, query_executor: Arc<Q>) -> Self {
        let http = Arc::new(HttpApi::new(common_state.clone(), write_buffer.clone(), query_executor.clone()));

        Self{
            common_state,
            http,
            write_buffer,
            query_executor,
        }
    }
}

pub async fn serve<W: WriteBuffer, Q: QueryExecutor>(server: Server<W, Q>, shutdown: CancellationToken) -> Result<()> {
    http::serve(Arc::clone(&server.http), shutdown).await?;

    Ok(())
}

/// On unix platforms we want to intercept SIGINT and SIGTERM
/// This method returns if either are signalled
#[cfg(unix)]
pub async fn wait_for_signal() {
    use tokio::signal::unix::{signal, SignalKind};
    let mut term = signal(SignalKind::terminate()).expect("failed to register signal handler");
    let mut int = signal(SignalKind::interrupt()).expect("failed to register signal handler");

    tokio::select! {
        _ = term.recv() => info!("Received SIGTERM"),
        _ = int.recv() => info!("Received SIGINT"),
    }
}

#[cfg(windows)]
/// ctrl_c is the cross-platform way to intercept the equivalent of SIGINT
/// This method returns if this occurs
pub async fn wait_for_signal() {
    let _ = tokio::signal::ctrl_c().await;
}



#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn it_works() {
        assert!(false);
    }
}
