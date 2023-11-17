//! InfluxDB 3.0 Edge server implementation
//!
//! The server is responsible for handling the HTTP API
#![deny(rustdoc::broken_intra_doc_links, rustdoc::bare_urls, rust_2018_idioms)]
#![warn(
missing_debug_implementations,
clippy::explicit_iter_loop,
clippy::use_self,
clippy::clone_on_ref_ptr,
// See https://github.com/influxdata/influxdb_iox/pull/1671
clippy::future_not_send
)]

mod http;
pub mod write_buffer;
pub mod query_executor;
pub mod catalog;

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
use datafusion::error::DataFusionError;
use datafusion::execution::context::SessionState;
use datafusion::execution::SendableRecordBatchStream;
use datafusion::prelude::Expr;
use futures::Stream;
use data_types::NamespaceName;
use iox_query::exec::IOxSessionContext;
use iox_query::QueryChunk;
use trace::ctx::SpanContext;
use trace_http::ctx::TraceHeaderParser;
use trace_http::ctx::RequestLogContext;

#[derive(Debug, Error)]
pub enum Error {
    #[error("http error: {0}")]
    Http(#[from] http::Error),

    #[error("database not found {db_name}")]
    DatabaseNotFound { db_name: String },

    #[error("datafusion error: {0}")]
    DataFusion(#[from] datafusion::error::DataFusionError),

    #[error("write buffer error: {0}")]
    WriteBuffer(#[from] write_buffer::Error),
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

#[async_trait]
pub trait WriteBuffer: Debug + Send + Sync + 'static {
    async fn write_lp(&self, database: NamespaceName<'static>, lp: &str) -> Result<(), write_buffer::Error>;

    fn get_table_chunks(&self, database_name: &str, table_name: &str, filters: &[Expr], projection: Option<&Vec<usize>>, ctx: &SessionState) -> Result<Vec<Arc<dyn QueryChunk>>, DataFusionError>;
}

#[async_trait]
pub trait QueryExecutor: Debug + Send + Sync + 'static {
    async fn query(&self, database: &str, q: &str, span_ctx: Option<SpanContext>, external_span_ctx: Option<RequestLogContext>) -> Result<SendableRecordBatchStream>;
}

impl<W, Q> Server<W, Q> {
    pub fn new(common_state: CommonServerState, write_buffer: Arc<W>, query_executor: Arc<Q>, max_http_request_size: usize) -> Self {
        let http = Arc::new(HttpApi::new(common_state.clone(), write_buffer.clone(), query_executor.clone(), max_http_request_size));

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

}
