mod common_state;

use std::sync::Arc;

use async_trait::async_trait;
use hyper::{Body, Request, Response};
use metric::Registry;
use snafu::Snafu;
use tokio_util::sync::CancellationToken;
use trace::TraceCollector;

pub use common_state::{CommonServerState, CommonServerStateError};

use crate::{http::error::HttpApiErrorSource, rpc::RpcBuilderInput};

#[derive(Debug, Snafu)]
pub enum RpcError {
    #[snafu(display("gRPC transport error: {}{}", source, details))]
    TransportError {
        source: tonic::transport::Error,
        details: String,
    },
}

// Custom impl to include underlying source (not included in tonic
// transport error)
impl From<tonic::transport::Error> for RpcError {
    fn from(source: tonic::transport::Error) -> Self {
        use std::error::Error;
        let details = source
            .source()
            .map(|e| format!(" ({e})"))
            .unwrap_or_else(|| "".to_string());

        Self::TransportError { source, details }
    }
}

#[async_trait]
pub trait ServerType: std::fmt::Debug + Send + Sync + 'static {
    /// Human name for this server type
    fn name(&self) -> &str;

    /// Metric registry associated with the server.
    fn metric_registry(&self) -> Arc<Registry>;

    /// Trace collector associated with the server, if any.
    fn trace_collector(&self) -> Option<Arc<dyn TraceCollector>>;

    /// Route given HTTP request.
    ///
    /// Note that this is only called if none of the shared, common routes (e.g. `/health`) match.
    async fn route_http_request(
        &self,
        req: Request<Body>,
    ) -> Result<Response<Body>, Box<dyn HttpApiErrorSource>>;

    /// Construct and serve gRPC subsystem.
    async fn server_grpc(self: Arc<Self>, builder_input: RpcBuilderInput) -> Result<(), RpcError>;

    /// Join shutdown worker.
    ///
    /// This MUST NOT exit before `shutdown` is called, otherwise the server is deemed to be dead and the process will exit.
    async fn join(self: Arc<Self>);

    /// Shutdown background worker.
    ///
    /// The provided [`CancellationToken`] MUST be used by the background worker
    /// to shutdown the "frontend" (HTTP & RPC servers) when appropriate - this
    /// should happen before [`Self::join()`] returns.
    fn shutdown(&self, frontend: CancellationToken);
}
