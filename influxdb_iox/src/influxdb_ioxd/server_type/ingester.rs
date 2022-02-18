use std::{
    fmt::{Debug, Display},
    sync::Arc,
};

use async_trait::async_trait;
use hyper::{Body, Request, Response};
use ingester::server::IngesterServer;
use metric::Registry;
use trace::TraceCollector;

use crate::influxdb_ioxd::{
    http::error::{HttpApiError, HttpApiErrorCode, HttpApiErrorSource},
    rpc::{add_gated_service, add_service, serve_builder, setup_builder, RpcBuilderInput},
    server_type::{common_state::CommonServerState, RpcError, ServerType},
};
use ingester::handler::IngestHandler;

#[derive(Debug)]
pub struct IngesterServerType<I: IngestHandler> {
    server: IngesterServer<I>,
    trace_collector: Option<Arc<dyn TraceCollector>>,
}

impl<I: IngestHandler> IngesterServerType<I> {
    pub fn new(server: IngesterServer<I>, common_state: &CommonServerState) -> Self {
        Self {
            server,
            trace_collector: common_state.trace_collector(),
        }
    }
}

#[async_trait]
impl<I: IngestHandler + Sync + Send + Debug + 'static> ServerType for IngesterServerType<I> {
    type RouteError = IoxHttpError;

    /// Return the [`metric::Registry`] used by the ingester.
    fn metric_registry(&self) -> Arc<Registry> {
        self.server.metric_registry()
    }

    /// Returns the trace collector for ingester traces.
    fn trace_collector(&self) -> Option<Arc<dyn TraceCollector>> {
        self.trace_collector.as_ref().map(Arc::clone)
    }

    /// Just return "not found".
    async fn route_http_request(
        &self,
        _req: Request<Body>,
    ) -> Result<Response<Body>, Self::RouteError> {
        Err(IoxHttpError::NotFound)
    }

    /// Provide a placeholder gRPC service.
    async fn server_grpc(self: Arc<Self>, builder_input: RpcBuilderInput) -> Result<(), RpcError> {
        let builder = setup_builder!(builder_input, self);
        add_gated_service!(builder, self.server.grpc().flight_service());
        serve_builder!(builder);

        Ok(())
    }

    async fn join(self: Arc<Self>) {
        self.server.join().await;
    }

    fn shutdown(&self) {
        self.server.shutdown();
    }
}

/// Simple error struct, we're not really providing an HTTP interface for the ingester.
#[derive(Debug)]
pub enum IoxHttpError {
    NotFound,
}

impl IoxHttpError {
    fn status_code(&self) -> HttpApiErrorCode {
        match self {
            IoxHttpError::NotFound => HttpApiErrorCode::NotFound,
        }
    }
}

impl Display for IoxHttpError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self)
    }
}

impl std::error::Error for IoxHttpError {}

impl HttpApiErrorSource for IoxHttpError {
    fn to_http_api_error(&self) -> HttpApiError {
        HttpApiError::new(self.status_code(), self.to_string())
    }
}
