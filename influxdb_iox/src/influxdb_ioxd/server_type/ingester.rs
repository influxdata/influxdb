use std::{
    fmt::{Debug, Display},
    sync::Arc,
};

use async_trait::async_trait;
use hyper::{Body, Request, Response};
use ingester::server::IngesterServer;
use metric::Registry;
use tokio_util::sync::CancellationToken;
use trace::TraceCollector;

use crate::influxdb_ioxd::{
    http::error::{HttpApiError, HttpApiErrorSource},
    rpc::RpcBuilderInput,
    server_type::{common_state::CommonServerState, RpcError, ServerType},
};
use ingester::handler::IngestHandler;

#[derive(Debug)]
pub struct IngesterServerType<I: IngestHandler> {
    server: IngesterServer<I>,
    shutdown: CancellationToken,
    trace_collector: Option<Arc<dyn TraceCollector>>,
}

impl<I: IngestHandler> IngesterServerType<I> {
    pub fn new(server: IngesterServer<I>, common_state: &CommonServerState) -> Self {
        Self {
            server,
            shutdown: CancellationToken::new(),
            trace_collector: common_state.trace_collector(),
        }
    }
}

#[async_trait]
impl<I: IngestHandler + Sync + Send + Debug + 'static> ServerType for IngesterServerType<I> {
    type RouteError = IoxHttpErrorAdaptor;

    /// Return the [`metric::Registry`] used by the router.
    fn metric_registry(&self) -> Arc<Registry> {
        self.server.metric_registry()
    }

    /// Returns the trace collector for router traces.
    fn trace_collector(&self) -> Option<Arc<dyn TraceCollector>> {
        self.trace_collector.as_ref().map(Arc::clone)
    }

    /// Dispatches `req` to the router [`HttpDelegate`] delegate.
    ///
    /// [`HttpDelegate`]: router2::server::http::HttpDelegate
    async fn route_http_request(
        &self,
        _req: Request<Body>,
    ) -> Result<Response<Body>, Self::RouteError> {
        unimplemented!();
    }

    /// Registers the services exposed by the router [`GrpcDelegate`] delegate.
    ///
    /// [`GrpcDelegate`]: router2::server::grpc::GrpcDelegate
    async fn server_grpc(self: Arc<Self>, _builder_input: RpcBuilderInput) -> Result<(), RpcError> {
        unimplemented!()
        // let builder = setup_builder!(builder_input, self);
        // add_service!(builder, self.server.grpc().write_service());
        // serve_builder!(builder);
        //
        // Ok(())
    }

    async fn join(self: Arc<Self>) {
        self.shutdown.cancelled().await;
    }

    fn shutdown(&self) {
        self.shutdown.cancel();
    }
}

/// This adaptor converts the `ingester` http error type into a type that
/// satisfies the requirements of influxdb_ioxd's runner framework, keeping the
/// two decoupled.
#[derive(Debug)]
pub struct IoxHttpErrorAdaptor(router2::server::http::Error);

impl Display for IoxHttpErrorAdaptor {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        Display::fmt(&self.0, f)
    }
}

impl std::error::Error for IoxHttpErrorAdaptor {}

impl HttpApiErrorSource for IoxHttpErrorAdaptor {
    fn to_http_api_error(&self) -> HttpApiError {
        HttpApiError::new(self.0.as_status_code(), self.to_string())
    }
}
