use std::{fmt::Display, sync::Arc};

use async_trait::async_trait;
use hyper::{Body, Request, Response};
use metric::Registry;
use router2::server::RouterServer;
use tokio_util::sync::CancellationToken;
use trace::TraceCollector;

use crate::influxdb_ioxd::{
    http::error::{HttpApiError, HttpApiErrorSource},
    rpc::{add_service, serve_builder, setup_builder, RpcBuilderInput},
    server_type::{common_state::CommonServerState, RpcError, ServerType},
};

#[derive(Debug)]
pub struct RouterServerType {
    server: RouterServer,
    shutdown: CancellationToken,
    trace_collector: Option<Arc<dyn TraceCollector>>,
}

impl RouterServerType {
    pub fn new(server: RouterServer, common_state: &CommonServerState) -> Self {
        Self {
            server,
            shutdown: CancellationToken::new(),
            trace_collector: common_state.trace_collector(),
        }
    }
}

#[async_trait]
impl ServerType for RouterServerType {
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
        req: Request<Body>,
    ) -> Result<Response<Body>, Self::RouteError> {
        self.server.http().route(req).map_err(IoxHttpErrorAdaptor)
    }

    /// Registers the services exposed by the router [`GrpcDelegate`] delegate.
    ///
    /// [`GrpcDelegate`]: router2::server::grpc::GrpcDelegate
    async fn server_grpc(self: Arc<Self>, builder_input: RpcBuilderInput) -> Result<(), RpcError> {
        let builder = setup_builder!(builder_input, self);
        add_service!(builder, self.server.grpc().write_service());
        serve_builder!(builder);

        Ok(())
    }

    async fn join(self: Arc<Self>) {
        self.shutdown.cancelled().await;
    }

    fn shutdown(&self) {
        self.shutdown.cancel();
    }
}

/// This adaptor converts the `router2` http error type into a type that
/// satisfies the requirements of influxdb_ioxd's runner framework, keeping the
/// two decoupled.
#[derive(Debug)]
pub struct IoxHttpErrorAdaptor(router2::server::http::Error);

impl Display for IoxHttpErrorAdaptor {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

impl std::error::Error for IoxHttpErrorAdaptor {}

impl HttpApiErrorSource for IoxHttpErrorAdaptor {
    fn to_http_api_error(&self) -> HttpApiError {
        HttpApiError::new(self.0.as_status_code(), self.to_string())
    }
}
