use std::sync::Arc;

use async_trait::async_trait;
use hyper::{Body, Request, Response};
use metric::Registry;
use server::{connection::ConnectionManager, ApplicationState, Server};
use trace::TraceCollector;

use crate::influxdb_ioxd::{
    http::metrics::LineProtocolMetrics,
    rpc::RpcBuilderInput,
    server_type::{RpcError, ServerType},
    serving_readiness::ServingReadiness,
};

mod http;
mod rpc;

pub use self::http::ApplicationError;

#[derive(Debug)]
pub struct DatabaseServerType<M>
where
    M: ConnectionManager + std::fmt::Debug + Send + Sync + 'static,
{
    pub application: Arc<ApplicationState>,
    pub server: Arc<Server<M>>,
    pub lp_metrics: Arc<LineProtocolMetrics>,
    pub max_request_size: usize,
    pub serving_readiness: ServingReadiness,
}

impl<M> DatabaseServerType<M>
where
    M: ConnectionManager + std::fmt::Debug + Send + Sync + 'static,
{
    pub fn new(
        application: Arc<ApplicationState>,
        server: Arc<Server<M>>,
        max_request_size: usize,
        serving_readiness: ServingReadiness,
    ) -> Self {
        let lp_metrics = Arc::new(LineProtocolMetrics::new(
            application.metric_registry().as_ref(),
        ));

        Self {
            application,
            server,
            lp_metrics,
            max_request_size,
            serving_readiness,
        }
    }
}

#[async_trait]
impl<M> ServerType for DatabaseServerType<M>
where
    M: ConnectionManager + std::fmt::Debug + Send + Sync + 'static,
{
    type RouteError = ApplicationError;

    fn metric_registry(&self) -> Arc<Registry> {
        Arc::clone(self.application.metric_registry())
    }

    fn trace_collector(&self) -> Option<Arc<dyn TraceCollector>> {
        self.application.trace_collector().clone()
    }

    async fn route_http_request(
        &self,
        req: Request<Body>,
    ) -> Result<Response<Body>, Self::RouteError> {
        self::http::route_request(self, req).await
    }

    async fn server_grpc(self: Arc<Self>, builder_input: RpcBuilderInput) -> Result<(), RpcError> {
        self::rpc::server_grpc(self, builder_input).await
    }
}
