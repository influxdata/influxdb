use async_trait::async_trait;
use futures::{future::FusedFuture, FutureExt};
use hyper::{Body, Request, Response};
use ioxd_common::{
    http::{error::HttpApiErrorSource, metrics::LineProtocolMetrics},
    rpc::RpcBuilderInput,
    server_type::{CommonServerState, RpcError, ServerType},
};
use metric::Registry;
use observability_deps::tracing::{error, info};
use server::{ApplicationState, Server};
use std::sync::Arc;
use tokio_util::sync::CancellationToken;
use trace::TraceCollector;

mod config;
mod http;
mod rpc;
pub mod setup;

pub use self::http::ApplicationError;

#[derive(Debug)]
pub struct DatabaseServerType {
    pub application: Arc<ApplicationState>,
    pub server: Arc<Server>,
    pub lp_metrics: Arc<LineProtocolMetrics>,
    pub max_request_size: usize,
    config_immutable: bool,
    shutdown: CancellationToken,
}

impl DatabaseServerType {
    pub fn new(
        application: Arc<ApplicationState>,
        server: Arc<Server>,
        common_state: &CommonServerState,
        config_immutable: bool,
    ) -> Self {
        let lp_metrics = Arc::new(LineProtocolMetrics::new(
            application.metric_registry().as_ref(),
        ));

        Self {
            application,
            server,
            lp_metrics,
            config_immutable,
            max_request_size: common_state.run_config().max_http_request_size,
            shutdown: CancellationToken::new(),
        }
    }
}

#[async_trait]
impl ServerType for DatabaseServerType {
    fn metric_registry(&self) -> Arc<Registry> {
        Arc::clone(self.application.metric_registry())
    }

    fn trace_collector(&self) -> Option<Arc<dyn TraceCollector>> {
        self.application.trace_collector().clone()
    }

    async fn route_http_request(
        &self,
        req: Request<Body>,
    ) -> Result<Response<Body>, Box<dyn HttpApiErrorSource>> {
        self::http::route_request(self, req)
            .await
            .map_err(|e| Box::new(e) as _)
    }

    async fn server_grpc(self: Arc<Self>, builder_input: RpcBuilderInput) -> Result<(), RpcError> {
        self::rpc::server_grpc(self, builder_input).await
    }

    async fn join(self: Arc<Self>) {
        let server_worker = self.server.join().fuse();
        futures::pin_mut!(server_worker);

        futures::select! {
            _ = server_worker => {},
            _ = self.shutdown.cancelled().fuse() => {},
        }

        self.server.shutdown();

        if !server_worker.is_terminated() {
            match server_worker.await {
                Ok(_) => info!("server worker shutdown"),
                Err(error) => error!(%error, "server worker error"),
            }
        }

        info!("server completed shutting down");

        self.application.join().await;
        info!("shared application state completed shutting down");
    }

    fn shutdown(&self) {
        self.shutdown.cancel();
    }
}
