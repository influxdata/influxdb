use async_trait::async_trait;
use clap_blocks::ingest_replica::IngestReplicaConfig;
use hyper::{Body, Request, Response};
use ingest_replica::IngestReplicaRpcInterface;
use iox_catalog::interface::Catalog;
use iox_query::exec::Executor;
use ioxd_common::{
    add_service,
    http::error::{HttpApiError, HttpApiErrorCode, HttpApiErrorSource},
    rpc::RpcBuilderInput,
    serve_builder,
    server_type::{CommonServerState, RpcError, ServerType},
    setup_builder,
};
use metric::Registry;
use std::{
    fmt::{Debug, Display},
    sync::Arc,
};
use thiserror::Error;
use tokio_util::sync::CancellationToken;
use trace::TraceCollector;

#[derive(Debug, Error)]
pub enum Error {
    #[error("error initializing ingester2: {0}")]
    Ingester(#[from] ingest_replica::InitError),
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

struct IngestReplicaServerType<I: IngestReplicaRpcInterface> {
    server: I,
    shutdown: CancellationToken,
    metrics: Arc<Registry>,
    trace_collector: Option<Arc<dyn TraceCollector>>,
    max_simultaneous_queries: usize,
}

impl<I: IngestReplicaRpcInterface> IngestReplicaServerType<I> {
    pub fn new(
        server: I,
        metrics: Arc<Registry>,
        common_state: &CommonServerState,
        max_simultaneous_queries: usize,
    ) -> Self {
        Self {
            server,
            shutdown: CancellationToken::new(),
            metrics,
            trace_collector: common_state.trace_collector(),
            max_simultaneous_queries,
        }
    }
}

impl<I: IngestReplicaRpcInterface> std::fmt::Debug for IngestReplicaServerType<I> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Ingester2")
    }
}

#[async_trait]
impl<I: IngestReplicaRpcInterface + Sync + Send + Debug + 'static> ServerType
    for IngestReplicaServerType<I>
{
    /// Return the [`metric::Registry`] used by the ingester.
    fn metric_registry(&self) -> Arc<Registry> {
        Arc::clone(&self.metrics)
    }

    /// Returns the trace collector for ingester traces.
    fn trace_collector(&self) -> Option<Arc<dyn TraceCollector>> {
        self.trace_collector.as_ref().map(Arc::clone)
    }

    /// Just return "not found".
    async fn route_http_request(
        &self,
        _req: Request<Body>,
    ) -> Result<Response<Body>, Box<dyn HttpApiErrorSource>> {
        Err(Box::new(IoxHttpError::NotFound))
    }

    /// Configure the gRPC services.
    async fn server_grpc(self: Arc<Self>, builder_input: RpcBuilderInput) -> Result<(), RpcError> {
        let builder = setup_builder!(builder_input, self);

        add_service!(builder, self.server.replication_service());
        add_service!(
            builder,
            self.server.query_service(self.max_simultaneous_queries)
        );

        serve_builder!(builder);

        Ok(())
    }

    async fn join(self: Arc<Self>) {
        self.shutdown.cancelled().await;
    }

    fn shutdown(&self, frontend: CancellationToken) {
        frontend.cancel();
        self.shutdown.cancel();
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
        write!(f, "{self:?}")
    }
}

impl std::error::Error for IoxHttpError {}

impl HttpApiErrorSource for IoxHttpError {
    fn to_http_api_error(&self) -> HttpApiError {
        HttpApiError::new(self.status_code(), self.to_string())
    }
}

/// Instantiate an ingester server type
pub async fn create_ingest_replica_server_type(
    common_state: &CommonServerState,
    catalog: Arc<dyn Catalog>,
    metrics: Arc<Registry>,
    ingest_replica_config: &IngestReplicaConfig,
    exec: Arc<Executor>,
) -> Result<Arc<dyn ServerType>> {
    let grpc = ingest_replica::new(
        catalog,
        ingest_replica_config
            .ingester_addresses
            .iter()
            .map(ToString::to_string)
            .collect(),
        exec,
        Arc::clone(&metrics),
    )
    .await?;

    Ok(Arc::new(IngestReplicaServerType::new(
        grpc,
        metrics,
        common_state,
        ingest_replica_config.concurrent_query_limit,
    )))
}
