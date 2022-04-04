use std::{
    fmt::{Debug, Display},
    sync::Arc,
};

use async_trait::async_trait;
use compactor::{
    handler::{CompactorHandler, CompactorHandlerImpl},
    server::CompactorServer,
};
use data_types2::KafkaPartition;
use hyper::{Body, Request, Response};
use iox_catalog::interface::Catalog;
use metric::Registry;
use object_store::DynObjectStore;
use query::exec::Executor;
use time::TimeProvider;
use trace::TraceCollector;

use clap_blocks::compactor::CompactorConfig;
use ioxd_common::{
    add_service,
    http::error::{HttpApiError, HttpApiErrorCode, HttpApiErrorSource},
    rpc::RpcBuilderInput,
    serve_builder,
    server_type::{CommonServerState, RpcError, ServerType},
    setup_builder,
};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum Error {
    #[error("Catalog error: {0}")]
    Catalog(#[from] iox_catalog::interface::Error),

    #[error("Kafka topic {0} not found in the catalog")]
    KafkaTopicNotFound(String),

    #[error("kafka_partition_range_start must be <= kafka_partition_range_end")]
    KafkaRange,
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

#[derive(Debug)]
pub struct CompactorServerType<C: CompactorHandler> {
    server: CompactorServer<C>,
    trace_collector: Option<Arc<dyn TraceCollector>>,
}

impl<C: CompactorHandler> CompactorServerType<C> {
    pub fn new(server: CompactorServer<C>, common_state: &CommonServerState) -> Self {
        Self {
            server,
            trace_collector: common_state.trace_collector(),
        }
    }
}

#[async_trait]
impl<C: CompactorHandler + std::fmt::Debug + 'static> ServerType for CompactorServerType<C> {
    /// Return the [`metric::Registry`] used by the compactor.
    fn metric_registry(&self) -> Arc<Registry> {
        self.server.metric_registry()
    }

    /// Returns the trace collector for compactor traces.
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

    /// Provide a placeholder gRPC service.
    async fn server_grpc(self: Arc<Self>, builder_input: RpcBuilderInput) -> Result<(), RpcError> {
        let builder = setup_builder!(builder_input, self);
        // TODO add a service here
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

/// Simple error struct, we're not really providing an HTTP interface for the compactor.
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

/// Instantiate a compactor server
pub async fn create_compactor_server_type(
    common_state: &CommonServerState,
    metric_registry: Arc<metric::Registry>,
    catalog: Arc<dyn Catalog>,
    object_store: Arc<DynObjectStore>,
    exec: Arc<Executor>,
    time_provider: Arc<dyn TimeProvider>,
    compactor_config: CompactorConfig,
) -> Result<Arc<dyn ServerType>> {
    if compactor_config.write_buffer_partition_range_start
        > compactor_config.write_buffer_partition_range_end
    {
        return Err(Error::KafkaRange);
    }

    let mut txn = catalog.start_transaction().await?;
    let kafka_topic = txn
        .kafka_topics()
        .get_by_name(&compactor_config.topic)
        .await?
        .ok_or(Error::KafkaTopicNotFound(compactor_config.topic))?;

    let kafka_partitions: Vec<_> = (compactor_config.write_buffer_partition_range_start
        ..=compactor_config.write_buffer_partition_range_end)
        .map(KafkaPartition::new)
        .collect();

    let mut sequencers = Vec::with_capacity(kafka_partitions.len());
    for k in kafka_partitions {
        let s = txn.sequencers().create_or_get(&kafka_topic, k).await?;
        sequencers.push(s.id);
    }
    txn.commit().await?;

    let compactor_config = compactor::handler::CompactorConfig::new(
        compactor_config.split_percentage,
        compactor_config.max_concurrent_compaction_size_bytes,
        compactor_config.compaction_max_size_bytes,
    );
    let compactor_handler = Arc::new(CompactorHandlerImpl::new(
        sequencers,
        catalog,
        object_store,
        exec,
        time_provider,
        Arc::clone(&metric_registry),
        compactor_config,
    ));

    let compactor = CompactorServer::new(metric_registry, compactor_handler);
    Ok(Arc::new(CompactorServerType::new(compactor, common_state)))
}
