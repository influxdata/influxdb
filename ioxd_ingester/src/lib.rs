use async_trait::async_trait;
use clap_blocks::{ingester::IngesterConfig, write_buffer::WriteBufferConfig};
use data_types::KafkaPartition;
use hyper::{Body, Request, Response};
use ingester::{
    handler::{IngestHandler, IngestHandlerImpl},
    lifecycle::LifecycleConfig,
    server::{grpc::GrpcDelegate, http::HttpDelegate, IngesterServer},
};
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
use object_store::DynObjectStore;
use std::{
    collections::BTreeMap,
    fmt::{Debug, Display},
    sync::{atomic::AtomicU64, Arc},
    time::Duration,
};
use thiserror::Error;
use trace::TraceCollector;

#[derive(Debug, Error)]
pub enum Error {
    #[error("Catalog error: {0}")]
    Catalog(#[from] iox_catalog::interface::Error),

    #[error("Kafka topic {0} not found in the catalog")]
    KafkaTopicNotFound(String),

    #[error("kafka_partition_range_start must be <= kafka_partition_range_end")]
    KafkaRange,

    #[error("error initializing ingester: {0}")]
    Ingester(#[from] ingester::handler::Error),

    #[error("error initializing write buffer {0}")]
    WriteBuffer(#[from] write_buffer::core::WriteBufferError),
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

pub struct IngesterServerType<I: IngestHandler> {
    server: IngesterServer<I>,
    trace_collector: Option<Arc<dyn TraceCollector>>,
}

impl<I: IngestHandler> std::fmt::Debug for IngesterServerType<I> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Ingester")
    }
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
    ) -> Result<Response<Body>, Box<dyn HttpApiErrorSource>> {
        Err(Box::new(IoxHttpError::NotFound))
    }

    /// Provide a placeholder gRPC service.
    async fn server_grpc(self: Arc<Self>, builder_input: RpcBuilderInput) -> Result<(), RpcError> {
        let builder = setup_builder!(builder_input, self);
        add_service!(builder, self.server.grpc().flight_service());
        add_service!(builder, self.server.grpc().write_info_service());
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

/// Instantiate an ingester server type
pub async fn create_ingester_server_type(
    common_state: &CommonServerState,
    metric_registry: Arc<metric::Registry>,
    catalog: Arc<dyn Catalog>,
    object_store: Arc<DynObjectStore>,
    exec: Arc<Executor>,
    write_buffer_config: &WriteBufferConfig,
    ingester_config: IngesterConfig,
) -> Result<Arc<dyn ServerType>> {
    let mut txn = catalog.start_transaction().await?;
    let kafka_topic = txn
        .kafka_topics()
        .get_by_name(write_buffer_config.topic())
        .await?
        .ok_or_else(|| Error::KafkaTopicNotFound(write_buffer_config.topic().to_string()))?;

    if ingester_config.write_buffer_partition_range_start
        > ingester_config.write_buffer_partition_range_end
    {
        return Err(Error::KafkaRange);
    }

    let kafka_partitions: Vec<_> = (ingester_config.write_buffer_partition_range_start
        ..=ingester_config.write_buffer_partition_range_end)
        .map(KafkaPartition::new)
        .collect();

    let mut sequencers = BTreeMap::new();
    for k in kafka_partitions {
        let s = txn.sequencers().create_or_get(&kafka_topic, k).await?;
        sequencers.insert(k, s);
    }
    txn.commit().await?;

    let trace_collector = common_state.trace_collector();

    let write_buffer = write_buffer_config
        .reading(Arc::clone(&metric_registry), trace_collector.clone())
        .await?;

    let lifecycle_config = LifecycleConfig::new(
        ingester_config.pause_ingest_size_bytes,
        ingester_config.persist_memory_threshold_bytes,
        ingester_config.persist_partition_size_threshold_bytes,
        Duration::from_secs(ingester_config.persist_partition_age_threshold_seconds),
        Duration::from_secs(ingester_config.persist_partition_cold_threshold_seconds),
    );
    let ingest_handler = Arc::new(
        IngestHandlerImpl::new(
            lifecycle_config,
            kafka_topic,
            sequencers,
            catalog,
            object_store,
            write_buffer,
            exec,
            Arc::clone(&metric_registry),
            ingester_config.skip_to_oldest_available,
            ingester_config.concurrent_request_limit,
        )
        .await?,
    );
    let http = HttpDelegate::new(Arc::clone(&ingest_handler));
    let grpc = GrpcDelegate::new(
        Arc::clone(&ingest_handler),
        Arc::new(AtomicU64::new(ingester_config.test_flight_do_get_panic)),
    );

    let ingester = IngesterServer::new(metric_registry, http, grpc, ingest_handler);
    let server_type = Arc::new(IngesterServerType::new(ingester, common_state));

    Ok(server_type)
}
