use async_trait::async_trait;
use clap_blocks::compactor::CompactorConfig;
use compactor::{
    handler::{CompactorHandler, CompactorHandlerImpl},
    server::CompactorServer,
};
use data_types::ShardIndex;
use hyper::{Body, Request, Response};
use iox_catalog::interface::Catalog;
use iox_query::exec::Executor;
use iox_time::TimeProvider;
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
use parquet_file::storage::ParquetStorage;
use std::{
    fmt::{Debug, Display},
    sync::Arc,
};
use thiserror::Error;
use trace::TraceCollector;

#[derive(Debug, Error)]
pub enum Error {
    #[error("Catalog error: {0}")]
    Catalog(#[from] iox_catalog::interface::Error),

    #[error("No topic named '{topic_name}' found in the catalog")]
    TopicCatalogLookup { topic_name: String },

    #[error("shard_index_range_start must be <= shard_index_range_end")]
    ShardIndexRange,
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

pub struct CompactorServerType<C: CompactorHandler> {
    server: CompactorServer<C>,
    trace_collector: Option<Arc<dyn TraceCollector>>,
}

impl<C: CompactorHandler> std::fmt::Debug for CompactorServerType<C> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Compactor")
    }
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
    let compactor = build_compactor_from_config(
        compactor_config,
        catalog,
        object_store,
        exec,
        time_provider,
        Arc::clone(&metric_registry),
    )
    .await?;

    let compactor_handler = Arc::new(CompactorHandlerImpl::new(compactor));
    let compactor = CompactorServer::new(metric_registry, compactor_handler);
    Ok(Arc::new(CompactorServerType::new(compactor, common_state)))
}

pub async fn build_compactor_from_config(
    compactor_config: CompactorConfig,
    catalog: Arc<dyn Catalog>,
    object_store: Arc<DynObjectStore>,
    exec: Arc<Executor>,
    time_provider: Arc<dyn TimeProvider>,
    metric_registry: Arc<Registry>,
) -> Result<compactor::compact::Compactor, Error> {
    if compactor_config.shard_index_range_start > compactor_config.shard_index_range_end {
        return Err(Error::ShardIndexRange);
    }

    let mut txn = catalog.start_transaction().await?;
    let topic = txn
        .topics()
        .get_by_name(&compactor_config.topic)
        .await?
        .ok_or(Error::TopicCatalogLookup {
            topic_name: compactor_config.topic,
        })?;

    let shard_indexes: Vec<_> = (compactor_config.shard_index_range_start
        ..=compactor_config.shard_index_range_end)
        .map(ShardIndex::new)
        .collect();

    let mut shards = Vec::with_capacity(shard_indexes.len());
    for k in shard_indexes {
        let s = txn.shards().create_or_get(&topic, k).await?;
        shards.push(s.id);
    }
    txn.commit().await?;

    let parquet_store = ParquetStorage::new(object_store);

    let compactor_config = compactor::handler::CompactorConfig::new(
        compactor_config.max_desired_file_size_bytes,
        compactor_config.percentage_max_file_size,
        compactor_config.split_percentage,
        compactor_config.max_cold_concurrent_size_bytes,
        compactor_config.max_number_partitions_per_shard,
        compactor_config.min_number_recent_ingested_files_per_partition,
        compactor_config.cold_input_size_threshold_bytes,
        compactor_config.cold_input_file_count_threshold,
        compactor_config.hot_multiple,
        compactor_config.memory_budget_bytes,
    );

    Ok(compactor::compact::Compactor::new(
        shards,
        catalog,
        parquet_store,
        exec,
        time_provider,
        backoff::BackoffConfig::default(),
        compactor_config,
        metric_registry,
    ))
}
