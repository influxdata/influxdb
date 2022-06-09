use async_trait::async_trait;
use clap_blocks::{querier::QuerierConfig, write_buffer::WriteBufferConfig};
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
use querier::{
    create_ingester_connection, QuerierCatalogCache, QuerierDatabase, QuerierHandler,
    QuerierHandlerImpl, QuerierServer,
};
use router::sequencer::Sequencer;
use sharder::JumpHash;
use std::{
    collections::BTreeSet,
    fmt::{Debug, Display},
    sync::Arc,
};
use thiserror::Error;
use trace::TraceCollector;

mod rpc;

pub struct QuerierServerType<C: QuerierHandler> {
    database: Arc<QuerierDatabase>,
    server: QuerierServer<C>,
    trace_collector: Option<Arc<dyn TraceCollector>>,
}

impl<C: QuerierHandler> std::fmt::Debug for QuerierServerType<C> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Querier")
    }
}

impl<C: QuerierHandler> QuerierServerType<C> {
    pub fn new(
        server: QuerierServer<C>,
        database: Arc<QuerierDatabase>,
        common_state: &CommonServerState,
    ) -> Self {
        Self {
            server,
            database,
            trace_collector: common_state.trace_collector(),
        }
    }
}

#[async_trait]
impl<C: QuerierHandler + std::fmt::Debug + 'static> ServerType for QuerierServerType<C> {
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
        add_service!(
            builder,
            rpc::query::make_flight_server(Arc::clone(&self.database))
        );
        add_service!(
            builder,
            rpc::query::make_storage_server(Arc::clone(&self.database))
        );
        add_service!(
            builder,
            rpc::namespace::namespace_service(Arc::clone(&self.database))
        );
        add_service!(
            builder,
            rpc::write_info::write_info_service(Arc::clone(&self.database))
        );
        add_service!(builder, self.server.handler().schema_service());
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

/// Arguments required to create a [`ServerType`] for the querier.
#[derive(Debug)]
pub struct QuerierServerTypeArgs<'a> {
    pub common_state: &'a CommonServerState,
    pub metric_registry: Arc<metric::Registry>,
    pub catalog: Arc<dyn Catalog>,
    pub object_store: Arc<DynObjectStore>,
    pub exec: Arc<Executor>,
    pub write_buffer_config: &'a WriteBufferConfig,
    pub time_provider: Arc<dyn TimeProvider>,
    pub ingester_addresses: Vec<String>,
    pub querier_config: QuerierConfig,
}

#[derive(Debug, Error)]
pub enum Error {
    #[error("failed to initialise write buffer connection: {0}")]
    WriteBuffer(#[from] write_buffer::core::WriteBufferError),
}

/// Instantiate a querier server
pub async fn create_querier_server_type(
    args: QuerierServerTypeArgs<'_>,
) -> Result<Arc<dyn ServerType>, Error> {
    let catalog_cache = Arc::new(QuerierCatalogCache::new(
        Arc::clone(&args.catalog),
        args.time_provider,
        Arc::clone(&args.metric_registry),
        args.querier_config.ram_pool_bytes(),
    ));

    let ingester_connection =
        create_ingester_connection(args.ingester_addresses, Arc::clone(&catalog_cache));

    let write_buffer = Arc::new(
        args.write_buffer_config
            .writing(
                Arc::clone(&args.metric_registry),
                args.common_state.trace_collector(),
            )
            .await?,
    );
    // Construct the (ordered) set of sequencers.
    //
    // The sort order must be deterministic in order for all nodes to shard to
    // the same sequencers, therefore we type assert the returned set is of the
    // ordered variety.
    let shards: BTreeSet<_> = write_buffer.sequencer_ids();
    //          ^ don't change this to an unordered set

    let _sharder: JumpHash<_> = shards
        .into_iter()
        .map(|id| Sequencer::new(id as _, Arc::clone(&write_buffer), &args.metric_registry))
        .map(Arc::new)
        .collect();

    let database = Arc::new(QuerierDatabase::new(
        catalog_cache,
        Arc::clone(&args.metric_registry),
        ParquetStorage::new(args.object_store),
        args.exec,
        ingester_connection,
        args.querier_config.max_concurrent_queries(),
    ));
    let querier_handler = Arc::new(QuerierHandlerImpl::new(args.catalog, Arc::clone(&database)));

    let querier = QuerierServer::new(args.metric_registry, querier_handler);
    Ok(Arc::new(QuerierServerType::new(
        querier,
        database,
        args.common_state,
    )))
}
