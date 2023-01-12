use async_trait::async_trait;
use clap_blocks::querier::{IngesterAddresses, QuerierConfig};
use hyper::{Body, Request, Response};
use iox_catalog::interface::Catalog;
use iox_query::exec::{Executor, ExecutorType};
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
use querier::{
    create_ingester_connections, QuerierCatalogCache, QuerierDatabase, QuerierHandler,
    QuerierHandlerImpl, QuerierServer,
};
use std::{
    fmt::{Debug, Display},
    sync::Arc,
};
use thiserror::Error;
use tokio::runtime::Handle;
use tokio_util::sync::CancellationToken;
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

    /// Configure the gRPC services.
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
        add_service!(builder, self.server.handler().catalog_service());
        add_service!(builder, self.server.handler().object_store_service());

        serve_builder!(builder);

        Ok(())
    }

    async fn join(self: Arc<Self>) {
        self.server.join().await;
    }

    fn shutdown(&self, frontend: CancellationToken) {
        frontend.cancel();
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
    pub time_provider: Arc<dyn TimeProvider>,
    pub ingester_addresses: IngesterAddresses,
    pub querier_config: QuerierConfig,
    pub rpc_write: bool,
}

#[derive(Debug, Error)]
pub enum Error {
    #[error("querier error: {0}")]
    Querier(#[from] querier::QuerierDatabaseError),
}

/// Instantiate a querier server
pub async fn create_querier_server_type(
    args: QuerierServerTypeArgs<'_>,
) -> Result<Arc<dyn ServerType>, Error> {
    let catalog_cache = Arc::new(QuerierCatalogCache::new(
        Arc::clone(&args.catalog),
        args.time_provider,
        Arc::clone(&args.metric_registry),
        Arc::clone(&args.object_store),
        args.querier_config.ram_pool_metadata_bytes(),
        args.querier_config.ram_pool_data_bytes(),
        &Handle::current(),
    ));

    // register cached object store with the execution context
    let parquet_store = catalog_cache.parquet_store();
    let existing = args
        .exec
        .new_context(ExecutorType::Query)
        .inner()
        .runtime_env()
        .register_object_store(
            "iox",
            parquet_store.id(),
            Arc::clone(parquet_store.object_store()),
        );
    assert!(existing.is_none());

    let ingester_connection = match args.ingester_addresses {
        IngesterAddresses::None => None,
        IngesterAddresses::ByShardIndex(map) => {
            if args.rpc_write {
                panic!(
                    "`INFLUXDB_IOX_RPC_MODE` is set but shard to ingester mappings were provided; \
                    either unset `INFLUXDB_IOX_RPC_MODE` or specify `--ingester-addresses` instead"
                );
            }
            Some(create_ingester_connections(
                Some(map),
                None,
                Arc::clone(&catalog_cache),
                args.querier_config.ingester_circuit_breaker_threshold,
            ))
        }
        IngesterAddresses::List(list) => {
            if !args.rpc_write {
                panic!(
                    "`INFLUXDB_IOX_RPC_MODE` is unset but ingester addresses were provided; \
                    either set `INFLUXDB_IOX_RPC_MODE` or specify shard to ingester mappings instead"
                );
            }
            Some(create_ingester_connections(
                None,
                Some(list),
                Arc::clone(&catalog_cache),
                args.querier_config.ingester_circuit_breaker_threshold,
            ))
        }
    };

    let database = Arc::new(
        QuerierDatabase::new(
            catalog_cache,
            Arc::clone(&args.metric_registry),
            args.exec,
            ingester_connection,
            args.querier_config.max_concurrent_queries(),
            args.rpc_write,
        )
        .await?,
    );
    let querier_handler = Arc::new(QuerierHandlerImpl::new(
        args.catalog,
        Arc::clone(&database),
        Arc::clone(&args.object_store),
    ));

    let querier = QuerierServer::new(args.metric_registry, querier_handler);
    Ok(Arc::new(QuerierServerType::new(
        querier,
        database,
        args.common_state,
    )))
}
