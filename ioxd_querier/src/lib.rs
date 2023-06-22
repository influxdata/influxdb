#![deny(rustdoc::broken_intra_doc_links, rust_2018_idioms)]
#![warn(
    clippy::clone_on_ref_ptr,
    clippy::dbg_macro,
    clippy::explicit_iter_loop,
    // See https://github.com/influxdata/influxdb_iox/pull/1671
    clippy::future_not_send,
    clippy::todo,
    clippy::use_self,
    missing_debug_implementations,
    unused_crate_dependencies
)]

// Workaround for "unused crate" lint false positives.
use workspace_hack as _;

use async_trait::async_trait;
use authz::{Authorizer, IoxAuthorizer};
use clap_blocks::querier::QuerierConfig;
use datafusion_util::config::register_iox_object_store;
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
use querier::{create_ingester_connections, QuerierCatalogCache, QuerierDatabase, QuerierServer};
use std::{
    fmt::{Debug, Display},
    sync::Arc,
};
use thiserror::Error;
use tokio::runtime::Handle;
use tokio_util::sync::CancellationToken;
use trace::TraceCollector;

mod rpc;

pub struct QuerierServerType {
    database: Arc<QuerierDatabase>,
    server: QuerierServer,
    trace_collector: Option<Arc<dyn TraceCollector>>,
    authz: Option<Arc<dyn Authorizer>>,
}

impl std::fmt::Debug for QuerierServerType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Querier")
    }
}

impl QuerierServerType {
    pub fn new(
        server: QuerierServer,
        database: Arc<QuerierDatabase>,
        common_state: &CommonServerState,
        authz: Option<Arc<dyn Authorizer>>,
    ) -> Self {
        Self {
            server,
            database,
            trace_collector: common_state.trace_collector(),
            authz,
        }
    }
}

#[async_trait]
impl ServerType for QuerierServerType {
    /// Human name for this server type
    fn name(&self) -> &str {
        "querier"
    }

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
            rpc::query::make_flight_server(
                Arc::clone(&self.database),
                self.authz.as_ref().map(Arc::clone)
            )
        );
        add_service!(
            builder,
            rpc::query::make_storage_server(Arc::clone(&self.database))
        );
        add_service!(
            builder,
            rpc::namespace::namespace_service(Arc::clone(&self.database))
        );
        add_service!(builder, self.server.schema_service());
        add_service!(builder, self.server.catalog_service());
        add_service!(builder, self.server.object_store_service());

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
            Self::NotFound => HttpApiErrorCode::NotFound,
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

/// Arguments required to create a [`ServerType`] for the querier.
#[derive(Debug)]
pub struct QuerierServerTypeArgs<'a> {
    pub common_state: &'a CommonServerState,
    pub metric_registry: Arc<metric::Registry>,
    pub catalog: Arc<dyn Catalog>,
    pub object_store: Arc<DynObjectStore>,
    pub exec: Arc<Executor>,
    pub time_provider: Arc<dyn TimeProvider>,
    pub querier_config: QuerierConfig,
}

#[derive(Debug, Error)]
pub enum Error {
    #[error("querier error: {0}")]
    Querier(#[from] querier::QuerierDatabaseError),

    #[error("authz configuration error for '{addr}': '{source}'")]
    AuthzConfig {
        source: Box<dyn std::error::Error>,
        addr: String,
    },
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
    let runtime_env = args
        .exec
        .new_context(ExecutorType::Query)
        .inner()
        .runtime_env();
    let existing = register_iox_object_store(
        runtime_env,
        parquet_store.id(),
        Arc::clone(parquet_store.object_store()),
    );
    assert!(existing.is_none());

    let authz = match &args.querier_config.authz_address {
        Some(addr) => {
            let authz = IoxAuthorizer::connect_lazy(addr.clone())
                .map(|c| Arc::new(c) as Arc<dyn Authorizer>)
                .map_err(|source| Error::AuthzConfig {
                    source,
                    addr: addr.clone(),
                })?;
            authz.probe().await.expect("Authz connection test failed.");

            Some(authz)
        }
        None => None,
    };

    let ingester_connections = if args.querier_config.ingester_addresses.is_empty() {
        None
    } else {
        let ingester_addresses = args
            .querier_config
            .ingester_addresses
            .iter()
            .map(|addr| addr.to_string().into())
            .collect();
        Some(create_ingester_connections(
            ingester_addresses,
            Arc::clone(&catalog_cache),
            args.querier_config.ingester_circuit_breaker_threshold,
        ))
    };

    let database = Arc::new(
        QuerierDatabase::new(
            catalog_cache,
            Arc::clone(&args.metric_registry),
            args.exec,
            ingester_connections,
            args.querier_config.max_concurrent_queries(),
            Arc::new(args.querier_config.datafusion_config),
        )
        .await?,
    );

    let querier = QuerierServer::new(
        args.catalog,
        Arc::clone(&database),
        args.metric_registry,
        args.object_store,
    );
    Ok(Arc::new(QuerierServerType::new(
        querier,
        database,
        args.common_state,
        authz,
    )))
}
