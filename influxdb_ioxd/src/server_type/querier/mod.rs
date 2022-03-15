use std::{
    fmt::{Debug, Display},
    sync::Arc,
};

use async_trait::async_trait;
use hyper::{Body, Request, Response};
use iox_catalog::interface::Catalog;
use metric::Registry;
use object_store::DynObjectStore;
use querier::{
    database::QuerierDatabase,
    handler::{QuerierHandler, QuerierHandlerImpl},
    server::QuerierServer,
};
use query::exec::Executor;
use time::TimeProvider;
use trace::TraceCollector;

use crate::{
    http::error::{HttpApiError, HttpApiErrorCode, HttpApiErrorSource},
    rpc::{add_service, serve_builder, setup_builder, RpcBuilderInput},
    server_type::{common_state::CommonServerState, RpcError, ServerType},
};

mod rpc;

#[derive(Debug)]
pub struct QuerierServerType<C: QuerierHandler> {
    database: Arc<QuerierDatabase>,
    server: QuerierServer<C>,
    trace_collector: Option<Arc<dyn TraceCollector>>,
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
            rpc::query::make_flight_server(Arc::clone(&self.database),)
        );
        add_service!(
            builder,
            rpc::query::make_storage_server(Arc::clone(&self.database),)
        );
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

/// Instantiate a querier server
pub async fn create_querier_server_type(
    common_state: &CommonServerState,
    metric_registry: Arc<metric::Registry>,
    catalog: Arc<dyn Catalog>,
    object_store: Arc<DynObjectStore>,
    time_provider: Arc<dyn TimeProvider>,
    exec: Arc<Executor>,
) -> Arc<dyn ServerType> {
    let database = Arc::new(QuerierDatabase::new(
        catalog,
        Arc::clone(&metric_registry),
        object_store,
        time_provider,
        exec,
    ));
    let querier_handler = Arc::new(QuerierHandlerImpl::new(Arc::clone(&database)));

    let querier = QuerierServer::new(metric_registry, querier_handler);
    Arc::new(QuerierServerType::new(querier, database, common_state))
}
