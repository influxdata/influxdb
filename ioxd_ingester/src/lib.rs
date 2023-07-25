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

use arrow_flight::flight_service_server::FlightServiceServer;
use async_trait::async_trait;
use clap_blocks::ingester::IngesterConfig;
use futures::FutureExt;
use generated_types::influxdata::iox::{
    catalog::v1::catalog_service_server::CatalogServiceServer,
    ingester::v1::{
        persist_service_server::PersistServiceServer, write_service_server::WriteServiceServer,
    },
};
use hyper::{Body, Request, Response};
use ingester::{GossipConfig, IngesterGuard, IngesterRpcInterface};
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
use parquet_file::storage::ParquetStorage;
use std::{
    fmt::{Debug, Display},
    sync::{Arc, Mutex},
    time::Duration,
};
use thiserror::Error;
use tokio::sync::oneshot;
use tokio_util::sync::CancellationToken;
use trace::TraceCollector;

/// Define a safe maximum ingester write response size.
///
/// The ingester SHOULD NOT ever generate a response larger than this.
const MAX_OUTGOING_MSG_BYTES: usize = 1024 * 1024; // 1 MiB

#[derive(Debug, Error)]
pub enum Error {
    #[error("error initializing ingester: {0}")]
    Ingester(#[from] ingester::InitError),
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

struct IngesterServerType<I: IngesterRpcInterface> {
    server: IngesterGuard<I>,
    shutdown: Mutex<Option<oneshot::Sender<CancellationToken>>>,
    metrics: Arc<Registry>,
    trace_collector: Option<Arc<dyn TraceCollector>>,
    max_simultaneous_queries: usize,
    max_incoming_msg_bytes: usize,
}

impl<I: IngesterRpcInterface> IngesterServerType<I> {
    pub fn new(
        server: IngesterGuard<I>,
        metrics: Arc<Registry>,
        common_state: &CommonServerState,
        max_simultaneous_queries: usize,
        max_incoming_msg_bytes: usize,
        shutdown: oneshot::Sender<CancellationToken>,
    ) -> Self {
        Self {
            server,
            shutdown: Mutex::new(Some(shutdown)),
            metrics,
            trace_collector: common_state.trace_collector(),
            max_simultaneous_queries,
            max_incoming_msg_bytes,
        }
    }
}

impl<I: IngesterRpcInterface> std::fmt::Debug for IngesterServerType<I> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Ingester")
    }
}

#[async_trait]
impl<I: IngesterRpcInterface + Sync + Send + Debug + 'static> ServerType for IngesterServerType<I> {
    /// Human name for this server type
    fn name(&self) -> &str {
        "ingester"
    }

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

        add_service!(
            builder,
            CatalogServiceServer::new(self.server.rpc().catalog_service())
        );
        add_service!(
            builder,
            WriteServiceServer::new(self.server.rpc().write_service())
                .max_decoding_message_size(self.max_incoming_msg_bytes)
                .max_encoding_message_size(MAX_OUTGOING_MSG_BYTES)
        );
        add_service!(
            builder,
            PersistServiceServer::new(self.server.rpc().persist_service())
        );
        add_service!(
            builder,
            FlightServiceServer::new(
                self.server
                    .rpc()
                    .query_service(self.max_simultaneous_queries)
            )
        );

        serve_builder!(builder);

        Ok(())
    }

    async fn join(self: Arc<Self>) {
        self.server.join().await;
    }

    fn shutdown(&self, frontend: CancellationToken) {
        if let Some(c) = self
            .shutdown
            .lock()
            .expect("shutdown mutex poisoned")
            .take()
        {
            let _ = c.send(frontend);
        }
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

const PERSIST_BACKGROUND_FETCH_TIME: Duration = Duration::from_secs(30);

/// Instantiate an ingester server type
pub async fn create_ingester_server_type(
    common_state: &CommonServerState,
    catalog: Arc<dyn Catalog>,
    metrics: Arc<Registry>,
    ingester_config: &IngesterConfig,
    exec: Arc<Executor>,
    object_store: ParquetStorage,
) -> Result<Arc<dyn ServerType>> {
    let (shutdown_tx, shutdown_rx) = oneshot::channel();

    let gossip = match ingester_config.gossip_config.gossip_bind_address {
        None => GossipConfig::Disabled,
        Some(v) => GossipConfig::Enabled {
            bind_addr: v.into(),
            peers: ingester_config.gossip_config.seed_list.clone(),
        },
    };

    let grpc = ingester::new(
        catalog,
        Arc::clone(&metrics),
        PERSIST_BACKGROUND_FETCH_TIME,
        ingester_config.wal_directory.clone(),
        Duration::from_secs(ingester_config.wal_rotation_period_seconds),
        exec,
        ingester_config.persist_max_parallelism,
        ingester_config.persist_queue_depth,
        ingester_config.persist_hot_partition_cost,
        object_store,
        gossip,
        shutdown_rx.map(|v| v.expect("shutdown sender dropped without calling shutdown")),
    )
    .await?;

    Ok(Arc::new(IngesterServerType::new(
        grpc,
        metrics,
        common_state,
        ingester_config.concurrent_query_limit,
        ingester_config.rpc_write_max_incoming_bytes,
        shutdown_tx,
    )))
}
