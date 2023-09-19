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
mod scheduler_config;

// Workaround for "unused crate" lint false positives.
use workspace_hack as _;

use async_trait::async_trait;
use backoff::BackoffConfig;
use clap_blocks::compactor::CompactorConfig;
use compactor::{compactor::Compactor, config::Config};
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
use parquet_file::storage::ParquetStorage;
use std::{
    fmt::{Debug, Display},
    sync::Arc,
    time::Duration,
};
use tokio_util::sync::CancellationToken;
use trace::TraceCollector;

use crate::scheduler_config::convert_scheduler_config;

pub struct CompactorServerType {
    compactor: Compactor,
    metric_registry: Arc<Registry>,
    trace_collector: Option<Arc<dyn TraceCollector>>,
}

impl std::fmt::Debug for CompactorServerType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Compactor")
    }
}

impl CompactorServerType {
    pub fn new(
        compactor: Compactor,
        metric_registry: Arc<metric::Registry>,
        common_state: &CommonServerState,
    ) -> Self {
        Self {
            compactor,
            metric_registry,
            trace_collector: common_state.trace_collector(),
        }
    }
}

#[async_trait]
impl ServerType for CompactorServerType {
    /// Human name for this server type
    fn name(&self) -> &str {
        "compactor"
    }

    /// Return the [`metric::Registry`] used by the compactor.
    fn metric_registry(&self) -> Arc<Registry> {
        Arc::clone(&self.metric_registry)
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

        serve_builder!(builder);

        Ok(())
    }

    async fn join(self: Arc<Self>) {
        self.compactor
            .join()
            .await
            .expect("clean compactor shutdown");
    }

    fn shutdown(&self, frontend: CancellationToken) {
        frontend.cancel();
        self.compactor.shutdown();
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

/// Instantiate a compactor server
#[allow(clippy::too_many_arguments)]
pub async fn create_compactor_server_type(
    common_state: &CommonServerState,
    metric_registry: Arc<metric::Registry>,
    catalog: Arc<dyn Catalog>,
    parquet_store_real: ParquetStorage,
    parquet_store_scratchpad: ParquetStorage,
    exec: Arc<Executor>,
    time_provider: Arc<dyn TimeProvider>,
    compactor_config: CompactorConfig,
) -> Arc<dyn ServerType> {
    let backoff_config = BackoffConfig::default();

    let compactor = Compactor::start(Config {
        metric_registry: Arc::clone(&metric_registry),
        trace_collector: common_state.trace_collector(),
        catalog,
        scheduler_config: convert_scheduler_config(
            compactor_config.compactor_scheduler_config.clone(),
        ),
        parquet_store_real,
        parquet_store_scratchpad,
        exec,
        time_provider,
        backoff_config,
        partition_concurrency: compactor_config.compaction_partition_concurrency,
        df_concurrency: compactor_config.compaction_df_concurrency,
        partition_scratchpad_concurrency: compactor_config
            .compaction_partition_scratchpad_concurrency,
        max_desired_file_size_bytes: compactor_config.max_desired_file_size_bytes,
        percentage_max_file_size: compactor_config.percentage_max_file_size,
        split_percentage: compactor_config.split_percentage,
        partition_timeout: Duration::from_secs(compactor_config.partition_timeout_secs),
        shadow_mode: compactor_config.shadow_mode,
        enable_scratchpad: compactor_config.enable_scratchpad,
        min_num_l1_files_to_compact: compactor_config.min_num_l1_files_to_compact,
        process_once: compactor_config.process_once,
        simulate_without_object_store: false,
        parquet_files_sink_override: None,
        all_errors_are_fatal: false,
        max_num_columns_per_table: compactor_config.max_num_columns_per_table,
        max_num_files_per_plan: compactor_config.max_num_files_per_plan,
        max_partition_fetch_queries_per_second: compactor_config
            .max_partition_fetch_queries_per_second,
        gossip_seeds: compactor_config.gossip_config.seed_list,
        gossip_bind_address: compactor_config
            .gossip_config
            .gossip_bind_address
            .map(Into::into),
    })
    .await;

    Arc::new(CompactorServerType::new(
        compactor,
        metric_registry,
        common_state,
    ))
}
