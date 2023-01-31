use async_trait::async_trait;
use backoff::BackoffConfig;
use clap_blocks::compactor2::Compactor2Config;
use compactor2::{
    compactor::Compactor2,
    config::{Config, ShardConfig},
};
use data_types::{PartitionId, TRANSITION_SHARD_NUMBER};
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

// There is only one shard with index 1
const TOPIC: &str = "iox-shared";
const TRANSITION_SHARD_INDEX: i32 = TRANSITION_SHARD_NUMBER;

pub struct Compactor2ServerType {
    compactor: Compactor2,
    metric_registry: Arc<Registry>,
    trace_collector: Option<Arc<dyn TraceCollector>>,
}

impl std::fmt::Debug for Compactor2ServerType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Compactor2")
    }
}

impl Compactor2ServerType {
    pub fn new(
        compactor: Compactor2,
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
impl ServerType for Compactor2ServerType {
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

/// Instantiate a compactor2 server that uses the RPC write path
#[allow(clippy::too_many_arguments)]
pub async fn create_compactor2_server_type(
    common_state: &CommonServerState,
    metric_registry: Arc<metric::Registry>,
    catalog: Arc<dyn Catalog>,
    parquet_store_real: ParquetStorage,
    parquet_store_scratchpad: ParquetStorage,
    exec: Arc<Executor>,
    time_provider: Arc<dyn TimeProvider>,
    compactor_config: Compactor2Config,
) -> Arc<dyn ServerType> {
    let backoff_config = BackoffConfig::default();
    let shard_id = Config::fetch_shard_id(
        Arc::clone(&catalog),
        backoff_config.clone(),
        TOPIC.to_string(),
        TRANSITION_SHARD_INDEX,
    )
    .await;

    assert!(
        compactor_config.shard_id.is_some() == compactor_config.shard_count.is_some(),
        "must provide or not provide shard ID and count"
    );
    let shard_config = compactor_config.shard_id.map(|shard_id| ShardConfig {
        shard_id,
        n_shards: compactor_config.shard_count.expect("just checked"),
    });

    let compactor = Compactor2::start(Config {
        shard_id,
        metric_registry: Arc::clone(&metric_registry),
        catalog,
        parquet_store_real,
        parquet_store_scratchpad,
        exec,
        time_provider,
        backoff_config,
        partition_concurrency: compactor_config.compaction_partition_concurrency,
        job_concurrency: compactor_config.compaction_job_concurrency,
        partition_scratchpad_concurrency: compactor_config
            .compaction_partition_scratchpad_concurrency,
        partition_threshold: Duration::from_secs(
            compactor_config.compaction_partition_minute_threshold * 60,
        ),
        max_desired_file_size_bytes: compactor_config.max_desired_file_size_bytes,
        percentage_max_file_size: compactor_config.percentage_max_file_size,
        split_percentage: compactor_config.split_percentage,
        partition_timeout: Duration::from_secs(compactor_config.partition_timeout_secs),
        partition_filter: compactor_config
            .partition_filter
            .map(|parts| parts.into_iter().map(PartitionId::new).collect()),
        shadow_mode: compactor_config.shadow_mode,
        ignore_partition_skip_marker: compactor_config.ignore_partition_skip_marker,
        max_input_files_per_partition: compactor_config.max_input_files_per_partition,
        max_input_parquet_bytes_per_partition: compactor_config
            .max_input_parquet_bytes_per_partition,
        shard_config,
    });

    Arc::new(Compactor2ServerType::new(
        compactor,
        metric_registry,
        common_state,
    ))
}
