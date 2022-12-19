use async_trait::async_trait;
use clap_blocks::{compactor::CompactorConfig, compactor2::Compactor2Config};
use compactor::{
    handler::{CompactorHandler, CompactorHandlerImpl},
    server::{grpc::GrpcDelegate, CompactorServer},
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

    #[error("split_percentage must be between 1 and 100, inclusive. Was: {split_percentage}")]
    SplitPercentageRange { split_percentage: u16 },
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

    /// Configure the gRPC services.
    async fn server_grpc(self: Arc<Self>, builder_input: RpcBuilderInput) -> Result<(), RpcError> {
        let builder = setup_builder!(builder_input, self);

        add_service!(builder, self.server.grpc().compaction_service());
        add_service!(builder, self.server.grpc().catalog_service());

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

/// Instantiate a compactor server that uses the RPC write path
// NOTE!!! This needs to be kept in sync with `create_compactor_server_type` until the switch to
// the RPC write path mode is complete! See the annotations about where these two functions line up
// and where they diverge.
pub async fn create_compactor2_server_type(
    common_state: &CommonServerState,
    metric_registry: Arc<metric::Registry>,
    catalog: Arc<dyn Catalog>,
    parquet_store: ParquetStorage,
    exec: Arc<Executor>,
    time_provider: Arc<dyn TimeProvider>,
    // Parameter difference: this function takes a `Compactor2Config` that doesn't have a write
    // buffer topic or shard indexes. All the other parameters here are the same.
    compactor_config: Compactor2Config,
) -> Result<Arc<dyn ServerType>> {
    let grpc_catalog = Arc::clone(&catalog);

    // Setup difference: build a compactor2 instead, which expects a `Compactor2Config`.
    let compactor = build_compactor2_from_config(
        compactor_config,
        catalog,
        parquet_store,
        exec,
        time_provider,
        Arc::clone(&metric_registry),
    )
    .await?;

    let compactor_handler = Arc::new(CompactorHandlerImpl::new(Arc::new(compactor)));

    let grpc = GrpcDelegate::new(grpc_catalog, Arc::clone(&compactor_handler));

    let compactor = CompactorServer::new(metric_registry, grpc, compactor_handler);
    Ok(Arc::new(CompactorServerType::new(compactor, common_state)))
}

// NOTE!!! This needs to be kept in sync with `build_compactor_from_config` until the switch to
// the RPC write path mode is complete! See the annotations about where these two functions line up
// and where they diverge.
pub async fn build_compactor2_from_config(
    // Parameter difference: this function takes a `Compactor2Config` that doesn't have a write
    // buffer topic or shard indexes. All the other parameters here are the same.
    compactor_config: Compactor2Config,
    catalog: Arc<dyn Catalog>,
    parquet_store: ParquetStorage,
    exec: Arc<Executor>,
    time_provider: Arc<dyn TimeProvider>,
    metric_registry: Arc<Registry>,
) -> Result<compactor::compact::Compactor, Error> {
    // 1. Shard index range checking: MISSING
    //    This function doesn't check the shard index range here like `build_compactor_from_config`
    //    does because shard indexes aren't relevant to `compactor2`.

    // 2. Split percentage value range checking
    if compactor_config.split_percentage < 1 || compactor_config.split_percentage > 100 {
        return Err(Error::SplitPercentageRange {
            split_percentage: compactor_config.split_percentage,
        });
    }

    // 3. Ensure topic and shard indexes are in the catalog: MISSING
    //    This isn't relevant to `compactor2`.

    // 4. Convert config type to handler config type
    let Compactor2Config {
        max_desired_file_size_bytes,
        percentage_max_file_size,
        split_percentage,
        max_number_partitions_per_shard,
        min_number_recent_ingested_files_per_partition,
        hot_multiple,
        warm_multiple,
        memory_budget_bytes,
        min_num_rows_allocated_per_record_batch_to_datafusion_plan,
        max_num_compacting_files,
        max_num_compacting_files_first_in_partition,
        minutes_without_new_writes_to_be_cold,
        hot_compaction_hours_threshold_1,
        hot_compaction_hours_threshold_2,
        max_parallel_partitions,
        warm_compaction_small_size_threshold_bytes,
        warm_compaction_min_small_file_count,
    } = compactor_config;

    let compactor_config = compactor::handler::CompactorConfig {
        max_desired_file_size_bytes,
        percentage_max_file_size,
        split_percentage,
        max_number_partitions_per_shard,
        min_number_recent_ingested_files_per_partition,
        hot_multiple,
        warm_multiple,
        memory_budget_bytes,
        min_num_rows_allocated_per_record_batch_to_datafusion_plan,
        max_num_compacting_files,
        max_num_compacting_files_first_in_partition,
        minutes_without_new_writes_to_be_cold,
        hot_compaction_hours_threshold_1,
        hot_compaction_hours_threshold_2,
        max_parallel_partitions,
        warm_compaction_small_size_threshold_bytes,
        warm_compaction_min_small_file_count,
    };
    // 4. END

    // 5. Create a new compactor: `compactor2` is assigned all shards (this argument can go away
    // completely when the switch to RPC mode is complete)
    Ok(compactor::compact::Compactor::new(
        compactor::compact::ShardAssignment::All,
        catalog,
        parquet_store,
        exec,
        time_provider,
        backoff::BackoffConfig::default(),
        compactor_config,
        metric_registry,
    ))
}

/// Instantiate a compactor server
// NOTE!!! This needs to be kept in sync with `create_compactor2_server_type` until the switch to
// the RPC write path mode is complete! See the annotations about where these two functions line up
// and where they diverge.
pub async fn create_compactor_server_type(
    common_state: &CommonServerState,
    metric_registry: Arc<metric::Registry>,
    catalog: Arc<dyn Catalog>,
    parquet_store: ParquetStorage,
    exec: Arc<Executor>,
    time_provider: Arc<dyn TimeProvider>,
    // Parameter difference: this function takes a `CompactorConfig` that has a write buffer topic
    // and requires shard indexes. All the other parameters here are the same.
    compactor_config: CompactorConfig,
) -> Result<Arc<dyn ServerType>> {
    let grpc_catalog = Arc::clone(&catalog);

    // Setup difference: build a compactor instead, which expects a `CompactorConfig`.
    let compactor = build_compactor_from_config(
        compactor_config,
        catalog,
        parquet_store,
        exec,
        time_provider,
        Arc::clone(&metric_registry),
    )
    .await?;

    let compactor_handler = Arc::new(CompactorHandlerImpl::new(Arc::new(compactor)));

    let grpc = GrpcDelegate::new(grpc_catalog, Arc::clone(&compactor_handler));

    let compactor = CompactorServer::new(metric_registry, grpc, compactor_handler);
    Ok(Arc::new(CompactorServerType::new(compactor, common_state)))
}

// NOTE!!! This needs to be kept in sync with `build_compactor2_from_config` until the switch to
// the RPC write path mode is complete! See the annotations about where these two functions line up
// and where they diverge.
pub async fn build_compactor_from_config(
    // Parameter difference: this function takes a `CompactorConfig` that has a write buffer topic
    // and requires shard indexes. All the other parameters here are the same.
    compactor_config: CompactorConfig,
    catalog: Arc<dyn Catalog>,
    parquet_store: ParquetStorage,
    exec: Arc<Executor>,
    time_provider: Arc<dyn TimeProvider>,
    metric_registry: Arc<Registry>,
) -> Result<compactor::compact::Compactor, Error> {
    // 1. Shard index range checking
    //    This function checks the shard index range; `compactor2` doesn't have shard indexes so
    //    `build_compactor2_from_config` doesn't have this check.
    if compactor_config.shard_index_range_start > compactor_config.shard_index_range_end {
        return Err(Error::ShardIndexRange);
    }

    // 2. Split percentage value range checking
    if compactor_config.split_percentage < 1 || compactor_config.split_percentage > 100 {
        return Err(Error::SplitPercentageRange {
            split_percentage: compactor_config.split_percentage,
        });
    }

    // 3. Ensure topic and shard indexes are in the catalog
    //    This isn't relevant to `compactor2`.
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
    // 3. END

    // 4. Convert config type to handler config type
    let CompactorConfig {
        max_desired_file_size_bytes,
        percentage_max_file_size,
        split_percentage,
        max_number_partitions_per_shard,
        min_number_recent_ingested_files_per_partition,
        hot_multiple,
        warm_multiple,
        memory_budget_bytes,
        min_num_rows_allocated_per_record_batch_to_datafusion_plan,
        max_num_compacting_files,
        max_num_compacting_files_first_in_partition,
        minutes_without_new_writes_to_be_cold,
        hot_compaction_hours_threshold_1,
        hot_compaction_hours_threshold_2,
        max_parallel_partitions,
        warm_compaction_small_size_threshold_bytes,
        warm_compaction_min_small_file_count,
        ..
    } = compactor_config;

    let compactor_config = compactor::handler::CompactorConfig {
        max_desired_file_size_bytes,
        percentage_max_file_size,
        split_percentage,
        max_number_partitions_per_shard,
        min_number_recent_ingested_files_per_partition,
        hot_multiple,
        warm_multiple,
        memory_budget_bytes,
        min_num_rows_allocated_per_record_batch_to_datafusion_plan,
        max_num_compacting_files,
        max_num_compacting_files_first_in_partition,
        minutes_without_new_writes_to_be_cold,
        hot_compaction_hours_threshold_1,
        hot_compaction_hours_threshold_2,
        max_parallel_partitions,
        warm_compaction_small_size_threshold_bytes,
        warm_compaction_min_small_file_count,
    };
    // 4. END

    // 5. Create a new compactor: Assigned only those shards specified in the CLI args.
    Ok(compactor::compact::Compactor::new(
        compactor::compact::ShardAssignment::Only(shards),
        catalog,
        parquet_store,
        exec,
        time_provider,
        backoff::BackoffConfig::default(),
        compactor_config,
        metric_registry,
    ))
}
