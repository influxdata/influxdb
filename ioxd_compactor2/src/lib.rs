use async_trait::async_trait;
use backoff::BackoffConfig;
use clap_blocks::compactor2::{CompactionType, Compactor2Config};
use compactor2::{
    compactor::Compactor2,
    config::{Config, PartitionsSourceConfig, ShardConfig},
};
use data_types::PartitionId;
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
    /// Human name for this server type
    fn name(&self) -> &str {
        "compactor2"
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
            IoxHttpError::NotFound => HttpApiErrorCode::NotFound,
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

    // if shard_count is specified, shard_id must be provided also.
    // shard_id may be specified explicitly or extracted from the host name.
    let mut shard_id = compactor_config.shard_id;
    if shard_id.is_none()
        && compactor_config.shard_count.is_some()
        && compactor_config.hostname.is_some()
    {
        let parsed_id = compactor_config
            .hostname
            .unwrap()
            .chars()
            .skip_while(|ch| !ch.is_ascii_digit())
            .take_while(|ch| ch.is_ascii_digit())
            .fold(None, |acc, ch| {
                ch.to_digit(10).map(|b| acc.unwrap_or(0) * 10 + b)
            });
        if parsed_id.is_some() {
            shard_id = Some(parsed_id.unwrap() as usize);
        }
    }
    assert!(
        shard_id.is_some() == compactor_config.shard_count.is_some(),
        "must provide or not provide shard ID and count"
    );
    let shard_config = shard_id.map(|shard_id| ShardConfig {
        shard_id,
        n_shards: compactor_config.shard_count.expect("just checked"),
    });

    let partitions_source = create_partition_source_config(
        compactor_config.partition_filter.as_deref(),
        compactor_config.process_all_partitions,
        compactor_config.compaction_type,
        compactor_config.compaction_partition_minute_threshold,
        compactor_config.compaction_cold_partition_minute_threshold,
    );

    // This is annoying to have two types that are so similar and have to convert between them, but
    // this way compactor2 doesn't have to know about clap_blocks and vice versa. It would also
    // be nice to have this as a `From` trait implementation, but this crate isn't allowed because
    // neither type is defined in ioxd_compactor. This feels like the right place to do the
    // conversion, though.
    let compaction_type = match compactor_config.compaction_type {
        CompactionType::Hot => compactor2::config::CompactionType::Hot,
        CompactionType::Cold => compactor2::config::CompactionType::Cold,
    };

    let compactor = Compactor2::start(Config {
        compaction_type,
        metric_registry: Arc::clone(&metric_registry),
        catalog,
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
        partitions_source,
        shadow_mode: compactor_config.shadow_mode,
        ignore_partition_skip_marker: compactor_config.ignore_partition_skip_marker,
        shard_config,
        min_num_l1_files_to_compact: compactor_config.min_num_l1_files_to_compact,
        process_once: compactor_config.process_once,
        simulate_without_object_store: false,
        parquet_files_sink_override: None,
        commit_wrapper: None,
        all_errors_are_fatal: false,
        max_num_columns_per_table: compactor_config.max_num_columns_per_table,
        max_num_files_per_plan: compactor_config.max_num_files_per_plan,
    });

    Arc::new(Compactor2ServerType::new(
        compactor,
        metric_registry,
        common_state,
    ))
}

fn create_partition_source_config(
    partition_filter: Option<&[i64]>,
    process_all_partitions: bool,
    compaction_type: CompactionType,
    compaction_partition_minute_threshold: u64,
    compaction_cold_partition_minute_threshold: u64,
) -> PartitionsSourceConfig {
    match (partition_filter, process_all_partitions, compaction_type) {
        (None, false, CompactionType::Hot) => PartitionsSourceConfig::CatalogRecentWrites {
            threshold: Duration::from_secs(compaction_partition_minute_threshold * 60),
        },
        (None, false, CompactionType::Cold) => PartitionsSourceConfig::CatalogColdForWrites {
            threshold: Duration::from_secs(compaction_cold_partition_minute_threshold * 60),
        },
        (None, true, _) => PartitionsSourceConfig::CatalogAll,
        (Some(ids), false, _) => {
            PartitionsSourceConfig::Fixed(ids.iter().cloned().map(PartitionId::new).collect())
        }
        (Some(_), true, _) => panic!(
            "provided partition ID filter and specific 'process all', this does not make sense"
        ),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    #[should_panic(
        expected = "provided partition ID filter and specific 'process all', this does not make sense"
    )]
    fn process_all_and_partition_filter_incompatible() {
        create_partition_source_config(
            Some(&[1, 7]),
            true,
            CompactionType::Hot, // arbitrary
            10,                  // arbitrary
            60,                  // arbitrary
        );
    }

    #[test]
    fn fixed_list_of_partitions() {
        let partitions_source_config = create_partition_source_config(
            Some(&[1, 7]),
            false,
            CompactionType::Hot, // arbitrary
            10,                  // arbitrary
            60,                  // arbitrary
        );

        assert_eq!(
            partitions_source_config,
            PartitionsSourceConfig::Fixed([PartitionId::new(1), PartitionId::new(7)].into())
        );
    }

    #[test]
    fn all_in_the_catalog() {
        let partitions_source_config = create_partition_source_config(
            None,
            true,
            CompactionType::Hot, // arbitrary
            10,                  // arbitrary
            60,                  // arbitrary
        );

        assert_eq!(partitions_source_config, PartitionsSourceConfig::CatalogAll,);
    }

    #[test]
    fn hot_compaction() {
        let partitions_source_config = create_partition_source_config(
            None,
            false,
            CompactionType::Hot,
            10,
            60, // arbitrary
        );

        assert_eq!(
            partitions_source_config,
            PartitionsSourceConfig::CatalogRecentWrites {
                threshold: Duration::from_secs(600)
            },
        );
    }

    #[test]
    fn cold_compaction() {
        let partitions_source_config = create_partition_source_config(
            None,
            false,
            CompactionType::Cold,
            10, // arbitrary
            60,
        );

        assert_eq!(
            partitions_source_config,
            PartitionsSourceConfig::CatalogColdForWrites {
                threshold: Duration::from_secs(3600)
            },
        );
    }
}
