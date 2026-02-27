//! This module handles the manipulation / execution of storage
//! plans. This is currently implemented using DataFusion, and this
//! interface abstracts away many of the details
pub(crate) mod context;
pub mod gapfill;
mod metrics;
pub mod query_tracing;
pub mod series_limit;
pub mod sleep;
pub(crate) mod split;
use datafusion_util::config::register_iox_object_store;
pub use executor::DedicatedExecutor;
use jemalloc_stats::AllocationMonitor;
use metric::Registry;
use object_store::DynObjectStore;
use parquet_file::storage::StorageId;
mod cross_rt_stream;
use tracing::warn;

use std::{collections::HashMap, fmt::Display, num::NonZeroUsize, sync::Arc};

use datafusion::{
    self,
    execution::{
        disk_manager::{DiskManagerBuilder, DiskManagerMode},
        memory_pool::{MemoryPool, UnboundedMemoryPool},
        runtime_env::{RuntimeEnv, RuntimeEnvBuilder},
    },
    logical_expr::{Expr, Extension, LogicalPlan, expr_rewriter::normalize_col},
};

pub use context::{
    IOxSessionConfig, IOxSessionContext, QueryConfig, QueryLanguage, SessionContextIOxExt,
};

use crate::{
    exec::metrics::DataFusionMemoryPoolMetricsBridge,
    memory_pool::{AllocationMonitoringMemoryPool, OomTrackingMemoryPool},
};

use self::split::StreamSplitNode;

const TESTING_MEM_POOL_SIZE: usize = 1024 * 1024 * 1024; // 1GB
const TESTING_PER_QUERY_MEM_POOL_SIZE: usize = 500; // 500 bytes
const TESTING_MAX_CONCURRENT_QUERIES: usize = 10;

/// Configuration for the per-query memory pool.
///
/// This enum defines the configuration for the per-query memory pool in the
/// querier, which is used to ensure each query has a minimum memory pre-allocated
/// for them. When the env variable `INFLUXDB_IOX_EXEC_PER_QUERY_MEM_POOL_BYTES`
/// is set to a non-zero size, this configuration is `Enabled`. Zero means disabled.
///
/// Other IOx components like the ingester, compactor, and catalog do not
/// require a separate per-query memory pool. So this configuration will be `Disabled`.
///
/// There are two fields in `PerQueryMemoryPoolConfig::Enabled`:
///
/// ```rs
/// Enabled {
///   per_query_mem_pool_size: usize,
///   max_concurrent_queries: usize,
/// }
/// ```
///
/// - `per_query_mem_pool_size` is configured by the env variable
///   `INFLUXDB_IOX_EXEC_PER_QUERY_MEM_POOL_BYTES`. If it is set to `Some`, the
///   `PerQueryMemoryPoolConfig::Enabled` will be set.
/// - `max_concurrent_queries` is configured by the env variable
///   `INFLUXDB_IOX_MAX_CONCURRENT_QUERIES`. It has a default value, so the configuration
///   `PerQueryMemoryPoolConfig::Enabled` does not depend on this env variable.
///
/// See the documentation for `INFLUXDB_IOX_EXEC_PER_QUERY_MEM_POOL_BYTES` for more
/// details.
#[derive(Debug, Clone, Copy)]
pub enum PerQueryMemoryPoolConfig {
    /// In the ingester, compactor, and catalog, a separate per-query memory
    /// pool is not required for query execution. Therefore, [`PerQueryMemoryPoolConfig`]
    /// should set to `Disabled`.
    Disabled,

    /// In the querier, a separate per-query memory pool can be configured on
    /// top of the central memory pool by enabling it with the following values.
    Enabled {
        /// The minimum size of the memory pool pre-reserved for each query, in bytes.
        ///
        /// In the querier, this value is configured by `INFLUXDB_IOX_EXEC_PER_QUERY_MEM_POOL_BYTES`.
        /// See `INFLUXDB_IOX_EXEC_PER_QUERY_MEM_POOL_BYTES` for details.
        per_query_mem_pool_size: usize,

        /// The maximum number of concurrent queries allowed.
        ///
        /// In the querier, this value is configured by `INFLUXDB_IOX_MAX_CONCURRENT_QUERIES`.
        /// It has a default value, ensuring that `max_concurrent_queries` is always set.
        max_concurrent_queries: usize,
    },
}

/// Configuration for an Executor
#[derive(Debug, Clone)]
pub struct ExecutorConfig {
    /// Target parallelism for query execution
    pub target_query_partitions: NonZeroUsize,

    /// Object stores
    pub object_stores: HashMap<StorageId, Arc<DynObjectStore>>,

    /// Metric registry
    pub metric_registry: Arc<Registry>,

    /// Memory pool size in bytes.
    pub mem_pool_size: usize,

    /// Configuration for the per-query memory pool.
    pub per_query_mem_pool_config: PerQueryMemoryPoolConfig,

    /// Optional heap memory limit in bytes.
    pub heap_memory_limit: Option<usize>,
}

impl ExecutorConfig {
    pub fn testing() -> Self {
        Self {
            target_query_partitions: NonZeroUsize::new(1).unwrap(),
            object_stores: HashMap::default(),
            metric_registry: Arc::new(Registry::default()),
            mem_pool_size: TESTING_MEM_POOL_SIZE,
            per_query_mem_pool_config: PerQueryMemoryPoolConfig::Disabled,
            heap_memory_limit: None,
        }
    }

    // Set the executor to use PerQueryMemoryPool in addition to
    // the central memory pool
    pub fn testing_with_per_query_memory_pool() -> Self {
        Self {
            target_query_partitions: NonZeroUsize::new(1).unwrap(),
            object_stores: HashMap::default(),
            metric_registry: Arc::new(Registry::default()),
            mem_pool_size: TESTING_MEM_POOL_SIZE,
            per_query_mem_pool_config: PerQueryMemoryPoolConfig::Enabled {
                per_query_mem_pool_size: TESTING_PER_QUERY_MEM_POOL_SIZE,
                max_concurrent_queries: TESTING_MAX_CONCURRENT_QUERIES,
            },
            heap_memory_limit: None,
        }
    }
}

impl Display for ExecutorConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "target_query_partitions={}, mem_pool_size={}",
            self.target_query_partitions, self.mem_pool_size
        )
    }
}

/// Handles executing DataFusion plans, and marshalling the results into rust
/// native structures.
#[derive(Debug)]
pub struct Executor {
    /// Executor
    executor: DedicatedExecutor,

    /// The default configuration options with which to create contexts
    config: ExecutorConfig,

    /// The DataFusion [RuntimeEnv] (including memory manager and disk
    /// manager) used for all executions
    runtime: Arc<RuntimeEnv>,
}

impl Display for Executor {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Executor({})", self.config)
    }
}

impl Executor {
    /// Get testing executor that runs a on single thread and a low memory bound
    /// to preserve resources.
    pub fn new_testing() -> Self {
        let config = ExecutorConfig::testing();
        let executor = DedicatedExecutor::new_testing();
        Self::new_with_config_and_executor(config, executor)
    }

    /// Get testing executor that runs a on single thread and a low memory bound
    /// to preserve resources (central memory pool) and a per query memory pool.
    pub fn new_testing_with_per_query_memory_pool() -> Self {
        let config = ExecutorConfig::testing_with_per_query_memory_pool();
        let executor = DedicatedExecutor::new_testing();
        Self::new_with_config_and_executor(config, executor)
    }

    /// Low-level constructor.
    ///
    /// This is mostly useful if you wanna keep the executor (because they are quite expensive to create) but need a fresh IOx runtime.
    ///
    /// # Panic
    /// Panics if the number of threads in `executor` is different from `config`.
    pub fn new_with_config_and_executor(
        config: ExecutorConfig,
        executor: DedicatedExecutor,
    ) -> Self {
        let central_mem_pool_limit = match config.per_query_mem_pool_config {
            PerQueryMemoryPoolConfig::Disabled => config.mem_pool_size,
            PerQueryMemoryPoolConfig::Enabled {
                per_query_mem_pool_size,
                max_concurrent_queries,
            } => {
                // Set aside a pre-reserved amount of memory for each query

                // Use `saturating_sub` to prevent underflow.
                //
                // If an underflow would occur, `saturating_sub` sets the central
                // memory pool size to 0. This ensures that the central memory will
                // trigger an error message due to insufficient memory.
                config
                    .mem_pool_size
                    .saturating_sub(per_query_mem_pool_size * max_concurrent_queries)
            }
        };

        let mut builder = RuntimeEnvBuilder::new()
            .with_disk_manager_builder(
                DiskManagerBuilder::default().with_mode(DiskManagerMode::Disabled),
            )
            .with_memory_limit(central_mem_pool_limit, 1.0);

        if let Some(heap_memory_limit) = config.heap_memory_limit {
            match AllocationMonitor::try_new(heap_memory_limit) {
                Ok(monitor) => {
                    let memory_pool = builder
                        .memory_pool
                        .as_ref()
                        .cloned()
                        .unwrap_or_else(|| Arc::new(UnboundedMemoryPool::default()));
                    builder = builder.with_memory_pool(Arc::new(
                        AllocationMonitoringMemoryPool::new(memory_pool, Arc::new(monitor)),
                    ));
                }
                Err(e) => {
                    warn!(
                        ?e,
                        "AllocationMonitor creation error, falling back to default memory pool"
                    );
                }
            }
        }

        // track OOMs in the central memory pool
        let memory_pool = builder
            .memory_pool
            .as_ref()
            .cloned()
            .unwrap_or_else(|| Arc::new(UnboundedMemoryPool::default()));
        builder = builder.with_memory_pool(Arc::new(OomTrackingMemoryPool::new(
            memory_pool,
            Arc::clone(&config.metric_registry),
        )));

        let runtime = builder.build_arc().expect("creating runtime");

        for (id, store) in &config.object_stores {
            register_iox_object_store(&runtime, id, Arc::clone(store));
        }

        // As there should only be a single memory pool for any executor,
        // verify that there was no existing instrument registered (for another pool)
        let mut created = false;
        let created_captured = &mut created;
        let bridge =
            DataFusionMemoryPoolMetricsBridge::new(&runtime.memory_pool, central_mem_pool_limit);
        let bridge_ctor = move || {
            *created_captured = true;
            bridge
        };
        config
            .metric_registry
            .register_instrument("datafusion_pool", bridge_ctor);
        assert!(
            created,
            "More than one execution pool created: previously existing instrument"
        );

        Self {
            executor,
            config,
            runtime,
        }
    }

    /// Return a new session config, suitable for executing a new query or system task.
    ///
    /// Note that this context (and all its clones) will be shut down once `Executor` is dropped.
    pub fn new_session_config(&self) -> IOxSessionConfig {
        let per_query_mem_pool_size = match self.config.per_query_mem_pool_config {
            PerQueryMemoryPoolConfig::Disabled => 0,
            PerQueryMemoryPoolConfig::Enabled {
                per_query_mem_pool_size,
                ..
            } => per_query_mem_pool_size,
        };

        IOxSessionConfig::new(
            self.executor.clone(),
            Arc::clone(&self.runtime),
            per_query_mem_pool_size,
        )
        .with_target_partitions(self.config.target_query_partitions)
    }

    /// Create a new execution context, suitable for executing a new query or system task
    ///
    /// Note that this context (and all its clones) will be shut down once `Executor` is dropped.
    pub fn new_context(&self) -> IOxSessionContext {
        self.new_session_config().build()
    }

    /// Initializes shutdown.
    pub fn shutdown(&self) {
        self.executor.shutdown();
    }

    /// Stops all subsequent task executions, and waits for the worker
    /// thread to complete. Note this will shutdown all created contexts.
    ///
    /// Only the first all to `join` will actually wait for the
    /// executing thread to complete. All other calls to join will
    /// complete immediately.
    pub async fn join(&self) {
        self.executor.join().await;
    }

    /// Returns the memory pool associated with this `Executor`
    pub fn pool(&self) -> Arc<dyn MemoryPool> {
        Arc::clone(&self.runtime.memory_pool)
    }

    /// Returns the runtime associated with this `Executor`
    pub fn runtime(&self) -> Arc<RuntimeEnv> {
        Arc::clone(&self.runtime)
    }

    /// Returns underlying config.
    pub fn config(&self) -> &ExecutorConfig {
        &self.config
    }

    /// Returns the underlying [`DedicatedExecutor`].
    pub fn executor(&self) -> &DedicatedExecutor {
        &self.executor
    }
}

// No need to implement `Drop` because this is done by DedicatedExecutor already

/// Create a StreamSplit node which takes an input stream of record
/// batches and produces multiple output streams based on  a list of `N` predicates.
/// The output will have `N+1` streams, and each row is sent to the stream
/// corresponding to the first predicate that evaluates to true, or the last stream if none do.
///
/// For example, if the input looks like:
/// ```text
///  X | time
/// ---+-----
///  a | 1000
///  b | 4000
///  c | 2000
/// ```
///
/// A StreamSplit with split_exprs = \[`time <= 1000`, `1000 < time <=2000`\] will produce the
/// following three output streams (output DataFusion Partitions):
///
///
/// ```text
///  X | time
/// ---+-----
///  a | 1000
/// ```
///
/// ```text
///  X | time
/// ---+-----
///  b | 2000
/// ```
/// and
/// ```text
///  X | time
/// ---+-----
///  b | 4000
/// ```
pub fn make_stream_split(input: LogicalPlan, split_exprs: Vec<Expr>) -> LogicalPlan {
    // rewrite the input expression so that it is fully qualified with the input schema
    let split_exprs = split_exprs
        .into_iter()
        .map(|split_expr| normalize_col(split_expr, &input).expect("normalize is infallable"))
        .collect::<Vec<_>>();

    let node = Arc::new(StreamSplitNode::new(input, split_exprs));
    LogicalPlan::Extension(Extension { node })
}

#[cfg(test)]
mod tests {
    use arrow::{
        array::Int64Array,
        datatypes::{DataType, Field, SchemaRef},
    };
    use datafusion::{
        error::DataFusionError,
        physical_expr::{EquivalenceProperties, LexOrdering, PhysicalSortExpr},
        physical_plan::{
            DisplayAs, ExecutionPlan, PlanProperties, RecordBatchStream,
            execution_plan::{Boundedness, EmissionType},
            expressions::Column,
            sorts::sort::SortExec,
        },
    };
    use futures::{Stream, StreamExt, stream::BoxStream};
    use metric::{Observation, RawReporter};

    use tokio::sync::Barrier;

    use super::*;
    use arrow::record_batch::RecordBatch;

    #[tokio::test]
    async fn test_metrics_integration() {
        let exec = Executor::new_testing();

        // start w/o any reservation
        assert_eq!(
            PoolMetrics::read(&exec.config.metric_registry),
            PoolMetrics {
                reserved: 0,
                limit: TESTING_MEM_POOL_SIZE as u64,
            },
        );

        // block some reservation
        let test_input = Arc::new(TestExec::default());
        let schema = test_input.schema();
        let plan = Arc::new(SortExec::new(
            LexOrdering::new(vec![PhysicalSortExpr {
                expr: Arc::new(Column::new_with_schema("c", &schema).unwrap()),
                options: Default::default(),
            }])
            .unwrap(),
            Arc::clone(&test_input) as _,
        ));
        let ctx = exec.new_context();
        let handle = tokio::spawn(async move {
            ctx.collect(plan).await.unwrap();
        });
        test_input.wait().await;
        assert_eq!(
            PoolMetrics::read(&exec.config.metric_registry),
            PoolMetrics {
                reserved: 1600,
                limit: TESTING_MEM_POOL_SIZE as u64,
            },
        );
        test_input.wait_for_finish().await;

        // end w/o any reservation
        handle.await.unwrap();
        assert_eq!(
            PoolMetrics::read(&exec.config.metric_registry),
            PoolMetrics {
                reserved: 0,
                limit: TESTING_MEM_POOL_SIZE as u64,
            },
        );
    }

    #[tokio::test]
    async fn test_metrics_integration_with_per_query_memory_pool() {
        const TESTING_CENTRAL_MEM_POOL_SIZE: usize = TESTING_MEM_POOL_SIZE
            - TESTING_PER_QUERY_MEM_POOL_SIZE * TESTING_MAX_CONCURRENT_QUERIES;

        let exec = Executor::new_testing_with_per_query_memory_pool();

        // start w/o any reservation
        assert_eq!(
            PoolMetrics::read(&exec.config.metric_registry),
            PoolMetrics {
                reserved: 0,
                limit: TESTING_CENTRAL_MEM_POOL_SIZE as u64,
            },
        );

        // block some reservation
        let test_input = Arc::new(TestExec::default());
        let schema = test_input.schema();
        let plan = Arc::new(SortExec::new(
            LexOrdering::new(vec![PhysicalSortExpr {
                expr: Arc::new(Column::new_with_schema("c", &schema).unwrap()),
                options: Default::default(),
            }])
            .unwrap(),
            Arc::clone(&test_input) as _,
        ));
        let ctx = exec.new_context();
        let handle = tokio::spawn(async move {
            ctx.collect(plan).await.unwrap();
        });
        test_input.wait().await;
        assert_eq!(
            PoolMetrics::read(&exec.config.metric_registry),
            PoolMetrics {
                // The plan needs 1600 bytes of memory. The pre-reserved memory for
                // this query is 500 bytes as defined in TESTING_PER_QUERY_MEM_POOL_SIZE.
                // So the central memory pool reserved an additional 1100 (1600 - 500) bytes.
                reserved: 1100,
                limit: TESTING_CENTRAL_MEM_POOL_SIZE as u64,
            },
        );
        test_input.wait_for_finish().await;

        // end w/o any reservation
        handle.await.unwrap();
        assert_eq!(
            PoolMetrics::read(&exec.config.metric_registry),
            PoolMetrics {
                reserved: 0,
                limit: TESTING_CENTRAL_MEM_POOL_SIZE as u64,
            },
        );
    }

    #[derive(Debug)]
    struct TestExec {
        schema: SchemaRef,
        // Barrier after a batch has been produced
        barrier: Arc<Barrier>,
        // Barrier right before the operator is complete
        barrier_finish: Arc<Barrier>,
        /// Cache holding plan properties like equivalences, output partitioning, output ordering etc.
        cache: PlanProperties,
    }

    impl Default for TestExec {
        fn default() -> Self {
            let schema = Arc::new(arrow::datatypes::Schema::new(vec![Field::new(
                "c",
                DataType::Int64,
                true,
            )]));

            let cache = Self::compute_properties(Arc::clone(&schema));

            Self {
                schema,
                barrier: Arc::new(Barrier::new(2)),
                barrier_finish: Arc::new(Barrier::new(2)),
                cache,
            }
        }
    }

    impl TestExec {
        /// wait for the first output to be produced
        pub async fn wait(&self) {
            self.barrier.wait().await;
        }

        /// wait for output to be done
        pub async fn wait_for_finish(&self) {
            self.barrier_finish.wait().await;
        }

        /// This function creates the cache object that stores the plan properties such as equivalence properties, partitioning, ordering, etc.
        fn compute_properties(schema: SchemaRef) -> PlanProperties {
            let eq_properties = EquivalenceProperties::new(schema);

            let output_partitioning =
                datafusion::physical_plan::Partitioning::UnknownPartitioning(1);

            PlanProperties::new(
                eq_properties,
                output_partitioning,
                EmissionType::Incremental,
                Boundedness::Bounded,
            )
        }
    }

    impl DisplayAs for TestExec {
        fn fmt_as(
            &self,
            _t: datafusion::physical_plan::DisplayFormatType,
            f: &mut std::fmt::Formatter<'_>,
        ) -> std::fmt::Result {
            write!(f, "TestExec")
        }
    }

    impl ExecutionPlan for TestExec {
        fn name(&self) -> &str {
            Self::static_name()
        }

        fn as_any(&self) -> &dyn std::any::Any {
            self
        }

        fn schema(&self) -> SchemaRef {
            Arc::clone(&self.schema)
        }

        fn properties(&self) -> &PlanProperties {
            &self.cache
        }

        fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
            vec![]
        }

        fn with_new_children(
            self: Arc<Self>,
            _children: Vec<Arc<dyn ExecutionPlan>>,
        ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
            unimplemented!()
        }

        fn execute(
            &self,
            _partition: usize,
            _context: Arc<datafusion::execution::TaskContext>,
        ) -> datafusion::error::Result<datafusion::physical_plan::SendableRecordBatchStream>
        {
            let barrier = Arc::clone(&self.barrier);
            let schema = Arc::clone(&self.schema);
            let barrier_finish = Arc::clone(&self.barrier_finish);
            let schema_finish = Arc::clone(&self.schema);
            let stream = futures::stream::iter([Ok(RecordBatch::try_new(
                Arc::clone(&self.schema),
                vec![Arc::new(Int64Array::from(vec![1i64; 100]))],
            )
            .unwrap())])
            .chain(futures::stream::once(async move {
                barrier.wait().await;
                Ok(RecordBatch::new_empty(schema))
            }))
            .chain(futures::stream::once(async move {
                barrier_finish.wait().await;
                Ok(RecordBatch::new_empty(schema_finish))
            }));
            let stream = BoxRecordBatchStream {
                schema: Arc::clone(&self.schema),
                inner: stream.boxed(),
            };
            Ok(Box::pin(stream))
        }

        fn statistics(&self) -> Result<datafusion::physical_plan::Statistics, DataFusionError> {
            Ok(datafusion::physical_plan::Statistics::new_unknown(
                &self.schema(),
            ))
        }
    }

    struct BoxRecordBatchStream {
        schema: SchemaRef,
        inner: BoxStream<'static, Result<RecordBatch, DataFusionError>>,
    }

    impl Stream for BoxRecordBatchStream {
        type Item = Result<RecordBatch, DataFusionError>;

        fn poll_next(
            mut self: std::pin::Pin<&mut Self>,
            cx: &mut std::task::Context<'_>,
        ) -> std::task::Poll<Option<Self::Item>> {
            let this = &mut *self;
            this.inner.poll_next_unpin(cx)
        }
    }

    impl RecordBatchStream for BoxRecordBatchStream {
        fn schema(&self) -> SchemaRef {
            Arc::clone(&self.schema)
        }
    }

    #[derive(Debug, PartialEq, Eq)]
    struct PoolMetrics {
        reserved: u64,
        limit: u64,
    }

    impl PoolMetrics {
        fn read(registry: &Registry) -> Self {
            let mut reporter = RawReporter::default();
            registry.report(&mut reporter);
            let metric = reporter.metric("datafusion_mem_pool_bytes").unwrap();

            let reserved = metric.observation(&[("state", "reserved")]).unwrap();
            let Observation::U64Gauge(reserved) = reserved else {
                panic!("wrong metric type")
            };
            let limit = metric.observation(&[("state", "limit")]).unwrap();
            let Observation::U64Gauge(limit) = limit else {
                panic!("wrong metric type")
            };

            Self {
                reserved: *reserved,
                limit: *limit,
            }
        }
    }
}
