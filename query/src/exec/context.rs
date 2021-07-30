//! This module contains plumbing to connect InfluxDB IOx extensions to
//! DataFusion

use std::{fmt, sync::Arc};

use arrow::record_batch::RecordBatch;
use datafusion::{
    catalog::catalog::CatalogProvider,
    execution::context::{ExecutionContextState, QueryPlanner},
    logical_plan::{LogicalPlan, UserDefinedLogicalNode},
    physical_plan::{
        coalesce_partitions::CoalescePartitionsExec,
        collect, displayable,
        planner::{DefaultPhysicalPlanner, ExtensionPlanner},
        ExecutionPlan, PhysicalPlanner, SendableRecordBatchStream,
    },
    prelude::*,
};

use crate::exec::{
    schema_pivot::{SchemaPivotExec, SchemaPivotNode},
    split::StreamSplitExec,
};

use observability_deps::tracing::{debug, trace};

// Reuse DataFusion error and Result types for this module
pub use datafusion::error::{DataFusionError as Error, Result};

use super::split::StreamSplitNode;
use super::task::DedicatedExecutor;

// The default catalog name - this impacts what SQL queries use if not specified
pub const DEFAULT_CATALOG: &str = "public";
// The default schema name - this impacts what SQL queries use if not specified
pub const DEFAULT_SCHEMA: &str = "iox";

/// This structure implements the DataFusion notion of "query planner"
/// and is needed to create plans with the IOx extension nodes.
struct IOxQueryPlanner {}

impl QueryPlanner for IOxQueryPlanner {
    /// Given a `LogicalPlan` created from above, create an
    /// `ExecutionPlan` suitable for execution
    fn create_physical_plan(
        &self,
        logical_plan: &LogicalPlan,
        ctx_state: &ExecutionContextState,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        // Teach the default physical planner how to plan SchemaPivot
        // and StreamSplit nodes.
        let physical_planner =
            DefaultPhysicalPlanner::with_extension_planners(vec![Arc::new(IOxExtensionPlanner {})]);
        // Delegate most work of physical planning to the default physical planner
        physical_planner.create_physical_plan(logical_plan, ctx_state)
    }
}

/// Physical planner for InfluxDB IOx extension plans
struct IOxExtensionPlanner {}

impl ExtensionPlanner for IOxExtensionPlanner {
    /// Create a physical plan for an extension node
    fn plan_extension(
        &self,
        planner: &dyn PhysicalPlanner,
        node: &dyn UserDefinedLogicalNode,
        logical_inputs: &[&LogicalPlan],
        physical_inputs: &[Arc<dyn ExecutionPlan>],
        ctx_state: &ExecutionContextState,
    ) -> Result<Option<Arc<dyn ExecutionPlan>>> {
        let any = node.as_any();
        let plan = if let Some(schema_pivot) = any.downcast_ref::<SchemaPivotNode>() {
            assert_eq!(physical_inputs.len(), 1, "Inconsistent number of inputs");
            Some(Arc::new(SchemaPivotExec::new(
                Arc::clone(&physical_inputs[0]),
                schema_pivot.schema().as_ref().clone().into(),
            )) as Arc<dyn ExecutionPlan>)
        } else if let Some(stream_split) = any.downcast_ref::<StreamSplitNode>() {
            assert_eq!(
                logical_inputs.len(),
                1,
                "Inconsistent number of logical inputs"
            );
            assert_eq!(
                physical_inputs.len(),
                1,
                "Inconsistent number of physical inputs"
            );

            let split_expr = planner.create_physical_expr(
                stream_split.split_expr(),
                logical_inputs[0].schema(),
                &physical_inputs[0].schema(),
                ctx_state,
            )?;

            Some(Arc::new(StreamSplitExec::new(
                Arc::clone(&physical_inputs[0]),
                split_expr,
            )) as Arc<dyn ExecutionPlan>)
        } else {
            None
        };
        Ok(plan)
    }
}

// Configuration for an IOx execution context
#[derive(Clone)]
pub struct IOxExecutionConfig {
    /// Configuration options to pass to DataFusion
    inner: ExecutionConfig,
}

impl Default for IOxExecutionConfig {
    fn default() -> Self {
        const BATCH_SIZE: usize = 1000;

        // Setup default configuration
        let inner = ExecutionConfig::new()
            .with_batch_size(BATCH_SIZE)
            .create_default_catalog_and_schema(true)
            .with_information_schema(true)
            .with_default_catalog_and_schema(DEFAULT_CATALOG, DEFAULT_SCHEMA)
            .with_query_planner(Arc::new(IOxQueryPlanner {}));

        Self { inner }
    }
}

impl fmt::Debug for IOxExecutionConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "IOxExecutionConfig ...")
    }
}

impl IOxExecutionConfig {
    pub fn new() -> Self {
        Default::default()
    }

    /// Set execution concurrency
    pub fn set_concurrency(&mut self, concurrency: usize) {
        self.inner.concurrency = concurrency;
    }
}

/// This is an execution context for planning in IOx.  It wraps a
/// DataFusion execution context with the information needed for planning.
///
/// Methods on this struct should be preferred to using the raw
/// DataFusion functions (such as `collect`) directly.
///
/// Eventually we envision this also managing additional resource
/// types such as Memory and providing visibility into what plans are
/// running
pub struct IOxExecutionContext {
    inner: ExecutionContext,

    /// Dedicated executor for query execution.
    ///
    /// DataFusion plans are "CPU" bound and thus can consume tokio
    /// executors threads for extended periods of time. We use a
    /// dedicated tokio runtime to run them so that other requests
    /// can be handled.
    exec: DedicatedExecutor,
}

impl fmt::Debug for IOxExecutionContext {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("IOxExecutionContext")
            .field("inner", &"<DataFusion ExecutionContext>")
            .finish()
    }
}

impl IOxExecutionContext {
    /// Create an ExecutionContext suitable for executing DataFusion plans
    pub fn new(exec: DedicatedExecutor, config: IOxExecutionConfig) -> Self {
        let inner = ExecutionContext::with_config(config.inner);

        Self { inner, exec }
    }

    /// returns a reference to the inner datafusion execution context
    pub fn inner(&self) -> &ExecutionContext {
        &self.inner
    }

    /// registers a catalog with the inner context
    pub fn register_catalog(&mut self, name: impl Into<String>, catalog: Arc<dyn CatalogProvider>) {
        self.inner.register_catalog(name, catalog);
    }

    ///

    /// Prepare a SQL statement for execution. This assumes that any
    /// tables referenced in the SQL have been registered with this context
    pub fn prepare_sql(&mut self, sql: &str) -> Result<Arc<dyn ExecutionPlan>> {
        debug!(text=%sql, "planning SQL query");
        let logical_plan = self.inner.create_logical_plan(sql)?;
        self.prepare_plan(&logical_plan)
    }

    /// Prepare (optimize + plan) a pre-created logical plan for execution
    pub fn prepare_plan(&self, plan: &LogicalPlan) -> Result<Arc<dyn ExecutionPlan>> {
        debug!(text=%plan.display_indent_schema(), "prepare_plan: initial plan");

        let plan = self.inner.optimize(plan)?;
        trace!(text=%plan.display_indent_schema(), graphviz=%plan.display_graphviz(), "optimized plan");

        let physical_plan = self.inner.create_physical_plan(&plan)?;

        debug!(text=%displayable(physical_plan.as_ref()).indent(), "prepare_plan: plan to run");
        Ok(physical_plan)
    }

    /// Executes the logical plan using DataFusion on a separate
    /// thread pool and produces RecordBatches
    pub async fn collect(&self, physical_plan: Arc<dyn ExecutionPlan>) -> Result<Vec<RecordBatch>> {
        debug!(
            "Running plan, physical:\n{}",
            displayable(physical_plan.as_ref()).indent()
        );

        self.exec.spawn(collect(physical_plan)).await.map_err(|e| {
            Error::Execution(format!("Error running IOxExecutionContext::collect: {}", e))
        })?
    }

    /// Executes the physical plan and produces a RecordBatchStream to stream
    /// over the result that iterates over the results.
    pub async fn execute(
        &self,
        physical_plan: Arc<dyn ExecutionPlan>,
    ) -> Result<SendableRecordBatchStream> {
        match physical_plan.output_partitioning().partition_count() {
            0 => unreachable!(),
            1 => self.execute_partition(physical_plan, 0).await,
            _ => {
                // Merge into a single partition
                self.execute_partition(Arc::new(CoalescePartitionsExec::new(physical_plan)), 0)
                    .await
            }
        }
    }

    /// Executes a single partition of a physical plan and produces a RecordBatchStream to stream
    /// over the result that iterates over the results.
    pub async fn execute_partition(
        &self,
        physical_plan: Arc<dyn ExecutionPlan>,
        partition: usize,
    ) -> Result<SendableRecordBatchStream> {
        self.exec
            .spawn(async move { physical_plan.execute(partition).await })
            .await
            .map_err(|e| {
                Error::Execution(format!("Error running IOxExecutionContext::execute: {}", e))
            })?
    }
}
