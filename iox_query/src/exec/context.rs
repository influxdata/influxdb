//! This module contains plumbing to connect InfluxDB IOx extensions to
//! DataFusion

use async_trait::async_trait;
use executor::DedicatedExecutor;
use std::{convert::TryInto, fmt, sync::Arc};

use arrow::record_batch::RecordBatch;

use datafusion::{
    catalog::catalog::CatalogProvider,
    config::OPT_COALESCE_TARGET_BATCH_SIZE,
    execution::{
        context::{QueryPlanner, SessionState, TaskContext},
        runtime_env::RuntimeEnv,
    },
    logical_plan::{LogicalPlan, UserDefinedLogicalNode},
    physical_plan::{
        coalesce_partitions::CoalescePartitionsExec,
        displayable,
        planner::{DefaultPhysicalPlanner, ExtensionPlanner},
        EmptyRecordBatchStream, ExecutionPlan, PhysicalPlanner, SendableRecordBatchStream,
    },
    prelude::*,
};
use futures::TryStreamExt;
use observability_deps::tracing::debug;
use trace::{
    ctx::SpanContext,
    span::{MetaValue, Span, SpanExt, SpanRecorder},
};

use crate::exec::{
    fieldlist::{FieldList, IntoFieldList},
    non_null_checker::NonNullCheckerExec,
    query_tracing::TracedStream,
    schema_pivot::{SchemaPivotExec, SchemaPivotNode},
    seriesset::{
        converter::{GroupGenerator, SeriesSetConverter},
        series::Series,
    },
    split::StreamSplitExec,
    stringset::{IntoStringSet, StringSetRef},
};

use crate::plan::{
    fieldlist::FieldListPlan,
    seriesset::{SeriesSetPlan, SeriesSetPlans},
    stringset::StringSetPlan,
};

// Reuse DataFusion error and Result types for this module
pub use datafusion::error::{DataFusionError as Error, Result};

use super::{
    non_null_checker::NonNullCheckerNode, seriesset::series::Either, split::StreamSplitNode,
};

// The default catalog name - this impacts what SQL queries use if not specified
pub const DEFAULT_CATALOG: &str = "public";
// The default schema name - this impacts what SQL queries use if not specified
pub const DEFAULT_SCHEMA: &str = "iox";

/// This structure implements the DataFusion notion of "query planner"
/// and is needed to create plans with the IOx extension nodes.
struct IOxQueryPlanner {}

#[async_trait]
impl QueryPlanner for IOxQueryPlanner {
    /// Given a `LogicalPlan` created from above, create an
    /// `ExecutionPlan` suitable for execution
    async fn create_physical_plan(
        &self,
        logical_plan: &LogicalPlan,
        session_state: &SessionState,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        // Teach the default physical planner how to plan SchemaPivot
        // and StreamSplit nodes.
        let physical_planner =
            DefaultPhysicalPlanner::with_extension_planners(vec![Arc::new(IOxExtensionPlanner {})]);
        // Delegate most work of physical planning to the default physical planner
        physical_planner
            .create_physical_plan(logical_plan, session_state)
            .await
    }
}

/// Physical planner for InfluxDB IOx extension plans
struct IOxExtensionPlanner {}

#[async_trait]
impl ExtensionPlanner for IOxExtensionPlanner {
    /// Create a physical plan for an extension node
    async fn plan_extension(
        &self,
        planner: &dyn PhysicalPlanner,
        node: &dyn UserDefinedLogicalNode,
        logical_inputs: &[&LogicalPlan],
        physical_inputs: &[Arc<dyn ExecutionPlan>],
        session_state: &SessionState,
    ) -> Result<Option<Arc<dyn ExecutionPlan>>> {
        let any = node.as_any();
        let plan = if let Some(schema_pivot) = any.downcast_ref::<SchemaPivotNode>() {
            assert_eq!(physical_inputs.len(), 1, "Inconsistent number of inputs");
            Some(Arc::new(SchemaPivotExec::new(
                Arc::clone(&physical_inputs[0]),
                schema_pivot.schema().as_ref().clone().into(),
            )) as Arc<dyn ExecutionPlan>)
        } else if let Some(non_null_checker) = any.downcast_ref::<NonNullCheckerNode>() {
            assert_eq!(physical_inputs.len(), 1, "Inconsistent number of inputs");
            Some(Arc::new(NonNullCheckerExec::new(
                Arc::clone(&physical_inputs[0]),
                non_null_checker.schema().as_ref().clone().into(),
                non_null_checker.value(),
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

            let split_exprs = stream_split
                .split_exprs()
                .iter()
                .map(|e| {
                    planner.create_physical_expr(
                        e,
                        logical_inputs[0].schema(),
                        &physical_inputs[0].schema(),
                        session_state,
                    )
                })
                .collect::<Result<Vec<_>>>()?;

            Some(Arc::new(StreamSplitExec::new(
                Arc::clone(&physical_inputs[0]),
                split_exprs,
            )) as Arc<dyn ExecutionPlan>)
        } else {
            None
        };
        Ok(plan)
    }
}

/// Configuration for an IOx execution context
///
/// Created from an Executor
#[derive(Clone)]
pub struct IOxSessionConfig {
    /// Executor to run on
    exec: DedicatedExecutor,

    /// DataFusion session configuration
    session_config: SessionConfig,

    /// Shared DataFusion runtime
    runtime: Arc<RuntimeEnv>,

    /// Default catalog
    default_catalog: Option<Arc<dyn CatalogProvider>>,

    /// Span context from which to create spans for this query
    span_ctx: Option<SpanContext>,
}

impl fmt::Debug for IOxSessionConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "IOxSessionConfig ...")
    }
}

const BATCH_SIZE: usize = 8 * 1024;
const COALESCE_BATCH_SIZE: usize = BATCH_SIZE / 2;

impl IOxSessionConfig {
    pub(super) fn new(exec: DedicatedExecutor, runtime: Arc<RuntimeEnv>) -> Self {
        let session_config = SessionConfig::new()
            .with_batch_size(BATCH_SIZE)
            // TODO add function in SessionCofig
            .set_u64(
                OPT_COALESCE_TARGET_BATCH_SIZE,
                COALESCE_BATCH_SIZE.try_into().unwrap(),
            )
            .create_default_catalog_and_schema(true)
            .with_information_schema(true)
            .with_default_catalog_and_schema(DEFAULT_CATALOG, DEFAULT_SCHEMA);

        Self {
            exec,
            session_config,
            runtime,
            default_catalog: None,
            span_ctx: None,
        }
    }

    /// Set execution concurrency
    pub fn with_target_partitions(mut self, target_partitions: usize) -> Self {
        self.session_config = self
            .session_config
            .with_target_partitions(target_partitions);
        self
    }

    /// Set the default catalog provider
    pub fn with_default_catalog(self, catalog: Arc<dyn CatalogProvider>) -> Self {
        Self {
            default_catalog: Some(catalog),
            ..self
        }
    }

    /// Set the span context from which to create  distributed tracing spans for this query
    pub fn with_span_context(self, span_ctx: Option<SpanContext>) -> Self {
        Self { span_ctx, ..self }
    }

    /// Create an ExecutionContext suitable for executing DataFusion plans
    pub fn build(self) -> IOxSessionContext {
        let state = SessionState::with_config_rt(self.session_config, self.runtime)
            .with_query_planner(Arc::new(IOxQueryPlanner {}));

        let inner = SessionContext::with_state(state);

        if let Some(default_catalog) = self.default_catalog {
            inner.register_catalog(DEFAULT_CATALOG, default_catalog);
        }

        let maybe_span = self.span_ctx.child_span("Query Execution");

        IOxSessionContext::new(inner, Some(self.exec), SpanRecorder::new(maybe_span))
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
///
/// An IOxSessionContext is created directly from an Executor, or from
/// an IOxSessionConfig created by an Executor
pub struct IOxSessionContext {
    inner: SessionContext,

    /// Optional dedicated executor for query execution.
    ///
    /// DataFusion plans are "CPU" bound and thus can consume tokio
    /// executors threads for extended periods of time. We use a
    /// dedicated tokio runtime to run them so that other requests
    /// can be handled.
    exec: Option<DedicatedExecutor>,

    /// Span context from which to create spans for this query
    recorder: SpanRecorder,
}

impl fmt::Debug for IOxSessionContext {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("IOxSessionContext")
            .field("inner", &"<DataFusion ExecutionContext>")
            .finish()
    }
}

impl IOxSessionContext {
    /// Constructor for testing.
    ///
    /// This is identical to [`Default::default`] but we do NOT implement [`Default`] to make the creation of untracked
    /// contexts more explicit.
    pub fn with_testing() -> Self {
        Self {
            inner: SessionContext::default(),
            exec: None,
            recorder: SpanRecorder::default(),
        }
    }

    /// Private constructor
    pub(crate) fn new(
        inner: SessionContext,
        exec: Option<DedicatedExecutor>,
        recorder: SpanRecorder,
    ) -> Self {
        // attach span to DataFusion session
        {
            let mut state = inner.state.write();
            state.config = state
                .config
                .clone()
                .with_extension(Arc::new(recorder.span().cloned()));
        }

        Self {
            inner,
            exec,
            recorder,
        }
    }

    /// returns a reference to the inner datafusion execution context
    pub fn inner(&self) -> &SessionContext {
        &self.inner
    }

    /// Prepare a SQL statement for execution. This assumes that any
    /// tables referenced in the SQL have been registered with this context
    pub async fn prepare_sql(&self, sql: &str) -> Result<Arc<dyn ExecutionPlan>> {
        let ctx = self.child_ctx("prepare_sql");
        debug!(text=%sql, "planning SQL query");
        let logical_plan = ctx.inner.create_logical_plan(sql)?;
        debug!(plan=%logical_plan.display_graphviz(), "logical plan");
        ctx.create_physical_plan(&logical_plan).await
    }

    /// Prepare (optimize + plan) a pre-created [`LogicalPlan`] for execution
    pub async fn create_physical_plan(&self, plan: &LogicalPlan) -> Result<Arc<dyn ExecutionPlan>> {
        let mut ctx = self.child_ctx("create_physical_plan");
        debug!(text=%plan.display_indent_schema(), "create_physical_plan: initial plan");
        let physical_plan = ctx.inner.create_physical_plan(plan).await?;

        ctx.recorder.event("physical plan");
        debug!(text=%displayable(physical_plan.as_ref()).indent(), "create_physical_plan: plan to run");
        Ok(physical_plan)
    }

    /// Executes the logical plan using DataFusion on a separate
    /// thread pool and produces RecordBatches
    pub async fn collect(&self, physical_plan: Arc<dyn ExecutionPlan>) -> Result<Vec<RecordBatch>> {
        debug!(
            "Running plan, physical:\n{}",
            displayable(physical_plan.as_ref()).indent()
        );
        let ctx = self.child_ctx("collect");
        let stream = ctx.execute_stream(physical_plan).await?;

        ctx.run(
            stream
                .err_into() // convert to DataFusionError
                .try_collect(),
        )
        .await
    }

    /// Executes the physical plan and produces a
    /// `SendableRecordBatchStream` to stream over the result that
    /// iterates over the results. The creation of the stream is
    /// performed in a separate thread pool.
    pub async fn execute_stream(
        &self,
        physical_plan: Arc<dyn ExecutionPlan>,
    ) -> Result<SendableRecordBatchStream> {
        match physical_plan.output_partitioning().partition_count() {
            0 => Ok(Box::pin(EmptyRecordBatchStream::new(
                physical_plan.schema(),
            ))),
            1 => self.execute_stream_partitioned(physical_plan, 0).await,
            _ => {
                // Merge into a single partition
                self.execute_stream_partitioned(
                    Arc::new(CoalescePartitionsExec::new(physical_plan)),
                    0,
                )
                .await
            }
        }
    }

    /// Executes a single partition of a physical plan and produces a
    /// `SendableRecordBatchStream` to stream over the result that
    /// iterates over the results. The creation of the stream is
    /// performed in a separate thread pool.
    pub async fn execute_stream_partitioned(
        &self,
        physical_plan: Arc<dyn ExecutionPlan>,
        partition: usize,
    ) -> Result<SendableRecordBatchStream> {
        let span = self
            .recorder
            .span()
            .map(|span| span.child("execute_stream_partitioned"));

        let task_context = Arc::new(TaskContext::from(self.inner()));

        self.run(async move {
            let stream = physical_plan.execute(partition, task_context)?;
            let stream = TracedStream::new(stream, span, physical_plan);
            Ok(Box::pin(stream) as _)
        })
        .await
    }

    /// Executes the SeriesSetPlans on the query executor, in
    /// parallel, producing series or groups
    ///
    /// TODO make this streaming rather than buffering the results
    pub async fn to_series_and_groups(
        &self,
        series_set_plans: SeriesSetPlans,
    ) -> Result<Vec<Either>> {
        let SeriesSetPlans {
            mut plans,
            group_columns,
        } = series_set_plans;

        if plans.is_empty() {
            return Ok(vec![]);
        }

        // sort plans by table (measurement) name
        plans.sort_by(|a, b| a.table_name.cmp(&b.table_name));

        // Run the plans in parallel
        let handles = plans
            .into_iter()
            .map(|plan| {
                let ctx = self.child_ctx("to_series_set");
                self.run(async move {
                    let SeriesSetPlan {
                        table_name,
                        plan,
                        tag_columns,
                        field_columns,
                    } = plan;

                    let tag_columns = Arc::new(tag_columns);

                    let physical_plan = ctx.create_physical_plan(&plan).await?;

                    let it = ctx.execute_stream(physical_plan).await?;

                    SeriesSetConverter::default()
                        .convert(table_name, tag_columns, field_columns, it)
                        .await
                        .map_err(|e| {
                            Error::Execution(format!(
                                "Error executing series set conversion: {}",
                                e
                            ))
                        })
                })
            })
            .collect::<Vec<_>>();

        // join_all ensures that the results are consumed in the same order they
        // were spawned maintaining the guarantee to return results ordered
        // by table name and plan sort order.
        let all_series_sets = futures::future::try_join_all(handles).await?;

        // convert to series sets
        let mut data: Vec<Series> = vec![];
        for series_sets in all_series_sets {
            for series_set in series_sets {
                // If all timestamps of returned columns are nulls,
                // there must be no data. We need to check this because
                // aggregate (e.g. count, min, max) returns one row that are
                // all null (even the values of aggregate) for min, max and 0 for count.
                // For influx read_group's series and group, we do not want to return 0
                // for count either.
                if series_set.is_timestamp_all_null() {
                    continue;
                }

                let series: Vec<Series> = series_set
                    .try_into()
                    .map_err(|e| Error::Execution(format!("Error converting to series: {}", e)))?;
                data.extend(series);
            }
        }

        // If we have group columns, sort the results, and create the
        // appropriate groups
        if let Some(group_columns) = group_columns {
            let grouper = GroupGenerator::new(group_columns);
            grouper
                .group(data)
                .map_err(|e| Error::Execution(format!("Error forming groups: {}", e)))
        } else {
            let data = data.into_iter().map(|series| series.into()).collect();
            Ok(data)
        }
    }

    /// Executes `plan` and return the resulting FieldList on the query executor
    pub async fn to_field_list(&self, plan: FieldListPlan) -> Result<FieldList> {
        let FieldListPlan {
            known_values,
            extra_plans,
        } = plan;

        // Run the plans in parallel
        let handles = extra_plans
            .into_iter()
            .map(|plan| {
                let ctx = self.child_ctx("to_field_list");
                self.run(async move {
                    let physical_plan = ctx.create_physical_plan(&plan).await?;

                    // TODO: avoid this buffering
                    let field_list =
                        ctx.collect(physical_plan)
                            .await?
                            .into_fieldlist()
                            .map_err(|e| {
                                Error::Execution(format!("Error converting to field list: {}", e))
                            })?;

                    Ok(field_list)
                })
            })
            .collect::<Vec<_>>();

        // collect them all up and combine them
        let mut results = Vec::new();

        if !known_values.is_empty() {
            let list = known_values.into_iter().map(|f| f.1).collect();
            results.push(FieldList { fields: list })
        }

        for join_handle in handles {
            let fieldlist = join_handle.await?;

            results.push(fieldlist);
        }

        // TODO: Stream this
        results
            .into_fieldlist()
            .map_err(|e| Error::Execution(format!("Error converting to field list: {}", e)))
    }

    /// Executes this plan on the query pool, and returns the
    /// resulting set of strings
    pub async fn to_string_set(&self, plan: StringSetPlan) -> Result<StringSetRef> {
        let ctx = self.child_ctx("to_string_set");
        match plan {
            StringSetPlan::Known(ss) => Ok(ss),
            StringSetPlan::Plan(plans) => ctx
                .run_logical_plans(plans)
                .await?
                .into_stringset()
                .map_err(|e| Error::Execution(format!("Error converting to stringset: {}", e))),
        }
    }

    /// Run the plan and return a record batch reader for reading the results
    pub async fn run_logical_plan(&self, plan: LogicalPlan) -> Result<Vec<RecordBatch>> {
        self.run_logical_plans(vec![plan]).await
    }

    /// plans and runs the plans in parallel and collects the results
    /// run each plan in parallel and collect the results
    async fn run_logical_plans(&self, plans: Vec<LogicalPlan>) -> Result<Vec<RecordBatch>> {
        let value_futures = plans
            .into_iter()
            .map(|plan| {
                let ctx = self.child_ctx("run_logical_plans");
                self.run(async move {
                    let physical_plan = ctx.create_physical_plan(&plan).await?;

                    // TODO: avoid this buffering
                    ctx.collect(physical_plan).await
                })
            })
            .collect::<Vec<_>>();

        // now, wait for all the values to resolve and collect them together
        let mut results = Vec::new();
        for join_handle in value_futures {
            let mut plan_result = join_handle.await?;
            results.append(&mut plan_result);
        }
        Ok(results)
    }

    /// Runs the provided future using this execution context
    pub async fn run<Fut, T>(&self, fut: Fut) -> Result<T>
    where
        Fut: std::future::Future<Output = Result<T>> + Send + 'static,
        T: Send + 'static,
    {
        match &self.exec {
            Some(exec) => exec
                .spawn(fut)
                .await
                .unwrap_or_else(|e| Err(Error::Execution(format!("Join Error: {}", e)))),
            None => unimplemented!("spawn onto current threadpool"),
        }
    }

    /// Returns a IOxSessionContext with a SpanRecorder that is a child of the current
    pub fn child_ctx(&self, name: &'static str) -> Self {
        Self::new(
            self.inner.clone(),
            self.exec.clone(),
            self.recorder.child(name),
        )
    }

    /// Record an event on the span recorder
    pub fn record_event(&mut self, name: &'static str) {
        self.recorder.event(name);
    }

    /// Record an event on the span recorder
    pub fn set_metadata(&mut self, name: &'static str, value: impl Into<MetaValue>) {
        self.recorder.set_metadata(name, value);
    }

    /// Returns the current [`Span`] if any
    pub fn span(&self) -> Option<&Span> {
        self.recorder.span()
    }

    /// Number of currently active tasks.
    pub fn tasks(&self) -> usize {
        self.exec.as_ref().map(|e| e.tasks()).unwrap_or_default()
    }
}

/// Extension trait to pull IOx spans out of DataFusion contexts.
pub trait SessionContextIOxExt {
    /// Get child span of the current context.
    fn child_span(&self, name: &'static str) -> Option<Span>;

    /// Get span context
    fn span_ctx(&self) -> Option<SpanContext>;
}

impl SessionContextIOxExt for SessionState {
    fn child_span(&self, name: &'static str) -> Option<Span> {
        self.config
            .get_extension::<Option<Span>>()
            .and_then(|span| span.as_ref().as_ref().map(|span| span.child(name)))
    }

    fn span_ctx(&self) -> Option<SpanContext> {
        self.config
            .get_extension::<Option<Span>>()
            .and_then(|span| span.as_ref().as_ref().map(|span| span.ctx.clone()))
    }
}
