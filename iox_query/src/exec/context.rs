//! This module contains plumbing to connect InfluxDB IOx extensions to
//! DataFusion

use super::{
    cross_rt_stream::CrossRtStream,
    gapfill::{plan_gap_fill, GapFill},
    non_null_checker::NonNullCheckerNode,
    seriesset::{series::Either, SeriesSet},
    split::StreamSplitNode,
};
use crate::{
    config::IoxConfigExt,
    exec::{
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
    },
    logical_optimizer::register_iox_logical_optimizers,
    physical_optimizer::register_iox_physical_optimizers,
    plan::{
        fieldlist::FieldListPlan,
        seriesset::{SeriesSetPlan, SeriesSetPlans},
        stringset::StringSetPlan,
    },
};
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use datafusion::{
    catalog::CatalogProvider,
    execution::{
        context::{QueryPlanner, SessionState, TaskContext},
        memory_pool::MemoryPool,
        runtime_env::RuntimeEnv,
    },
    logical_expr::{LogicalPlan, UserDefinedLogicalNode},
    physical_plan::{
        coalesce_partitions::CoalescePartitionsExec, displayable, stream::RecordBatchStreamAdapter,
        EmptyRecordBatchStream, ExecutionPlan, RecordBatchStream, SendableRecordBatchStream,
    },
    physical_planner::{DefaultPhysicalPlanner, ExtensionPlanner, PhysicalPlanner},
    prelude::*,
};
use datafusion_util::config::{iox_session_config, DEFAULT_CATALOG};
use executor::DedicatedExecutor;
use futures::{Stream, StreamExt, TryStreamExt};
use observability_deps::tracing::{debug, warn};
use query_functions::{register_scalar_functions, selectors::register_selector_aggregates};
use std::{fmt, num::NonZeroUsize, sync::Arc};
use trace::{
    ctx::SpanContext,
    span::{MetaValue, Span, SpanExt, SpanRecorder},
};

// Reuse DataFusion error and Result types for this module
pub use datafusion::error::{DataFusionError, Result};

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
        } else if let Some(gap_fill) = any.downcast_ref::<GapFill>() {
            let gap_fill_exec = plan_gap_fill(
                session_state.execution_props(),
                gap_fill,
                logical_inputs,
                physical_inputs,
            )?;
            Some(Arc::new(gap_fill_exec) as Arc<dyn ExecutionPlan>)
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

impl IOxSessionConfig {
    pub(super) fn new(exec: DedicatedExecutor, runtime: Arc<RuntimeEnv>) -> Self {
        let mut session_config = iox_session_config();
        session_config
            .options_mut()
            .extensions
            .insert(IoxConfigExt::default());

        Self {
            exec,
            session_config,
            runtime,
            default_catalog: None,
            span_ctx: None,
        }
    }

    /// Set execution concurrency
    pub fn with_target_partitions(mut self, target_partitions: NonZeroUsize) -> Self {
        self.session_config = self
            .session_config
            .with_target_partitions(target_partitions.get());
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

    /// Set DataFusion [config option].
    ///
    /// May be used to set [IOx-specific] option as well.
    ///
    ///
    /// [config option]: datafusion::common::config::ConfigOptions
    /// [IOx-specific]: crate::config::IoxConfigExt
    pub fn with_config_option(mut self, key: &str, value: &str) -> Self {
        // ignore invalid config
        if let Err(e) = self.session_config.options_mut().set(key, value) {
            warn!(
                key,
                value,
                %e,
                "invalid DataFusion config",
            );
        }
        self
    }

    /// Create an ExecutionContext suitable for executing DataFusion plans
    pub fn build(self) -> IOxSessionContext {
        let maybe_span = self.span_ctx.child_span("Query Execution");
        let recorder = SpanRecorder::new(maybe_span);

        // attach span to DataFusion session
        let session_config = self
            .session_config
            .with_extension(Arc::new(recorder.span().cloned()));

        let state = SessionState::with_config_rt(session_config, self.runtime)
            .with_query_planner(Arc::new(IOxQueryPlanner {}));
        let state = register_iox_physical_optimizers(state);
        let state = register_iox_logical_optimizers(state);

        let inner = SessionContext::with_state(state);
        register_selector_aggregates(&inner);
        register_scalar_functions(&inner);
        if let Some(default_catalog) = self.default_catalog {
            inner.register_catalog(DEFAULT_CATALOG, default_catalog);
        }

        IOxSessionContext::new(inner, self.exec, recorder)
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

    /// Dedicated executor for query execution.
    ///
    /// DataFusion plans are "CPU" bound and thus can consume tokio
    /// executors threads for extended periods of time. We use a
    /// dedicated tokio runtime to run them so that other requests
    /// can be handled.
    exec: DedicatedExecutor,

    /// Span context from which to create spans for this query
    recorder: SpanRecorder,
}

impl fmt::Debug for IOxSessionContext {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("IOxSessionContext")
            .field("inner", &"<DataFusion ExecutionContext>")
            .field("exec", &self.exec)
            .field("recorder", &self.recorder)
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
            exec: DedicatedExecutor::new_testing(),
            recorder: SpanRecorder::default(),
        }
    }

    /// Private constructor
    pub(crate) fn new(
        inner: SessionContext,
        exec: DedicatedExecutor,
        recorder: SpanRecorder,
    ) -> Self {
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

    /// Plan a SQL statement. This assumes that any tables referenced
    /// in the SQL have been registered with this context. Use
    /// `create_physical_plan` to actually execute the query.
    pub async fn sql_to_logical_plan(&self, sql: &str) -> Result<LogicalPlan> {
        let ctx = self.child_ctx("sql_to_logical_plan");
        debug!(text=%sql, "planning SQL query");
        let plan = ctx.inner.state().create_logical_plan(sql).await?;
        // ensure the plan does not contain unwanted statements
        let verifier = SQLOptions::new()
            .with_allow_ddl(false) // no CREATE ...
            .with_allow_dml(false) // no INSERT or COPY
            .with_allow_statements(false); // no SET VARIABLE, etc
        verifier.verify_plan(&plan)?;
        Ok(plan)
    }

    /// Create a logical plan that reads a single [`RecordBatch`]. Use
    /// `create_physical_plan` to actually execute the query.
    pub fn batch_to_logical_plan(&self, batch: RecordBatch) -> Result<LogicalPlan> {
        let ctx = self.child_ctx("batch_to_logical_plan");
        debug!(num_rows = batch.num_rows(), "planning RecordBatch query");
        ctx.inner.read_batch(batch)?.into_optimized_plan()
    }

    /// Plan a SQL statement and convert it to an execution plan. This assumes that any
    /// tables referenced in the SQL have been registered with this context
    pub async fn sql_to_physical_plan(&self, sql: &str) -> Result<Arc<dyn ExecutionPlan>> {
        let logical_plan = self.sql_to_logical_plan(sql).await?;

        let ctx = self.child_ctx("sql_to_physical_plan");
        ctx.create_physical_plan(&logical_plan).await
    }

    /// Prepare (optimize + plan) a pre-created [`LogicalPlan`] for execution
    pub async fn create_physical_plan(
        &self,
        logical_plan: &LogicalPlan,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let mut ctx = self.child_ctx("create_physical_plan");
        debug!(text=%logical_plan.display_indent_schema(), "create_physical_plan: initial plan");
        let physical_plan = ctx.inner.state().create_physical_plan(logical_plan).await?;

        ctx.recorder.event("physical plan");
        debug!(text=%displayable(physical_plan.as_ref()).indent(false), "create_physical_plan: plan to run");
        Ok(physical_plan)
    }

    /// Executes the logical plan using DataFusion on a separate
    /// thread pool and produces RecordBatches
    pub async fn collect(&self, physical_plan: Arc<dyn ExecutionPlan>) -> Result<Vec<RecordBatch>> {
        debug!(
            "Running plan, physical:\n{}",
            displayable(physical_plan.as_ref()).indent(false)
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

        let stream = self
            .run(async move {
                let stream = physical_plan.execute(partition, task_context)?;
                Ok(TracedStream::new(stream, span, physical_plan))
            })
            .await?;
        // Wrap the resulting stream into `CrossRtStream`. This is required because polling the DataFusion result stream
        // actually drives the (potentially CPU-bound) work. We need to make sure that this work stays within the
        // dedicated executor because otherwise this may block the top-level tokio/tonic runtime which may lead to
        // requests timetouts (either for new requests, metrics or even for HTTP2 pings on the active connection).
        let schema = stream.schema();
        let stream = CrossRtStream::new_with_df_error_stream(stream, self.exec.clone());
        let stream = RecordBatchStreamAdapter::new(schema, stream);
        Ok(Box::pin(stream))
    }

    /// Executes the SeriesSetPlans on the query executor, in
    /// parallel, producing series or groups
    pub async fn to_series_and_groups(
        &self,
        series_set_plans: SeriesSetPlans,
        memory_pool: Arc<dyn MemoryPool>,
        points_per_batch: usize,
    ) -> Result<impl Stream<Item = Result<Either>>> {
        let SeriesSetPlans {
            mut plans,
            group_columns,
        } = series_set_plans;

        if plans.is_empty() {
            return Ok(futures::stream::empty().boxed());
        }

        // sort plans by table (measurement) name
        plans.sort_by(|a, b| a.table_name.cmp(&b.table_name));

        // Run the plans in parallel
        let ctx = self.child_ctx("to_series_set");
        let exec = self.exec.clone();
        let data = futures::stream::iter(plans)
            .then(move |plan| {
                let ctx = ctx.child_ctx("for plan");
                let exec = exec.clone();

                async move {
                    let stream = Self::run_inner(exec.clone(), async move {
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
                    })
                    .await?;

                    Ok::<_, DataFusionError>(CrossRtStream::new_with_df_error_stream(stream, exec))
                }
            })
            .try_flatten()
            .try_filter_map(move |series_set: SeriesSet| async move {
                // If all timestamps of returned columns are nulls,
                // there must be no data. We need to check this because
                // aggregate (e.g. count, min, max) returns one row that are
                // all null (even the values of aggregate) for min, max and 0 for count.
                // For influx read_group's series and group, we do not want to return 0
                // for count either.
                if series_set.is_timestamp_all_null() {
                    return Ok(None);
                }

                let series: Vec<Series> =
                    series_set.try_into_series(points_per_batch).map_err(|e| {
                        DataFusionError::Execution(format!("Error converting to series: {e}"))
                    })?;
                Ok(Some(futures::stream::iter(series).map(Ok)))
            })
            .try_flatten();

        // If we have group columns, sort the results, and create the
        // appropriate groups
        if let Some(group_columns) = group_columns {
            let grouper = GroupGenerator::new(group_columns, memory_pool);
            Ok(grouper.group(data).await?.boxed())
        } else {
            Ok(data.map_ok(|series| series.into()).boxed())
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
                                DataFusionError::Context(
                                    "Error converting to field list".to_string(),
                                    Box::new(DataFusionError::External(Box::new(e))),
                                )
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
        results.into_fieldlist().map_err(|e| {
            DataFusionError::Context(
                "Error converting to field list".to_string(),
                Box::new(DataFusionError::External(Box::new(e))),
            )
        })
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
                .map_err(|e| {
                    DataFusionError::Context(
                        "Error converting to stringset".to_string(),
                        Box::new(DataFusionError::External(Box::new(e))),
                    )
                }),
        }
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
        Self::run_inner(self.exec.clone(), fut).await
    }

    async fn run_inner<Fut, T>(exec: DedicatedExecutor, fut: Fut) -> Result<T>
    where
        Fut: std::future::Future<Output = Result<T>> + Send + 'static,
        T: Send + 'static,
    {
        exec.spawn(fut).await.unwrap_or_else(|e| {
            Err(DataFusionError::Context(
                "Join Error".to_string(),
                Box::new(DataFusionError::External(Box::new(e))),
            ))
        })
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

    /// Returns a new child span of the current context
    pub fn child_span(&self, name: &'static str) -> Option<Span> {
        self.recorder.child_span(name)
    }

    /// Number of currently active tasks.
    pub fn tasks(&self) -> usize {
        self.exec.tasks()
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
        self.config()
            .get_extension::<Option<Span>>()
            .and_then(|span| span.as_ref().as_ref().map(|span| span.child(name)))
    }

    fn span_ctx(&self) -> Option<SpanContext> {
        self.config()
            .get_extension::<Option<Span>>()
            .and_then(|span| span.as_ref().as_ref().map(|span| span.ctx.clone()))
    }
}
