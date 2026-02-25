/// Implementation of a "sleep" operation in DataFusion.
///
/// The sleep operation passes through its input data and sleeps asynchronously for a duration determined by an
/// expression. The async sleep is implemented as a special [execution plan](SleepExpr) so we can perform this as part
/// of the async data stream. In contrast to a UDF, this will NOT block any threads.
use std::{collections::HashSet, sync::Arc, time::Duration};

use arrow::{
    array::{Array, Float32Array, Float64Array, Int64Array},
    datatypes::{DataType, TimeUnit},
};
use datafusion::{
    common::DFSchemaRef,
    error::DataFusionError,
    execution::{TaskContext, context::SessionState},
    logical_expr::{LogicalPlan, UserDefinedLogicalNodeCore},
    physical_plan::{
        DisplayAs, DisplayFormatType, ExecutionPlan, PhysicalExpr, PlanProperties,
        SendableRecordBatchStream, Statistics, stream::RecordBatchStreamAdapter,
    },
    physical_planner::PhysicalPlanner,
    prelude::Expr,
};
use futures::TryStreamExt;

/// Logical plan note that represents a "sleep" operation.
///
/// This will be lowered to [`SleepExpr`].
///
/// See [module](super) docs for more details.
#[derive(Clone, Debug, Hash, PartialEq, Eq, PartialOrd)]
pub struct SleepNode {
    input: LogicalPlan,
    duration: Vec<Expr>,
}

impl SleepNode {
    pub fn new(input: LogicalPlan, duration: Vec<Expr>) -> Self {
        Self { input, duration }
    }

    pub fn plan(
        &self,
        planner: &dyn PhysicalPlanner,
        logical_inputs: &[&LogicalPlan],
        physical_inputs: &[Arc<dyn ExecutionPlan>],
        session_state: &SessionState,
    ) -> Result<SleepExpr, DataFusionError> {
        let duration = self
            .duration
            .iter()
            .map(|e| planner.create_physical_expr(e, logical_inputs[0].schema(), session_state))
            .collect::<Result<Vec<_>, _>>()?;
        Ok(SleepExpr::new(Arc::clone(&physical_inputs[0]), duration))
    }
}

impl UserDefinedLogicalNodeCore for SleepNode {
    fn name(&self) -> &str {
        "Sleep"
    }

    fn inputs(&self) -> Vec<&LogicalPlan> {
        vec![&self.input]
    }

    fn schema(&self) -> &DFSchemaRef {
        self.input.schema()
    }

    fn expressions(&self) -> Vec<Expr> {
        self.duration.clone()
    }

    fn fmt_for_explain(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let duration = self
            .duration
            .iter()
            .map(|e| e.to_string())
            .collect::<Vec<String>>()
            .join(", ");

        write!(f, "{}: duration=[{}]", self.name(), duration)
    }

    fn with_exprs_and_inputs(
        &self,
        exprs: Vec<Expr>,
        inputs: Vec<LogicalPlan>,
    ) -> datafusion::common::Result<Self> {
        Ok(Self::new(inputs[0].clone(), exprs.to_vec()))
    }

    /// Projection pushdown is an optmization that pushes a `Projection` node further down
    /// into the query plan.
    ///
    /// So instead of:
    ///
    /// ```text
    /// Projection: a, b, c
    ///   SleepNode: ...
    ///     TableScan: table
    /// ```
    ///
    /// You will get:
    ///
    /// ```text
    /// SleepNode: ...
    ///   TableScan: table, projection=[a, b, c]
    /// ```
    /// By default, DataFusion will not optimize projection pushdown through a user defined node.
    /// Overriding this trait method allows projection pushdown.
    ///
    /// When `ExtractSleep` was an [`OptimizerRule`] this was not needed, because
    /// the [`SleepNode`] was inserted *after* projection pushdown.
    ///
    /// Now that `ExtractSleep` is an [`AnalyzerRule`], it inserts the [`SleepNode`]
    /// *before* projection pushdown.
    ///
    /// [`OptimizerRule`]: datafusion::optimizer::OptimizerRule
    /// [`AnalyzerRule`]: datafusion::optimizer::AnalyzerRule
    fn necessary_children_exprs(&self, output_columns: &[usize]) -> Option<Vec<Vec<usize>>> {
        Some(vec![Vec::from(output_columns)])
    }

    /// Filter pushdown (or predicate pushdown) is an optmization that pushes a `Filter` node further down
    /// into the query plan.
    ///
    /// So instead of:
    ///
    /// ```text
    /// Filter: predicate
    ///   SleepNode: ...
    ///     TableScan: table
    /// ```
    /// You will get:
    ///
    /// ```text
    /// SleepNode: ...
    ///   TableScan: table, filters=[predicate]
    /// ```
    /// By default, DataFusion will not optimize filter pushdown through a user defined node.
    /// Overriding this trait method allows filter pushdown.
    ///
    /// When `ExtractSleep` was an [`OptimizerRule`] this was not needed, because
    /// the [`SleepNode`] was inserted *after* filter pushdown.
    ///
    /// Now that `ExtractSleep` is an [`AnalyzerRule`], it inserts the [`SleepNode`]
    /// *before* filter pushdown.
    ///
    /// [`OptimizerRule`]: datafusion::optimizer::OptimizerRule
    /// [`AnalyzerRule`]: datafusion::optimizer::AnalyzerRule
    fn prevent_predicate_push_down_columns(&self) -> std::collections::HashSet<String> {
        HashSet::new()
    }
}

/// Physical node that implements a "sleep" operation.
///
/// This was lowered from [`SleepNode`].
///
/// See [module](super) docs for more details.
#[derive(Debug)]
pub struct SleepExpr {
    /// Input data.
    input: Arc<dyn ExecutionPlan>,

    /// Expression that determines the sum of the sleep duration.
    duration: Vec<Arc<dyn PhysicalExpr>>,

    /// Cache holding plan properties like equivalences, output partitioning, output ordering etc.
    cache: PlanProperties,
}

impl SleepExpr {
    pub fn new(input: Arc<dyn ExecutionPlan>, duration: Vec<Arc<dyn PhysicalExpr>>) -> Self {
        let cache = Self::compute_properties(&input);

        Self {
            input,
            duration,
            cache,
        }
    }

    /// This function creates the cache object that stores the plan properties such as equivalence properties, partitioning, ordering, etc.
    fn compute_properties(input: &Arc<dyn ExecutionPlan>) -> PlanProperties {
        input.properties().clone()
    }
}

impl DisplayAs for SleepExpr {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default
            | DisplayFormatType::Verbose
            | DisplayFormatType::TreeRender => {
                let duration = self
                    .duration
                    .iter()
                    .map(|e| e.to_string())
                    .collect::<Vec<String>>()
                    .join(", ");

                write!(f, "Sleep: duration=[{duration}]")
            }
        }
    }
}

impl ExecutionPlan for SleepExpr {
    fn name(&self) -> &str {
        Self::static_name()
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn properties(&self) -> &datafusion::physical_plan::PlanProperties {
        &self.cache
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        assert_eq!(children.len(), 1);

        Ok(Arc::new(Self::new(
            Arc::clone(&children[0]),
            self.duration.clone(),
        )))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> datafusion::error::Result<SendableRecordBatchStream> {
        let stream = self.input.execute(partition, context)?;

        let duration = self.duration.clone();
        let stream = RecordBatchStreamAdapter::new(
            stream.schema(),
            stream.and_then(move |batch| {
                let duration = duration.clone();

                async move {
                    let mut sum = Duration::ZERO;
                    for expr in duration {
                        let array = expr.evaluate(&batch)?.into_array(batch.num_rows())?;
                        let d = array_to_duration(&array)?;
                        if let Some(d) = d {
                            sum += d;
                        }
                    }
                    if !sum.is_zero() {
                        tokio::time::sleep(sum).await;
                    }
                    Ok(batch)
                }
            }),
        );
        Ok(Box::pin(stream))
    }

    fn statistics(&self) -> Result<Statistics, DataFusionError> {
        Ok(Statistics::new_unknown(&self.schema()))
    }
}

fn array_to_duration(array: &dyn Array) -> Result<Option<Duration>, DataFusionError> {
    match array.data_type() {
        DataType::Null => Ok(None),
        DataType::Duration(tunit) => {
            let array = arrow::compute::cast(array, &DataType::Int64)?;
            let array = array
                .as_any()
                .downcast_ref::<Int64Array>()
                .expect("just casted");
            let Some(sum) = arrow::compute::sum(array) else {
                return Ok(None);
            };
            if sum < 0 {
                return Err(DataFusionError::Execution(format!(
                    "duration must be non-negative but is {sum}{tunit:?}"
                )));
            }
            let sum = sum as u64;
            let duration = match tunit {
                TimeUnit::Second => Duration::from_secs(sum),
                TimeUnit::Millisecond => Duration::from_millis(sum),
                TimeUnit::Microsecond => Duration::from_micros(sum),
                TimeUnit::Nanosecond => Duration::from_nanos(sum),
            };
            Ok(Some(duration))
        }
        DataType::Float32 => {
            let array = array
                .as_any()
                .downcast_ref::<Float32Array>()
                .expect("just checked");
            let Some(sum) = arrow::compute::sum(array) else {
                return Ok(None);
            };
            if sum < 0.0 || !sum.is_finite() {
                return Err(DataFusionError::Execution(format!(
                    "duration must be non-negative but is {sum}s"
                )));
            }
            Ok(Some(Duration::from_secs_f32(sum)))
        }
        DataType::Float64 => {
            let array = array
                .as_any()
                .downcast_ref::<Float64Array>()
                .expect("just checked");
            let Some(sum) = arrow::compute::sum(array) else {
                return Ok(None);
            };
            if sum < 0.0 || !sum.is_finite() {
                return Err(DataFusionError::Execution(format!(
                    "duration must be non-negative but is {sum}s"
                )));
            }
            Ok(Some(Duration::from_secs_f64(sum)))
        }
        other => Err(DataFusionError::Internal(format!(
            "Expected duration pattern to sleep(...), got: {other:?}"
        ))),
    }
}
