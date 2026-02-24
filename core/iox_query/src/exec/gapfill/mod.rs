//! This module contains code that implements
//! a gap-filling extension to DataFusion

mod algo;
mod buffered_input;
mod date_bin_gap_expander;
mod date_bin_wallclock_gap_expander;
#[cfg(test)]
mod exec_tests;
mod gap_expander;
mod params;
mod stream;

use self::stream::GapFillStream;
use arrow::datatypes::Schema;
use arrow::{compute::SortOptions, datatypes::SchemaRef};
use datafusion::common::{DFSchema, plan_datafusion_err};
use datafusion::logical_expr::ExprSchemable;
use datafusion::physical_expr::{LexOrdering, OrderingRequirements, ScalarFunctionExpr};
use datafusion::physical_plan::metrics::MetricsSet;
use datafusion::{
    common::DFSchemaRef,
    error::{DataFusionError, Result},
    execution::{
        context::{SessionState, TaskContext},
        memory_pool::MemoryConsumer,
    },
    logical_expr::{LogicalPlan, UserDefinedLogicalNodeCore},
    physical_expr::{EquivalenceProperties, PhysicalSortExpr},
    physical_plan::{
        DisplayAs, DisplayFormatType, Distribution, ExecutionPlan, ExecutionPlanProperties,
        PhysicalExpr, PlanProperties, SendableRecordBatchStream, Statistics,
        coop::cooperative,
        metrics::{BaselineMetrics, ExecutionPlanMetricsSet},
    },
    prelude::Expr,
    scalar::ScalarValue,
};
use datafusion_util::ThenWithOpt;
pub use gap_expander::{ExpandedValue, GapExpander};
use std::cmp::Ordering;
use std::{
    convert::Infallible,
    fmt::{self, Debug},
    ops::{Bound, Range},
    sync::Arc,
};

/// A logical node that represents the gap filling operation.
#[derive(Clone, Debug, Hash, PartialEq, Eq)]
pub struct GapFill {
    /// The incoming logical plan
    pub input: Arc<LogicalPlan>,
    /// Series expressions
    pub series_expr: Vec<Expr>,
    /// Time binning expr
    pub time_expr: Expr,
    /// Filling expressions
    pub fill_expr: Vec<FillExpr>,
    /// The time range of the time column inferred from predicates
    /// in the overall query. The lower bound may be [`Bound::Unbounded`]
    /// which implies that gap-filling should just start from the
    /// first point in each series.
    pub time_range: Range<Bound<Expr>>,
    /// The schema after the gap-fill operation
    pub schema: DFSchemaRef,
}

// Manual impl because GapFillParams has a Range and is not PartialOrd
impl PartialOrd for GapFill {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        other
            .input
            .partial_cmp(&self.input)
            .then_with_opt(|| self.series_expr.partial_cmp(&other.series_expr))
            .then_with_opt(|| self.fill_expr.partial_cmp(&other.fill_expr))
    }
}

#[derive(Clone, Debug, Hash, PartialEq, Eq, PartialOrd)]
pub struct FillExpr {
    pub expr: Expr,
    pub strategy: FillStrategy,
}

impl std::fmt::Display for FillExpr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.strategy.display_with_expr(&self.expr))
    }
}

/// Describes how to fill gaps in an aggregate column.
#[derive(Clone, Debug, Hash, PartialEq, Eq, PartialOrd)]
pub enum FillStrategy {
    /// Fill with the given 'default value' - this includes a default value of Null.
    Default(ScalarValue),
    /// Fill with the most recent value in the input column.
    /// Null values in the input are preserved.
    PrevNullAsIntentional,
    /// Fill with the most recent non-null value in the input column.
    /// This is the InfluxQL behavior for `FILL(PREVIOUS)`.
    PrevNullAsMissing,
    /// Fill the gaps between points linearly.
    /// Null values will not be considered as missing, so two non-null values
    /// with a null in between will not be filled.
    LinearInterpolate,
}

impl FillStrategy {
    // used with both `Expr` and `PhysicalExpr`, normally to display this within `explain` queries
    fn display_with_expr(&self, expr: &impl fmt::Display) -> String {
        match self {
            Self::PrevNullAsIntentional => format!("LOCF(null-as-intentional, {expr})"),
            Self::PrevNullAsMissing => format!("LOCF({expr})"),
            Self::LinearInterpolate => format!("INTERPOLATE({expr})"),
            Self::Default(scalar) if scalar.is_null() => expr.to_string(),
            Self::Default(val) => format!("COALESCE({expr}, {val})"),
        }
    }
}

impl GapFill {
    /// Create a new gap-filling operator.
    pub fn try_new(
        input: Arc<LogicalPlan>,
        series_expr: Vec<Expr>,
        time_expr: Expr,
        fill_expr: Vec<FillExpr>,
        time_range: Range<Bound<Expr>>,
    ) -> Result<Self> {
        let (time_alias, time_col) = {
            let (time_alias, time_expr) = if let Expr::Alias(alias) = &time_expr {
                (Some(alias.name.clone()), alias.expr.as_ref())
            } else {
                (None, &time_expr)
            };
            let Expr::ScalarFunction(time_func) = time_expr else {
                return Err(DataFusionError::Internal(
                    "GapFill time expression must be a ScalarFunctionExpr".to_string(),
                ));
            };
            let time_col = time_func.args.get(1).ok_or_else(|| {
                DataFusionError::Internal(
                    "GapFill time expression must have at least two arguments".to_string(),
                )
            })?;
            (time_alias, time_col.clone())
        };

        if time_range.end == Bound::Unbounded {
            return Err(DataFusionError::Internal(
                "missing upper bound in GapFill time range".to_string(),
            ));
        }

        let time_schema_expr = if let Some(alias) = &time_alias {
            time_col.alias(alias)
        } else {
            time_col
        };

        let fields = series_expr
            .iter()
            .chain(std::iter::once(&time_schema_expr))
            .chain(fill_expr.iter().map(|fe| &fe.expr))
            .map(|expr| expr.to_field(input.schema().as_ref()))
            .collect::<Result<Vec<_>>>()?;

        let schema = Arc::new(DFSchema::new_with_metadata(
            fields,
            input.schema().metadata().clone(),
        )?);

        Ok(Self {
            input,
            series_expr,
            time_expr,
            fill_expr,
            time_range,
            schema,
        })
    }

    // Find the expression that matches `e` and replace its fill strategy.
    // If such an expression is found, return the old strategy, and `None` otherwise.
    pub(crate) fn replace_fill_strategy(
        &mut self,
        e: &Expr,
        mut fs: FillStrategy,
    ) -> Option<FillStrategy> {
        for fe in &mut self.fill_expr {
            if &fe.expr == e {
                std::mem::swap(&mut fe.strategy, &mut fs);
                return Some(fs);
            }
        }
        None
    }
}

impl UserDefinedLogicalNodeCore for GapFill {
    fn name(&self) -> &str {
        "GapFill"
    }

    fn inputs(&self) -> Vec<&LogicalPlan> {
        vec![self.input.as_ref()]
    }

    fn schema(&self) -> &DFSchemaRef {
        &self.schema
    }

    fn expressions(&self) -> Vec<Expr> {
        let mut exprs = Vec::with_capacity(self.series_expr.len() + 1 + self.fill_expr.len() + 2);
        for e in &self.series_expr {
            exprs.push(e.clone());
        }
        exprs.push(self.time_expr.clone());
        for fe in &self.fill_expr {
            exprs.push(fe.expr.clone());
        }
        if let Some(start) = bound_extract(&self.time_range.start) {
            exprs.push(start.clone());
        }
        exprs.push(
            bound_extract(&self.time_range.end)
                .unwrap_or_else(|| panic!("upper time bound is required"))
                .clone(),
        );
        exprs
    }

    fn fmt_for_explain(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let fill_expr: String = self
            .fill_expr
            .iter()
            .map(|e| e.to_string())
            .collect::<Vec<String>>()
            .join(", ");

        let series_expr = self
            .series_expr
            .iter()
            .map(|e| e.to_string())
            .collect::<Vec<String>>()
            .join(", ");

        write!(
            f,
            "{}: series=[{series_expr}], time={}, fill=[{fill_expr}], range={:?}",
            self.name(),
            self.time_expr,
            self.time_range,
        )
    }

    fn with_exprs_and_inputs(
        &self,
        mut series_expr: Vec<Expr>,
        inputs: Vec<LogicalPlan>,
    ) -> Result<Self> {
        let plan = inputs[0].clone();
        let mut fill_expr = series_expr.split_off(self.series_expr.len() + 1);
        let time_expr = series_expr
            .pop()
            .expect("there should be at least one series expr (the time expr)");
        let mut e_iter = fill_expr.split_off(self.fill_expr.len()).into_iter();
        let time_range = match try_map_range(&self.time_range, |b| {
            try_map_bound(b.as_ref(), |_| {
                Ok::<_, Infallible>(e_iter.next().expect("expr count should match template"))
            })
        }) {
            Ok(tr) => tr,
            Err(infallible) => match infallible {},
        };

        let fill_expr = fill_expr
            .into_iter()
            .zip(self.fill_expr.iter().map(|fe| fe.strategy.clone()))
            .map(|(e, fs)| FillExpr {
                expr: e,
                strategy: fs,
            })
            .collect();
        Self::try_new(
            Arc::new(plan),
            series_expr,
            time_expr,
            fill_expr,
            time_range,
        )
    }

    /// Projection pushdown is an optmization that pushes a `Projection` node further down
    /// into the query plan.
    ///
    /// So instead of:
    ///
    /// ```text
    /// Projection: a, b, c
    ///   GapFill: ...
    ///     Aggregate: ...
    ///       TableScan: table
    /// ```
    ///
    /// You will get:
    ///
    /// ```text
    /// GapFill: ...
    ///   Aggregate: ...
    ///     TableScan: table, projection=[a, b, c]
    /// ```
    /// By default, DataFusion will not optimize projection pushdown through a user defined node.
    /// Overriding this trait method allows projection pushdown.
    ///
    /// When `HandleGapfill` was an [`OptimizerRule`] this was not needed, because
    /// the [`GapFill`] node was inserted *after* projection pushdown.
    ///
    /// Now that `HandleGapFill` is an [`AnalyzerRule`], it inserts the [`GapFill`] node
    /// *before* projection pushdown.
    ///
    /// [`OptimizerRule`]: datafusion::optimizer::OptimizerRule
    /// [`AnalyzerRule`]: datafusion::optimizer::AnalyzerRule
    fn necessary_children_exprs(&self, output_columns: &[usize]) -> Option<Vec<Vec<usize>>> {
        Some(vec![Vec::from(output_columns)])
    }
}

/// Called by the extension planner to plan a [GapFill] node.
pub(crate) fn plan_gap_fill(
    session_state: &SessionState,
    gap_fill: &GapFill,
    logical_inputs: &[&LogicalPlan],
    physical_inputs: &[Arc<dyn ExecutionPlan>],
) -> Result<GapFillExec> {
    let input_dfschema = match logical_inputs {
        [input] => input.schema().as_ref(),
        _ => {
            return Err(DataFusionError::Internal(format!(
                "GapFillExec: wrong number of logical inputs; expect 1, found {}",
                logical_inputs.len()
            )));
        }
    };

    let phys_input = match physical_inputs {
        [input] => input,
        _ => {
            return Err(DataFusionError::Internal(format!(
                "GapFillExec: wrong number of physical inputs; expected 1, found {}",
                physical_inputs.len()
            )));
        }
    };

    let series_expr = gap_fill
        .series_expr
        .iter()
        .map(|expr| session_state.create_physical_expr(expr.clone(), input_dfschema))
        .collect::<Result<Vec<_>>>()?;
    let time_expr =
        session_state.create_physical_expr(gap_fill.time_expr.clone(), input_dfschema)?;
    let time_range = try_map_range(&gap_fill.time_range, |b| {
        try_map_bound(b.as_ref(), |e| {
            session_state.create_physical_expr(e.clone(), input_dfschema)
        })
    })?;

    let fill_expr = gap_fill
        .fill_expr
        .iter()
        .map(|fe| {
            Ok(PhysicalFillExpr {
                expr: session_state.create_physical_expr(fe.expr.clone(), input_dfschema)?,
                strategy: fe.strategy.clone(),
            })
        })
        .collect::<Result<Vec<_>>>()?;

    GapFillExec::try_new(
        Arc::clone(phys_input),
        series_expr,
        time_expr,
        fill_expr,
        time_range,
    )
}

fn try_map_range<T, U, E, F>(tr: &Range<T>, mut f: F) -> Result<Range<U>, E>
where
    F: FnMut(&T) -> Result<U, E>,
{
    Ok(Range {
        start: f(&tr.start)?,
        end: f(&tr.end)?,
    })
}

fn try_map_bound<T, U, E, F>(bt: Bound<T>, mut f: F) -> Result<Bound<U>, E>
where
    F: FnMut(T) -> Result<U, E>,
{
    Ok(match bt {
        Bound::Excluded(t) => Bound::Excluded(f(t)?),
        Bound::Included(t) => Bound::Included(f(t)?),
        Bound::Unbounded => Bound::Unbounded,
    })
}

fn bound_extract<T>(b: &Bound<T>) -> Option<&T> {
    match b {
        Bound::Included(t) | Bound::Excluded(t) => Some(t),
        Bound::Unbounded => None,
    }
}

/// A physical node for the gap-fill operation.
pub struct GapFillExec {
    input: Arc<dyn ExecutionPlan>,
    // Expressions which separate the time-series that are being filled.
    series_expr: Vec<Arc<dyn PhysicalExpr>>,
    /// The time expression within the series.
    time_expr: Arc<dyn PhysicalExpr>,
    /// Expressions the describe how values are filled.
    fill_expr: Vec<PhysicalFillExpr>,
    /// The output schema.
    schema: SchemaRef,
    // The sort expressions for the required sort order of the input:
    // all of the group exressions, with the time column being last.
    sort_expr: LexOrdering,
    /// The time range of source input to DATE_BIN_GAPFILL.
    /// Inferred from predicates in the overall query.
    time_range: Range<Bound<Arc<dyn PhysicalExpr>>>,
    /// Metrics reporting behavior during execution.
    metrics: ExecutionPlanMetricsSet,
    /// Cache holding plan properties like equivalences, output partitioning, output ordering etc.
    cache: PlanProperties,
}

impl GapFillExec {
    fn try_new(
        input: Arc<dyn ExecutionPlan>,
        series_expr: Vec<Arc<dyn PhysicalExpr>>,
        time_expr: Arc<dyn PhysicalExpr>,
        fill_expr: Vec<PhysicalFillExpr>,
        time_range: Range<Bound<Arc<dyn PhysicalExpr>>>,
    ) -> Result<Self> {
        let time_col = {
            let Some(time_func) = time_expr.as_any().downcast_ref::<ScalarFunctionExpr>() else {
                return Err(DataFusionError::Internal(format!(
                    "GapFill time expression must be a ScalarFunctionExpr: {}",
                    time_expr
                )));
            };
            let Some(time_col) = time_func.args().get(1) else {
                return Err(DataFusionError::Internal(format!(
                    "GapFill time expression must have at least two arguments: {}",
                    time_expr
                )));
            };

            Arc::clone(time_col)
        };
        let sort_expr = {
            let sort_expr: Vec<_> = series_expr
                .iter()
                .map(|expr| PhysicalSortExpr {
                    expr: Arc::clone(expr),
                    options: SortOptions::default(),
                })
                // Add the time input as the lowest priority sort key.
                .chain(std::iter::once(PhysicalSortExpr {
                    expr: Arc::clone(&time_col),
                    options: SortOptions::default(),
                }))
                .collect();

            LexOrdering::new(sort_expr)
                .ok_or_else(|| plan_datafusion_err!("GapFill sort key empty"))?
        };

        let input_schema = input.schema();
        let fields = series_expr
            .iter()
            .chain(std::iter::once(&time_col))
            .chain(fill_expr.iter().map(|fe| &fe.expr))
            .map(|expr| expr.return_field(&input_schema))
            .collect::<Result<Vec<_>>>()?;
        let schema = Arc::new(Schema::new_with_metadata(
            fields,
            input_schema.metadata().clone(),
        ));

        let cache = Self::compute_properties(&input, Arc::clone(&schema));

        Ok(Self {
            input,
            series_expr,
            time_expr,
            fill_expr,
            schema,
            sort_expr,
            time_range,
            metrics: ExecutionPlanMetricsSet::new(),
            cache,
        })
    }

    /// This function creates the cache object that stores the plan properties such as equivalence properties, partitioning, ordering, etc.
    fn compute_properties(input: &Arc<dyn ExecutionPlan>, schema: SchemaRef) -> PlanProperties {
        let eq_properties = match input.properties().output_ordering() {
            None => EquivalenceProperties::new(schema),
            Some(output_ordering) => EquivalenceProperties::new_with_orderings(
                schema,
                std::iter::once(output_ordering.iter().cloned()),
            ),
        };

        PlanProperties::new(
            eq_properties,
            input.properties().output_partitioning().clone(),
            input.pipeline_behavior(),
            input.boundedness(),
        )
    }
}

impl Debug for GapFillExec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "GapFillExec")
    }
}

impl ExecutionPlan for GapFillExec {
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

    fn required_input_distribution(&self) -> Vec<Distribution> {
        vec![if self.series_expr.is_empty() {
            // If there are no series expressions then the input is a
            // single time series. There is no advantage to partitioning
            // in that case.
            Distribution::SinglePartition
        } else {
            Distribution::HashPartitioned(self.series_expr.clone())
        }]
    }

    fn required_input_ordering(&self) -> Vec<Option<OrderingRequirements>> {
        vec![Some(OrderingRequirements::new(
            self.sort_expr.clone().into(),
        ))]
    }

    fn maintains_input_order(&self) -> Vec<bool> {
        vec![true]
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        match children.as_slice() {
            [child] => Ok(Arc::new(Self::try_new(
                Arc::clone(child),
                self.series_expr.clone(),
                Arc::clone(&self.time_expr),
                self.fill_expr.clone(),
                self.time_range.clone(),
            )?)),
            _ => Err(DataFusionError::Internal(format!(
                "GapFillExec wrong number of children: expected 1, found {}",
                children.len()
            ))),
        }
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        if partition
            >= self
                .input
                .properties()
                .output_partitioning()
                .partition_count()
        {
            return Err(DataFusionError::Internal(format!(
                "GapFillExec invalid partition {partition}"
            )));
        }

        let baseline_metrics = BaselineMetrics::new(&self.metrics, partition);
        let output_batch_size = context.session_config().batch_size();
        let config_options = Arc::clone(context.session_config().options());
        let reservation = MemoryConsumer::new(format!("GapFillExec[{partition}]"))
            .register(context.memory_pool());
        let input_stream = self.input.execute(partition, context)?;
        let stream = GapFillStream::try_new(
            self,
            output_batch_size,
            input_stream,
            reservation,
            baseline_metrics,
            config_options,
        )?;

        Ok(Box::pin(cooperative(stream)))
    }

    fn statistics(&self) -> Result<Statistics> {
        Ok(Statistics::new_unknown(&self.schema()))
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }
}

impl DisplayAs for GapFillExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match t {
            DisplayFormatType::Default
            | DisplayFormatType::Verbose
            | DisplayFormatType::TreeRender => {
                let series_expr: Vec<_> = self.series_expr.iter().map(|e| e.to_string()).collect();
                let fill_expr: Vec<_> = self
                    .fill_expr
                    .iter()
                    .map(
                        |PhysicalFillExpr {
                             expr: e,
                             strategy: fs,
                         }| fs.display_with_expr(e),
                    )
                    .collect();

                let time_range = match try_map_range(&self.time_range, |b| {
                    try_map_bound(b.as_ref(), |e| Ok::<_, Infallible>(e.to_string()))
                }) {
                    Ok(tr) => tr,
                    Err(infallible) => match infallible {},
                };

                write!(
                    f,
                    "GapFillExec: series_expr=[{}], time_expr={}, fill_expr=[{}], time_range={:?}",
                    series_expr.join(", "),
                    self.time_expr,
                    fill_expr.join(", "),
                    time_range
                )
            }
        }
    }
}

/// A physical expression that represents a fill operation.
#[derive(Debug, Clone)]
pub struct PhysicalFillExpr {
    pub expr: Arc<dyn PhysicalExpr>,
    pub strategy: FillStrategy,
}

impl std::fmt::Display for PhysicalFillExpr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.strategy.display_with_expr(&self.expr))
    }
}

#[cfg(test)]
mod test {
    use std::ops::{Bound, Range};

    use crate::{
        exec::Executor,
        test::{format_execution_plan, format_logical_plan},
    };

    use super::*;
    use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
    use datafusion::{
        common::DFSchema,
        datasource::empty::EmptyTable,
        error::Result,
        logical_expr::{ExprSchemable, Extension, logical_plan},
        prelude::{col, date_bin, lit},
        scalar::ScalarValue,
    };
    use datafusion_util::lit_timestamptz_nano;

    use test_helpers::assert_error;

    fn schema() -> Schema {
        Schema::new(vec![
            Field::new(
                "time",
                DataType::Timestamp(TimeUnit::Nanosecond, None),
                false,
            ),
            Field::new("loc", DataType::Utf8, false),
            Field::new("temp", DataType::Float64, false),
        ])
    }

    fn table_scan() -> Result<LogicalPlan> {
        let schema = schema();
        logical_plan::table_scan(Some("temps"), &schema, None)?.build()
    }

    fn fill_strategy_null(cols: Vec<Expr>, schema: &DFSchema) -> Vec<FillExpr> {
        cols.into_iter()
            .map(|e| {
                e.get_type(schema)
                    .and_then(|dt| dt.try_into())
                    .map(|null| FillExpr {
                        expr: e,
                        strategy: FillStrategy::Default(null),
                    })
            })
            .collect::<Result<Vec<_>>>()
            .unwrap()
    }

    #[test]
    fn test_try_new_errs() {
        let scan = table_scan().unwrap();
        let schema = Arc::clone(scan.schema());
        let result = GapFill::try_new(
            Arc::new(scan),
            vec![col("loc")],
            date_bin(
                lit(ScalarValue::new_interval_dt(0, 60_000)),
                col("time"),
                lit_timestamptz_nano(0),
            ),
            fill_strategy_null(vec![col("temp")], schema.as_ref()),
            Range {
                start: Bound::Included(lit_timestamptz_nano(1000)),
                end: Bound::Unbounded,
            },
        );

        assert_error!(result, DataFusionError::Internal(ref msg) if msg == "missing upper bound in GapFill time range");
    }

    #[test]
    fn fmt_logical_plan() -> Result<()> {
        // This test case does not make much sense but
        // just verifies we can construct a logical gapfill node
        // and show its plan.
        let scan = table_scan()?;
        let schema = Arc::clone(scan.schema());
        let gapfill = GapFill::try_new(
            Arc::new(scan),
            vec![col("loc")],
            date_bin(
                lit(ScalarValue::new_interval_dt(0, 60_000)),
                col("time"),
                lit_timestamptz_nano(0),
            ),
            fill_strategy_null(vec![col("temp")], &schema),
            Range {
                start: Bound::Included(lit_timestamptz_nano(1000)),
                end: Bound::Excluded(lit_timestamptz_nano(2000)),
            },
        )?;
        let plan = LogicalPlan::Extension(Extension {
            node: Arc::new(gapfill),
        });

        insta::assert_yaml_snapshot!(
            format_logical_plan(&plan),
            @r#"
        - " GapFill: series=[loc], time=date_bin(IntervalDayTime(\"IntervalDayTime { days: 0, milliseconds: 60000 }\"), time, TimestampNanosecond(0, None)), fill=[temp], range=Included(Literal(TimestampNanosecond(1000, None), None))..Excluded(Literal(TimestampNanosecond(2000, None), None))"
        - "   TableScan: temps"
        "#
        );
        Ok(())
    }

    async fn format_explain(sql: &str) -> Result<Vec<String>> {
        let executor = Executor::new_testing();
        let context = executor.new_context();
        context
            .inner()
            .register_table("temps", Arc::new(EmptyTable::new(Arc::new(schema()))))?;
        let physical_plan = context.sql_to_physical_plan(sql).await?;
        Ok(format_execution_plan(&physical_plan))
    }

    #[tokio::test]
    async fn plan_gap_fill() -> Result<()> {
        // show that the optimizer rule can fire and that physical
        // planning will succeed.
        let sql = "SELECT date_bin_gapfill(interval '1 minute', time, timestamp '1970-01-01T00:00:00Z') AS minute, avg(temp)\
                   \nFROM temps\
                   \nWHERE time >= '1980-01-01T00:00:00Z' and time < '1981-01-01T00:00:00Z'\
                   \nGROUP BY minute;";

        let explain = format_explain(sql).await?;
        insta::assert_yaml_snapshot!(
            explain,
            @r#"
        - " ProjectionExec: expr=[date_bin_gapfill(IntervalMonthDayNano(\"IntervalMonthDayNano { months: 0, days: 0, nanoseconds: 60000000000 }\"),temps.time,Utf8(\"1970-01-01T00:00:00Z\"))@0 as minute, avg(temps.temp)@1 as avg(temps.temp)]"
        - "   GapFillExec: series_expr=[], time_expr=date_bin(IntervalMonthDayNano { months: 0, days: 0, nanoseconds: 60000000000 }, date_bin_gapfill(IntervalMonthDayNano(\"IntervalMonthDayNano { months: 0, days: 0, nanoseconds: 60000000000 }\"),temps.time,Utf8(\"1970-01-01T00:00:00Z\"))@0, 0), fill_expr=[avg(temps.temp)@1], time_range=Included(\"315532800000000000\")..Excluded(\"347155200000000000\")"
        - "     SortExec: expr=[date_bin_gapfill(IntervalMonthDayNano(\"IntervalMonthDayNano { months: 0, days: 0, nanoseconds: 60000000000 }\"),temps.time,Utf8(\"1970-01-01T00:00:00Z\"))@0 ASC], preserve_partitioning=[false]"
        - "       AggregateExec: mode=Single, gby=[date_bin(IntervalMonthDayNano { months: 0, days: 0, nanoseconds: 60000000000 }, time@0, 0) as date_bin_gapfill(IntervalMonthDayNano(\"IntervalMonthDayNano { months: 0, days: 0, nanoseconds: 60000000000 }\"),temps.time,Utf8(\"1970-01-01T00:00:00Z\"))], aggr=[avg(temps.temp)]"
        - "         EmptyExec"
        "#
        );
        Ok(())
    }

    #[tokio::test]
    async fn gap_fill_exec_sort_order() -> Result<()> {
        // The call to `date_bin_gapfill` should be last in the SortExec
        // expressions, even though it was not last on the SELECT list
        // or the GROUP BY clause.
        let sql = "SELECT \
           \n  loc,\
           \n  date_bin_gapfill(interval '1 minute', time, timestamp '1970-01-01T00:00:00Z') AS minute,\
           \n  concat('zz', loc) AS loczz,\
           \n  avg(temp)\
           \nFROM temps\
           \nWHERE time >= '1980-01-01T00:00:00Z' and time < '1981-01-01T00:00:00Z'
           \nGROUP BY loc, minute, loczz;";

        let explain = format_explain(sql).await?;
        insta::assert_yaml_snapshot!(
            explain,
            @r#"
        - " ProjectionExec: expr=[loc@0 as loc, date_bin_gapfill(IntervalMonthDayNano(\"IntervalMonthDayNano { months: 0, days: 0, nanoseconds: 60000000000 }\"),temps.time,Utf8(\"1970-01-01T00:00:00Z\"))@2 as minute, concat(Utf8(\"zz\"),temps.loc)@1 as loczz, avg(temps.temp)@3 as avg(temps.temp)]"
        - "   GapFillExec: series_expr=[loc@0, concat(Utf8(\"zz\"),temps.loc)@2], time_expr=date_bin(IntervalMonthDayNano { months: 0, days: 0, nanoseconds: 60000000000 }, date_bin_gapfill(IntervalMonthDayNano(\"IntervalMonthDayNano { months: 0, days: 0, nanoseconds: 60000000000 }\"),temps.time,Utf8(\"1970-01-01T00:00:00Z\"))@1, 0), fill_expr=[avg(temps.temp)@3], time_range=Included(\"315532800000000000\")..Excluded(\"347155200000000000\")"
        - "     SortExec: expr=[loc@0 ASC, concat(Utf8(\"zz\"),temps.loc)@2 ASC, date_bin_gapfill(IntervalMonthDayNano(\"IntervalMonthDayNano { months: 0, days: 0, nanoseconds: 60000000000 }\"),temps.time,Utf8(\"1970-01-01T00:00:00Z\"))@1 ASC], preserve_partitioning=[false]"
        - "       AggregateExec: mode=Single, gby=[loc@1 as loc, date_bin(IntervalMonthDayNano { months: 0, days: 0, nanoseconds: 60000000000 }, time@0, 0) as date_bin_gapfill(IntervalMonthDayNano(\"IntervalMonthDayNano { months: 0, days: 0, nanoseconds: 60000000000 }\"),temps.time,Utf8(\"1970-01-01T00:00:00Z\")), concat(zz, loc@1) as concat(Utf8(\"zz\"),temps.loc)], aggr=[avg(temps.temp)]"
        - "         EmptyExec"
        "#
        );
        Ok(())
    }
}
