//! This module contains code that implements
//! a gap-filling extension to DataFusion

mod algo;
mod buffered_input;
#[cfg(test)]
mod exec_tests;
mod params;
mod stream;

use std::{
    fmt::{self, Debug},
    ops::{Bound, Range},
    sync::Arc,
};

use arrow::{compute::SortOptions, datatypes::SchemaRef};
use datafusion::{
    common::DFSchemaRef,
    error::{DataFusionError, Result},
    execution::{context::TaskContext, memory_pool::MemoryConsumer},
    logical_expr::{LogicalPlan, UserDefinedLogicalNodeCore},
    physical_expr::{
        create_physical_expr, execution_props::ExecutionProps, PhysicalSortExpr,
        PhysicalSortRequirement,
    },
    physical_plan::{
        expressions::Column,
        metrics::{BaselineMetrics, ExecutionPlanMetricsSet},
        DisplayAs, DisplayFormatType, Distribution, ExecutionPlan, Partitioning, PhysicalExpr,
        SendableRecordBatchStream, Statistics,
    },
    prelude::Expr,
};

use self::stream::GapFillStream;

/// A logical node that represents the gap filling operation.
#[derive(Clone, Debug, Hash, PartialEq, Eq)]
pub struct GapFill {
    /// The incoming logical plan
    pub input: Arc<LogicalPlan>,
    /// Grouping expressions
    pub group_expr: Vec<Expr>,
    /// Aggregate expressions
    pub aggr_expr: Vec<Expr>,
    /// Parameters to configure the behavior of the
    /// gap-filling operation
    pub params: GapFillParams,
}

/// Parameters to the GapFill operation
#[derive(Clone, Debug, Hash, PartialEq, Eq)]
pub struct GapFillParams {
    /// The stride argument from the call to DATE_BIN_GAPFILL
    pub stride: Expr,
    /// The source time column
    pub time_column: Expr,
    /// The origin argument from the call to DATE_BIN_GAPFILL
    pub origin: Option<Expr>,
    /// The time range of the time column inferred from predicates
    /// in the overall query. The lower bound may be [`Bound::Unbounded`]
    /// which implies that gap-filling should just start from the
    /// first point in each series.
    pub time_range: Range<Bound<Expr>>,
    /// What to do when filling aggregate columns.
    /// The first item in the tuple will be the column
    /// reference for the aggregate column.
    pub fill_strategy: Vec<(Expr, FillStrategy)>,
}

/// Describes how to fill gaps in an aggregate column.
#[derive(Clone, Debug, Hash, PartialEq, Eq)]
pub enum FillStrategy {
    /// Fill with null values.
    /// This is the InfluxQL behavior for `FILL(NULL)` or `FILL(NONE)`.
    Null,
    /// Fill with the most recent value in the input column.
    /// Null values in the input are preserved.
    #[allow(dead_code)]
    PrevNullAsIntentional,
    /// Fill with the most recent non-null value in the input column.
    /// This is the InfluxQL behavior for `FILL(PREVIOUS)`.
    PrevNullAsMissing,
    /// Fill the gaps between points linearly.
    /// Null values will not be considered as missing, so two non-null values
    /// with a null in between will not be filled.
    LinearInterpolate,
}

impl GapFillParams {
    // Extract the expressions so they can be optimized.
    fn expressions(&self) -> Vec<Expr> {
        let mut exprs = vec![self.stride.clone(), self.time_column.clone()];
        if let Some(e) = self.origin.as_ref() {
            exprs.push(e.clone())
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

    #[allow(clippy::wrong_self_convention)] // follows convention of UserDefinedLogicalNode
    fn from_template(&self, exprs: &[Expr], aggr_expr: &[Expr]) -> Self {
        assert!(
            exprs.len() >= 3,
            "should be a at least stride, source and origin in params"
        );
        let mut iter = exprs.iter().cloned();
        let stride = iter.next().unwrap();
        let time_column = iter.next().unwrap();
        let origin = self.origin.as_ref().map(|_| iter.next().unwrap());
        let time_range = try_map_range(&self.time_range, |b| {
            try_map_bound(b.as_ref(), |_| {
                Ok(iter.next().expect("expr count should match template"))
            })
        })
        .unwrap();

        let fill_strategy = aggr_expr
            .iter()
            .cloned()
            .zip(
                self.fill_strategy
                    .iter()
                    .map(|(_expr, fill_strategy)| fill_strategy)
                    .cloned(),
            )
            .collect();

        Self {
            stride,
            time_column,
            origin,
            time_range,
            fill_strategy,
        }
    }

    // Find the expression that matches `e` and replace its fill strategy.
    // If such an expression is found, return the old strategy, and `None` otherwise.
    fn replace_fill_strategy(&mut self, e: &Expr, mut fs: FillStrategy) -> Option<FillStrategy> {
        for expr_fs in &mut self.fill_strategy {
            if &expr_fs.0 == e {
                std::mem::swap(&mut fs, &mut expr_fs.1);
                return Some(fs);
            }
        }
        None
    }
}

impl GapFill {
    /// Create a new gap-filling operator.
    pub fn try_new(
        input: Arc<LogicalPlan>,
        group_expr: Vec<Expr>,
        aggr_expr: Vec<Expr>,
        params: GapFillParams,
    ) -> Result<Self> {
        if params.time_range.end == Bound::Unbounded {
            return Err(DataFusionError::Internal(
                "missing upper bound in GapFill time range".to_string(),
            ));
        }
        Ok(Self {
            input,
            group_expr,
            aggr_expr,
            params,
        })
    }

    // Find the expression that matches `e` and replace its fill strategy.
    // If such an expression is found, return the old strategy, and `None` otherwise.
    pub(crate) fn replace_fill_strategy(
        &mut self,
        e: &Expr,
        fs: FillStrategy,
    ) -> Option<FillStrategy> {
        self.params.replace_fill_strategy(e, fs)
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
        self.input.schema()
    }

    fn expressions(&self) -> Vec<Expr> {
        self.group_expr
            .iter()
            .chain(&self.aggr_expr)
            .chain(&self.params.expressions())
            .cloned()
            .collect()
    }

    fn fmt_for_explain(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let aggr_expr: String = self
            .params
            .fill_strategy
            .iter()
            .map(|(e, fs)| match fs {
                FillStrategy::PrevNullAsIntentional => format!("LOCF(null-as-intentional, {})", e),
                FillStrategy::PrevNullAsMissing => format!("LOCF({})", e),
                FillStrategy::LinearInterpolate => format!("INTERPOLATE({})", e),
                FillStrategy::Null => e.to_string(),
            })
            .collect::<Vec<String>>()
            .join(", ");

        let group_expr = self
            .group_expr
            .iter()
            .map(|e| e.to_string())
            .collect::<Vec<String>>()
            .join(", ");

        write!(
            f,
            "{}: groupBy=[{group_expr}], aggr=[[{aggr_expr}]], time_column={}, stride={}, range={:?}",
            self.name(),
            self.params.time_column,
            self.params.stride,
            self.params.time_range,
        )
    }

    fn from_template(&self, exprs: &[Expr], inputs: &[LogicalPlan]) -> Self {
        let mut group_expr: Vec<_> = exprs.to_vec();
        let mut aggr_expr = group_expr.split_off(self.group_expr.len());
        let param_expr = aggr_expr.split_off(self.aggr_expr.len());
        let params = self.params.from_template(&param_expr, &aggr_expr);
        Self::try_new(Arc::new(inputs[0].clone()), group_expr, aggr_expr, params)
            .expect("should not fail")
    }
}

/// Called by the extension planner to plan a [GapFill] node.
pub(crate) fn plan_gap_fill(
    execution_props: &ExecutionProps,
    gap_fill: &GapFill,
    logical_inputs: &[&LogicalPlan],
    physical_inputs: &[Arc<dyn ExecutionPlan>],
) -> Result<GapFillExec> {
    if logical_inputs.len() != 1 {
        return Err(DataFusionError::Internal(
            "GapFillExec: wrong number of logical inputs".to_string(),
        ));
    }
    if physical_inputs.len() != 1 {
        return Err(DataFusionError::Internal(
            "GapFillExec: wrong number of physical inputs".to_string(),
        ));
    }

    let input_dfschema = logical_inputs[0].schema().as_ref();
    let input_schema = physical_inputs[0].schema();
    let input_schema = input_schema.as_ref();

    let group_expr: Result<Vec<_>> = gap_fill
        .group_expr
        .iter()
        .map(|e| create_physical_expr(e, input_dfschema, input_schema, execution_props))
        .collect();
    let group_expr = group_expr?;

    let aggr_expr: Result<Vec<_>> = gap_fill
        .aggr_expr
        .iter()
        .map(|e| create_physical_expr(e, input_dfschema, input_schema, execution_props))
        .collect();
    let aggr_expr = aggr_expr?;

    let logical_time_column = gap_fill.params.time_column.try_into_col()?;
    let time_column = Column::new_with_schema(&logical_time_column.name, input_schema)?;

    let stride = create_physical_expr(
        &gap_fill.params.stride,
        input_dfschema,
        input_schema,
        execution_props,
    )?;

    let time_range = &gap_fill.params.time_range;
    let time_range = try_map_range(time_range, |b| {
        try_map_bound(b.as_ref(), |e| {
            create_physical_expr(e, input_dfschema, input_schema, execution_props)
        })
    })?;

    let origin = gap_fill
        .params
        .origin
        .as_ref()
        .map(|e| create_physical_expr(e, input_dfschema, input_schema, execution_props))
        .transpose()?;

    let fill_strategy = gap_fill
        .params
        .fill_strategy
        .iter()
        .map(|(e, fs)| {
            Ok((
                create_physical_expr(e, input_dfschema, input_schema, execution_props)?,
                fs.clone(),
            ))
        })
        .collect::<Result<Vec<(Arc<dyn PhysicalExpr>, FillStrategy)>>>()?;

    let params = GapFillExecParams {
        stride,
        time_column,
        origin,
        time_range,
        fill_strategy,
    };
    GapFillExec::try_new(
        Arc::clone(&physical_inputs[0]),
        group_expr,
        aggr_expr,
        params,
    )
}

fn try_map_range<T, U, F>(tr: &Range<T>, mut f: F) -> Result<Range<U>>
where
    F: FnMut(&T) -> Result<U>,
{
    Ok(Range {
        start: f(&tr.start)?,
        end: f(&tr.end)?,
    })
}

fn try_map_bound<T, U, F>(bt: Bound<T>, mut f: F) -> Result<Bound<U>>
where
    F: FnMut(T) -> Result<U>,
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
    // The group by expressions from the original aggregation node.
    group_expr: Vec<Arc<dyn PhysicalExpr>>,
    // The aggregate expressions from the original aggregation node.
    aggr_expr: Vec<Arc<dyn PhysicalExpr>>,
    // The sort expressions for the required sort order of the input:
    // all of the group exressions, with the time column being last.
    sort_expr: Vec<PhysicalSortExpr>,
    // Parameters (besides streaming data) to gap filling
    params: GapFillExecParams,
    /// Metrics reporting behavior during execution.
    metrics: ExecutionPlanMetricsSet,
}

#[derive(Clone, Debug)]
struct GapFillExecParams {
    /// The uniform interval of incoming timestamps
    stride: Arc<dyn PhysicalExpr>,
    /// The timestamp column produced by date_bin
    time_column: Column,
    /// The origin argument from the all to DATE_BIN_GAPFILL
    origin: Option<Arc<dyn PhysicalExpr>>,
    /// The time range of source input to DATE_BIN_GAPFILL.
    /// Inferred from predicates in the overall query.
    time_range: Range<Bound<Arc<dyn PhysicalExpr>>>,
    /// What to do when filling aggregate columns.
    /// The 0th element in each tuple is the aggregate column.
    fill_strategy: Vec<(Arc<dyn PhysicalExpr>, FillStrategy)>,
}

impl GapFillExec {
    fn try_new(
        input: Arc<dyn ExecutionPlan>,
        group_expr: Vec<Arc<dyn PhysicalExpr>>,
        aggr_expr: Vec<Arc<dyn PhysicalExpr>>,
        params: GapFillExecParams,
    ) -> Result<Self> {
        let sort_expr = {
            let mut sort_expr: Vec<_> = group_expr
                .iter()
                .map(|expr| PhysicalSortExpr {
                    expr: Arc::clone(expr),
                    options: SortOptions::default(),
                })
                .collect();

            // Ensure that the time column is the last component in the sort
            // expressions.
            let time_idx = group_expr
                .iter()
                .enumerate()
                .find(|(_i, e)| {
                    e.as_any()
                        .downcast_ref::<Column>()
                        .map_or(false, |c| c.index() == params.time_column.index())
                })
                .map(|(i, _)| i);

            if let Some(time_idx) = time_idx {
                let last_elem = sort_expr.len() - 1;
                sort_expr.swap(time_idx, last_elem);
            } else {
                return Err(DataFusionError::Internal(
                    "could not find time column for GapFillExec".to_string(),
                ));
            }

            sort_expr
        };

        Ok(Self {
            input,
            group_expr,
            aggr_expr,
            sort_expr,
            params,
            metrics: ExecutionPlanMetricsSet::new(),
        })
    }
}

impl Debug for GapFillExec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "GapFillExec")
    }
}

impl ExecutionPlan for GapFillExec {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.input.schema()
    }

    fn output_partitioning(&self) -> Partitioning {
        Partitioning::UnknownPartitioning(1)
    }

    fn required_input_distribution(&self) -> Vec<Distribution> {
        // It seems like it could be possible to partition on all the
        // group keys except for the time expression. For now, keep it simple.
        vec![Distribution::SinglePartition]
    }

    fn output_ordering(&self) -> Option<&[datafusion::physical_expr::PhysicalSortExpr]> {
        self.input.output_ordering()
    }

    fn required_input_ordering(&self) -> Vec<Option<Vec<PhysicalSortRequirement>>> {
        vec![Some(PhysicalSortRequirement::from_sort_exprs(
            &self.sort_expr,
        ))]
    }

    fn maintains_input_order(&self) -> Vec<bool> {
        vec![true]
    }

    fn children(&self) -> Vec<Arc<dyn ExecutionPlan>> {
        vec![Arc::clone(&self.input)]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        match children.len() {
            1 => Ok(Arc::new(Self::try_new(
                Arc::clone(&children[0]),
                self.group_expr.clone(),
                self.aggr_expr.clone(),
                self.params.clone(),
            )?)),
            _ => Err(DataFusionError::Internal(
                "GapFillExec wrong number of children".to_string(),
            )),
        }
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        if partition != 0 {
            return Err(DataFusionError::Internal(format!(
                "GapFillExec invalid partition {partition}, there can be only one partition"
            )));
        }

        let baseline_metrics = BaselineMetrics::new(&self.metrics, partition);
        let output_batch_size = context.session_config().batch_size();
        let reservation = MemoryConsumer::new(format!("GapFillExec[{partition}]"))
            .register(context.memory_pool());
        let input_stream = self.input.execute(partition, context)?;
        Ok(Box::pin(GapFillStream::try_new(
            self,
            output_batch_size,
            input_stream,
            reservation,
            baseline_metrics,
        )?))
    }

    fn statistics(&self) -> Statistics {
        Statistics::default()
    }
}

impl DisplayAs for GapFillExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                let group_expr: Vec<_> = self.group_expr.iter().map(|e| e.to_string()).collect();
                let aggr_expr: Vec<_> = self
                    .params
                    .fill_strategy
                    .iter()
                    .map(|(e, fs)| match fs {
                        FillStrategy::PrevNullAsIntentional => {
                            format!("LOCF(null-as-intentional, {})", e)
                        }
                        FillStrategy::PrevNullAsMissing => format!("LOCF({})", e),
                        FillStrategy::LinearInterpolate => format!("INTERPOLATE({})", e),
                        FillStrategy::Null => e.to_string(),
                    })
                    .collect();
                let time_range = try_map_range(&self.params.time_range, |b| {
                    try_map_bound(b.as_ref(), |e| Ok(e.to_string()))
                })
                .map_err(|_| fmt::Error {})?;
                write!(
                    f,
                    "GapFillExec: group_expr=[{}], aggr_expr=[{}], stride={}, time_range={:?}",
                    group_expr.join(", "),
                    aggr_expr.join(", "),
                    self.params.stride,
                    time_range
                )
            }
        }
    }
}

#[cfg(test)]
mod test {
    use std::ops::{Bound, Range};

    use crate::{
        exec::{Executor, ExecutorType},
        test::{format_execution_plan, format_logical_plan},
    };

    use super::*;
    use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
    use datafusion::{
        datasource::empty::EmptyTable,
        error::Result,
        logical_expr::{logical_plan, Extension, UserDefinedLogicalNode},
        prelude::{col, lit, lit_timestamp_nano},
        scalar::ScalarValue,
    };

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

    fn fill_strategy_null(cols: Vec<Expr>) -> Vec<(Expr, FillStrategy)> {
        cols.into_iter().map(|e| (e, FillStrategy::Null)).collect()
    }

    #[test]
    fn test_try_new_errs() {
        let scan = table_scan().unwrap();
        let result = GapFill::try_new(
            Arc::new(scan),
            vec![col("loc"), col("time")],
            vec![col("temp")],
            GapFillParams {
                stride: lit(ScalarValue::IntervalDayTime(Some(60_000))),
                time_column: col("time"),
                origin: None,
                time_range: Range {
                    start: Bound::Included(lit_timestamp_nano(1000)),
                    end: Bound::Unbounded,
                },
                fill_strategy: fill_strategy_null(vec![col("temp")]),
            },
        );

        assert_error!(result, DataFusionError::Internal(ref msg) if msg == "missing upper bound in GapFill time range");
    }

    fn assert_gapfill_from_template_roundtrip(gapfill: &GapFill) {
        let gapfill_as_node: &dyn UserDefinedLogicalNode = gapfill;
        let scan = table_scan().unwrap();
        let exprs = gapfill_as_node.expressions();
        let want_exprs = gapfill.group_expr.len()
            + gapfill.aggr_expr.len()
            + 2 // stride, time
            + gapfill.params.origin.iter().count()
            + bound_extract(&gapfill.params.time_range.start).iter().count()
            + bound_extract(&gapfill.params.time_range.end).iter().count();
        assert_eq!(want_exprs, exprs.len());
        let gapfill_ft = gapfill_as_node.from_template(&exprs, &[scan]);
        let gapfill_ft = gapfill_ft
            .as_any()
            .downcast_ref::<GapFill>()
            .expect("should be a GapFill");
        assert_eq!(gapfill.group_expr, gapfill_ft.group_expr);
        assert_eq!(gapfill.aggr_expr, gapfill_ft.aggr_expr);
        assert_eq!(gapfill.params, gapfill_ft.params);
    }

    #[test]
    fn test_from_template() {
        for params in vec![
            // no origin, no start bound
            GapFillParams {
                stride: lit(ScalarValue::IntervalDayTime(Some(60_000))),
                time_column: col("time"),
                origin: None,
                time_range: Range {
                    start: Bound::Unbounded,
                    end: Bound::Excluded(lit_timestamp_nano(2000)),
                },
                fill_strategy: fill_strategy_null(vec![col("temp")]),
            },
            // no origin, yes start bound
            GapFillParams {
                stride: lit(ScalarValue::IntervalDayTime(Some(60_000))),
                time_column: col("time"),
                origin: None,
                time_range: Range {
                    start: Bound::Included(lit_timestamp_nano(1000)),
                    end: Bound::Excluded(lit_timestamp_nano(2000)),
                },
                fill_strategy: fill_strategy_null(vec![col("temp")]),
            },
            // yes origin, no start bound
            GapFillParams {
                stride: lit(ScalarValue::IntervalDayTime(Some(60_000))),
                time_column: col("time"),
                origin: Some(lit_timestamp_nano(1_000_000_000)),
                time_range: Range {
                    start: Bound::Unbounded,
                    end: Bound::Excluded(lit_timestamp_nano(2000)),
                },
                fill_strategy: fill_strategy_null(vec![col("temp")]),
            },
            // yes origin, yes start bound
            GapFillParams {
                stride: lit(ScalarValue::IntervalDayTime(Some(60_000))),
                time_column: col("time"),
                origin: Some(lit_timestamp_nano(1_000_000_000)),
                time_range: Range {
                    start: Bound::Included(lit_timestamp_nano(1000)),
                    end: Bound::Excluded(lit_timestamp_nano(2000)),
                },
                fill_strategy: fill_strategy_null(vec![col("temp")]),
            },
        ] {
            let scan = table_scan().unwrap();
            let gapfill = GapFill::try_new(
                Arc::new(scan.clone()),
                vec![col("loc"), col("time")],
                vec![col("temp")],
                params,
            )
            .unwrap();
            assert_gapfill_from_template_roundtrip(&gapfill);
        }
    }

    #[test]
    fn fmt_logical_plan() -> Result<()> {
        // This test case does not make much sense but
        // just verifies we can construct a logical gapfill node
        // and show its plan.
        let scan = table_scan()?;
        let gapfill = GapFill::try_new(
            Arc::new(scan),
            vec![col("loc"), col("time")],
            vec![col("temp")],
            GapFillParams {
                stride: lit(ScalarValue::IntervalDayTime(Some(60_000))),
                time_column: col("time"),
                origin: None,
                time_range: Range {
                    start: Bound::Included(lit_timestamp_nano(1000)),
                    end: Bound::Excluded(lit_timestamp_nano(2000)),
                },
                fill_strategy: fill_strategy_null(vec![col("temp")]),
            },
        )?;
        let plan = LogicalPlan::Extension(Extension {
            node: Arc::new(gapfill),
        });

        insta::assert_yaml_snapshot!(
            format_logical_plan(&plan),
            @r###"
        ---
        - " GapFill: groupBy=[loc, time], aggr=[[temp]], time_column=time, stride=IntervalDayTime(\"60000\"), range=Included(Literal(TimestampNanosecond(1000, None)))..Excluded(Literal(TimestampNanosecond(2000, None)))"
        - "   TableScan: temps"
        "###
        );
        Ok(())
    }

    async fn format_explain(sql: &str) -> Result<Vec<String>> {
        let executor = Executor::new_testing();
        let context = executor.new_context(ExecutorType::Query);
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
            @r###"
        ---
        - " ProjectionExec: expr=[date_bin_gapfill(IntervalMonthDayNano(\"60000000000\"),temps.time,Utf8(\"1970-01-01T00:00:00Z\"))@0 as minute, AVG(temps.temp)@1 as AVG(temps.temp)]"
        - "   GapFillExec: group_expr=[date_bin_gapfill(IntervalMonthDayNano(\"60000000000\"),temps.time,Utf8(\"1970-01-01T00:00:00Z\"))@0], aggr_expr=[AVG(temps.temp)@1], stride=60000000000, time_range=Included(\"315532800000000000\")..Excluded(\"347155200000000000\")"
        - "     SortExec: expr=[date_bin_gapfill(IntervalMonthDayNano(\"60000000000\"),temps.time,Utf8(\"1970-01-01T00:00:00Z\"))@0 ASC]"
        - "       AggregateExec: mode=Final, gby=[date_bin_gapfill(IntervalMonthDayNano(\"60000000000\"),temps.time,Utf8(\"1970-01-01T00:00:00Z\"))@0 as date_bin_gapfill(IntervalMonthDayNano(\"60000000000\"),temps.time,Utf8(\"1970-01-01T00:00:00Z\"))], aggr=[AVG(temps.temp)]"
        - "         AggregateExec: mode=Partial, gby=[date_bin(60000000000, time@0, 0) as date_bin_gapfill(IntervalMonthDayNano(\"60000000000\"),temps.time,Utf8(\"1970-01-01T00:00:00Z\"))], aggr=[AVG(temps.temp)]"
        - "           EmptyExec: produce_one_row=false"
        "###
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
            @r###"
        ---
        - " ProjectionExec: expr=[loc@0 as loc, date_bin_gapfill(IntervalMonthDayNano(\"60000000000\"),temps.time,Utf8(\"1970-01-01T00:00:00Z\"))@1 as minute, concat(Utf8(\"zz\"),temps.loc)@2 as loczz, AVG(temps.temp)@3 as AVG(temps.temp)]"
        - "   GapFillExec: group_expr=[loc@0, date_bin_gapfill(IntervalMonthDayNano(\"60000000000\"),temps.time,Utf8(\"1970-01-01T00:00:00Z\"))@1, concat(Utf8(\"zz\"),temps.loc)@2], aggr_expr=[AVG(temps.temp)@3], stride=60000000000, time_range=Included(\"315532800000000000\")..Excluded(\"347155200000000000\")"
        - "     SortExec: expr=[loc@0 ASC,concat(Utf8(\"zz\"),temps.loc)@2 ASC,date_bin_gapfill(IntervalMonthDayNano(\"60000000000\"),temps.time,Utf8(\"1970-01-01T00:00:00Z\"))@1 ASC]"
        - "       AggregateExec: mode=Final, gby=[loc@0 as loc, date_bin_gapfill(IntervalMonthDayNano(\"60000000000\"),temps.time,Utf8(\"1970-01-01T00:00:00Z\"))@1 as date_bin_gapfill(IntervalMonthDayNano(\"60000000000\"),temps.time,Utf8(\"1970-01-01T00:00:00Z\")), concat(Utf8(\"zz\"),temps.loc)@2 as concat(Utf8(\"zz\"),temps.loc)], aggr=[AVG(temps.temp)]"
        - "         AggregateExec: mode=Partial, gby=[loc@1 as loc, date_bin(60000000000, time@0, 0) as date_bin_gapfill(IntervalMonthDayNano(\"60000000000\"),temps.time,Utf8(\"1970-01-01T00:00:00Z\")), concat(zz, loc@1) as concat(Utf8(\"zz\"),temps.loc)], aggr=[AVG(temps.temp)]"
        - "           EmptyExec: produce_one_row=false"
        "###
        );
        Ok(())
    }
}
