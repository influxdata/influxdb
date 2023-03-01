//! This module contains code that implements
//! a gap-filling extension to DataFusion

mod algo;
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
    execution::context::TaskContext,
    logical_expr::{LogicalPlan, UserDefinedLogicalNode},
    physical_expr::{create_physical_expr, execution_props::ExecutionProps, PhysicalSortExpr},
    physical_plan::{
        expressions::Column,
        metrics::{BaselineMetrics, ExecutionPlanMetricsSet},
        DisplayFormatType, Distribution, ExecutionPlan, Partitioning, PhysicalExpr,
        SendableRecordBatchStream, Statistics,
    },
    prelude::Expr,
};

use self::stream::GapFillStream;

/// A logical node that represents the gap filling operation.
#[derive(Clone, Debug)]
pub struct GapFill {
    input: Arc<LogicalPlan>,
    group_expr: Vec<Expr>,
    aggr_expr: Vec<Expr>,
    params: GapFillParams,
}

/// Parameters to the GapFill operation
#[derive(Clone, Debug, PartialEq)]
pub(crate) struct GapFillParams {
    /// The stride argument from the call to DATE_BIN_GAPFILL
    pub stride: Expr,
    /// The source time column
    pub time_column: Expr,
    /// The origin argument from the call to DATE_BIN_GAPFILL
    pub origin: Expr,
    /// The time range of the time column inferred from predicates
    /// in the overall query
    pub time_range: Range<Bound<Expr>>,
}

impl GapFillParams {
    // Extract the expressions so they can be optimized.
    fn expressions(&self) -> Vec<Expr> {
        vec![
            self.stride.clone(),
            self.time_column.clone(),
            self.origin.clone(),
            bound_extract(&self.time_range.start)
                .unwrap_or_else(|| panic!("lower time bound is required"))
                .clone(),
            bound_extract(&self.time_range.end)
                .unwrap_or_else(|| panic!("upper time bound is required"))
                .clone(),
        ]
    }

    #[allow(clippy::wrong_self_convention)] // follows convention of UserDefinedLogicalNode
    fn from_template(&self, exprs: &[Expr]) -> Self {
        assert!(
            exprs.len() >= 3,
            "should be a at least stride, source and origin in params"
        );
        let mut iter = exprs.iter().cloned();
        let stride = iter.next().unwrap();
        let time_column = iter.next().unwrap();
        let origin = iter.next().unwrap();
        let time_range = try_map_range(&self.time_range, |b| {
            try_map_bound(b.as_ref(), |_| {
                Ok(iter.next().expect("expr count should match template"))
            })
        })
        .unwrap();
        Self {
            stride,
            time_column,
            origin,
            time_range,
        }
    }
}

impl GapFill {
    pub(crate) fn try_new(
        input: Arc<LogicalPlan>,
        group_expr: Vec<Expr>,
        aggr_expr: Vec<Expr>,
        params: GapFillParams,
    ) -> Result<Self> {
        Ok(Self {
            input,
            group_expr,
            aggr_expr,
            params,
        })
    }
}

impl UserDefinedLogicalNode for GapFill {
    fn as_any(&self) -> &dyn std::any::Any {
        self
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
        write!(
            f,
            "GapFill: groupBy=[{:?}], aggr=[{:?}], time_column={}, stride={}, range={:?}",
            self.group_expr,
            self.aggr_expr,
            self.params.time_column,
            self.params.stride,
            self.params.time_range,
        )
    }

    fn from_template(
        &self,
        exprs: &[Expr],
        inputs: &[LogicalPlan],
    ) -> Arc<dyn UserDefinedLogicalNode> {
        let mut group_expr: Vec<_> = exprs.to_vec();
        let mut aggr_expr = group_expr.split_off(self.group_expr.len());
        let param_expr = aggr_expr.split_off(self.aggr_expr.len());
        let params = self.params.from_template(&param_expr);
        let gapfill = Self::try_new(Arc::new(inputs[0].clone()), group_expr, aggr_expr, params)
            .expect("should not fail");
        Arc::new(gapfill)
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

    let origin = create_physical_expr(
        &gap_fill.params.origin,
        input_dfschema,
        input_schema,
        execution_props,
    )?;

    let params = GapFillExecParams {
        stride,
        time_column,
        origin,
        time_range,
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
    origin: Arc<dyn PhysicalExpr>,
    /// The time range of source input to DATE_BIN_GAPFILL.
    /// Inferred from predicates in the overall query.
    time_range: Range<Bound<Arc<dyn PhysicalExpr>>>,
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

    fn required_input_ordering(&self) -> Vec<Option<&[PhysicalSortExpr]>> {
        vec![Some(&self.sort_expr)]
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
        if self.output_partitioning().partition_count() <= partition {
            return Err(DataFusionError::Internal(format!(
                "GapFillExec invalid partition {partition}"
            )));
        }

        let baseline_metrics = BaselineMetrics::new(&self.metrics, partition);
        let output_batch_size = context.session_config().batch_size();
        let input_stream = self.input.execute(partition, context)?;
        Ok(Box::pin(GapFillStream::try_new(
            self.schema(),
            &self.sort_expr,
            &self.aggr_expr,
            &self.params,
            output_batch_size,
            input_stream,
            baseline_metrics,
        )?))
    }

    fn fmt_as(&self, t: DisplayFormatType, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match t {
            DisplayFormatType::Default => {
                let group_expr: Vec<_> = self.group_expr.iter().map(|e| e.to_string()).collect();
                let aggr_expr: Vec<_> = self.aggr_expr.iter().map(|e| e.to_string()).collect();
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

    fn statistics(&self) -> Statistics {
        Statistics::default()
    }
}

#[cfg(test)]
mod test {
    use std::{
        cmp::Ordering,
        ops::{Bound, Range},
    };

    use crate::{
        exec::{Executor, ExecutorType},
        test::{format_execution_plan, format_logical_plan},
    };

    use super::*;
    use arrow::{
        array::{ArrayRef, DictionaryArray, Int64Array, TimestampNanosecondArray},
        datatypes::{DataType, Field, Int32Type, Schema, TimeUnit},
        record_batch::RecordBatch,
    };
    use arrow_util::assert_batches_eq;
    use datafusion::{
        datasource::empty::EmptyTable,
        error::Result,
        logical_expr::{logical_plan, Extension},
        physical_plan::{collect, expressions::lit as phys_lit, memory::MemoryExec},
        prelude::{col, lit, lit_timestamp_nano, SessionConfig, SessionContext},
        scalar::ScalarValue,
    };
    use observability_deps::tracing::debug;
    use schema::{InfluxColumnType, InfluxFieldType};

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

    #[test]
    fn test_from_template() -> Result<()> {
        let scan = table_scan()?;
        let gapfill = GapFill::try_new(
            Arc::new(scan.clone()),
            vec![col("loc"), col("time")],
            vec![col("temp")],
            GapFillParams {
                stride: lit(ScalarValue::IntervalDayTime(Some(60_000))),
                time_column: col("time"),
                origin: lit_timestamp_nano(0),
                time_range: Range {
                    start: Bound::Included(lit_timestamp_nano(1000)),
                    end: Bound::Excluded(lit_timestamp_nano(2000)),
                },
            },
        )?;
        let exprs = gapfill.expressions();
        assert_eq!(8, exprs.len());
        let gapfill_ft = gapfill.from_template(&exprs, &[scan]);
        let gapfill_ft = gapfill_ft
            .as_any()
            .downcast_ref::<GapFill>()
            .expect("should be a GapFill");
        assert_eq!(gapfill.group_expr, gapfill_ft.group_expr);
        assert_eq!(gapfill.aggr_expr, gapfill_ft.aggr_expr);
        assert_eq!(gapfill.params, gapfill_ft.params);
        Ok(())
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
                origin: lit_timestamp_nano(0),
                time_range: Range {
                    start: Bound::Included(lit_timestamp_nano(1000)),
                    end: Bound::Excluded(lit_timestamp_nano(2000)),
                },
            },
        )?;
        let plan = LogicalPlan::Extension(Extension {
            node: Arc::new(gapfill),
        });

        insta::assert_yaml_snapshot!(
            format_logical_plan(&plan),
            @r###"
        ---
        - " GapFill: groupBy=[[loc, time]], aggr=[[temp]], time_column=time, stride=IntervalDayTime(\"60000\"), range=Included(TimestampNanosecond(1000, None))..Excluded(TimestampNanosecond(2000, None))"
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
        let physical_plan = context.prepare_sql(sql).await?;
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
        - " ProjectionExec: expr=[date_bin_gapfill(IntervalDayTime(\"60000\"),temps.time,Utf8(\"1970-01-01T00:00:00Z\"))@0 as minute, AVG(temps.temp)@1 as AVG(temps.temp)]"
        - "   GapFillExec: group_expr=[date_bin_gapfill(IntervalDayTime(\"60000\"),temps.time,Utf8(\"1970-01-01T00:00:00Z\"))@0], aggr_expr=[AVG(temps.temp)@1], stride=60000, time_range=Included(\"315532800000000000\")..Excluded(\"347155200000000000\")"
        - "     SortExec: expr=[date_bin_gapfill(IntervalDayTime(\"60000\"),temps.time,Utf8(\"1970-01-01T00:00:00Z\"))@0 ASC]"
        - "       AggregateExec: mode=Final, gby=[date_bin_gapfill(IntervalDayTime(\"60000\"),temps.time,Utf8(\"1970-01-01T00:00:00Z\"))@0 as date_bin_gapfill(IntervalDayTime(\"60000\"),temps.time,Utf8(\"1970-01-01T00:00:00Z\"))], aggr=[AVG(temps.temp)]"
        - "         AggregateExec: mode=Partial, gby=[datebin(60000, time@0, 0) as date_bin_gapfill(IntervalDayTime(\"60000\"),temps.time,Utf8(\"1970-01-01T00:00:00Z\"))], aggr=[AVG(temps.temp)]"
        - "           CoalesceBatchesExec: target_batch_size=8192"
        - "             FilterExec: time@0 >= 315532800000000000 AND time@0 < 347155200000000000"
        - "               EmptyExec: produce_one_row=false"
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
        - " ProjectionExec: expr=[loc@0 as loc, date_bin_gapfill(IntervalDayTime(\"60000\"),temps.time,Utf8(\"1970-01-01T00:00:00Z\"))@1 as minute, concat(Utf8(\"zz\"),temps.loc)@2 as loczz, AVG(temps.temp)@3 as AVG(temps.temp)]"
        - "   GapFillExec: group_expr=[loc@0, date_bin_gapfill(IntervalDayTime(\"60000\"),temps.time,Utf8(\"1970-01-01T00:00:00Z\"))@1, concat(Utf8(\"zz\"),temps.loc)@2], aggr_expr=[AVG(temps.temp)@3], stride=60000, time_range=Included(\"315532800000000000\")..Excluded(\"347155200000000000\")"
        - "     SortExec: expr=[loc@0 ASC,concat(Utf8(\"zz\"),temps.loc)@2 ASC,date_bin_gapfill(IntervalDayTime(\"60000\"),temps.time,Utf8(\"1970-01-01T00:00:00Z\"))@1 ASC]"
        - "       AggregateExec: mode=Final, gby=[loc@0 as loc, date_bin_gapfill(IntervalDayTime(\"60000\"),temps.time,Utf8(\"1970-01-01T00:00:00Z\"))@1 as date_bin_gapfill(IntervalDayTime(\"60000\"),temps.time,Utf8(\"1970-01-01T00:00:00Z\")), concat(Utf8(\"zz\"),temps.loc)@2 as concat(Utf8(\"zz\"),temps.loc)], aggr=[AVG(temps.temp)]"
        - "         AggregateExec: mode=Partial, gby=[loc@1 as loc, datebin(60000, time@0, 0) as date_bin_gapfill(IntervalDayTime(\"60000\"),temps.time,Utf8(\"1970-01-01T00:00:00Z\")), concat(zz, loc@1) as concat(Utf8(\"zz\"),temps.loc)], aggr=[AVG(temps.temp)]"
        - "           CoalesceBatchesExec: target_batch_size=8192"
        - "             FilterExec: time@0 >= 315532800000000000 AND time@0 < 347155200000000000"
        - "               EmptyExec: produce_one_row=false"
        "###
        );
        Ok(())
    }

    #[tokio::test]
    async fn test_gapfill_simple() -> Result<()> {
        test_helpers::maybe_start_logging();
        for output_batch_size in [1, 2, 4, 8] {
            for input_batch_size in [1, 2] {
                let batch = TestRecords {
                    group_cols: vec![vec![Some("a"), Some("a")]],
                    time_col: vec![Some(1_000), Some(1_100)],
                    agg_cols: vec![vec![Some(10), Some(11)]],
                    input_batch_size,
                };
                let params = get_params_ms(&batch, 25, 975, 1_125);
                let tc = TestCase {
                    test_records: batch,
                    output_batch_size,
                    params,
                };
                let batches = tc.run().await?;
                let expected = [
                    "+----+--------------------------+----+",
                    "| g0 | time                     | a0 |",
                    "+----+--------------------------+----+",
                    "| a  | 1970-01-01T00:00:00.975Z |    |",
                    "| a  | 1970-01-01T00:00:01Z     | 10 |",
                    "| a  | 1970-01-01T00:00:01.025Z |    |",
                    "| a  | 1970-01-01T00:00:01.050Z |    |",
                    "| a  | 1970-01-01T00:00:01.075Z |    |",
                    "| a  | 1970-01-01T00:00:01.100Z | 11 |",
                    "| a  | 1970-01-01T00:00:01.125Z |    |",
                    "+----+--------------------------+----+",
                ];
                assert_batches_eq!(expected, &batches);
                assert_batch_count(&batches, output_batch_size);
            }
        }
        Ok(())
    }

    #[tokio::test]
    async fn test_gapfill_simple_no_group_no_aggr() -> Result<()> {
        // There may be no group columns in a gap fill query,
        // and there may be no aggregate columns as well.
        // Such a query is not all that useful but it should work.
        test_helpers::maybe_start_logging();
        for output_batch_size in [1, 2, 4, 8] {
            for input_batch_size in [1, 2, 4] {
                let batch = TestRecords {
                    group_cols: vec![],
                    time_col: vec![None, Some(1_000), Some(1_100)],
                    agg_cols: vec![],
                    input_batch_size,
                };
                let params = get_params_ms(&batch, 25, 975, 1_125);
                let tc = TestCase {
                    test_records: batch,
                    output_batch_size,
                    params,
                };
                let batches = tc.run().await?;
                let expected = [
                    "+--------------------------+",
                    "| time                     |",
                    "+--------------------------+",
                    "|                          |",
                    "| 1970-01-01T00:00:00.975Z |",
                    "| 1970-01-01T00:00:01Z     |",
                    "| 1970-01-01T00:00:01.025Z |",
                    "| 1970-01-01T00:00:01.050Z |",
                    "| 1970-01-01T00:00:01.075Z |",
                    "| 1970-01-01T00:00:01.100Z |",
                    "| 1970-01-01T00:00:01.125Z |",
                    "+--------------------------+",
                ];
                assert_batches_eq!(expected, &batches);
                assert_batch_count(&batches, output_batch_size);
            }
        }
        Ok(())
    }

    #[tokio::test]
    async fn test_gapfill_multi_group_simple() -> Result<()> {
        test_helpers::maybe_start_logging();
        for output_batch_size in [1, 2, 4, 8, 16] {
            for input_batch_size in [1, 2, 4] {
                let records = TestRecords {
                    group_cols: vec![vec![Some("a"), Some("a"), Some("b"), Some("b")]],
                    time_col: vec![Some(1_000), Some(1_100), Some(1_025), Some(1_050)],
                    agg_cols: vec![vec![Some(10), Some(11), Some(20), Some(21)]],
                    input_batch_size,
                };
                let params = get_params_ms(&records, 25, 975, 1_125);
                let tc = TestCase {
                    test_records: records,
                    output_batch_size,
                    params,
                };
                let batches = tc.run().await?;
                let expected = [
                    "+----+--------------------------+----+",
                    "| g0 | time                     | a0 |",
                    "+----+--------------------------+----+",
                    "| a  | 1970-01-01T00:00:00.975Z |    |",
                    "| a  | 1970-01-01T00:00:01Z     | 10 |",
                    "| a  | 1970-01-01T00:00:01.025Z |    |",
                    "| a  | 1970-01-01T00:00:01.050Z |    |",
                    "| a  | 1970-01-01T00:00:01.075Z |    |",
                    "| a  | 1970-01-01T00:00:01.100Z | 11 |",
                    "| a  | 1970-01-01T00:00:01.125Z |    |",
                    "| b  | 1970-01-01T00:00:00.975Z |    |",
                    "| b  | 1970-01-01T00:00:01Z     |    |",
                    "| b  | 1970-01-01T00:00:01.025Z | 20 |",
                    "| b  | 1970-01-01T00:00:01.050Z | 21 |",
                    "| b  | 1970-01-01T00:00:01.075Z |    |",
                    "| b  | 1970-01-01T00:00:01.100Z |    |",
                    "| b  | 1970-01-01T00:00:01.125Z |    |",
                    "+----+--------------------------+----+",
                ];
                assert_batches_eq!(expected, &batches);
                assert_batch_count(&batches, output_batch_size);
            }
        }
        Ok(())
    }

    #[tokio::test]
    async fn test_gapfill_multi_group_with_nulls() -> Result<()> {
        test_helpers::maybe_start_logging();
        for output_batch_size in [1, 2, 4, 8, 16, 32] {
            for input_batch_size in [1, 2, 4, 8] {
                let records = TestRecords {
                    group_cols: vec![vec![
                        Some("a"),
                        Some("a"),
                        Some("a"),
                        Some("a"),
                        Some("b"),
                        Some("b"),
                        Some("b"),
                    ]],
                    time_col: vec![
                        None,
                        None,
                        Some(1_000),
                        Some(1_100),
                        None,
                        Some(1_000),
                        Some(1_100),
                    ],
                    agg_cols: vec![vec![
                        Some(1),
                        None,
                        Some(10),
                        Some(11),
                        Some(2),
                        Some(20),
                        Some(21),
                    ]],
                    input_batch_size,
                };
                let params = get_params_ms(&records, 25, 975, 1_125);
                let tc = TestCase {
                    test_records: records,
                    output_batch_size,
                    params,
                };
                let batches = tc.run().await?;
                let expected = [
                    "+----+--------------------------+----+",
                    "| g0 | time                     | a0 |",
                    "+----+--------------------------+----+",
                    "| a  |                          | 1  |",
                    "| a  |                          |    |",
                    "| a  | 1970-01-01T00:00:00.975Z |    |",
                    "| a  | 1970-01-01T00:00:01Z     | 10 |",
                    "| a  | 1970-01-01T00:00:01.025Z |    |",
                    "| a  | 1970-01-01T00:00:01.050Z |    |",
                    "| a  | 1970-01-01T00:00:01.075Z |    |",
                    "| a  | 1970-01-01T00:00:01.100Z | 11 |",
                    "| a  | 1970-01-01T00:00:01.125Z |    |",
                    "| b  |                          | 2  |",
                    "| b  | 1970-01-01T00:00:00.975Z |    |",
                    "| b  | 1970-01-01T00:00:01Z     | 20 |",
                    "| b  | 1970-01-01T00:00:01.025Z |    |",
                    "| b  | 1970-01-01T00:00:01.050Z |    |",
                    "| b  | 1970-01-01T00:00:01.075Z |    |",
                    "| b  | 1970-01-01T00:00:01.100Z | 21 |",
                    "| b  | 1970-01-01T00:00:01.125Z |    |",
                    "+----+--------------------------+----+",
                ];
                assert_batches_eq!(expected, &batches);
                assert_batch_count(&batches, output_batch_size);
            }
        }
        Ok(())
    }

    #[tokio::test]
    async fn test_gapfill_multi_group_cols_with_nulls() -> Result<()> {
        test_helpers::maybe_start_logging();
        for output_batch_size in [1, 2, 4, 8, 16, 32] {
            for input_batch_size in [1, 2, 4, 8] {
                let records = TestRecords {
                    group_cols: vec![
                        vec![
                            Some("a"),
                            Some("a"),
                            Some("a"),
                            Some("a"),
                            Some("a"),
                            Some("a"),
                            Some("a"),
                        ],
                        vec![
                            Some("c"),
                            Some("c"),
                            Some("c"),
                            Some("c"),
                            Some("d"),
                            Some("d"),
                            Some("d"),
                        ],
                    ],
                    time_col: vec![
                        None,
                        None,
                        Some(1_000),
                        Some(1_100),
                        None,
                        Some(1_000),
                        Some(1_100),
                    ],
                    agg_cols: vec![vec![
                        Some(1),
                        None,
                        Some(10),
                        Some(11),
                        Some(2),
                        Some(20),
                        Some(21),
                    ]],
                    input_batch_size,
                };
                let params = get_params_ms(&records, 25, 975, 1_125);
                let tc = TestCase {
                    test_records: records,
                    output_batch_size,
                    params,
                };
                let batches = tc.run().await?;
                let expected = [
                    "+----+----+--------------------------+----+",
                    "| g0 | g1 | time                     | a0 |",
                    "+----+----+--------------------------+----+",
                    "| a  | c  |                          | 1  |",
                    "| a  | c  |                          |    |",
                    "| a  | c  | 1970-01-01T00:00:00.975Z |    |",
                    "| a  | c  | 1970-01-01T00:00:01Z     | 10 |",
                    "| a  | c  | 1970-01-01T00:00:01.025Z |    |",
                    "| a  | c  | 1970-01-01T00:00:01.050Z |    |",
                    "| a  | c  | 1970-01-01T00:00:01.075Z |    |",
                    "| a  | c  | 1970-01-01T00:00:01.100Z | 11 |",
                    "| a  | c  | 1970-01-01T00:00:01.125Z |    |",
                    "| a  | d  |                          | 2  |",
                    "| a  | d  | 1970-01-01T00:00:00.975Z |    |",
                    "| a  | d  | 1970-01-01T00:00:01Z     | 20 |",
                    "| a  | d  | 1970-01-01T00:00:01.025Z |    |",
                    "| a  | d  | 1970-01-01T00:00:01.050Z |    |",
                    "| a  | d  | 1970-01-01T00:00:01.075Z |    |",
                    "| a  | d  | 1970-01-01T00:00:01.100Z | 21 |",
                    "| a  | d  | 1970-01-01T00:00:01.125Z |    |",
                    "+----+----+--------------------------+----+",
                ];
                assert_batches_eq!(expected, &batches);
                assert_batch_count(&batches, output_batch_size);
            }
        }
        Ok(())
    }

    #[tokio::test]
    async fn test_gapfill_multi_aggr_cols_with_nulls() -> Result<()> {
        test_helpers::maybe_start_logging();
        for output_batch_size in [1, 2, 4, 8, 16, 32] {
            for input_batch_size in [1, 2, 4, 8] {
                let records = TestRecords {
                    group_cols: vec![
                        vec![
                            Some("a"),
                            Some("a"),
                            Some("a"),
                            Some("a"),
                            Some("b"),
                            Some("b"),
                            Some("b"),
                        ],
                        vec![
                            Some("c"),
                            Some("c"),
                            Some("c"),
                            Some("c"),
                            Some("d"),
                            Some("d"),
                            Some("d"),
                        ],
                    ],
                    time_col: vec![
                        None,
                        None,
                        Some(1_000),
                        Some(1_100),
                        None,
                        Some(1_000),
                        Some(1_100),
                    ],
                    agg_cols: vec![
                        vec![
                            Some(1),
                            None,
                            Some(10),
                            Some(11),
                            Some(2),
                            Some(20),
                            Some(21),
                        ],
                        vec![
                            Some(3),
                            Some(3),
                            Some(30),
                            None,
                            Some(4),
                            Some(40),
                            Some(41),
                        ],
                    ],
                    input_batch_size,
                };
                let params = get_params_ms(&records, 25, 975, 1_125);
                let tc = TestCase {
                    test_records: records,
                    output_batch_size,
                    params,
                };
                let batches = tc.run().await?;
                let expected = [
                    "+----+----+--------------------------+----+----+",
                    "| g0 | g1 | time                     | a0 | a1 |",
                    "+----+----+--------------------------+----+----+",
                    "| a  | c  |                          | 1  | 3  |",
                    "| a  | c  |                          |    | 3  |",
                    "| a  | c  | 1970-01-01T00:00:00.975Z |    |    |",
                    "| a  | c  | 1970-01-01T00:00:01Z     | 10 | 30 |",
                    "| a  | c  | 1970-01-01T00:00:01.025Z |    |    |",
                    "| a  | c  | 1970-01-01T00:00:01.050Z |    |    |",
                    "| a  | c  | 1970-01-01T00:00:01.075Z |    |    |",
                    "| a  | c  | 1970-01-01T00:00:01.100Z | 11 |    |",
                    "| a  | c  | 1970-01-01T00:00:01.125Z |    |    |",
                    "| b  | d  |                          | 2  | 4  |",
                    "| b  | d  | 1970-01-01T00:00:00.975Z |    |    |",
                    "| b  | d  | 1970-01-01T00:00:01Z     | 20 | 40 |",
                    "| b  | d  | 1970-01-01T00:00:01.025Z |    |    |",
                    "| b  | d  | 1970-01-01T00:00:01.050Z |    |    |",
                    "| b  | d  | 1970-01-01T00:00:01.075Z |    |    |",
                    "| b  | d  | 1970-01-01T00:00:01.100Z | 21 | 41 |",
                    "| b  | d  | 1970-01-01T00:00:01.125Z |    |    |",
                    "+----+----+--------------------------+----+----+",
                ];
                assert_batches_eq!(expected, &batches);
                assert_batch_count(&batches, output_batch_size);
            }
        }
        Ok(())
    }

    fn assert_batch_count(actual_batches: &[RecordBatch], batch_size: usize) {
        let num_rows = actual_batches.iter().map(|b| b.num_rows()).sum::<usize>();
        let expected_batch_count = f64::ceil(num_rows as f64 / batch_size as f64) as usize;
        assert_eq!(expected_batch_count, actual_batches.len());
    }

    type ExprVec = Vec<Arc<dyn PhysicalExpr>>;

    struct TestRecords {
        group_cols: Vec<Vec<Option<&'static str>>>,
        // Stored as millisecods since intervals use millis,
        // to let test cases be consistent and easier to read.
        time_col: Vec<Option<i64>>,
        agg_cols: Vec<Vec<Option<i64>>>,
        input_batch_size: usize,
    }

    impl TestRecords {
        fn schema(&self) -> SchemaRef {
            // In order to test input with null timestamps, we need the
            // timestamp column to be nullable. Unforunately this means
            // we can't use the IOx schema builder here.
            let mut fields = vec![];
            for i in 0..self.group_cols.len() {
                fields.push(Field::new(
                    format!("g{i}"),
                    (&InfluxColumnType::Tag).into(),
                    true,
                ));
            }
            fields.push(Field::new(
                "time",
                (&InfluxColumnType::Timestamp).into(),
                true,
            ));
            for i in 0..self.agg_cols.len() {
                fields.push(Field::new(
                    format!("a{i}"),
                    (&InfluxColumnType::Field(InfluxFieldType::Integer)).into(),
                    true,
                ));
            }
            Schema::new(fields).into()
        }

        fn len(&self) -> usize {
            self.time_col.len()
        }

        fn exprs(&self) -> Result<(ExprVec, ExprVec)> {
            let mut group_expr: ExprVec = vec![];
            let mut aggr_expr: ExprVec = vec![];
            let ngroup_cols = self.group_cols.len();
            for i in 0..self.schema().fields().len() {
                match i.cmp(&ngroup_cols) {
                    Ordering::Less => group_expr.push(Arc::new(Column::new(&format!("g{i}"), i))),
                    Ordering::Equal => group_expr.push(Arc::new(Column::new("t", i))),
                    Ordering::Greater => {
                        let idx = i - ngroup_cols + 1;
                        aggr_expr.push(Arc::new(Column::new(&format!("a{idx}"), i)));
                    }
                }
            }
            Ok((group_expr, aggr_expr))
        }
    }

    impl TryFrom<TestRecords> for Vec<RecordBatch> {
        type Error = DataFusionError;

        fn try_from(value: TestRecords) -> Result<Self> {
            let mut arrs: Vec<ArrayRef> =
                Vec::with_capacity(value.group_cols.len() + value.agg_cols.len() + 1);
            for gc in &value.group_cols {
                let arr = Arc::new(DictionaryArray::<Int32Type>::from_iter(gc.iter().cloned()));
                arrs.push(arr);
            }
            // Scale from milliseconds to the nanoseconds that are actually stored.
            let scaled_times: TimestampNanosecondArray = value
                .time_col
                .iter()
                .map(|o| o.map(|v| v * 1_000_000))
                .collect();
            arrs.push(Arc::new(scaled_times));
            for ac in &value.agg_cols {
                let arr = Arc::new(Int64Array::from_iter(ac));
                arrs.push(arr);
            }

            let one_batch =
                RecordBatch::try_new(value.schema(), arrs).map_err(DataFusionError::ArrowError)?;
            let mut batches = vec![];
            let mut offset = 0;
            while offset < one_batch.num_rows() {
                let len = std::cmp::min(value.input_batch_size, one_batch.num_rows() - offset);
                let batch = one_batch.slice(offset, len);
                batches.push(batch);
                offset += value.input_batch_size;
            }
            Ok(batches)
        }
    }

    struct TestCase {
        test_records: TestRecords,
        output_batch_size: usize,
        params: GapFillExecParams,
    }

    impl TestCase {
        async fn run(self) -> Result<Vec<RecordBatch>> {
            let schema = self.test_records.schema();
            let (group_expr, aggr_expr) = self.test_records.exprs()?;

            let input_batch_size = self.test_records.input_batch_size;

            let num_records = self.test_records.len();
            let batches: Vec<RecordBatch> = self.test_records.try_into()?;
            assert_batch_count(&batches, input_batch_size);
            assert_eq!(
                batches.iter().map(|b| b.num_rows()).sum::<usize>(),
                num_records
            );

            debug!(
                "input_batch_size is {input_batch_size}, output_batch_size is {}",
                self.output_batch_size
            );
            let input = Arc::new(MemoryExec::try_new(&[batches], schema, None)?);

            let plan = Arc::new(GapFillExec::try_new(
                input,
                group_expr,
                aggr_expr,
                self.params,
            )?);

            let session_ctx = SessionContext::with_config(
                SessionConfig::default().with_batch_size(self.output_batch_size),
            );
            let task_ctx = Arc::new(TaskContext::from(&session_ctx));
            collect(plan, task_ctx).await
        }
    }

    fn get_params_ms(batch: &TestRecords, stride: i64, start: i64, end: i64) -> GapFillExecParams {
        GapFillExecParams {
            // interval day time is milliseconds in the low 32-bit word
            stride: phys_lit(ScalarValue::IntervalDayTime(Some(stride))), // milliseconds
            time_column: Column::new("t", batch.group_cols.len()),
            origin: phys_lit(ScalarValue::TimestampNanosecond(Some(0), None)),
            // timestamps are nanos, so scale them accordingly
            time_range: Range {
                start: Bound::Included(phys_lit(ScalarValue::TimestampNanosecond(
                    Some(start * 1_000_000),
                    None,
                ))),
                end: Bound::Included(phys_lit(ScalarValue::TimestampNanosecond(
                    Some(end * 1_000_000),
                    None,
                ))),
            },
        }
    }
}
