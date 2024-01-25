//! This module contains DataFusion utility functions and helpers

use std::{
    cmp::{max, min},
    sync::Arc,
};

use arrow::{
    array::TimestampNanosecondArray,
    compute::SortOptions,
    datatypes::{Schema as ArrowSchema, SchemaRef as ArrowSchemaRef},
    record_batch::RecordBatch,
};

use data_types::TimestampMinMax;
use datafusion::common::stats::Precision;
use datafusion::physical_expr::{analyze, AnalysisContext, ExprBoundaries};
use datafusion::{
    self,
    common::ToDFSchema,
    datasource::{provider_as_source, MemTable},
    error::DataFusionError,
    execution::context::ExecutionProps,
    logical_expr::{interval_arithmetic::Interval, LogicalPlan, LogicalPlanBuilder},
    optimizer::simplify_expressions::{ExprSimplifier, SimplifyContext},
    physical_expr::create_physical_expr,
    physical_plan::{
        expressions::{col as physical_col, PhysicalSortExpr},
        PhysicalExpr,
    },
    prelude::{Column, Expr},
};

use itertools::Itertools;
use observability_deps::tracing::trace;
use schema::{sort::SortKey, TIME_COLUMN_NAME};
use snafu::{ensure, OptionExt, ResultExt, Snafu};

#[derive(Debug, Snafu)]
#[allow(missing_copy_implementations, missing_docs)]
pub enum Error {
    #[snafu(display("The Record batch is empty"))]
    EmptyBatch,

    #[snafu(display("Error while searching Time column in a Record Batch"))]
    TimeColumn { source: arrow::error::ArrowError },

    #[snafu(display("Error while casting Timenanosecond on Time column"))]
    TimeCasting,

    #[snafu(display("Time column does not have value"))]
    TimeValue,

    #[snafu(display("Time column is null"))]
    TimeNull,
}

/// A specialized `Error`
pub type Result<T, E = Error> = std::result::Result<T, E>;

/// Create a logical plan that produces the record batch
pub fn make_scan_plan(batch: RecordBatch) -> std::result::Result<LogicalPlan, DataFusionError> {
    let schema = batch.schema();
    let partitions = vec![vec![batch]];
    let projection = None; // scan all columns

    let table = MemTable::try_new(schema, partitions)?;

    let source = provider_as_source(Arc::new(table));

    LogicalPlanBuilder::scan("memtable", source, projection)?.build()
}

pub fn logical_sort_key_exprs(sort_key: &SortKey) -> Vec<Expr> {
    sort_key
        .iter()
        .map(|(key, options)| {
            let expr = Expr::Column(Column::from_name(key.as_ref()));
            expr.sort(!options.descending, options.nulls_first)
        })
        .collect()
}

pub fn arrow_sort_key_exprs(
    sort_key: &SortKey,
    input_schema: &ArrowSchema,
) -> Vec<PhysicalSortExpr> {
    sort_key
        .iter()
        .flat_map(|(key, options)| {
            // Skip over missing columns
            let expr = physical_col(key, input_schema).ok()?;
            Some(PhysicalSortExpr {
                expr,
                options: SortOptions {
                    descending: options.descending,
                    nulls_first: options.nulls_first,
                },
            })
        })
        .collect()
}

/// Build a datafusion physical expression from a logical one
pub fn df_physical_expr(
    schema: ArrowSchemaRef,
    expr: Expr,
) -> std::result::Result<Arc<dyn PhysicalExpr>, DataFusionError> {
    let df_schema = Arc::clone(&schema).to_dfschema_ref()?;

    let props = ExecutionProps::new();
    let simplifier =
        ExprSimplifier::new(SimplifyContext::new(&props).with_schema(Arc::clone(&df_schema)));

    // apply type coercion here to ensure types match
    trace!(%df_schema, "input schema");
    let expr = simplifier.coerce(expr, Arc::clone(&df_schema))?;
    trace!(%expr, "coerced logical expression");

    create_physical_expr(&expr, df_schema.as_ref(), schema.as_ref(), &props)
}

/// Return min and max for column `time` of the given set of record batches by
/// performing an `O(n)` scan of all provided batches.
pub fn compute_timenanosecond_min_max<'a, I>(batches: I) -> Result<TimestampMinMax>
where
    I: IntoIterator<Item = &'a RecordBatch>,
{
    let mut min_time = i64::MAX;
    let mut max_time = i64::MIN;
    for batch in batches {
        let (mi, ma) = compute_timenanosecond_min_max_for_one_record_batch(batch)?;
        min_time = min(min_time, mi);
        max_time = max(max_time, ma);
    }
    Ok(TimestampMinMax {
        min: min_time,
        max: max_time,
    })
}

/// Return min and max for column `time` in the given record batch by performing
/// an `O(n)` scan of `batch`.
pub fn compute_timenanosecond_min_max_for_one_record_batch(
    batch: &RecordBatch,
) -> Result<(i64, i64)> {
    ensure!(batch.num_columns() > 0, EmptyBatchSnafu);

    let index = batch
        .schema()
        .index_of(TIME_COLUMN_NAME)
        .context(TimeColumnSnafu {})?;

    let time_col = batch
        .column(index)
        .as_any()
        .downcast_ref::<TimestampNanosecondArray>()
        .context(TimeCastingSnafu {})?;

    let (min, max) = match time_col.iter().minmax() {
        itertools::MinMaxResult::NoElements => return Err(Error::TimeValue),
        itertools::MinMaxResult::OneElement(val) => {
            let val = val.context(TimeNullSnafu)?;
            (val, val)
        }
        itertools::MinMaxResult::MinMax(min, max) => {
            (min.context(TimeNullSnafu)?, max.context(TimeNullSnafu)?)
        }
    };

    Ok((min, max))
}

/// Determine the possible maximum range for each of the fields in a
/// ['ArrowSchema'] once the ['Expr'] has been applied. The returned
/// Vec includes an Interval for every field in the schema in the same
/// order. Any fileds that are not constrained by the expression will
/// have an unbounded interval.
pub fn calculate_field_intervals(
    schema: ArrowSchemaRef,
    expr: Expr,
) -> Result<Vec<Interval>, DataFusionError> {
    // make unknown boundaries for each column
    // TODO use upstream code when https://github.com/apache/arrow-datafusion/pull/8377 is merged
    let fields = schema.fields();
    let boundaries = fields
        .iter()
        .enumerate()
        .map(|(i, field)| {
            let column = datafusion::physical_expr::expressions::Column::new(field.name(), i);
            let interval = Interval::make_unbounded(field.data_type())?;
            Ok(ExprBoundaries {
                column,
                interval,
                distinct_count: Precision::Absent,
            })
        })
        .collect::<Result<Vec<_>, DataFusionError>>()?;

    let context = AnalysisContext::new(boundaries);
    let analysis_result = analyze(
        &df_physical_expr(Arc::clone(&schema), expr)?,
        context,
        &schema,
    )?;

    let intervals = analysis_result
        .boundaries
        .into_iter()
        .map(|b| b.interval)
        .collect::<Vec<_>>();

    Ok(intervals)
}

/// Determine the possible maximum range for the named field in the
/// ['ArrowSchema'] once the ['Expr'] has been applied.
pub fn calculate_field_interval(
    schema: ArrowSchemaRef,
    expr: Expr,
    name: &str,
) -> Result<Interval, DataFusionError> {
    let idx = schema.index_of(name)?;
    let mut intervals = calculate_field_intervals(Arc::clone(&schema), expr)?;
    Ok(intervals.swap_remove(idx))
}

#[cfg(test)]
mod tests {
    use datafusion::common::rounding::next_down;
    use datafusion::common::ScalarValue;
    use datafusion::logical_expr::{col, lit};
    use schema::{builder::SchemaBuilder, InfluxFieldType, TIME_DATA_TIMEZONE};

    use super::*;

    fn time_interval(lower: Option<i64>, upper: Option<i64>) -> Interval {
        let lower = ScalarValue::TimestampNanosecond(lower, TIME_DATA_TIMEZONE());
        let upper = ScalarValue::TimestampNanosecond(upper, TIME_DATA_TIMEZONE());
        Interval::try_new(lower, upper).unwrap()
    }

    fn f64_interval(lower: Option<f64>, upper: Option<f64>) -> Interval {
        let lower = ScalarValue::Float64(lower);
        let upper = ScalarValue::Float64(upper);
        Interval::try_new(lower, upper).unwrap()
    }

    #[test]
    fn test_calculate_field_intervals() {
        let schema = SchemaBuilder::new()
            .timestamp()
            .influx_field("a", InfluxFieldType::Float)
            .build()
            .unwrap()
            .as_arrow();
        let expr = col("time")
            .gt_eq(lit("2020-01-01T00:00:00Z"))
            .and(col("time").lt(lit("2020-01-02T00:00:00Z")))
            .and(col("a").gt_eq(lit(1000000.0)))
            .and(col("a").lt(lit(2000000.0)));
        let intervals = calculate_field_intervals(schema, expr).unwrap();
        // 2020-01-01T00:00:00Z == 1577836800000000000
        // 2020-01-02T00:00:00Z == 1577923200000000000
        assert_eq!(
            vec![
                time_interval(Some(1577836800000000000), Some(1577923200000000000i64 - 1),),
                f64_interval(Some(1000000.0), Some(next_down(2000000.0)))
            ],
            intervals
        );
    }

    #[test]
    fn test_calculate_field_intervals_no_constraints() {
        let schema = SchemaBuilder::new()
            .timestamp()
            .influx_field("a", InfluxFieldType::Float)
            .build()
            .unwrap()
            .as_arrow();
        // must be a predicate (boolean expression)
        let expr = lit("test").eq(lit("foo"));
        let intervals = calculate_field_intervals(schema, expr).unwrap();
        assert_eq!(
            vec![time_interval(None, None), f64_interval(None, None)],
            intervals
        );
    }

    #[test]
    fn test_calculate_field_interval() {
        let schema = SchemaBuilder::new()
            .timestamp()
            .influx_field("a", InfluxFieldType::Float)
            .build()
            .unwrap()
            .as_arrow();
        let expr = col("time")
            .gt_eq(lit("2020-01-01T00:00:00Z"))
            .and(col("time").lt(lit("2020-01-02T00:00:00Z")))
            .and(col("a").gt_eq(lit(1000000.0)))
            .and(col("a").lt(lit(2000000.0)));

        // Note
        // 2020-01-01T00:00:00Z == 1577836800000000000
        // 2020-01-02T00:00:00Z == 1577923200000000000
        let interval = calculate_field_interval(Arc::clone(&schema), expr.clone(), "time").unwrap();
        assert_eq!(
            time_interval(Some(1577836800000000000), Some(1577923200000000000 - 1),),
            interval
        );

        let interval = calculate_field_interval(Arc::clone(&schema), expr.clone(), "a").unwrap();
        assert_eq!(
            f64_interval(Some(1000000.0), Some(next_down(2000000.0))),
            interval
        );

        assert_eq!(
            "Arrow error: Schema error: Unable to get field named \"b\". Valid fields: [\"time\", \"a\"]",
            calculate_field_interval(Arc::clone(&schema), expr.clone(), "b").unwrap_err().to_string(),
        );
    }
}
