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
use datafusion::{common::DFSchema, physical_expr::LexOrdering};
use datafusion::{
    common::internal_datafusion_err,
    physical_expr::{AnalysisContext, ExprBoundaries, analyze},
    physical_plan::{ExecutionPlan, union::UnionExec},
};
use datafusion::{common::stats::Precision, execution::context::SessionState};
use datafusion::{
    error::DataFusionError,
    logical_expr::{SortExpr, interval_arithmetic::Interval},
    physical_plan::expressions::{PhysicalSortExpr, col as physical_col},
    prelude::{Column, Expr},
};
use itertools::Itertools;
use schema::{TIME_COLUMN_NAME, sort::SortKey};
use snafu::{OptionExt, ResultExt, Snafu, ensure};

#[derive(Debug, Snafu)]
#[expect(missing_docs)]
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

pub fn logical_sort_key_exprs(sort_key: &SortKey) -> Vec<SortExpr> {
    sort_key
        .iter()
        .map(|(key, options)| {
            let expr = Expr::Column(Column::from_name(key.as_ref()));
            expr.sort(!options.descending, options.nulls_first)
        })
        .collect()
}

/// Build a datafusion [`LexOrdering`] from an iox [`SortKey`].
pub fn arrow_sort_key_exprs(sort_key: &SortKey, input_schema: &ArrowSchema) -> Option<LexOrdering> {
    LexOrdering::new(sort_key.iter().flat_map(|(key, options)| {
        // Skip over missing columns
        let expr = physical_col(key, input_schema).ok()?;
        Some(PhysicalSortExpr {
            expr,
            options: SortOptions {
                descending: options.descending,
                nulls_first: options.nulls_first,
            },
        })
    }))
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
    session_state: &SessionState,
    schema: ArrowSchemaRef,
    expr: Expr,
) -> Result<Vec<Option<Interval>>, DataFusionError> {
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
                interval: Some(interval),
                distinct_count: Precision::Absent,
            })
        })
        .collect::<Result<Vec<_>, DataFusionError>>()?;

    let context = AnalysisContext::new(boundaries);
    let df_schema = DFSchema::try_from(Arc::clone(&schema))?;
    let analysis_result = analyze(
        &session_state.create_physical_expr(expr, &df_schema)?,
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
    session_state: &SessionState,
    schema: ArrowSchemaRef,
    expr: Expr,
    name: &str,
) -> Result<Option<Interval>, DataFusionError> {
    let idx = schema.index_of(name)?;
    let mut intervals = calculate_field_intervals(session_state, Arc::clone(&schema), expr)?;
    Ok(intervals.swap_remove(idx))
}

/// Take in 1 or more children, and returns a singular [`ExecutionPlan`].
///
/// If there are multiple children, it unions in order to return a singular execution plan.
pub fn union_multiple_children(
    mut children: Vec<Arc<dyn ExecutionPlan>>,
) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
    if children.is_empty() {
        return Err(internal_datafusion_err!(
            "expected at least 1 child execution plan node"
        ));
    }

    if children.len() == 1 {
        return Ok(children.pop().expect("unreachable"));
    }
    Ok(Arc::new(UnionExec::new(children)))
}

#[cfg(test)]
mod tests {
    use datafusion::common::ScalarValue;
    use datafusion::common::rounding::next_down;
    use datafusion::execution::context::SessionContext;
    use datafusion::logical_expr::{col, lit};
    use schema::{InfluxFieldType, TIME_DATA_TIMEZONE, builder::SchemaBuilder};

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
        let session_state = SessionContext::new().state();
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
        let intervals = calculate_field_intervals(&session_state, schema, expr).unwrap();
        // 2020-01-01T00:00:00Z == 1577836800000000000
        // 2020-01-02T00:00:00Z == 1577923200000000000
        assert_eq!(
            vec![
                Some(time_interval(
                    Some(1577836800000000000),
                    Some(1577923200000000000i64 - 1),
                )),
                Some(f64_interval(Some(1000000.0), Some(next_down(2000000.0))))
            ],
            intervals
        );
    }

    #[test]
    fn test_calculate_field_intervals_no_constraints() {
        let session_state = SessionContext::new().state();
        let schema = SchemaBuilder::new()
            .timestamp()
            .influx_field("a", InfluxFieldType::Float)
            .build()
            .unwrap()
            .as_arrow();
        // must be a predicate (boolean expression)
        let expr = lit("test").eq(lit("foo"));
        let intervals = calculate_field_intervals(&session_state, schema, expr).unwrap();
        assert_eq!(vec![None, None], intervals);
    }

    #[test]
    fn test_calculate_field_interval() {
        let session_state = SessionContext::new().state();
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
        let interval =
            calculate_field_interval(&session_state, Arc::clone(&schema), expr.clone(), "time")
                .unwrap();
        assert_eq!(
            Some(time_interval(
                Some(1577836800000000000),
                Some(1577923200000000000 - 1),
            )),
            interval
        );

        let interval =
            calculate_field_interval(&session_state, Arc::clone(&schema), expr.clone(), "a")
                .unwrap();
        assert_eq!(
            Some(f64_interval(Some(1000000.0), Some(next_down(2000000.0)))),
            interval
        );

        assert_eq!(
            "Arrow error: Schema error: Unable to get field named \"b\". Valid fields: [\"time\", \"a\"]",
            calculate_field_interval(&session_state, Arc::clone(&schema), expr.clone(), "b")
                .unwrap_err()
                .to_string(),
        );
    }
}
