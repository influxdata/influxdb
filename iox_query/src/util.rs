//! This module contains DataFusion utility functions and helpers

use std::{
    cmp::{max, min},
    sync::Arc,
};

use arrow::{
    array::TimestampNanosecondArray, compute::SortOptions, datatypes::Schema as ArrowSchema,
    record_batch::RecordBatch,
};

use data_types::TimestampMinMax;
use datafusion::{
    self,
    common::ToDFSchema,
    datasource::{provider_as_source, MemTable},
    error::DataFusionError,
    execution::context::ExecutionProps,
    logical_expr::{LogicalPlan, LogicalPlanBuilder},
    optimizer::simplify_expressions::{ExprSimplifier, SimplifyContext},
    physical_expr::create_physical_expr,
    physical_plan::{
        expressions::{col as physical_col, PhysicalSortExpr},
        ColumnStatistics, ExecutionPlan, PhysicalExpr, Statistics,
    },
    prelude::{Column, Expr},
    scalar::ScalarValue,
};

use itertools::Itertools;
use observability_deps::tracing::trace;
use schema::{sort::SortKey, InfluxColumnType, Schema, TIME_COLUMN_NAME};
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

/// Returns the pk in arrow's expression used for data sorting
pub fn arrow_pk_sort_exprs(
    key_columns: Vec<&str>,
    input_schema: &ArrowSchema,
) -> Vec<PhysicalSortExpr> {
    let mut sort_exprs = vec![];
    for key in key_columns {
        let expr = physical_col(key, input_schema).expect("pk in schema");
        sort_exprs.push(PhysicalSortExpr {
            expr,
            options: SortOptions {
                descending: false,
                nulls_first: false,
            },
        });
    }

    sort_exprs
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
    input: &dyn ExecutionPlan,
    expr: Expr,
) -> std::result::Result<Arc<dyn PhysicalExpr>, DataFusionError> {
    let schema = input.schema();

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

/// Return min and max for column `time` of the given set of record batches
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

/// Return min and max for column `time` in the given record batch
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

/// Create basic table summary.
///
/// This contains:
/// - correct column types
/// - [total count](Statistics::num_rows)
/// - [min](ColumnStatistics::min_value)/[max](ColumnStatistics::max_value) for the timestamp column
pub fn create_basic_summary(
    row_count: u64,
    schema: &Schema,
    ts_min_max: Option<TimestampMinMax>,
) -> Statistics {
    let mut columns = Vec::with_capacity(schema.len());

    for (t, _field) in schema.iter() {
        let stats = match t {
            InfluxColumnType::Timestamp => ColumnStatistics {
                null_count: Some(0),
                max_value: Some(ScalarValue::TimestampNanosecond(
                    ts_min_max.map(|v| v.max),
                    None,
                )),
                min_value: Some(ScalarValue::TimestampNanosecond(
                    ts_min_max.map(|v| v.min),
                    None,
                )),
                distinct_count: None,
            },
            _ => ColumnStatistics::default(),
        };
        columns.push(stats)
    }

    Statistics {
        num_rows: Some(row_count as usize),
        total_byte_size: None,
        column_statistics: Some(columns),
        is_exact: true,
    }
}

#[cfg(test)]
mod tests {
    use datafusion::scalar::ScalarValue;
    use schema::{builder::SchemaBuilder, InfluxFieldType};

    use super::*;

    #[test]
    fn test_create_basic_summary_no_columns_no_rows() {
        let schema = SchemaBuilder::new().build().unwrap();
        let row_count = 0;

        let actual = create_basic_summary(row_count, &schema, None);
        let expected = Statistics {
            num_rows: Some(row_count as usize),
            total_byte_size: None,
            column_statistics: Some(vec![]),
            is_exact: true,
        };
        assert_eq!(actual, expected);
    }

    #[test]
    fn test_create_basic_summary_no_rows() {
        let schema = full_schema();
        let row_count = 0;
        let ts_min_max = TimestampMinMax { min: 10, max: 20 };

        let actual = create_basic_summary(row_count, &schema, Some(ts_min_max));
        let expected = Statistics {
            num_rows: Some(0),
            total_byte_size: None,
            column_statistics: Some(vec![
                ColumnStatistics::default(),
                ColumnStatistics::default(),
                ColumnStatistics::default(),
                ColumnStatistics::default(),
                ColumnStatistics::default(),
                ColumnStatistics::default(),
                ColumnStatistics {
                    null_count: Some(0),
                    min_value: Some(ScalarValue::TimestampNanosecond(Some(10), None)),
                    max_value: Some(ScalarValue::TimestampNanosecond(Some(20), None)),
                    distinct_count: None,
                },
            ]),
            is_exact: true,
        };
        assert_eq!(actual, expected);
    }

    #[test]
    fn test_create_basic_summary() {
        let schema = full_schema();
        let row_count = 3;
        let ts_min_max = TimestampMinMax { min: 42, max: 42 };

        let actual = create_basic_summary(row_count, &schema, Some(ts_min_max));
        let expected = Statistics {
            num_rows: Some(3),
            total_byte_size: None,
            column_statistics: Some(vec![
                ColumnStatistics::default(),
                ColumnStatistics::default(),
                ColumnStatistics::default(),
                ColumnStatistics::default(),
                ColumnStatistics::default(),
                ColumnStatistics::default(),
                ColumnStatistics {
                    null_count: Some(0),
                    min_value: Some(ScalarValue::TimestampNanosecond(Some(42), None)),
                    max_value: Some(ScalarValue::TimestampNanosecond(Some(42), None)),
                    distinct_count: None,
                },
            ]),
            is_exact: true,
        };
        assert_eq!(actual, expected);
    }

    fn full_schema() -> Schema {
        SchemaBuilder::new()
            .tag("tag")
            .influx_field("field_bool", InfluxFieldType::Boolean)
            .influx_field("field_float", InfluxFieldType::Float)
            .influx_field("field_integer", InfluxFieldType::Integer)
            .influx_field("field_string", InfluxFieldType::String)
            .influx_field("field_uinteger", InfluxFieldType::UInteger)
            .timestamp()
            .build()
            .unwrap()
    }
}
