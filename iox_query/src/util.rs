//! This module contains DataFusion utility functions and helpers

use std::{
    cmp::{max, min},
    convert::TryInto,
    sync::Arc,
};

use arrow::{
    array::TimestampNanosecondArray,
    compute::SortOptions,
    datatypes::{DataType, Schema as ArrowSchema},
    record_batch::RecordBatch,
};

use data_types::{
    ColumnSummary, InfluxDbType, StatValues, Statistics, TableSummary, TimestampMinMax,
};
use datafusion::{
    self,
    common::{DFSchema, ToDFSchema},
    datasource::{provider_as_source, MemTable},
    error::{DataFusionError, Result as DatafusionResult},
    execution::context::ExecutionProps,
    logical_expr::{
        expr_rewriter::ExprRewriter, BinaryExpr, ExprSchemable, LogicalPlan, LogicalPlanBuilder,
    },
    optimizer::simplify_expressions::{ExprSimplifier, SimplifyContext},
    physical_expr::create_physical_expr,
    physical_plan::{
        expressions::{col as physical_col, PhysicalSortExpr},
        ExecutionPlan, PhysicalExpr,
    },
    prelude::{binary_expr, lit, Expr},
    scalar::ScalarValue,
};

use itertools::Itertools;
use observability_deps::tracing::trace;
use predicate::rpc_predicate::{FIELD_COLUMN_NAME, MEASUREMENT_COLUMN_NAME};
use schema::{sort::SortKey, InfluxColumnType, InfluxFieldType, Schema, TIME_COLUMN_NAME};
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

/// Rewrites the provided expr such that references to any column that
/// are not present in `schema` become null.
///
/// So for example, if the predicate is
///
/// `(STATE = 'CA') OR (READING >0)`
///
/// but the schema only has `STATE` (and not `READING`), then the
/// predicate is rewritten to
///
/// `(STATE = 'CA') OR (NULL >0)`
///
/// This matches the Influx data model where any value that is not
/// explicitly specified is implicitly NULL. Since different chunks
/// and measurements can have different subsets of the columns, only
/// parts of the predicate make sense.
/// See comments on 'is_null_column'
#[derive(Debug)]
pub struct MissingColumnsToNull<'a> {
    schema: &'a Schema,
    df_schema: DFSchema,
}

impl<'a> MissingColumnsToNull<'a> {
    pub fn new(schema: &'a Schema) -> Self {
        let df_schema: DFSchema = schema
            .as_arrow()
            .as_ref()
            .clone()
            .try_into()
            .expect("Create DF Schema");

        Self { schema, df_schema }
    }

    /// Returns true if `expr` is a `Expr::Column` reference to a
    /// column that doesn't exist in this schema
    fn is_null_column(&self, expr: &Expr) -> bool {
        if let Expr::Column(column) = &expr {
            if column.name != MEASUREMENT_COLUMN_NAME && column.name != FIELD_COLUMN_NAME {
                return self.schema.find_index_of(&column.name).is_none();
            }
        }
        false
    }

    /// Rewrites an arg like col if col refers to a non existent
    /// column into a null literal with "type" of `other_arg`, if possible
    fn rewrite_op_arg(&self, arg: Expr, other_arg: &Expr) -> DatafusionResult<Expr> {
        if self.is_null_column(&arg) {
            let other_datatype = match other_arg.get_type(&self.df_schema) {
                Ok(other_datatype) => other_datatype,
                Err(_) => {
                    // the other arg is also unknown and will be
                    // rewritten, default to Int32 (sins due to
                    // https://github.com/apache/arrow-datafusion/issues/1179)
                    DataType::Int32
                }
            };

            let scalar: ScalarValue = (&other_datatype).try_into()?;
            Ok(Expr::Literal(scalar))
        } else {
            Ok(arg)
        }
    }
}

impl<'a> ExprRewriter for MissingColumnsToNull<'a> {
    fn mutate(&mut self, expr: Expr) -> DatafusionResult<Expr> {
        // Ideally this would simply find all Expr::Columns and
        // replace them with a constant NULL value. However, doing do
        // is blocked on DF bug
        // https://github.com/apache/arrow-datafusion/issues/1179
        //
        // Until then, we need to know what type of expr the column is
        // being compared with, so workaround by finding the datatype of the other arg
        match expr {
            Expr::BinaryExpr(BinaryExpr { left, op, right }) => {
                let left = self.rewrite_op_arg(*left, &right)?;
                let right = self.rewrite_op_arg(*right, &left)?;
                Ok(binary_expr(left, op, right))
            }
            Expr::IsNull(expr) if self.is_null_column(&expr) => Ok(lit(true)),
            Expr::IsNotNull(expr) if self.is_null_column(&expr) => Ok(lit(false)),
            expr => Ok(expr),
        }
    }
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
/// - [total count](StatValues::total_count) for all columns
/// - [min](StatValues::min)/[max](StatValues::max) for the timestamp column
pub fn create_basic_summary(
    row_count: u64,
    schema: &Schema,
    ts_min_max: TimestampMinMax,
) -> TableSummary {
    let mut columns = Vec::with_capacity(schema.len());
    for i in 0..schema.len() {
        let (t, field) = schema.field(i);

        let influxdb_type = match t {
            InfluxColumnType::Tag => InfluxDbType::Tag,
            InfluxColumnType::Field(_) => InfluxDbType::Field,
            InfluxColumnType::Timestamp => InfluxDbType::Timestamp,
        };

        let stats = match t {
            InfluxColumnType::Tag | InfluxColumnType::Field(InfluxFieldType::String) => {
                Statistics::String(StatValues {
                    min: None,
                    max: None,
                    total_count: row_count,
                    null_count: None,
                    distinct_count: None,
                })
            }
            InfluxColumnType::Timestamp => Statistics::I64(StatValues {
                min: Some(ts_min_max.min),
                max: Some(ts_min_max.max),
                total_count: row_count,
                null_count: None,
                distinct_count: None,
            }),
            InfluxColumnType::Field(InfluxFieldType::Integer) => Statistics::I64(StatValues {
                min: None,
                max: None,
                total_count: row_count,
                null_count: None,
                distinct_count: None,
            }),
            InfluxColumnType::Field(InfluxFieldType::UInteger) => Statistics::U64(StatValues {
                min: None,
                max: None,
                total_count: row_count,
                null_count: None,
                distinct_count: None,
            }),
            InfluxColumnType::Field(InfluxFieldType::Float) => Statistics::F64(StatValues {
                min: None,
                max: None,
                total_count: row_count,
                null_count: None,
                distinct_count: None,
            }),
            InfluxColumnType::Field(InfluxFieldType::Boolean) => Statistics::Bool(StatValues {
                min: None,
                max: None,
                total_count: row_count,
                null_count: None,
                distinct_count: None,
            }),
        };

        columns.push(ColumnSummary {
            name: field.name().clone(),
            influxdb_type,
            stats,
        })
    }

    TableSummary { columns }
}

#[cfg(test)]
mod tests {
    use arrow::datatypes::DataType;
    use datafusion::{
        logical_expr::expr_rewriter::ExprRewritable,
        prelude::{col, lit},
        scalar::ScalarValue,
    };
    use schema::builder::SchemaBuilder;

    use super::*;

    #[test]
    fn test_missing_colums_to_null() {
        let schema = SchemaBuilder::new()
            .tag("tag")
            .field("str", DataType::Utf8)
            .unwrap()
            .field("int", DataType::Int64)
            .unwrap()
            .field("uint", DataType::UInt64)
            .unwrap()
            .field("float", DataType::Float64)
            .unwrap()
            .field("bool", DataType::Boolean)
            .unwrap()
            .build()
            .unwrap();

        // The fact that these need to be typed is due to
        // https://github.com/apache/arrow-datafusion/issues/1179
        let utf8_null = Expr::Literal(ScalarValue::Utf8(None));
        let int32_null = Expr::Literal(ScalarValue::Int32(None));

        // no rewrite
        let expr = lit(1);
        let expected = expr.clone();
        assert_rewrite(&schema, &expr, &expected);

        // tag != str (no rewrite)
        let expr = col("tag").not_eq(col("str"));
        let expected = expr.clone();
        assert_rewrite(&schema, &expr, &expected);

        // tag == str (no rewrite)
        let expr = col("tag").eq(col("str"));
        let expected = expr.clone();
        assert_rewrite(&schema, &expr, &expected);

        // int < 5 (no rewrite, int part of schema)
        let expr = col("int").lt(lit(5));
        let expected = expr.clone();
        assert_rewrite(&schema, &expr, &expected);

        // unknown < 5 --> NULL < 5 (unknown not in schema)
        let expr = col("unknown").lt(lit(5));
        let expected = int32_null.clone().lt(lit(5));
        assert_rewrite(&schema, &expr, &expected);

        // 5 < unknown --> 5 < NULL (unknown not in schema)
        let expr = lit(5).lt(col("unknown"));
        let expected = lit(5).lt(int32_null.clone());
        assert_rewrite(&schema, &expr, &expected);

        // _measurement < 5 --> _measurement < 5 (special column)
        let expr = col("_measurement").lt(lit(5));
        let expected = expr.clone();
        assert_rewrite(&schema, &expr, &expected);

        // _field < 5 --> _field < 5 (special column)
        let expr = col("_field").lt(lit(5));
        let expected = expr.clone();
        assert_rewrite(&schema, &expr, &expected);

        // _field < 5 OR col("unknown") < 5 --> _field < 5 OR (NULL < 5)
        let expr = col("_field").lt(lit(5)).or(col("unknown").lt(lit(5)));
        let expected = col("_field").lt(lit(5)).or(int32_null.clone().lt(lit(5)));
        assert_rewrite(&schema, &expr, &expected);

        // unknown < unknown2 -->  NULL < NULL (both unknown columns)
        let expr = col("unknown").lt(col("unknown2"));
        let expected = int32_null.clone().lt(int32_null);
        assert_rewrite(&schema, &expr, &expected);

        // int < 5 OR unknown != "foo"
        let expr = col("int").lt(lit(5)).or(col("unknown").not_eq(lit("foo")));
        let expected = col("int").lt(lit(5)).or(utf8_null.not_eq(lit("foo")));
        assert_rewrite(&schema, &expr, &expected);

        // int IS NULL
        let expr = col("int").is_null();
        let expected = expr.clone();
        assert_rewrite(&schema, &expr, &expected);

        // unknown IS NULL --> true
        let expr = col("unknown").is_null();
        let expected = lit(true);
        assert_rewrite(&schema, &expr, &expected);

        // int IS NOT NULL
        let expr = col("int").is_not_null();
        let expected = expr.clone();
        assert_rewrite(&schema, &expr, &expected);

        // unknown IS NOT NULL --> false
        let expr = col("unknown").is_not_null();
        let expected = lit(false);
        assert_rewrite(&schema, &expr, &expected);
    }

    fn assert_rewrite(schema: &Schema, expr: &Expr, expected: &Expr) {
        let mut rewriter = MissingColumnsToNull::new(schema);
        let rewritten_expr = expr
            .clone()
            .rewrite(&mut rewriter)
            .expect("Rewrite successful");

        assert_eq!(
            &rewritten_expr, expected,
            "Mismatch rewriting\nInput: {expr}\nRewritten: {rewritten_expr}\nExpected: {expected}"
        );
    }

    #[test]
    fn test_create_basic_summary_no_columns_no_rows() {
        let schema = SchemaBuilder::new().build().unwrap();
        let row_count = 0;
        let ts_min_max = TimestampMinMax { min: 10, max: 20 };

        let actual = create_basic_summary(row_count, &schema, ts_min_max);
        let expected = TableSummary { columns: vec![] };
        assert_eq!(actual, expected);
    }

    #[test]
    fn test_create_basic_summary_no_rows() {
        let schema = full_schema();
        let row_count = 0;
        let ts_min_max = TimestampMinMax { min: 10, max: 20 };

        let actual = create_basic_summary(row_count, &schema, ts_min_max);
        let expected = TableSummary {
            columns: vec![
                ColumnSummary {
                    name: String::from("tag"),
                    influxdb_type: InfluxDbType::Tag,
                    stats: Statistics::String(StatValues {
                        min: None,
                        max: None,
                        total_count: 0,
                        null_count: None,
                        distinct_count: None,
                    }),
                },
                ColumnSummary {
                    name: String::from("field_bool"),
                    influxdb_type: InfluxDbType::Field,
                    stats: Statistics::Bool(StatValues {
                        min: None,
                        max: None,
                        total_count: 0,
                        null_count: None,
                        distinct_count: None,
                    }),
                },
                ColumnSummary {
                    name: String::from("field_float"),
                    influxdb_type: InfluxDbType::Field,
                    stats: Statistics::F64(StatValues {
                        min: None,
                        max: None,
                        total_count: 0,
                        null_count: None,
                        distinct_count: None,
                    }),
                },
                ColumnSummary {
                    name: String::from("field_integer"),
                    influxdb_type: InfluxDbType::Field,
                    stats: Statistics::I64(StatValues {
                        min: None,
                        max: None,
                        total_count: 0,
                        null_count: None,
                        distinct_count: None,
                    }),
                },
                ColumnSummary {
                    name: String::from("field_string"),
                    influxdb_type: InfluxDbType::Field,
                    stats: Statistics::String(StatValues {
                        min: None,
                        max: None,
                        total_count: 0,
                        null_count: None,
                        distinct_count: None,
                    }),
                },
                ColumnSummary {
                    name: String::from("field_uinteger"),
                    influxdb_type: InfluxDbType::Field,
                    stats: Statistics::U64(StatValues {
                        min: None,
                        max: None,
                        total_count: 0,
                        null_count: None,
                        distinct_count: None,
                    }),
                },
                ColumnSummary {
                    name: String::from("time"),
                    influxdb_type: InfluxDbType::Timestamp,
                    stats: Statistics::I64(StatValues {
                        min: Some(10),
                        max: Some(20),
                        total_count: 0,
                        null_count: None,
                        distinct_count: None,
                    }),
                },
            ],
        };
        assert_eq!(actual, expected);
    }

    #[test]
    fn test_create_basic_summary() {
        let schema = full_schema();
        let row_count = 3;
        let ts_min_max = TimestampMinMax { min: 42, max: 42 };

        let actual = create_basic_summary(row_count, &schema, ts_min_max);
        let expected = TableSummary {
            columns: vec![
                ColumnSummary {
                    name: String::from("tag"),
                    influxdb_type: InfluxDbType::Tag,
                    stats: Statistics::String(StatValues {
                        min: None,
                        max: None,
                        total_count: 3,
                        null_count: None,
                        distinct_count: None,
                    }),
                },
                ColumnSummary {
                    name: String::from("field_bool"),
                    influxdb_type: InfluxDbType::Field,
                    stats: Statistics::Bool(StatValues {
                        min: None,
                        max: None,
                        total_count: 3,
                        null_count: None,
                        distinct_count: None,
                    }),
                },
                ColumnSummary {
                    name: String::from("field_float"),
                    influxdb_type: InfluxDbType::Field,
                    stats: Statistics::F64(StatValues {
                        min: None,
                        max: None,
                        total_count: 3,
                        null_count: None,
                        distinct_count: None,
                    }),
                },
                ColumnSummary {
                    name: String::from("field_integer"),
                    influxdb_type: InfluxDbType::Field,
                    stats: Statistics::I64(StatValues {
                        min: None,
                        max: None,
                        total_count: 3,
                        null_count: None,
                        distinct_count: None,
                    }),
                },
                ColumnSummary {
                    name: String::from("field_string"),
                    influxdb_type: InfluxDbType::Field,
                    stats: Statistics::String(StatValues {
                        min: None,
                        max: None,
                        total_count: 3,
                        null_count: None,
                        distinct_count: None,
                    }),
                },
                ColumnSummary {
                    name: String::from("field_uinteger"),
                    influxdb_type: InfluxDbType::Field,
                    stats: Statistics::U64(StatValues {
                        min: None,
                        max: None,
                        total_count: 3,
                        null_count: None,
                        distinct_count: None,
                    }),
                },
                ColumnSummary {
                    name: String::from("time"),
                    influxdb_type: InfluxDbType::Timestamp,
                    stats: Statistics::I64(StatValues {
                        min: Some(42),
                        max: Some(42),
                        total_count: 3,
                        null_count: None,
                        distinct_count: None,
                    }),
                },
            ],
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
