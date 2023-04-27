use crate::plan::{error, util_copy};
use arrow::datatypes::{DataType, TimeUnit};
use datafusion::common::{DFSchema, DFSchemaRef, Result};
use datafusion::logical_expr::utils::expr_as_column_expr;
use datafusion::logical_expr::{lit, Expr, ExprSchemable, LogicalPlan, Operator};
use datafusion::scalar::ScalarValue;
use influxdb_influxql_parser::expression::BinaryOperator;
use influxdb_influxql_parser::literal::Number;
use influxdb_influxql_parser::string::Regex;
use query_functions::clean_non_meta_escapes;
use query_functions::coalesce_struct::coalesce_struct;
use schema::Schema;
use std::sync::Arc;

pub(in crate::plan) fn binary_operator_to_df_operator(op: BinaryOperator) -> Operator {
    match op {
        BinaryOperator::Add => Operator::Plus,
        BinaryOperator::Sub => Operator::Minus,
        BinaryOperator::Mul => Operator::Multiply,
        BinaryOperator::Div => Operator::Divide,
        BinaryOperator::Mod => Operator::Modulo,
        BinaryOperator::BitwiseAnd => Operator::BitwiseAnd,
        BinaryOperator::BitwiseOr => Operator::BitwiseOr,
        BinaryOperator::BitwiseXor => Operator::BitwiseXor,
    }
}

/// Return the IOx schema for the specified DataFusion schema.
pub(in crate::plan) fn schema_from_df(schema: &DFSchema) -> Result<Schema> {
    let s: Arc<arrow::datatypes::Schema> = Arc::new(schema.into());
    s.try_into().map_err(|err| {
        error::map::internal(format!(
            "unable to convert DataFusion schema to IOx schema: {err}"
        ))
    })
}

/// Container for both the DataFusion and equivalent IOx schema.
pub(in crate::plan) struct Schemas {
    pub(in crate::plan) df_schema: DFSchemaRef,
    pub(in crate::plan) iox_schema: Schema,
}

impl Schemas {
    pub(in crate::plan) fn new(df_schema: &DFSchemaRef) -> Result<Self> {
        Ok(Self {
            df_schema: Arc::clone(df_schema),
            iox_schema: schema_from_df(df_schema)?,
        })
    }
}

/// Sanitize an InfluxQL regular expression and create a compiled [`regex::Regex`].
pub(crate) fn parse_regex(re: &Regex) -> Result<regex::Regex> {
    let pattern = clean_non_meta_escapes(re.as_str());
    regex::Regex::new(&pattern)
        .map_err(|e| error::map::query(format!("invalid regular expression '{re}': {e}")))
}

/// Returns `n` as a scalar value of the specified `data_type`.
fn number_to_scalar(n: &Number, data_type: &DataType) -> Result<ScalarValue> {
    Ok(match (n, data_type) {
        (Number::Integer(v), DataType::Int64) => ScalarValue::from(*v),
        (Number::Integer(v), DataType::Float64) => ScalarValue::from(*v as f64),
        (Number::Integer(v), DataType::UInt64) => ScalarValue::from(*v as u64),
        (Number::Integer(v), DataType::Timestamp(TimeUnit::Nanosecond, tz)) => {
            ScalarValue::TimestampNanosecond(Some(*v), tz.clone())
        }
        (Number::Float(v), DataType::Int64) => ScalarValue::from(*v as i64),
        (Number::Float(v), DataType::Float64) => ScalarValue::from(*v),
        (Number::Float(v), DataType::UInt64) => ScalarValue::from(*v as u64),
        (Number::Float(v), DataType::Timestamp(TimeUnit::Nanosecond, tz)) => {
            ScalarValue::TimestampNanosecond(Some(*v as i64), tz.clone())
        }
        (n, DataType::Struct(fields)) => ScalarValue::Struct(
            Some(
                fields
                    .iter()
                    .map(|f| number_to_scalar(n, f.data_type()))
                    .collect::<Result<Vec<_>>>()?,
            ),
            fields.clone(),
        ),
        (n, data_type) => {
            // The only output data types expected are Int64, Float64 or UInt64
            return error::internal(format!("no conversion from {n} to {data_type}"));
        }
    })
}

/// Rebuilds an `Expr` as a projection on top of a collection of `Expr`'s.
///
/// For example, the expression `a + b < 1` would require, as input, the 2
/// individual columns, `a` and `b`. But, if the base expressions already
/// contain the `a + b` result, then that may be used in lieu of the `a` and
/// `b` columns.
///
/// This is useful in the context of a query like:
///
/// SELECT a + b < 1 ... GROUP BY a + b
///
/// where post-aggregation, `a + b` need not be a projection against the
/// individual columns `a` and `b`, but rather it is a projection against the
/// `a + b` found in the GROUP BY.
///
/// `fill_if_null` will be used to coalesce any expressions from `NULL`.
/// This is used with the `FILL(<value>)` strategy.
pub(crate) fn rebase_expr(
    expr: &Expr,
    base_exprs: &[Expr],
    fill_if_null: &Option<Number>,
    plan: &LogicalPlan,
) -> Result<Expr> {
    if let Some(value) = fill_if_null {
        util_copy::clone_with_replacement(expr, &|nested_expr| {
            Ok(if base_exprs.contains(nested_expr) {
                let col_expr = expr_as_column_expr(nested_expr, plan)?;
                let data_type = col_expr.get_type(plan.schema())?;
                Some(coalesce_struct(vec![
                    col_expr,
                    lit(number_to_scalar(value, &data_type)?),
                ]))
            } else {
                None
            })
        })
    } else {
        util_copy::clone_with_replacement(expr, &|nested_expr| {
            Ok(if base_exprs.contains(nested_expr) {
                Some(expr_as_column_expr(nested_expr, plan)?)
            } else {
                None
            })
        })
    }
}
