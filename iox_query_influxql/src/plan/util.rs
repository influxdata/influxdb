use crate::plan::{error, util_copy};
use arrow::datatypes::DataType;
use datafusion::common::{DFSchema, DFSchemaRef, Result};
use datafusion::logical_expr::utils::expr_as_column_expr;
use datafusion::logical_expr::{coalesce, lit, Expr, ExprSchemable, LogicalPlan, Operator};
use influxdb_influxql_parser::expression::BinaryOperator;
use influxdb_influxql_parser::literal::Number;
use influxdb_influxql_parser::string::Regex;
use query_functions::clean_non_meta_escapes;
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

/// Returns `n` as a literal expression of the specified `data_type`.
fn number_to_expr(n: &Number, data_type: DataType) -> Result<Expr> {
    Ok(match (n, data_type) {
        (Number::Integer(v), DataType::Int64) => lit(*v),
        (Number::Integer(v), DataType::Float64) => lit(*v as f64),
        (Number::Integer(v), DataType::UInt64) => lit(*v as u64),
        (Number::Float(v), DataType::Int64) => lit(*v as i64),
        (Number::Float(v), DataType::Float64) => lit(*v),
        (Number::Float(v), DataType::UInt64) => lit(*v as u64),
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
                Some(coalesce(vec![col_expr, number_to_expr(value, data_type)?]))
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
