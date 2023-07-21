//! Rewrite a DataFusion logical expression such that it behaves as similarly to InfluxQL as
//! possible.
//!
//! InfluxQL tries extremely hard to permit expressions, even when the operands are incompatible.
//! InfluxQL opts to coerce binary expressions to `NULL` or `false`, depending on whether the
//! operator is arithmetic vs conditional. There are, however, cases where evaluation fails
//! and returns an error.
//!
//! ## Implementation
//!
//! Most of the rules have been determined by examining the source code of [`Reduce`], [`EvalBool`]
//! and [`Eval`] from the Go repository.
//!
//! There are a number of edge cases and issues in the source implementation that result in
//! unexpected behaviour.
//!
//! ## Unexpected failures
//!
//! There are a number of cases where a user might expect an expression to succeed, however,
//! InfluxQL may return an error or evaluate the expression to `false` or `NULL`.
//!
//! The following are examples of where this behaviour may be observed.
//!
//! ## Numeric fields are special
//!
//! InfluxQL allows numeric fields to be referenced as part of an arithmetic expression.
//!
//! Using the `metrics.lp` data set included in this repository, observe the following queries:
//!
//! ```text
//! select free from disk limit 1;
//!
//! name: disk
//! time                free
//! ----                ----
//! 1591894310000000000 882941362176
//! ```
//! Now with expressions in the projection and condition:
//!
//! ```text
//! > select free, free + 1 from disk where free + 1 = 882941362176 + 1 limit 1;
//! name: disk
//! time                free         free_1
//! ----                ----         ------
//! 1591894310000000000 882941362176 882941362177
//! ```
//!
//! ## Tags in expressions
//!
//! Tags are only permitted in simple binary expressions, where the tag field
//! is exclusively referenced on one side and a literal string is the other.
//!
//! **🚫 Returns an error for expressions in the projection list**
//!
//! ```text
//! > select device + 'foo' from disk LIMIT 5;
//! ERR: type error: device::tag + 'foo': incompatible types: tag and string
//! ```
//!
//! **🚫 Unexpected results in conditional expressions**
//!
//! Unlike a column expression, a conditional evaluates to false 🙄
//!
//! ```text
//! > select device from disk where device + '' = 'disk1s1' LIMIT 5;
//! <null>
//! ```
//!
//! **🚫 Can't concat strings to test against a tag**
//!
//! This is an issue because the inverted index is searched for tag literal values,
//! and expressions are not supported.
//!
//! ```text
//! > select device from disk where device = 'disk1s1' + '' LIMIT 5;
//! <null>
//! ```
//!
//! ## String (and boolean) fields
//!
//! ```text
//! > select uptime_format from system;
//! name: system
//! time                uptime_format
//! ----                -------------
//! 1591894310000000000 5 days,  7:06
//! ..
//! 1591894370000000000 5 days,  7:07
//! ..
//! 1591894430000000000 5 days,  7:08
//! ..
//! ```
//!
//! **🚫 Can't perform any operations on strings in the projection**
//!
//! ```text
//! > select uptime_format + 'foo' from system;
//! ERR: type error: uptime_format::string + 'foo': incompatible types: string and string
//! ```
//!
//! **✅ Can perform operations on the non-field expression**
//!
//! ```text
//! > select uptime_format from system where uptime_format = '5 days,  ' + '7:08';
//! name: system
//! time                uptime_format
//! ----                -------------
//! 1591894430000000000 5 days,  7:08
//! 1591894440000000000 5 days,  7:08
//! 1591894450000000000 5 days,  7:08
//! ```
//!
//! **🚫 Unable to perform operations on the field expression**
//!
//! ```text
//! > select uptime_format from system where uptime_format + '' = '5 days,  ' + '7:08';
//! <null>
//! ```
//!
//!
//! [`Reduce`]: https://github.com/influxdata/influxql/blob/1ba470371ec093d57a726b143fe6ccbacf1b452b/ast.go#L4850-L4852
//! [`EvalBool`]: https://github.com/influxdata/influxql/blob/1ba470371ec093d57a726b143fe6ccbacf1b452b/ast.go#L4181-L4183
//! [`Eval`]: https://github.com/influxdata/influxql/blob/1ba470371ec093d57a726b143fe6ccbacf1b452b/ast.go#L4137
use std::sync::Arc;

use crate::plan::util::IQLSchema;
use arrow::datatypes::DataType;
use datafusion::common::tree_node::{Transformed, TreeNode, TreeNodeRewriter};
use datafusion::common::{Result, ScalarValue};
use datafusion::logical_expr::expr::{AggregateFunction, AggregateUDF, WindowFunction};
use datafusion::logical_expr::{
    binary_expr, cast, coalesce, lit, BinaryExpr, Expr, ExprSchemable, GetIndexedField, Operator,
};
use datafusion::optimizer::simplify_expressions::{ExprSimplifier, SimplifyContext};
use datafusion::physical_expr::execution_props::ExecutionProps;
use datafusion::prelude::{when, Column};
use observability_deps::tracing::trace;
use predicate::rpc_predicate::{iox_expr_rewrite, simplify_predicate};

/// Perform a series of passes to rewrite `expr` in compliance with InfluxQL behavior
/// in an effort to ensure the query executes without error.
pub(super) fn rewrite_conditional_expr(
    exec_props: &ExecutionProps,
    expr: Expr,
    schema: &IQLSchema<'_>,
) -> Result<Expr> {
    let simplify_context =
        SimplifyContext::new(exec_props).with_schema(Arc::clone(&schema.df_schema));
    let simplifier = ExprSimplifier::new(simplify_context);

    Ok(expr)
        .map(|expr| log_rewrite(expr, "original"))
        // make regex matching with invalid types produce false
        .and_then(|expr| expr.rewrite(&mut FixRegularExpressions { schema }))
        .map(|expr| log_rewrite(expr, "after fix_regular_expressions"))
        // rewrite exprs with incompatible operands to NULL or FALSE
        // (seems like FixRegularExpressions could be combined into this pass)
        .and_then(|expr| rewrite_expr(expr, schema))
        .map(|expr| log_rewrite(expr, "after rewrite_expr"))
        // Convert tag column references to CASE WHEN <tag> IS NULL THEN '' ELSE <tag> END
        .and_then(|expr| rewrite_tag_columns(expr, schema))
        .map(|expr| log_rewrite(expr, "after rewrite_tag_columns"))
        // Push comparison operators into CASE exprs:
        //     CASE WHEN tag0 IS NULL THEN '' ELSE tag0 END = 'foo'
        // becomes
        //     CASE WHEN tag0 IS NULL THEN '' = 'foo' ELSE tag0 = 'foo' END
        .and_then(iox_expr_rewrite)
        .map(|expr| log_rewrite(expr, "after iox_expr_rewrite"))
        // Coerce operand types to be compatible:
        // - convert numeric types so that operands agree
        // - convert Utf8 to Dictionary as needed
        // The next step will fail with type errors if we don't do this.
        .and_then(|expr| simplifier.coerce(expr, Arc::clone(&schema.df_schema)))
        .map(|expr| log_rewrite(expr, "after coerce"))
        // DataFusion expression simplification. This is important here because:
        //     CASE WHEN tag0 IS NULL THEN '' = 'foo' ELSE tag0 = 'foo' END
        // becomes
        //     tag0 IS NOT NULL AND tag0 = 'foo'
        .and_then(|expr| simplifier.simplify(expr))
        .map(|expr| log_rewrite(expr, "after simplify"))
        // Further simplify:
        //     tag0 IS NOT NULL AND tag0 = 'foo'
        // becomes
        //     tags = 'foo'
        // (this could be upstreamed into DataFusion)
        .and_then(simplify_predicate)
        .map(|expr| log_rewrite(expr, "after simplify_predicate"))
        .map(|expr| {
            if matches!(
                expr,
                Expr::Literal(ScalarValue::Null) | Expr::Literal(ScalarValue::Boolean(None))
            ) {
                lit(false)
            } else {
                expr
            }
        })
}

fn log_rewrite(expr: Expr, description: &str) -> Expr {
    trace!(?expr, %description, "After rewrite");
    expr
}

/// Perform a series of passes to rewrite `expr`, used as a column projection,
/// to match the behavior of InfluxQL.
pub(super) fn rewrite_field_expr(expr: Expr, schema: &IQLSchema<'_>) -> Result<Expr> {
    rewrite_expr(expr, schema)
}

/// The expression was rewritten
fn yes(expr: Expr) -> Result<Transformed<Expr>> {
    Ok(Transformed::Yes(expr))
}

/// The expression was not rewritten
fn no(expr: Expr) -> Result<Transformed<Expr>> {
    Ok(Transformed::No(expr))
}

/// Rewrite the expression tree and return a result or `NULL` if some of the operands are
/// incompatible.
///
/// Rewrite and coerce the expression tree to model the behavior
/// of an InfluxQL query.
fn rewrite_expr(expr: Expr, schema: &IQLSchema<'_>) -> Result<Expr> {
    expr.transform(&|expr| {
        match expr {
            Expr::BinaryExpr(BinaryExpr {
                ref left,
                op,
                ref right,
            }) => {
                let lhs_type = left.get_type(&schema.df_schema)?;
                let rhs_type = right.get_type(&schema.df_schema)?;

                match (lhs_type, op, rhs_type) {
                    //
                    // NULL types
                    //

                    // Operations between a NULL and any numeric type should return a NULL result
                    (
                        DataType::Null,
                        _,
                        DataType::Float64 | DataType::UInt64 | DataType::Int64
                    ) |
                    (
                        DataType::Float64 | DataType::UInt64 | DataType::Int64,
                        _,
                        DataType::Null
                    ) |
                    // and any operations on NULLs return a NULL, as DataFusion is unable
                    // to process these expressions.
                    (
                        DataType::Null,
                        _,
                        DataType::Null
                    ) => yes(lit(ScalarValue::Null)),

                    // NULL using AND or OR is rewritten as `false`, which the optimiser
                    // may short circuit.
                    (
                        DataType::Null,
                        Operator::Or | Operator::And,
                        _
                    ) => yes(binary_expr(lit(false), op, (**right).clone())),
                    (
                        _,
                        Operator::Or | Operator::And,
                        DataType::Null
                    ) => yes(binary_expr((**left).clone(), op, lit(false))),

                    // NULL with other operators is passed through to DataFusion, which is expected
                    // evaluate to false.
                    (
                        DataType::Null,
                        Operator::Eq | Operator::NotEq | Operator::Gt | Operator::Lt | Operator::GtEq | Operator::LtEq,
                        _
                    ) |
                    (
                        _,
                        Operator::Eq | Operator::NotEq | Operator::Gt | Operator::Lt | Operator::GtEq | Operator::LtEq,
                        DataType::Null
                    ) => no(expr),

                    // Any other operations with NULL should return false
                    (DataType::Null, ..) | (.., DataType::Null) => yes(lit(false)),

                    //
                    // Boolean types
                    //
                    // See: https://github.com/influxdata/influxql/blob/1ba470371ec093d57a726b143fe6ccbacf1b452b/ast.go#L4206
                    (
                        DataType::Boolean,
                        Operator::And | Operator::Or | Operator::Eq | Operator::NotEq | Operator::BitwiseAnd | Operator::BitwiseXor | Operator::BitwiseOr,
                        DataType::Boolean,
                    ) => yes(rewrite_boolean((**left).clone(), op, (**right).clone())),

                    //
                    // Numeric types
                    //
                    // The following match arms ensure the appropriate target type is chosen based
                    // on the data types of the operands. In cases where the operands are mixed
                    // types, Float64 is the highest priority, followed by UInt64 and then Int64.
                    //

                    // Float on either side of a binary expression involving numeric types
                    (
                        DataType::Float64,
                        _,
                        DataType::Float64 | DataType::Int64 | DataType::UInt64
                    ) |
                    (
                        DataType::Int64 | DataType::UInt64,
                        _,
                        DataType::Float64
                    ) => match op {
                        // Dividing by zero would return a `NULL` in DataFusion and other SQL
                        // implementations, however, InfluxQL coalesces the result to `0`.

                        // See: https://github.com/influxdata/influxql/blob/1ba470371ec093d57a726b143fe6ccbacf1b452b/ast.go#L4268-L4270
                        Operator::Divide => yes(coalesce(vec![expr, lit(0_f64)])),
                        _ => no(expr),
                    },
                    //
                    // If either of the types UInt64 and the other is UInt64 or Int64
                    //
                    (DataType::UInt64, ..) |
                    (.., DataType::UInt64) => match op {
                        Operator::Divide => yes(coalesce(vec![expr, lit(0_u64)])),
                        _ => no(expr),
                    }
                    //
                    // Finally, if both sides are Int64
                    //
                    (
                        DataType::Int64,
                        _,
                        DataType::Int64
                    ) => match op {
                        // Like Float64, dividing by zero should return 0 for InfluxQL, and
                        // the expression should be promoted to Float64, so cast both sides.
                        //
                        // See: https://github.com/influxdata/influxql/blob/1ba470371ec093d57a726b143fe6ccbacf1b452b/ast.go#L4331-L4336
                        Operator::Divide => yes(coalesce(vec![binary_expr(cast((**left).clone(), DataType::Float64), Operator::Divide, cast((**right).clone(), DataType::Float64)), lit(0_f64)])),
                        _ => no(expr),
                    },

                    //
                    // String
                    //

                    // Match any of the operators supported by InfluxQL, when the operands a strings
                    (
                        DataType::Utf8,
                        Operator::Eq | Operator::NotEq | Operator::RegexMatch | Operator::RegexNotMatch | Operator::StringConcat,
                        DataType::Utf8
                    ) => no(expr),
                    // Rewrite the + operator to the string-concatenation operator
                    (
                        DataType::Utf8,
                        Operator::Plus,
                        DataType::Utf8
                    ) => yes(binary_expr((**left).clone(), Operator::StringConcat, (**right).clone())),

                    //
                    // Dictionary (tag column) is treated the same as Utf8
                    //
                    (
                        DataType::Dictionary(..),
                        Operator::Eq | Operator::NotEq | Operator::RegexMatch | Operator::RegexNotMatch | Operator::StringConcat,
                        DataType::Utf8
                    ) |
                    (
                        DataType::Utf8,
                        Operator::Eq | Operator::NotEq | Operator::RegexMatch | Operator::RegexNotMatch | Operator::StringConcat,
                        DataType::Dictionary(..)
                    ) => no(expr),
                    (
                        DataType::Dictionary(..),
                        Operator::Plus,
                        DataType::Utf8
                    ) |
                    (
                        DataType::Utf8,
                        Operator::Plus,
                        DataType::Dictionary(..)
                    ) => no(expr),

                    //
                    // Timestamp (time-range) expressions should pass through to DataFusion.
                    //
                    (DataType::Timestamp(..), ..) => no(expr),
                    (.., DataType::Timestamp(..)) => no(expr),

                    //
                    // Unhandled binary expressions with conditional operators
                    // should return `false`.
                    //
                    // See: https://github.com/influxdata/influxql/blob/1ba470371ec093d57a726b143fe6ccbacf1b452b/ast.go#L4552-L4557
                    (
                        _,
                        Operator::Eq
                        | Operator::NotEq
                        | Operator::Gt
                        | Operator::GtEq
                        | Operator::Lt
                        | Operator::LtEq
                        // These are deviations to resolve ambiguous behaviour in the original implementation
                        | Operator::And
                        | Operator::Or,
                        _
                    ) => yes(lit(false)),

                    //
                    // Everything else should result in `NULL`.
                    //
                    // See: https://github.com/influxdata/influxql/blob/1ba470371ec093d57a726b143fe6ccbacf1b452b/ast.go#L4558
                    _ => yes(lit(ScalarValue::Null)),
                }
            }

            // Invoking an aggregate or window function on a tag column should return `NULL`
            // to be consistent with OG.
            Expr::AggregateFunction(AggregateFunction { ref args, .. } )
            | Expr::AggregateUDF(AggregateUDF { ref args, .. } )
            | Expr::WindowFunction(WindowFunction { ref args, .. } ) => match &args[0] {
               Expr::Column(Column { ref name, ..  }) if schema.is_tag_field(name) => yes(lit(ScalarValue::Null)),
               _ => no(expr),
            }

            // If the InfluxQL query used a selector on a tag column,  like `last(tag_col)`
            // then there will be an indexed field. Convert this to `NULL` as well.
            Expr::GetIndexedField(GetIndexedField { expr: ref e, .. }) => match e.as_ref() {
               Expr::Literal(ScalarValue::Null) => yes(lit(ScalarValue::Null)),
               _ => no(expr),
            }

            //
            // Literals and other expressions are passed through to DataFusion,
            // as it will handle evaluating function calls, etc
            //
            // See: https://github.com/influxdata/influxql/blob/1ba470371ec093d57a726b143fe6ccbacf1b452b/ast.go#L4638-L4647
            _ => no(expr),
        }
    })
}

/// Rewrite conditional operators to `false` and any
/// other operators to `NULL`.
fn rewrite_any_binary_expr(op: Operator) -> Expr {
    match op {
        Operator::Eq
        | Operator::NotEq
        | Operator::Gt
        | Operator::GtEq
        | Operator::Lt
        | Operator::LtEq => lit(false),
        _ => lit(ScalarValue::Null),
    }
}

/// Rewrite a binary expression where one of the operands is a [DataType::Boolean].
///
/// > **Note**
/// >
/// > InfluxQL allows bitwise operations on boolean data types, which must be cast to
/// > an integer to perform the bitwise operation and back to a boolean.
fn rewrite_boolean(lhs: Expr, op: Operator, rhs: Expr) -> Expr {
    match op {
        Operator::And | Operator::Or | Operator::Eq | Operator::NotEq => binary_expr(lhs, op, rhs),
        // DataFusion doesn't support arithmetic operators for boolean types,
        // so cast both sides to an i64 and then the result back to a boolean
        Operator::BitwiseAnd | Operator::BitwiseXor | Operator::BitwiseOr => cast(
            binary_expr(cast(lhs, DataType::Int8), op, cast(rhs, DataType::Int8)),
            DataType::Boolean,
        ),
        _ => rewrite_any_binary_expr(op),
    }
}

/// Rewrite regex conditional expressions to match InfluxQL behaviour.
struct FixRegularExpressions<'a> {
    schema: &'a IQLSchema<'a>,
}

impl<'a> TreeNodeRewriter for FixRegularExpressions<'a> {
    type N = Expr;

    fn mutate(&mut self, expr: Expr) -> Result<Expr> {
        match expr {
            // InfluxQL evaluates regular expression conditions to false if the column is numeric
            // or the column doesn't exist.
            Expr::BinaryExpr(BinaryExpr {
                left,
                op: op @ (Operator::RegexMatch | Operator::RegexNotMatch),
                right,
            }) => {
                Ok(if let Expr::Column(ref col) = *left {
                    match self.schema.df_schema.field_from_column(col)?.data_type() {
                        DataType::Dictionary(..) | DataType::Utf8 => {
                            Expr::BinaryExpr(BinaryExpr { left, op, right })
                        }
                        // Any other column type should evaluate to false
                        _ => lit(false),
                    }
                } else {
                    // If this is not a simple column expression, evaluate to false,
                    // to be consistent with InfluxQL.
                    //
                    // References:
                    //
                    // * https://github.com/influxdata/influxdb/blob/9308b6586a44e5999180f64a96cfb91e372f04dd/tsdb/index.go#L2487-L2488
                    // * https://github.com/influxdata/influxdb/blob/9308b6586a44e5999180f64a96cfb91e372f04dd/tsdb/index.go#L2509-L2510
                    //
                    // The query engine does not correctly evaluate tag keys and values, always evaluating to false.
                    //
                    // Reference example:
                    //
                    // * `SELECT f64 FROM m0 WHERE tag0 = '' + tag0`
                    lit(false)
                })
            }
            _ => Ok(expr),
        }
    }
}

/// Rewrite tag references into
/// ```sql
///         case when tag0 is null then "" else tag0 end
/// ```
/// This ensures that we treat tags with the same semantics as OG InfluxQL.
fn rewrite_tag_columns(expr: Expr, schema: &IQLSchema<'_>) -> Result<Expr> {
    expr.transform(&|expr| match expr {
        Expr::Column(ref c) if schema.is_tag_field(&c.name) => {
            yes(when(expr.clone().is_null(), lit("")).otherwise(expr)?)
        }
        e => no(e),
    })
}

#[cfg(test)]
mod test {
    use crate::plan::ir::DataSourceSchema;

    use super::*;
    use datafusion::logical_expr::lit_timestamp_nano;
    use datafusion::prelude::col;
    use datafusion_util::AsExpr;

    use chrono::{DateTime, NaiveDate, Utc};
    use datafusion::common::{DFSchemaRef, ToDFSchema};
    use schema::{InfluxFieldType, SchemaBuilder};
    use std::sync::Arc;

    fn new_schema() -> IQLSchema<'static> {
        let iox_schema = SchemaBuilder::new()
            .measurement("m0")
            .timestamp()
            .tag("tag0")
            .tag("tag1")
            .influx_field("float_field", InfluxFieldType::Float)
            .influx_field("integer_field", InfluxFieldType::Integer)
            .influx_field("unsigned_field", InfluxFieldType::UInteger)
            .influx_field("string_field", InfluxFieldType::String)
            .influx_field("boolean_field", InfluxFieldType::Boolean)
            .build()
            .expect("schema failed");
        let df_schema: DFSchemaRef = Arc::clone(iox_schema.inner()).to_dfschema_ref().unwrap();
        let ds_schema = DataSourceSchema::Table(iox_schema);
        IQLSchema::new_from_ds_schema(&df_schema, ds_schema).unwrap()
    }

    /// Tests which validate that division is coalesced to `0`, to handle division by zero,
    /// which normally returns a `NULL`, but presents as `0` for InfluxQL.
    ///
    /// The rewriter does not check whether the divisor is a literal 0, which it could reduce the
    /// binary expression to a scalar value, `0`.
    #[test]
    fn test_division() {
        let schemas = new_schema();
        let rewrite = |expr| rewrite_expr(expr, &schemas).unwrap().to_string();

        // Float64
        let expr = lit(5.0) / "float_field".as_expr();
        assert_eq!(
            rewrite(expr),
            "coalesce(Float64(5) / float_field, Float64(0))"
        );
        let expr = lit(5_u64) / "float_field".as_expr();
        assert_eq!(
            rewrite(expr),
            "coalesce(UInt64(5) / float_field, Float64(0))"
        );
        let expr = lit(5_i64) / "float_field".as_expr();
        assert_eq!(
            rewrite(expr),
            "coalesce(Int64(5) / float_field, Float64(0))"
        );
        let expr = lit(5.0) / "unsigned_field".as_expr();
        assert_eq!(
            rewrite(expr),
            "coalesce(Float64(5) / unsigned_field, Float64(0))"
        );
        let expr = lit(5.0) / "integer_field".as_expr();
        assert_eq!(
            rewrite(expr),
            "coalesce(Float64(5) / integer_field, Float64(0))"
        );

        // UInt64
        let expr = lit(5_u64) / "unsigned_field".as_expr();
        assert_eq!(
            rewrite(expr),
            "coalesce(UInt64(5) / unsigned_field, UInt64(0))"
        );
        let expr = lit(5_i64) / "unsigned_field".as_expr();
        assert_eq!(
            rewrite(expr),
            "coalesce(Int64(5) / unsigned_field, UInt64(0))"
        );
        // integer field combined with an unsigned is coalesced to a Uint64
        let expr = lit(5_u64) / "integer_field".as_expr();
        assert_eq!(
            rewrite(expr),
            "coalesce(UInt64(5) / integer_field, UInt64(0))"
        );

        // Int64 values are cast to Float64 to be consistent with InfluxQL
        let expr = lit(5_i64) / "integer_field".as_expr();
        assert_eq!(
            rewrite(expr),
            "coalesce(CAST(Int64(5) AS Float64) / CAST(integer_field AS Float64), Float64(0))"
        );
    }

    /// Verifies expressions pass through, with the expectation that
    /// DataFusion will perform any necessary coercions.
    #[test]
    fn test_pass_thru() {
        test_helpers::maybe_start_logging();
        let schemas = new_schema();
        let rewrite = |expr| rewrite_expr(expr, &schemas).unwrap().to_string();

        let expr = lit(5.5).gt(lit(1_i64));
        assert_eq!(rewrite(expr), "Float64(5.5) > Int64(1)");

        let expr = lit(5.5) + lit(1_i64);
        assert_eq!(rewrite(expr), "Float64(5.5) + Int64(1)");

        let expr = (lit(5.5) + lit(1_i64)).gt_eq(lit(3_u64) - lit(9_i64));
        assert_eq!(
            rewrite(expr),
            "Float64(5.5) + Int64(1) >= UInt64(3) - Int64(9)"
        );

        // regular expressions
        let expr = binary_expr("tag0".as_expr(), Operator::RegexMatch, lit("foo"));
        assert_eq!(rewrite(expr), r#"tag0 ~ Utf8("foo")"#);

        let expr = binary_expr("tag0".as_expr(), Operator::RegexNotMatch, lit("foo"));
        assert_eq!(rewrite(expr), r#"tag0 !~ Utf8("foo")"#);
    }

    fn execution_props() -> ExecutionProps {
        let start_time = NaiveDate::from_ymd_opt(2023, 1, 1)
            .unwrap()
            .and_hms_opt(0, 0, 0)
            .unwrap();
        let start_time = DateTime::<Utc>::from_utc(start_time, Utc);
        let mut props = ExecutionProps::new();
        props.query_execution_start_time = start_time;
        props
    }

    #[test]
    fn test_string_operations() {
        let props = execution_props();
        let schemas = new_schema();
        let rewrite = |expr| {
            rewrite_conditional_expr(&props, expr, &schemas)
                .unwrap()
                .to_string()
        };

        // Should rewrite as a string concatenation
        // NOTE: InfluxQL does not allow operations on string fields

        let expr = "string_field".as_expr() + lit("bar");
        assert_eq!(rewrite(expr), r#"string_field || Utf8("bar")"#);

        let expr = "string_field".as_expr().eq(lit("bar"));
        assert_eq!(rewrite(expr), r#"string_field = Utf8("bar")"#);

        let expr = "string_field".as_expr().gt(lit("bar"));
        assert_eq!(rewrite(expr), r#"Boolean(false)"#);
    }

    /// Validates operations for boolean operands, with particular attention
    /// to the supported bitwise operators.
    #[test]
    fn test_boolean_operations() {
        let schemas = new_schema();
        let rewrite = |expr| rewrite_expr(expr, &schemas).unwrap().to_string();

        let expr = "boolean_field".as_expr().and(lit(true));
        assert_eq!(rewrite(expr), "boolean_field AND Boolean(true)");

        let expr = "boolean_field".as_expr().or(lit(true));
        assert_eq!(rewrite(expr), "boolean_field OR Boolean(true)");

        let expr = "boolean_field".as_expr().eq(lit(true));
        assert_eq!(rewrite(expr), "boolean_field = Boolean(true)");

        let expr = "boolean_field".as_expr().not_eq(lit(true));
        assert_eq!(rewrite(expr), "boolean_field != Boolean(true)");

        // DataFusion does not support bitwise operations when the operands are
        // Boolean data types. These must be case to an an Int8 in order to perform
        // the operation and then cast back to Boolean

        let expr = binary_expr("boolean_field".as_expr(), Operator::BitwiseOr, lit(true));
        assert_eq!(
            rewrite(expr),
            "CAST(CAST(boolean_field AS Int8) | CAST(Boolean(true) AS Int8) AS Boolean)"
        );

        let expr = binary_expr("boolean_field".as_expr(), Operator::BitwiseAnd, lit(true));
        assert_eq!(
            rewrite(expr),
            "CAST(CAST(boolean_field AS Int8) & CAST(Boolean(true) AS Int8) AS Boolean)"
        );

        let expr = binary_expr("boolean_field".as_expr(), Operator::BitwiseXor, lit(true));
        assert_eq!(
            rewrite(expr),
            "CAST(CAST(boolean_field AS Int8) # CAST(Boolean(true) AS Int8) AS Boolean)"
        );

        // Unsupported operations

        let expr = "boolean_field".as_expr().gt(lit(true));
        assert_eq!(rewrite(expr), "Boolean(false)");

        let expr = "boolean_field"
            .as_expr()
            .gt(lit(true))
            .or("boolean_field".as_expr().not_eq(lit(true)));
        assert_eq!(
            rewrite(expr),
            "Boolean(false) OR boolean_field != Boolean(true)"
        );
    }

    /// Tests cases to validate Boolean and NULL data types
    #[test]
    fn test_rewrite_conditional_null() {
        let schemas = new_schema();
        let rewrite = |expr| rewrite_expr(expr, &schemas).unwrap().to_string();

        // NULL on either side and boolean on the other of a binary expression
        let expr = lit(ScalarValue::Null).eq(lit(true));
        assert_eq!(rewrite(expr), "NULL = Boolean(true)");
        let expr = lit(ScalarValue::Null).and(lit(true));
        assert_eq!(rewrite(expr), "Boolean(false) AND Boolean(true)");
        let expr = lit(true).or(lit(ScalarValue::Null));
        assert_eq!(rewrite(expr), "Boolean(true) OR Boolean(false)");

        // NULL on either side of a binary expression, that is not a boolean
        let expr = lit(ScalarValue::Null).eq(lit("test"));
        assert_eq!(rewrite(expr), r#"NULL = Utf8("test")"#);
        let expr = lit("test").eq(lit(ScalarValue::Null));
        assert_eq!(rewrite(expr), r#"Utf8("test") = NULL"#);

        // STRING + INTEGER conditional_op STRING
        // |> null conditional_op STRING
        let expr = (lit("foo") + lit(1)).eq(lit("bar"));
        assert_eq!(rewrite(expr), r#"NULL = Utf8("bar")"#);

        let expr = (lit("foo") + lit(1)).eq(lit("bar") + lit(false));
        assert_eq!(rewrite(expr), r#"NULL"#);

        // valid boolean operations
        let expr = lit(false).eq(lit(true));
        assert_eq!(rewrite(expr), "Boolean(false) = Boolean(true)");

        // booleans don't support mathematical operators
        let expr = lit(false) + lit(true);
        assert_eq!(rewrite(expr), "NULL");
    }

    #[test]
    fn test_time_range() {
        let schemas = new_schema();
        let rewrite = |expr| rewrite_expr(expr, &schemas).unwrap().to_string();

        let expr = "time".as_expr().gt_eq(lit_timestamp_nano(1000));
        assert_eq!(rewrite(expr), "time >= TimestampNanosecond(1000, None)");

        let expr = lit_timestamp_nano(1000).lt_eq("time".as_expr());
        assert_eq!(rewrite(expr), "TimestampNanosecond(1000, None) <= time");

        let expr = "time"
            .as_expr()
            .gt_eq(lit_timestamp_nano(1000))
            .and("tag0".as_expr().eq(lit("foo")));
        assert_eq!(
            rewrite(expr),
            r#"time >= TimestampNanosecond(1000, None) AND tag0 = Utf8("foo")"#
        );

        let expr = "time"
            .as_expr()
            .gt_eq(lit_timestamp_nano(1000))
            .and("float_field".as_expr().eq(lit(false)));
        assert_eq!(
            rewrite(expr),
            r#"time >= TimestampNanosecond(1000, None) AND Boolean(false)"#
        );
    }

    /// Tests cases where arithmetic expressions are coerced to `NULL`, as they have no
    /// valid operation for the given the operands. These are used when projecting columns.
    #[test]
    fn test_rewrite_expr_coercion_reduce_to_null() {
        let schemas = new_schema();
        let rewrite = |expr| rewrite_expr(expr, &schemas).unwrap().to_string();

        //
        // FLOAT types
        //
        let expr = "float_field".as_expr() + lit(true);
        assert_eq!(rewrite(expr), "NULL");
        let expr = "float_field".as_expr() + lit(ScalarValue::Null);
        assert_eq!(rewrite(expr), "NULL");

        //
        // STRING types
        //
        let expr = "string_field".as_expr() + lit(true);
        assert_eq!(rewrite(expr), "NULL");
        let expr = lit(true) - "string_field".as_expr();
        assert_eq!(rewrite(expr), "NULL");
        let expr = lit(5) + "string_field".as_expr();
        assert_eq!(rewrite(expr), "NULL");
        let expr = "string_field".as_expr() - lit(3.3);
        assert_eq!(rewrite(expr), "NULL");

        //
        // BOOLEAN types
        //
        let expr = "boolean_field".as_expr() + lit(true);
        assert_eq!(rewrite(expr), "NULL");
        let expr = lit(true) - "boolean_field".as_expr();
        assert_eq!(rewrite(expr), "NULL");
        let expr = lit(5) + "boolean_field".as_expr();
        assert_eq!(rewrite(expr), "NULL");
        let expr = "boolean_field".as_expr() - lit(3.3);
        assert_eq!(rewrite(expr), "NULL");
    }

    #[test]
    fn test_rewrite_tag_columns_eq() {
        test_helpers::maybe_start_logging();
        let props = execution_props();
        let schemas = new_schema();
        let rewrite = |expr| {
            rewrite_conditional_expr(&props, expr, &schemas)
                .unwrap()
                .to_string()
        };

        // Equality with a non-empty literal
        let expr = col("tag0").eq(lit("foo"));
        assert_eq!(rewrite(expr), r#"tag0 = Dictionary(Int32, Utf8("foo"))"#);

        // Equality with an empty literal
        // It doesn't seem possible to insert an empty tag with line protocol.
        // so this could perhaps be simplified to remove the IS NULL check.
        // Such an optimization would be an IOx-ism and not true for DataFusion generally.
        let expr = col("tag0").eq(lit(""));
        assert_eq!(
            rewrite(expr),
            r#"tag0 IS NULL OR tag0 = Dictionary(Int32, Utf8(""))"#
        );

        // Inequality with a non-empty literal
        let expr = col("tag0").not_eq(lit("foo"));
        assert_eq!(
            rewrite(expr),
            r#"tag0 IS NULL OR tag0 != Dictionary(Int32, Utf8("foo"))"#
        );

        // Inequality with an empty literal
        let expr = col("tag0").not_eq(lit(""));
        assert_eq!(rewrite(expr), r#"tag0 != Dictionary(Int32, Utf8(""))"#);
    }

    fn regex_match(left: Expr, right: Expr) -> Expr {
        Expr::BinaryExpr(BinaryExpr {
            left: Box::new(left),
            op: Operator::RegexMatch,
            right: Box::new(right),
        })
    }

    fn regex_not_match(left: Expr, right: Expr) -> Expr {
        Expr::BinaryExpr(BinaryExpr {
            left: Box::new(left),
            op: Operator::RegexNotMatch,
            right: Box::new(right),
        })
    }

    #[test]
    fn test_rewrite_tag_columns_regex() {
        let props = execution_props();
        test_helpers::maybe_start_logging();
        let schemas = new_schema();
        let rewrite = |expr| {
            rewrite_conditional_expr(&props, expr, &schemas)
                .unwrap()
                .to_string()
        };

        let expr = regex_match(col("tag0"), lit("^$"));
        assert_eq!(
            rewrite(expr),
            r#"tag0 IS NULL OR CAST(tag0 AS Utf8) = Utf8("")"#
        );
        let expr = regex_match(col("tag0"), lit("^foo$"));
        assert_eq!(rewrite(expr), r#"CAST(tag0 AS Utf8) = Utf8("foo")"#);
        let expr = regex_not_match(col("tag0"), lit("^$"));
        assert_eq!(rewrite(expr), r#"CAST(tag0 AS Utf8) != Utf8("")"#);
        let expr = regex_not_match(col("tag0"), lit("^foo$"));
        assert_eq!(
            rewrite(expr),
            r#"tag0 IS NULL OR CAST(tag0 AS Utf8) != Utf8("foo")"#
        );
    }

    #[test]
    fn test_fields_pass_thru() {
        test_helpers::maybe_start_logging();
        let props = execution_props();
        let schemas = new_schema();
        let rewrite = |expr| {
            rewrite_conditional_expr(&props, expr, &schemas)
                .unwrap()
                .to_string()
        };

        // Field predicates are handled naively which is
        // consistent with InfluxQL OG.

        let expr = col("string_field").eq(lit(""));
        assert_eq!(rewrite(expr), r#"string_field = Utf8("")"#);

        let expr = col("string_field").eq(lit("foo"));
        assert_eq!(rewrite(expr), r#"string_field = Utf8("foo")"#);

        let expr = col("string_field").not_eq(lit(""));
        assert_eq!(rewrite(expr), r#"string_field != Utf8("")"#);

        let expr = col("string_field").not_eq(lit("foo"));
        assert_eq!(rewrite(expr), r#"string_field != Utf8("foo")"#);
    }
}
