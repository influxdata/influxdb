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
use crate::plan::error;
use crate::plan::util::Schemas;
use arrow::datatypes::DataType;
use datafusion::common::tree_node::{
    Transformed, TreeNode, TreeNodeRewriter, TreeNodeVisitor, VisitRecursion,
};
use datafusion::common::{Column, Result, ScalarValue};
use datafusion::logical_expr::{
    binary_expr, cast, coalesce, lit, BinaryExpr, Expr, ExprSchemable, Operator,
};
use datafusion::optimizer::utils::{conjunction, disjunction};
use schema::{InfluxColumnType, InfluxFieldType};

/// Perform a series of passes to rewrite `expr` in compliance with InfluxQL behavior
/// in an effort to ensure the query executes without error.
pub(super) fn rewrite_conditional_expr(expr: Expr, schemas: &Schemas) -> Result<Expr> {
    let expr = expr.rewrite(&mut FixRegularExpressions { schemas })?;
    let expr = rewrite_time_range_exprs(expr)?;
    rewrite_conditional(expr, schemas)
}

/// Perform a series of passes to rewrite `expr`, used as a column projection,
/// to match the behavior of InfluxQL.
pub(super) fn rewrite_field_expr(expr: Expr, schemas: &Schemas) -> Result<Expr> {
    rewrite_expr(expr, schemas)
}

/// Traverse `expr` and promote time range expressions to the root
/// binary node on the left hand side and combined using the conjunction (`AND`)
/// operator to ensure the time range is applied to the entire predicate.
///
/// Additionally, multiple `time = <timestamp>` expressions are combined
/// using the disjunction (OR) operator, whereas, InfluxQL only supports
/// a single `time = <timestamp>` expression.
///
/// # NOTE
///
/// Combining relational operators like `time > now() - 5s` and equality
/// operators like `time = <timestamp>` with a disjunction (`OR`)
/// will evaluate to false, like InfluxQL.
///
/// # Background
///
/// The InfluxQL query engine always promotes the time range expression to filter
/// all results. It is misleading that time ranges are written in the `WHERE` clause,
/// as the `WHERE` predicate is not evaluated in its entirety for each row. Rather,
/// InfluxQL extracts the time range to form a time bound for the entire query and
/// removes any time range expressions from the filter predicate. The time range
/// is determined using the `>` and `≥` operators to form the lower bound and
/// the `<` and `≤` operators to form the upper bound. When multiple instances of
/// the lower or upper bound operators are found, the time bounds will form the
/// intersection. For example
///
/// ```sql
/// WHERE time >= 1000 AND time >= 2000 AND time < 10000 and time < 9000
/// ```
///
/// is equivalent to
///
/// ```sql
/// WHERE time >= 2000 AND time < 9000
/// ```
///
/// Further, InfluxQL only allows a single `time = <value>` binary expression. Multiple
/// occurrences result in an empty result set.
///
/// ## Examples
///
/// Lets illustrate how InfluxQL applies predicates with a typical example, using the
/// `metrics.lp` data in the IOx repository:
///
/// ```sql
/// SELECT cpu, usage_idle FROM cpu
/// WHERE
///   time > '2020-06-11T16:53:30Z' AND time < '2020-06-11T16:55:00Z' AND cpu = 'cpu0'
/// ```
///
/// InfluxQL first filters rows based on the time range:
///
/// ```sql
/// '2020-06-11T16:53:30Z' < time <  '2020-06-11T16:55:00Z'
/// ```
///
/// and then applies the predicate to the individual rows:
///
/// ```sql
/// cpu = 'cpu0'
/// ```
///
/// Producing the following result:
///
/// ```text
/// name: cpu
/// time                 cpu  usage_idle
/// ----                 ---  ----------
/// 2020-06-11T16:53:40Z cpu0 90.29029029029029
/// 2020-06-11T16:53:50Z cpu0 89.8
/// 2020-06-11T16:54:00Z cpu0 90.09009009009009
/// 2020-06-11T16:54:10Z cpu0 88.82235528942115
/// ```
///
/// The following example is a little more complicated, but shows again how InfluxQL
/// separates the time ranges from the predicate:
///
/// ```sql
/// SELECT cpu, usage_idle FROM cpu
/// WHERE
///   time > '2020-06-11T16:53:30Z' AND time < '2020-06-11T16:55:00Z' AND cpu = 'cpu0' OR cpu = 'cpu1'
/// ```
///
/// InfluxQL first filters rows based on the time range:
///
/// ```sql
/// '2020-06-11T16:53:30Z' < time <  '2020-06-11T16:55:00Z'
/// ```
///
/// and then applies the predicate to the individual rows:
///
/// ```sql
/// cpu = 'cpu0' OR cpu = 'cpu1'
/// ```
///
/// This is certainly quite different to SQL, which would evaluate the predicate as:
///
/// ```sql
/// SELECT cpu, usage_idle FROM cpu
/// WHERE
///   (time > '2020-06-11T16:53:30Z' AND time < '2020-06-11T16:55:00Z' AND cpu = 'cpu0') OR cpu = 'cpu1'
/// ```
///
/// ## Time ranges are not normal
///
/// Here we demonstrate how the operators combining time ranges do not matter. Using the
/// original query:
///
/// ```sql
/// SELECT cpu, usage_idle FROM cpu
/// WHERE
///   time > '2020-06-11T16:53:30Z' AND time < '2020-06-11T16:55:00Z' AND cpu = 'cpu0'
/// ```
///
/// we replace all `AND` operators with `OR`:
///
/// ```sql
/// SELECT cpu, usage_idle FROM cpu
/// WHERE
///   time > '2020-06-11T16:53:30Z' OR time < '2020-06-11T16:55:00Z' OR cpu = 'cpu0'
/// ```
///
/// This should return all rows, but yet it returns the same result 🤯:
///
/// ```text
/// name: cpu
/// time                 cpu  usage_idle
/// ----                 ---  ----------
/// 2020-06-11T16:53:40Z cpu0 90.29029029029029
/// 2020-06-11T16:53:50Z cpu0 89.8
/// 2020-06-11T16:54:00Z cpu0 90.09009009009009
/// 2020-06-11T16:54:10Z cpu0 88.82235528942115
/// ```
///
/// It becomes clearer, if we again review at how InfluxQL OG evaluates the `WHERE`
/// predicate, InfluxQL first filters rows based on the time range, which uses the
/// rules previously defined by finding `>` and `≥` to determine the lower bound
/// and `<` and `≤`:
///
/// ```sql
/// '2020-06-11T16:53:30Z' < time <  '2020-06-11T16:55:00Z'
/// ```
///
/// and then applies the predicate to the individual rows:
///
/// ```sql
/// cpu = 'cpu0'
/// ```
///
/// ## How to think of time ranges intuitively
///
/// Imagine a slight variation of InfluxQL has a separate _time bounds clause_.
/// It could have two forms, first as a `BETWEEN`
///
/// ```sql
/// SELECT cpu, usage_idle FROM cpu
/// WITH TIME BETWEEN '2020-06-11T16:53:30Z' AND '2020-06-11T16:55:00Z'
/// WHERE
///   cpu = 'cpu0'
/// ```
///
/// or as an `IN` to select multiple points:
///
/// ```sql
/// SELECT cpu, usage_idle FROM cpu
/// WITH TIME IN ('2004-04-09T12:00:00Z', '2004-04-09T12:00:10Z', ...)
/// WHERE
///   cpu = 'cpu0'
/// ```
fn rewrite_time_range_exprs(expr: Expr) -> Result<Expr> {
    let mut has_or = false;
    let mut has_time_range = false;
    expr.apply(&mut |expr| {
        match expr {
            Expr::BinaryExpr(BinaryExpr {
                op: Operator::Or, ..
            }) => has_or = true,
            expr @ Expr::BinaryExpr(_) if is_time_range(expr) => has_time_range = true,
            _ => return Ok(VisitRecursion::Continue),
        }

        Ok(if has_or && has_time_range {
            // no need to continue if we've found both
            VisitRecursion::Stop
        } else {
            VisitRecursion::Continue
        })
    })?;

    // if there is no time range expressions or there are no OR operators,
    // we don't need to rewrite the expression
    if !has_time_range || !has_or {
        return Ok(expr);
    }

    let mut rw = SeparateTimeRanges::default();
    expr.visit(&mut rw)?;

    // When `expr` contains both time expressions using relational
    // operators like > or <= and equality, such as
    //
    // WHERE time > now() - 5s OR time = '2004-04-09:12:00:00Z' AND cpu = 'cpu0'
    //
    // the entire expression evaluates to `false` to be consistent with InfluxQL.
    if !rw.relational.is_empty() && !rw.equality.is_empty() {
        return Ok(lit(false));
    }

    let lhs = if !rw.relational.is_empty() {
        conjunction(rw.relational)
    } else if !rw.equality.is_empty() {
        disjunction(rw.equality)
    } else {
        None
    };

    let rhs = rw
        .stack
        .pop()
        .ok_or_else(|| error::map::internal("expected expression on stack"))?;

    Ok(match (lhs, rhs) {
        (Some(lhs), Some(rhs)) => binary_expr(lhs, Operator::And, rhs),
        (Some(expr), None) | (None, Some(expr)) => expr,
        (None, None) => lit(true),
    })
}

/// Returns `true` if `expr` refers to the `time` column.
fn is_time_column(expr: &Expr) -> bool {
    matches!(expr, Expr::Column(Column{ name, .. }) if name == "time")
}

/// Returns `true` if `expr` is a time range expression
///
/// Examples include:
///
/// ```text
/// time = '2004-04-09T12:00:00Z'
/// ```
///
/// or
///
/// ```text
/// time > now() - 5m
/// ```
fn is_time_range(expr: &Expr) -> bool {
    use Operator::*;
    match expr {
        Expr::BinaryExpr(BinaryExpr {
            left,
            op: Eq | NotEq | Gt | Lt | GtEq | LtEq,
            right,
        }) => is_time_column(left) || is_time_column(right),
        _ => false,
    }
}

#[derive(Default)]
struct SeparateTimeRanges {
    stack: Vec<Option<Expr>>,
    /// Relational time range expressions, such as:
    ///
    /// ```sql
    /// time >= now()
    /// ```
    relational: Vec<Expr>,

    /// Equality time range expressions, such as:
    ///
    /// ```sql
    /// time = '2004-04-09T12:00:00Z'
    equality: Vec<Expr>,
}

impl TreeNodeVisitor for SeparateTimeRanges {
    type N = Expr;

    fn pre_visit(&mut self, node: &Self::N) -> Result<VisitRecursion> {
        if matches!(node, Expr::BinaryExpr(_)) {
            Ok(VisitRecursion::Continue)
        } else {
            Ok(VisitRecursion::Skip)
        }
    }

    fn post_visit(&mut self, node: &Self::N) -> Result<VisitRecursion> {
        use Operator::*;

        match node {
            node @ Expr::BinaryExpr(_) if is_time_range(node) => {
                self.stack.push(None);
                // separate equality from relational time-range expressions
                if matches!(
                    node,
                    Expr::BinaryExpr(BinaryExpr {
                        op: Gt | GtEq | Lt | LtEq,
                        ..
                    })
                ) {
                    self.relational.push(node.clone())
                } else {
                    self.equality.push(node.clone())
                }

                Ok(VisitRecursion::Continue)
            }

            node @ Expr::BinaryExpr(BinaryExpr {
                op:
                    Eq | NotEq | Gt | GtEq | Lt | LtEq | RegexMatch | RegexNotMatch | RegexIMatch
                    | RegexNotIMatch,
                ..
            }) => {
                self.stack.push(Some(node.clone()));
                Ok(VisitRecursion::Continue)
            }
            Expr::BinaryExpr(BinaryExpr {
                op: op @ (And | Or),
                ..
            }) => {
                let right = self
                    .stack
                    .pop()
                    .ok_or_else(|| error::map::internal("invalid expr stack"))?;
                let left = self
                    .stack
                    .pop()
                    .ok_or_else(|| error::map::internal("invalid expr stack"))?;
                self.stack.push(match (left, right) {
                    (Some(left), Some(right)) => Some(binary_expr(left, *op, right)),
                    (None, Some(node)) | (Some(node), None) => Some(node),
                    (None, None) => None,
                });

                Ok(VisitRecursion::Continue)
            }
            _ => Ok(VisitRecursion::Continue),
        }
    }
}

/// Rewrite the expression tree and return a boolean result.
fn rewrite_conditional(expr: Expr, schemas: &Schemas) -> Result<Expr> {
    let expr = rewrite_expr(expr, schemas)?;
    Ok(match expr {
        Expr::Literal(ScalarValue::Null) => lit(false),
        _ => expr,
    })
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
fn rewrite_expr(expr: Expr, schemas: &Schemas) -> Result<Expr> {
    expr.transform(&|expr| {
        match expr {
            Expr::BinaryExpr(BinaryExpr {
                ref left,
                op,
                ref right,
            }) => {
                let lhs_type = left.get_type(&schemas.df_schema)?;
                let rhs_type = right.get_type(&schemas.df_schema)?;

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
                        // Like Float64, dividing by zero should return 0 for InfluxQL
                        //
                        // See: https://github.com/influxdata/influxql/blob/1ba470371ec093d57a726b143fe6ccbacf1b452b/ast.go#L4338-L4340
                        Operator::Divide => yes(coalesce(vec![expr, lit(0_i64)])),
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
    schemas: &'a Schemas,
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
                if let Expr::Column(ref col) = *left {
                    match self.schemas.iox_schema.field_by_name(&col.name) {
                        Some((InfluxColumnType::Tag, _)) => {
                            // Regular expressions expect to be compared with a Utf8
                            let left =
                                Box::new(left.cast_to(&DataType::Utf8, &self.schemas.df_schema)?);
                            Ok(Expr::BinaryExpr(BinaryExpr { left, op, right }))
                        }
                        Some((InfluxColumnType::Field(InfluxFieldType::String), _)) => {
                            Ok(Expr::BinaryExpr(BinaryExpr { left, op, right }))
                        }
                        // Any other column type should evaluate to false
                        _ => Ok(lit(false)),
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
                    Ok(lit(false))
                }
            }
            _ => Ok(expr),
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use datafusion::common::{DFSchemaRef, ToDFSchema};
    use datafusion::logical_expr::{lit_timestamp_nano, now};
    use datafusion_util::AsExpr;
    use schema::{InfluxFieldType, SchemaBuilder};
    use std::sync::Arc;

    fn new_schemas() -> Schemas {
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
        Schemas {
            df_schema,
            iox_schema,
        }
    }

    #[test]
    fn test_rewrite_time_range_exprs() {
        use datafusion::common::ScalarValue as V;

        let rewrite = |expr| rewrite_time_range_exprs(expr).unwrap().to_string();

        // does not need a rewrite
        let expr = "time"
            .as_expr()
            .gt_eq(now() - lit(V::new_interval_dt(0, 1000)));

        assert_eq!(rewrite(expr), r#"time >= now() - IntervalDayTime("1000")"#);

        // does not need a rewrite
        let expr = "time"
            .as_expr()
            .gt_eq(now() - lit(V::new_interval_dt(0, 1000)))
            .and("time".as_expr().lt(now()));
        assert_eq!(
            rewrite(expr),
            r#"time >= now() - IntervalDayTime("1000") AND time < now()"#
        );

        // does not need a rewrite
        let expr = "time"
            .as_expr()
            .gt_eq(now() - lit(V::new_interval_dt(0, 1000)))
            .and("cpu".as_expr().eq(lit("cpu0")));
        assert_eq!(
            rewrite(expr),
            r#"time >= now() - IntervalDayTime("1000") AND cpu = Utf8("cpu0")"#
        );

        // does not need a rewrite
        let expr = "time".as_expr().eq(lit_timestamp_nano(0));
        assert_eq!(rewrite(expr), r#"time = TimestampNanosecond(0, None)"#);

        // The following expressions require rewrites to promote the time range to the top
        // of the expression tree

        // instance = 'instance-01' OR instance = 'instance-02' AND time >= now() - 60s
        let expr = "instance"
            .as_expr()
            .eq(lit("instance-01"))
            .or("instance".as_expr().eq(lit("instance-02")))
            .and(
                "time"
                    .as_expr()
                    .gt_eq(now() - lit(V::new_interval_dt(0, 60_000))),
            );
        assert_eq!(
            rewrite(expr),
            r#"time >= now() - IntervalDayTime("60000") AND (instance = Utf8("instance-01") OR instance = Utf8("instance-02"))"#
        );

        // time >= now - 60s AND time < now() AND cpu = 'cpu0' OR cpu = 'cpu1'
        let expr = "time"
            .as_expr()
            .gt_eq(now() - lit(V::new_interval_dt(0, 60_000)))
            .and("time".as_expr().lt(now()))
            .and(
                "cpu"
                    .as_expr()
                    .eq(lit("cpu0"))
                    .or("cpu".as_expr().eq(lit("cpu1"))),
            );
        assert_eq!(
            rewrite(expr),
            r#"time >= now() - IntervalDayTime("60000") AND time < now() AND (cpu = Utf8("cpu0") OR cpu = Utf8("cpu1"))"#
        );

        // time >= now - 60s AND time < now() OR cpu = 'cpu0' OR cpu = 'cpu1'
        //
        // Expects the time range to be combined with a conjunction (AND)
        let expr = "time"
            .as_expr()
            .gt_eq(now() - lit(V::new_interval_dt(0, 60_000)))
            .and("time".as_expr().lt(now()))
            .or("cpu"
                .as_expr()
                .eq(lit("cpu0"))
                .or("cpu".as_expr().eq(lit("cpu1"))));
        assert_eq!(
            rewrite(expr),
            r#"time >= now() - IntervalDayTime("60000") AND time < now() AND (cpu = Utf8("cpu0") OR cpu = Utf8("cpu1"))"#
        );

        // time = 0 OR time = 10 AND cpu = 'cpu0'
        let expr = "time".as_expr().eq(lit_timestamp_nano(0)).or("time"
            .as_expr()
            .eq(lit_timestamp_nano(10))
            .and("cpu".as_expr().eq(lit("cpu0"))));
        assert_eq!(
            rewrite(expr),
            r#"(time = TimestampNanosecond(0, None) OR time = TimestampNanosecond(10, None)) AND cpu = Utf8("cpu0")"#
        );

        // no time
        let expr = "f64".as_expr().gt_eq(lit(19.5_f64)).or(binary_expr(
            "f64".as_expr(),
            Operator::RegexMatch,
            lit("foo"),
        ));
        assert_eq!(
            rewrite(expr),
            "f64 >= Float64(19.5) OR (f64 ~ Utf8(\"foo\"))"
        );

        // fallible

        let expr = "time"
            .as_expr()
            .eq(lit_timestamp_nano(0))
            .or("time".as_expr().gt(now()));
        assert_eq!(rewrite(expr), "Boolean(false)");
    }

    /// Tests which validate that division is coalesced to `0`, to handle division by zero,
    /// which normally returns a `NULL`, but presents as `0` for InfluxQL.
    ///
    /// The rewriter does not check whether the divisor is a literal 0, which it could reduce the
    /// binary expression to a scalar value, `0`.
    #[test]
    fn test_division_by_zero() {
        let schemas = new_schemas();
        let rewrite = |expr| rewrite_conditional(expr, &schemas).unwrap().to_string();

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

        // Int64
        let expr = lit(5_i64) / "integer_field".as_expr();
        assert_eq!(
            rewrite(expr),
            "coalesce(Int64(5) / integer_field, Int64(0))"
        );
    }

    /// Verifies expressions pass through, with the expectation that
    /// DataFusion will perform any necessary coercions.
    #[test]
    fn test_pass_thru() {
        let schemas = new_schemas();
        let rewrite = |expr| rewrite_conditional(expr, &schemas).unwrap().to_string();

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

    #[test]
    fn test_string_operations() {
        let schemas = new_schemas();
        let rewrite = |expr| rewrite_conditional(expr, &schemas).unwrap().to_string();

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
        let schemas = new_schemas();
        let rewrite = |expr| rewrite_conditional(expr, &schemas).unwrap().to_string();

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
        let schemas = new_schemas();
        let rewrite = |expr| rewrite_conditional(expr, &schemas).unwrap().to_string();

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
        assert_eq!(rewrite(expr), r#"Boolean(false)"#);

        // valid boolean operations
        let expr = lit(false).eq(lit(true));
        assert_eq!(rewrite(expr), "Boolean(false) = Boolean(true)");

        // booleans don't support mathematical operators
        let expr = lit(false) + lit(true);
        assert_eq!(rewrite(expr), "Boolean(false)");
    }

    #[test]
    fn test_time_range() {
        let schemas = new_schemas();
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
        let schemas = new_schemas();
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
}
