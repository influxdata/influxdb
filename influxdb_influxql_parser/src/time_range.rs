//! Process InfluxQL time range expressions
//!
use crate::expression::walk::{walk_expression, Expression};
use crate::expression::{
    lit, Binary, BinaryOperator, ConditionalBinary, ConditionalExpression, Expr, VarRef,
};
use crate::functions::is_now_function;
use crate::literal::{nanos_to_timestamp, Duration, Literal};
use crate::timestamp::{parse_timestamp, Timestamp};
use std::ops::ControlFlow;

/// Result type for operations that return an [`Expr`] and could result in an [`ExprError`].
pub type ExprResult = Result<Expr, ExprError>;

/// Traverse `expr` and separate time range expressions from other predicates.
///
/// # NOTE
///
/// Combining relational operators like `time > now() - 5s` and equality
/// operators like `time = <timestamp>` with a disjunction (`OR`)
/// will evaluate to `false`, like InfluxQL.
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
pub fn split_cond(
    ctx: &ReduceContext,
    cond: &ConditionalExpression,
) -> Result<(Option<ConditionalExpression>, TimeRange), ExprError> {
    if !has_time_range(cond) {
        return Ok((Some(cond.clone()), TimeRange::default()));
    }

    let mut time_range = TimeRange::default();
    let mut stack: Vec<Option<ConditionalExpression>> = vec![];

    let res = walk_expression(cond, &mut |expr| {
        if let Expression::Conditional(cond) = expr {
            use crate::expression::ConditionalOperator::*;
            use ConditionalExpression as CE;
            match cond {
                CE::Binary(ConditionalBinary {
                    lhs,
                    op: op @ (Eq | NotEq | Gt | Lt | GtEq | LtEq),
                    rhs,
                }) if is_time_field(lhs) || is_time_field(rhs) => {
                    if matches!(op, NotEq) {
                        // Stop recursing, as != is an invalid operator for time expressions
                        return ControlFlow::Break(error::map::expr(
                            "invalid time comparison operator: !=",
                        ));
                    }

                    stack.push(None);

                    /// Op is the limited set of operators expected from here on,
                    /// to avoid repeated wildcard match arms with unreachable!().
                    enum Op {
                        Eq,
                        Gt,
                        GtEq,
                        Lt,
                        LtEq,
                    }

                    // Map the DataFusion Operator to Op
                    let op = match op {
                        Eq => Op::Eq,
                        Gt => Op::Gt,
                        GtEq => Op::GtEq,
                        Lt => Op::Lt,
                        LtEq => Op::LtEq,
                        _ => unreachable!("expected: Eq | Gt | GtEq | Lt | LtEq"),
                    };

                    let (expr, op) = if is_time_field(lhs) {
                        (rhs, op)
                    } else {
                        (
                            lhs,
                            match op {
                                Op::Eq => Op::Eq,
                                // swap the relational operators when the conditional is `expression OP "time"`
                                Op::Gt => Op::Lt,
                                Op::GtEq => Op::LtEq,
                                Op::Lt => Op::Gt,
                                Op::LtEq => Op::GtEq,
                            },
                        )
                    };

                    let Some(expr) = expr.expr() else {
                        return ControlFlow::Break(error::map::internal("expected Expr"))
                    };

                    // simplify binary expressions to a constant, including resolve `now()`
                    let expr = match reduce_time_expr(ctx, expr) {
                        Ok(e) => e,
                        Err(err) => return ControlFlow::Break(err),
                    };

                    let ts = match expr {
                        Expr::Literal(Literal::Timestamp(ts)) => ts.timestamp_nanos(),
                        expr => {
                            return ControlFlow::Break(error::map::internal(format!(
                                "expected Timestamp, got: {}",
                                expr
                            )))
                        }
                    };

                    // See InfluxQL OG for details.
                    //
                    // https://github.com/influxdata/influxql/blob/802555d6b3a35cd464a6d8afa2a6511002cf3c2c/ast.go#L5836-L5846

                    let mut other = TimeRange::default();
                    match op {
                        Op::Eq => {
                            other.lower = Some(ts);
                            other.upper = Some(ts);
                        }
                        Op::Gt => {
                            other.lower = Some(ts + 1);
                        }
                        Op::GtEq => {
                            other.lower = Some(ts);
                        }
                        Op::Lt => {
                            other.upper = Some(ts - 1);
                        }
                        Op::LtEq => {
                            other.upper = Some(ts);
                        }
                    }
                    time_range.intersect(other);
                }
                node @ CE::Binary(ConditionalBinary {
                    op: Eq | NotEq | Gt | GtEq | Lt | LtEq | EqRegex | NotEqRegex,
                    ..
                }) => {
                    stack.push(Some(node.clone()));
                }
                CE::Binary(ConditionalBinary {
                    op: op @ (And | Or),
                    ..
                }) => {
                    let Some(right) = stack
                        .pop() else {
                        return ControlFlow::Break(error::map::internal("invalid expr stack"))
                    };
                    let Some(left) = stack
                        .pop() else {
                        return ControlFlow::Break(error::map::internal("invalid expr stack"))
                    };
                    stack.push(match (left, right) {
                        (Some(left), Some(right)) => Some(CE::Binary(ConditionalBinary {
                            lhs: Box::new(left),
                            op: *op,
                            rhs: Box::new(right),
                        })),
                        (None, Some(node)) | (Some(node), None) => Some(node),
                        (None, None) => None,
                    });
                }
                _ => {}
            }
        }
        ControlFlow::Continue(())
    });

    if let ControlFlow::Break(err) = res {
        return Err(err);
    }

    let cond = stack
        .pop()
        .ok_or_else(|| error::map::internal("expected an element on stack"))?;

    Ok((cond, time_range))
}

/// Search `cond` for expressions involving the `time` column.
pub fn has_time_range(cond: &ConditionalExpression) -> bool {
    walk_expression(cond, &mut |e| {
        if let Expression::Conditional(cond) = e {
            if is_time_field(cond) {
                return ControlFlow::Break(());
            }
        }
        ControlFlow::Continue(())
    })
    .is_break()
}

/// Represents the time range as [lower, upper].
///
/// A value of [`None`] is unbounded.
#[derive(Clone, Copy, Default, Debug, Eq, PartialEq)]
pub struct TimeRange {
    /// The lower bound of the time range.
    pub lower: Option<i64>,
    /// The upper bound of the time range.
    pub upper: Option<i64>,
}

impl TimeRange {
    /// Create a new time range with the specified lower and upper bounds.
    pub fn new(lower: Option<i64>, upper: Option<i64>) -> Self {
        Self { lower, upper }
    }

    /// Returns `true` if the `lower` and `upper` bounds are `None`.
    pub fn is_unbounded(self) -> bool {
        self.lower.is_none() && self.upper.is_none()
    }

    /// Update the receiver so it is the intersection with `other`.
    fn intersect(&mut self, other: Self) {
        *self = self.intersected(other)
    }

    /// Return a time range that is the intersection of the receiver and `other`.
    pub fn intersected(self, other: Self) -> Self {
        let lower = other.lower.map_or(self.lower, |other| match self.lower {
            None => Some(other),
            Some(existing) if other > existing => Some(other),
            _ => self.lower,
        });

        let upper = other.upper.map_or(self.upper, |other| match self.upper {
            None => Some(other),
            Some(existing) if other < existing => Some(other),
            _ => self.upper,
        });

        Self { lower, upper }
    }
}

/// Simplifies an InfluxQL duration `expr` to a nanosecond interval represented as an `i64`.
pub fn duration_expr_to_nanoseconds(ctx: &ReduceContext, expr: &Expr) -> Result<i64, ExprError> {
    match reduce_time_expr(ctx, expr)? {
        Expr::Literal(Literal::Timestamp(v)) => Ok(v.timestamp_nanos()),
        _ => error::expr("invalid duration expression"),
    }
}

/// Represents an error that occurred whilst simplifying an InfluxQL expression.
#[derive(Debug)]
pub enum ExprError {
    /// An error in the expression that can be resolved by the client.
    Expression(String),

    /// An internal error that signals a bug.
    Internal(String),
}

/// Helper functions for creating errors.
mod error {
    use super::ExprError;

    pub(crate) fn expr<T>(s: impl Into<String>) -> Result<T, ExprError> {
        Err(map::expr(s))
    }

    pub(crate) fn internal<T>(s: impl Into<String>) -> Result<T, ExprError> {
        Err(map::internal(s))
    }

    pub(crate) mod map {
        use super::*;

        pub(crate) fn expr(s: impl Into<String>) -> ExprError {
            ExprError::Expression(s.into())
        }

        pub(crate) fn internal(s: impl Into<String>) -> ExprError {
            ExprError::Internal(s.into())
        }
    }
}

/// Context used when simplifying InfluxQL time range expressions.
#[derive(Default, Debug, Clone, Copy)]
pub struct ReduceContext {
    /// The value for the `now()` function.
    pub now: Option<Timestamp>,
    /// The timezone to evaluate literal timestamp strings.
    pub tz: Option<chrono_tz::Tz>,
}

/// Simplify the time range expression and return a literal [timestamp](Timestamp).
fn reduce_time_expr(ctx: &ReduceContext, expr: &Expr) -> ExprResult {
    match reduce_expr(ctx, expr)? {
        expr @ Expr::Literal(Literal::Timestamp(_)) => Ok(expr),
        Expr::Literal(Literal::String(ref s)) => {
            parse_timestamp_expr(s, ctx.tz).map_err(map_expr_err(expr))
        }
        Expr::Literal(Literal::Duration(v)) => Ok(lit(nanos_to_timestamp(*v))),
        Expr::Literal(Literal::Float(v)) => Ok(lit(nanos_to_timestamp(v as i64))),
        Expr::Literal(Literal::Integer(v)) => Ok(lit(nanos_to_timestamp(v))),
        _ => error::expr("invalid time range expression"),
    }
}

fn reduce_expr(ctx: &ReduceContext, expr: &Expr) -> ExprResult {
    match expr {
        Expr::Binary(ref v) => reduce_binary_expr(ctx, v).map_err(map_expr_err(expr)),
        Expr::Call (call) if is_now_function(call.name.as_str()) => ctx.now.map(lit).ok_or_else(|| error::map::internal("unable to resolve now")),
        Expr::Call (call) => {
            error::expr(
                format!("invalid function call '{}'", call.name),
            )
        }
        Expr::Nested(expr) => reduce_expr(ctx, expr),
        Expr::Literal(val) => match val {
            Literal::Integer(_) |
            Literal::Float(_) |
            Literal::String(_) |
            Literal::Timestamp(_) |
            Literal::Duration(_) => Ok(Expr::Literal(val.clone())),
            _ => error::expr(format!(
                "found literal '{val}', expected duration, float, integer, or timestamp string"
            )),
        },

        Expr::VarRef { .. } | Expr::BindParameter(_) | Expr::Wildcard(_) | Expr::Distinct(_) => error::expr(format!(
            "found symbol '{expr}', expected now() or a literal duration, float, integer and timestamp string"
        )),
    }
}

fn reduce_binary_expr(ctx: &ReduceContext, expr: &Binary) -> ExprResult {
    let lhs = reduce_expr(ctx, &expr.lhs)?;
    let op = expr.op;
    let rhs = reduce_expr(ctx, &expr.rhs)?;

    match lhs {
        Expr::Literal(Literal::Duration(v)) => reduce_binary_lhs_duration(ctx, v, op, rhs),
        Expr::Literal(Literal::Integer(v)) => reduce_binary_lhs_integer(ctx, v, op, rhs),
        Expr::Literal(Literal::Float(v)) => reduce_binary_lhs_float(v, op, rhs),
        Expr::Literal(Literal::Timestamp(v)) => reduce_binary_lhs_timestamp(ctx, v, op, rhs),
        Expr::Literal(Literal::String(v)) => reduce_binary_lhs_string(ctx, v, op, rhs),
        _ => Ok(Expr::Binary(Binary {
            lhs: Box::new(lhs),
            op,
            rhs: Box::new(rhs),
        })),
    }
}

/// Reduce `duration OP expr`.
///
/// ```text
/// duration  = duration ( ADD | SUB ) ( duration | NOW() )
/// duration  = duration ( MUL | DIV ) ( float | integer )
/// timestamp = duration ADD string
/// timestamp = duration ADD timestamp
/// ```
fn reduce_binary_lhs_duration(
    ctx: &ReduceContext,
    lhs: Duration,
    op: BinaryOperator,
    rhs: Expr,
) -> ExprResult {
    match rhs {
        Expr::Literal(ref val) => match val {
            // durations may be added and subtracted from other durations
            Literal::Duration(Duration(v)) => match op {
                BinaryOperator::Add => Ok(lit(Duration(
                    lhs.checked_add(*v)
                        .ok_or_else(|| error::map::expr("overflow"))?,
                ))),
                BinaryOperator::Sub => Ok(lit(Duration(
                    lhs.checked_sub(*v)
                        .ok_or_else(|| error::map::expr("overflow"))?,
                ))),
                _ => error::expr(format!("found operator '{op}', expected +, -")),
            },
            // durations may only be scaled by float literals
            Literal::Float(v) => {
                reduce_binary_lhs_duration(ctx, lhs, op, Expr::Literal(Literal::Integer(*v as i64)))
            }
            Literal::Integer(v) => match op {
                BinaryOperator::Mul => Ok(lit(Duration(*lhs * *v))),
                BinaryOperator::Div => Ok(lit(Duration(*lhs / *v))),
                _ => error::expr(format!("found operator '{op}', expected *, /")),
            },
            // A timestamp may be added to a duration
            Literal::Timestamp(v) if matches!(op, BinaryOperator::Add) => {
                Ok(lit(*v + chrono::Duration::nanoseconds(*lhs)))
            }
            Literal::String(v) => {
                reduce_binary_lhs_duration(ctx, lhs, op, parse_timestamp_expr(v, ctx.tz)?)
            }
            // This should not occur, as acceptable literals are validated in `reduce_expr`.
            _ => error::internal(format!(
                "unexpected literal '{rhs}' for duration expression"
            )),
        },
        _ => error::expr("invalid duration expression"),
    }
}

/// Reduce `integer OP expr`.
///
/// ```text
/// integer   = integer ( ADD | SUB | MUL | DIV | MOD | BitwiseAND | BitwiseOR | BitwiseXOR ) integer
/// float     = integer as float OP float
/// timestamp = integer as timestamp OP duration
/// ```
fn reduce_binary_lhs_integer(
    ctx: &ReduceContext,
    lhs: i64,
    op: BinaryOperator,
    rhs: Expr,
) -> ExprResult {
    match rhs {
        Expr::Literal(Literal::Float(_)) => reduce_binary_lhs_float(lhs as f64, op, rhs),
        Expr::Literal(Literal::Integer(v)) => Ok(lit(op.reduce(lhs, v))),
        Expr::Literal(Literal::Duration(_)) => {
            reduce_binary_lhs_timestamp(ctx, nanos_to_timestamp(lhs), op, rhs)
        }
        Expr::Literal(Literal::String(v)) => {
            reduce_binary_lhs_duration(ctx, Duration(lhs), op, parse_timestamp_expr(&v, ctx.tz)?)
        }
        _ => error::expr("invalid integer expression"),
    }
}

/// Reduce `float OP expr`.
///
/// ```text
/// float = float ( ADD | SUB | MUL | DIV | MOD ) ( float | integer)
/// ```
fn reduce_binary_lhs_float(lhs: f64, op: BinaryOperator, rhs: Expr) -> ExprResult {
    Ok(lit(match rhs {
        Expr::Literal(Literal::Float(v)) => op
            .try_reduce(lhs, v)
            .ok_or_else(|| error::map::expr("invalid operator for float expression"))?,
        Expr::Literal(Literal::Integer(v)) => op
            .try_reduce(lhs, v)
            .ok_or_else(|| error::map::expr("invalid operator for float expression"))?,
        _ => return error::expr("invalid float expression"),
    }))
}

/// Reduce `timestamp OP expr`.
///
/// The right-hand `expr` must be of a type that can be
/// coalesced to a duration, which includes a `duration`, `integer` or a
/// `string`. A `string` is parsed as a timestamp an interpreted as
/// the number of nanoseconds from the Unix epoch.
///
/// ```text
/// timestamp = timestamp ( ADD | SUB ) ( duration | integer | string | timestamp )
/// ```
fn reduce_binary_lhs_timestamp(
    ctx: &ReduceContext,
    lhs: Timestamp,
    op: BinaryOperator,
    rhs: Expr,
) -> ExprResult {
    match rhs {
        Expr::Literal(Literal::Duration(d)) => match op {
            BinaryOperator::Add => Ok(lit(lhs + chrono::Duration::nanoseconds(*d))),
            BinaryOperator::Sub => Ok(lit(lhs - chrono::Duration::nanoseconds(*d))),
            _ => error::expr(format!(
                "invalid operator '{op}' for timestamp and duration: expected +, -"
            )),
        },
        Expr::Literal(Literal::Integer(_))
        // NOTE: This is a slight deviation from InfluxQL, for which the only valid binary
        // operator for two timestamps is subtraction. By converting the timestamp to a
        // duration and calling this function recursively, we permit the addition operator.
        | Expr::Literal(Literal::Timestamp(_))
        | Expr::Literal(Literal::String(_)) => {
            reduce_binary_lhs_timestamp(ctx, lhs, op, expr_to_duration(ctx, rhs)?)
        }
        _ => error::expr(format!(
            "invalid expression '{rhs}': expected duration, integer or timestamp string"
        )),
    }
}

fn expr_to_duration(ctx: &ReduceContext, expr: Expr) -> ExprResult {
    Ok(lit(match expr {
        Expr::Literal(Literal::Duration(v)) => v,
        Expr::Literal(Literal::Integer(v)) => Duration(v),
        Expr::Literal(Literal::Timestamp(v)) => Duration(v.timestamp_nanos()),
        Expr::Literal(Literal::String(v)) => {
            Duration(parse_timestamp_nanos(&v, ctx.tz)?.timestamp_nanos())
        }
        _ => return error::expr(format!("unable to cast {expr} to duration")),
    }))
}

/// Reduce `string OP expr`.
///
/// If `expr` is a string, concatenates the two values and returns a new string.
/// If `expr` is a duration, integer or timestamp, the left-hand
/// string is parsed as a timestamp and the expression evaluated as
/// `timestamp OP expr`
fn reduce_binary_lhs_string(
    ctx: &ReduceContext,
    lhs: String,
    op: BinaryOperator,
    rhs: Expr,
) -> ExprResult {
    match rhs {
        Expr::Literal(Literal::String(ref s)) => match op {
            // concatenate the two strings
            BinaryOperator::Add => Ok(lit(lhs + s)),
            _ => reduce_binary_lhs_timestamp(ctx, parse_timestamp_nanos(&lhs, ctx.tz)?, op, rhs),
        },
        Expr::Literal(Literal::Duration(_))
        | Expr::Literal(Literal::Timestamp(_))
        | Expr::Literal(Literal::Integer(_)) => {
            reduce_binary_lhs_timestamp(ctx, parse_timestamp_nanos(&lhs, ctx.tz)?, op, rhs)
        }
        _ => error::expr(format!(
            "found '{rhs}', expected duration, integer or timestamp string"
        )),
    }
}

/// Returns true if the conditional expression is a single node that
/// refers to the `time` column.
///
/// In a conditional expression, this comparison is case-insensitive per the [Go implementation][go]
///
/// [go]: https://github.com/influxdata/influxql/blob/1ba470371ec093d57a726b143fe6ccbacf1b452b/ast.go#L5751-L5753
fn is_time_field(cond: &ConditionalExpression) -> bool {
    if let ConditionalExpression::Expr(expr) = cond {
        if let Expr::VarRef(VarRef { ref name, .. }) = **expr {
            name.eq_ignore_ascii_case("time")
        } else {
            false
        }
    } else {
        false
    }
}

fn parse_timestamp_nanos(s: &str, tz: Option<chrono_tz::Tz>) -> Result<Timestamp, ExprError> {
    parse_timestamp(s, tz)
        .ok_or_else(|| error::map::expr(format!("'{s}' is not a valid timestamp")))
}

/// Parse s as a timestamp in the specified timezone and return the timestamp
/// as a literal timestamp expression.
fn parse_timestamp_expr(s: &str, tz: Option<chrono_tz::Tz>) -> ExprResult {
    Ok(Expr::Literal(Literal::Timestamp(parse_timestamp_nanos(
        s, tz,
    )?)))
}

fn map_expr_err(expr: &Expr) -> impl Fn(ExprError) -> ExprError + '_ {
    move |err| {
        error::map::expr(format!(
            "invalid expression \"{expr}\": {}",
            match err {
                ExprError::Expression(str) | ExprError::Internal(str) => str,
            }
        ))
    }
}

#[cfg(test)]
mod test {
    use crate::expression::ConditionalExpression;
    use crate::time_range::{
        duration_expr_to_nanoseconds, reduce_time_expr, split_cond, ExprError, ExprResult,
        ReduceContext, TimeRange,
    };
    use crate::timestamp::Timestamp;
    use chrono::{NaiveDate, NaiveDateTime, NaiveTime, Offset, Utc};
    use test_helpers::assert_error;

    /// Return a `ReduceContext` with a value of
    /// now set to `2023-01-01T00:00:00Z` / `1672531200000000000`
    /// and not timezone.
    fn reduce_context() -> ReduceContext {
        ReduceContext {
            now: Some(Timestamp::from_utc(
                NaiveDateTime::new(
                    NaiveDate::from_ymd_opt(2023, 1, 1).unwrap(),
                    NaiveTime::from_hms_opt(0, 0, 0).unwrap(),
                ),
                Utc.fix(),
            )),
            tz: None,
        }
    }

    #[test]
    fn test_split_cond() {
        fn split_exprs(s: &str) -> Result<(Option<ConditionalExpression>, TimeRange), ExprError> {
            let ctx = reduce_context();
            let cond: ConditionalExpression = s.parse().unwrap();
            split_cond(&ctx, &cond)
        }

        macro_rules! range {
            (lower=$LOWER:literal) => {
                TimeRange {
                    lower: Some($LOWER),
                    upper: None,
                }
            };
            (lower=$LOWER:literal, upper ex=$UPPER:literal) => {
                TimeRange {
                    lower: Some($LOWER),
                    upper: Some($UPPER - 1),
                }
            };
            (lower=$LOWER:literal, upper=$UPPER:literal) => {
                TimeRange {
                    lower: Some($LOWER),
                    upper: Some($UPPER),
                }
            };
            (lower ex=$LOWER:literal, upper=$UPPER:literal) => {
                TimeRange {
                    lower: Some($LOWER + 1),
                    upper: Some($UPPER),
                }
            };
            (lower ex=$LOWER:literal) => {
                TimeRange {
                    lower: Some($LOWER + 1),
                    upper: None,
                }
            };
            (upper=$UPPER:literal) => {
                TimeRange {
                    lower: None,
                    upper: Some($UPPER),
                }
            };
            (upper ex=$UPPER:literal) => {
                TimeRange {
                    lower: None,
                    upper: Some($UPPER - 1),
                }
            };
            (eq=$TS:literal) => {
                TimeRange {
                    lower: Some($TS),
                    upper: Some($TS),
                }
            };
        }

        let (cond, tr) = split_exprs("time >= now() - 1s").unwrap();
        assert!(cond.is_none());
        assert_eq!(tr, range!(lower = 1672531199000000000));

        // reduces the lower bound to a single expression
        let (cond, tr) = split_exprs("time >= now() - 1s AND time >= now() - 500ms").unwrap();
        assert!(cond.is_none());
        assert_eq!(tr, range!(lower = 1672531199500000000));

        let (cond, tr) = split_exprs("time <= now() - 1s").unwrap();
        assert!(cond.is_none());
        assert_eq!(tr, range!(upper = 1672531199000000000));

        // reduces the upper bound to a single expression
        let (cond, tr) = split_exprs("time <= now() + 1s AND time <= now() + 500ms").unwrap();
        assert!(cond.is_none());
        assert_eq!(tr, range!(upper = 1672531200500000000));

        let (cond, tr) = split_exprs("time >= now() - 1s AND time < now()").unwrap();
        assert!(cond.is_none());
        assert_eq!(
            tr,
            range!(lower=1672531199000000000, upper ex=1672531200000000000)
        );

        let (cond, tr) = split_exprs("time >= now() - 1s AND cpu = 'cpu0'").unwrap();
        assert_eq!(cond.unwrap().to_string(), "cpu = 'cpu0'");
        assert_eq!(tr, range!(lower = 1672531199000000000));

        let (cond, tr) = split_exprs("time = 0").unwrap();
        assert!(cond.is_none());
        assert_eq!(tr, range!(eq = 0));

        let (cond, tr) = split_exprs(
            "instance = 'instance-01' OR instance = 'instance-02' AND time >= now() - 1s",
        )
        .unwrap();
        assert_eq!(
            cond.unwrap().to_string(),
            "instance = 'instance-01' OR instance = 'instance-02'"
        );
        assert_eq!(tr, range!(lower = 1672531199000000000));

        let (cond, tr) =
            split_exprs("time >= now() - 1s AND time < now() AND cpu = 'cpu0' OR cpu = 'cpu1'")
                .unwrap();
        assert_eq!(cond.unwrap().to_string(), "cpu = 'cpu0' OR cpu = 'cpu1'");
        assert_eq!(
            tr,
            range!(lower=1672531199000000000, upper ex=1672531200000000000)
        );

        // time >= now - 60s AND time < now() OR cpu = 'cpu0' OR cpu = 'cpu1'
        //
        // Split the time range, despite using the disjunction (OR) operator
        let (cond, tr) =
            split_exprs("time >= now() - 1s AND time < now() OR cpu = 'cpu0' OR cpu = 'cpu1'")
                .unwrap();
        assert_eq!(cond.unwrap().to_string(), "cpu = 'cpu0' OR cpu = 'cpu1'");
        assert_eq!(
            tr,
            range!(lower=1672531199000000000, upper ex=1672531200000000000)
        );

        let (cond, tr) = split_exprs("time = 0 OR time = 10 AND cpu = 'cpu0'").unwrap();
        assert_eq!(cond.unwrap().to_string(), "cpu = 'cpu0'");
        // Models InfluxQL behaviour, which will result in no results being returned because
        // upper < lower
        assert_eq!(tr, range!(lower = 10, upper = 0));

        // no time
        let (cond, tr) = split_exprs("f64 >= 19.5 OR f64 =~ /foo/").unwrap();
        assert_eq!(cond.unwrap().to_string(), "f64 >= 19.5 OR f64 =~ /foo/");
        assert!(tr.is_unbounded());

        let (cond, tr) = split_exprs("time > now() OR time = 1000").unwrap();
        assert!(cond.is_none());
        assert_eq!(tr, range!(lower ex = 1672531200000000000, upper = 1000));

        // fallible

        assert_error!(split_exprs("time > '2004-04-09T'"), ExprError::Expression(ref s) if s == "invalid expression \"'2004-04-09T'\": '2004-04-09T' is not a valid timestamp");
    }

    #[test]
    fn test_rewrite_time_expression_no_timezone() {
        fn process_expr(s: &str) -> ExprResult {
            let cond: ConditionalExpression =
                s.parse().expect("unexpected error parsing expression");
            let ctx = ReduceContext {
                now: Some(Timestamp::from_utc(
                    NaiveDateTime::new(
                        NaiveDate::from_ymd_opt(2004, 4, 9).unwrap(),
                        NaiveTime::from_hms_opt(12, 13, 14).unwrap(),
                    ),
                    Utc.fix(),
                )),
                tz: None,
            };
            reduce_time_expr(&ctx, cond.expr().unwrap())
        }

        macro_rules! assert_expr {
            ($S: expr, $EXPECTED: expr) => {
                let expr = process_expr($S).unwrap();
                assert_eq!(expr.to_string(), $EXPECTED);
            };
        }

        //
        // Valid literals
        //

        // Duration
        assert_expr!("1d", "1970-01-02T00:00:00+00:00");

        // Single integer interpreted as a Unix nanosecond epoch
        assert_expr!("1157082310000000000", "2006-09-01T03:45:10+00:00");

        // Single float interpreted as a Unix nanosecond epoch
        assert_expr!("1157082310000000000.0", "2006-09-01T03:45:10+00:00");

        // Single string interpreted as a timestamp
        assert_expr!(
            "'2004-04-09 02:33:45.123456789'",
            "2004-04-09T02:33:45.123456789+00:00"
        );

        // now
        assert_expr!("now()", "2004-04-09T12:13:14+00:00");

        //
        // Expressions
        //

        // now() OP expr
        assert_expr!("now() - 5m", "2004-04-09T12:08:14+00:00");
        assert_expr!("(now() - 5m)", "2004-04-09T12:08:14+00:00");
        assert_expr!("now() - 5m - 60m", "2004-04-09T11:08:14+00:00");
        assert_expr!("now() - 500", "2004-04-09T12:13:13.999999500+00:00");
        assert_expr!("now() - (5m + 60m)", "2004-04-09T11:08:14+00:00");

        // expr OP now()
        assert_expr!("5m + now()", "2004-04-09T12:18:14+00:00");

        // duration OP expr
        assert_expr!("1w3d + 1d", "1970-01-12T00:00:00+00:00");
        assert_expr!("1w3d - 1d", "1970-01-10T00:00:00+00:00");

        // string OP expr
        assert_expr!("'2004-04-09' - '2004-04-08'", "1970-01-02T00:00:00+00:00");

        assert_expr!("'2004-04-09' + '02:33:45'", "2004-04-09T02:33:45+00:00");

        // integer OP expr
        assert_expr!("1157082310000000000 - 1s", "2006-09-01T03:45:09+00:00");

        // nested evaluation order
        assert_expr!("now() - (6m - (1m * 5))", r#"2004-04-09T12:12:14+00:00"#);

        // Fallible

        use super::ExprError::Expression;
        assert_error!(process_expr("foo + 1"), Expression(ref s) if s == "invalid expression \"foo + 1\": found symbol 'foo', expected now() or a literal duration, float, integer and timestamp string");

        assert_error!(process_expr("5m - now()"), Expression(ref s) if s == "invalid expression \"5m - now()\": unexpected literal '2004-04-09T12:13:14+00:00' for duration expression");

        assert_error!(process_expr("'2004-04-09' + false"), Expression(ref s) if s == "invalid expression \"'2004-04-09' + false\": found literal 'false', expected duration, float, integer, or timestamp string");

        assert_error!(process_expr("1s * 1s"), Expression(ref s) if s == "invalid expression \"1000ms * 1000ms\": found operator '*', expected +, -");
        assert_error!(process_expr("1s + 0.5"), Expression(ref s) if s == "invalid expression \"1000ms + 0.5\": found operator '+', expected *, /");

        assert_error!(process_expr("'2004-04-09T'"), Expression(ref s) if s == "invalid expression \"'2004-04-09T'\": '2004-04-09T' is not a valid timestamp");
        assert_error!(process_expr("now() * 1"), Expression(ref s) if s == "invalid expression \"now() * 1\": invalid operator '*' for timestamp and duration: expected +, -");
        assert_error!(process_expr("'2' + now()"), Expression(ref s) if s == "invalid expression \"'2' + now()\": '2' is not a valid timestamp");
        assert_error!(process_expr("'2' + '3'"), Expression(ref s) if s == "invalid expression \"'2' + '3'\": '23' is not a valid timestamp");
        assert_error!(process_expr("'2' + '3' + 10s"), Expression(ref s) if s == "invalid expression \"'2' + '3' + 10s\": '23' is not a valid timestamp");
    }

    #[test]
    fn test_rewrite_time_expression_with_timezone() {
        fn process_expr(s: &str) -> ExprResult {
            let cond: ConditionalExpression =
                s.parse().expect("unexpected error parsing expression");
            let ctx = ReduceContext {
                now: None,
                tz: Some(chrono_tz::Australia::Hobart),
            };
            reduce_time_expr(&ctx, cond.expr().unwrap())
        }

        macro_rules! assert_expr {
            ($S: expr, $EXPECTED: expr) => {
                let expr = process_expr($S).unwrap();
                assert_eq!(expr.to_string(), $EXPECTED);
            };
        }

        assert_expr!(
            "'2004-04-09 10:05:00.123456789'",
            "2004-04-09T10:05:00.123456789+10:00"
        );
        assert_expr!("'2004-04-09'", "2004-04-09T00:00:00+10:00");
        assert_expr!(
            "'2004-04-09T10:05:00.123456789Z'",
            "2004-04-09T20:05:00.123456789+10:00"
        );
    }

    #[test]
    fn test_expr_to_duration() {
        fn parse(s: &str) -> Result<i64, ExprError> {
            let ctx = reduce_context();
            let expr = s
                .parse::<ConditionalExpression>()
                .unwrap()
                .expr()
                .unwrap()
                .clone();
            duration_expr_to_nanoseconds(&ctx, &expr)
        }

        let cases = vec![
            ("10s", 10_000_000_000_i64),
            ("10s + 1d", 86_410_000_000_000),
            ("5d10ms", 432_000_010_000_000),
            ("-2d10ms", -172800010000000),
            ("-2d10ns", -172800000000010),
            ("now()", 1672531200000000000),
            ("'2023-01-01T00:00:00Z'", 1672531200000000000),
        ];

        for (interval_str, exp) in cases {
            let got = parse(interval_str).unwrap();
            assert_eq!(got, exp, "Actual: {got:?}");
        }
    }

    #[test]
    fn test_time_range_is_unbounded() {
        let a = TimeRange::new(Some(1000), Some(5000));
        assert!(!a.is_unbounded());
        let a = TimeRange::new(None, Some(5000));
        assert!(!a.is_unbounded());
        let a = TimeRange::new(Some(1000), None);
        assert!(!a.is_unbounded());
        let a = TimeRange::new(None, None);
        assert!(a.is_unbounded());
    }

    #[test]
    fn test_time_range_intersect() {
        let a = TimeRange::new(Some(1000), Some(5000));
        let b = TimeRange::new(Some(2000), Some(6000));
        assert_eq!(a.intersected(b), TimeRange::new(Some(2000), Some(5000)));
        assert_eq!(b.intersected(a), TimeRange::new(Some(2000), Some(5000)));

        let a = TimeRange::new(Some(1000), None);
        let b = TimeRange::new(Some(2000), Some(6000));
        assert_eq!(a.intersected(b), TimeRange::new(Some(2000), Some(6000)));

        let a = TimeRange::new(None, None);
        let b = TimeRange::new(Some(2000), Some(6000));
        assert_eq!(a.intersected(b), TimeRange::new(Some(2000), Some(6000)));
    }
}
