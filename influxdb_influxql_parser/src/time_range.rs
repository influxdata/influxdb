//! Process InfluxQL time range expressions
//!
use crate::expression::walk::{walk_expression, Expression};
use crate::expression::{lit, Binary, BinaryOperator, ConditionalExpression, Expr, VarRef};
use crate::functions::is_now_function;
use crate::literal::{nanos_to_timestamp, Duration, Literal};
use crate::timestamp::{parse_timestamp, Timestamp};
use std::ops::ControlFlow;

/// Result type for operations that could result in an [`ExprError`].
pub type ExprResult = Result<Expr, ExprError>;

#[allow(dead_code)]
fn split_cond(
    cond: &ConditionalExpression,
) -> (Option<ConditionalExpression>, Option<ConditionalExpression>) {
    // search the tree for an expression involving `time`.
    let no_time = walk_expression(cond, &mut |e| {
        if let Expression::Conditional(cond) = e {
            if is_time_field(cond) {
                return ControlFlow::Break(());
            }
        }
        ControlFlow::Continue(())
    })
    .is_continue();

    if no_time {
        return (None, None);
    }

    unimplemented!()
}

/// Simplifies an InfluxQL duration `expr` to a nanosecond interval represented as an `i64`.
pub fn duration_expr_to_nanoseconds(expr: &Expr) -> Result<i64, ExprError> {
    let ctx = ReduceContext::default();
    match reduce_expr(&ctx, expr)? {
        Expr::Literal(Literal::Duration(v)) => Ok(*v),
        Expr::Literal(Literal::Float(v)) => Ok(v as i64),
        Expr::Literal(Literal::Integer(v)) => Ok(v),
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

    pub(crate) mod map {
        use super::*;

        pub(crate) fn expr(s: impl Into<String>) -> ExprError {
            ExprError::Expression(s.into())
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

#[allow(dead_code)]
fn reduce(
    ctx: &ReduceContext,
    cond: &ConditionalExpression,
) -> Result<ConditionalExpression, ExprError> {
    Ok(match cond {
        ConditionalExpression::Expr(expr) => {
            ConditionalExpression::Expr(Box::new(reduce_expr(ctx, expr)?))
        }
        ConditionalExpression::Binary(_) => unimplemented!(),
        ConditionalExpression::Grouped(_) => unimplemented!(),
    })
}

/// Simplify the time range expression.
pub fn reduce_time_expr(ctx: &ReduceContext, expr: &Expr) -> ExprResult {
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
        Expr::Call (call) if is_now_function(call.name.as_str()) => ctx.now.map(lit).ok_or_else(|| ExprError::Internal("unable to resolve now".into())),
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
            _ => Err(ExprError::Internal(format!(
                "unexpected literal '{rhs}' for duration expression"
            ))),
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
        ReduceContext,
    };
    use crate::timestamp::Timestamp;
    use chrono::{NaiveDate, NaiveDateTime, NaiveTime, Offset, Utc};
    use test_helpers::assert_error;

    #[ignore]
    #[test]
    fn test_split_cond() {
        let cond: ConditionalExpression = "time > now() - 1h".parse().unwrap();
        let (cond, time) = split_cond(&cond);
        println!("{cond:?}, {time:?}");
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
            let expr = s
                .parse::<ConditionalExpression>()
                .unwrap()
                .expr()
                .unwrap()
                .clone();
            duration_expr_to_nanoseconds(&expr)
        }

        let cases = vec![
            ("10s", 10_000_000_000_i64),
            ("10s + 1d", 86_410_000_000_000),
            ("5d10ms", 432_000_010_000_000),
            ("-2d10ms", -172800010000000),
            ("-2d10ns", -172800000000010),
        ];

        for (interval_str, exp) in cases {
            let got = parse(interval_str).unwrap();
            assert_eq!(got, exp, "Actual: {got:?}");
        }
    }
}
