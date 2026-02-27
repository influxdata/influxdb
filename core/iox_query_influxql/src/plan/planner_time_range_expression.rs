//! APIs for transforming InfluxQL [expressions][influxdb_influxql_parser::expression::Expr].
use datafusion::{
    logical_expr::{Expr as DFExpr, lit},
    scalar::ScalarValue,
};
use datafusion_util::AsExpr;
use influxdb_influxql_parser::time_range::TimeRange;
use std::sync::Arc;

/// Assumes the provided nanosecond i64 value is in UTC.
///
/// Returns a datafusion expression in UTC, relying upon the datafusion
/// engine to perform any casting (to other timezones) as needed.
fn lower_bound_to_df_expr<E: AsExpr + ?Sized>(
    time_expr: &E,
    tz: &Option<Arc<str>>,
    v: Option<i64>,
) -> Option<DFExpr> {
    v.map(|ts| {
        time_expr
            .as_expr()
            .gt_eq(lit(ScalarValue::TimestampNanosecond(Some(ts), tz.clone())))
    })
}

/// Assumes the provided nanosecond i64 value is in UTC.
///
/// Returns a datafusion expression in UTC, relying upon the datafusion
/// engine to perform any casting (to other timezones) as needed.
fn upper_bound_to_df_expr<E: AsExpr + ?Sized>(
    time_expr: &E,
    tz: &Option<Arc<str>>,
    v: Option<i64>,
) -> Option<DFExpr> {
    v.map(|ts| {
        time_expr
            .as_expr()
            .lt_eq(lit(ScalarValue::TimestampNanosecond(Some(ts), tz.clone())))
    })
}

/// Assumes the provided nanosecond i64 value is in UTC.
///
/// Returns a datafusion expression in UTC, relying upon the datafusion
/// engine to perform any casting (to other timezones) as needed.
pub(super) fn time_range_to_df_expr<E: AsExpr + ?Sized>(
    time_expr: &E,
    tz: &Option<Arc<str>>,
    cond: TimeRange,
) -> Option<DFExpr> {
    match (cond.lower, cond.upper) {
        (Some(lower), Some(upper)) if lower == upper => {
            // Use ts = <lower> if the lower and upper are equal
            Some(time_expr.as_expr().eq(lit(ScalarValue::TimestampNanosecond(
                Some(lower),
                tz.clone(),
            ))))
        }
        // If lower > upper, then the expression lower < time < upper is always false
        (Some(lower), Some(upper)) if lower > upper => Some(lit(false)),
        (lower, upper) => match (
            lower_bound_to_df_expr(time_expr, tz, lower),
            upper_bound_to_df_expr(time_expr, tz, upper),
        ) {
            (Some(e), None) | (None, Some(e)) => Some(e),
            (Some(lower), Some(upper)) => Some(lower.and(upper)),
            (None, None) => None,
        },
    }
}
