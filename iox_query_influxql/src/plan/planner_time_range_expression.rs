//! APIs for transforming InfluxQL [expressions][influxdb_influxql_parser::expression::Expr].
use crate::plan::error;
use datafusion::common::{Result, ScalarValue};
use datafusion::logical_expr::{lit, Expr as DFExpr};
use datafusion_util::AsExpr;
use influxdb_influxql_parser::expression::Expr;
use influxdb_influxql_parser::time_range::{duration_expr_to_nanoseconds, TimeRange};

type ExprResult = Result<DFExpr>;

/// Simplifies `expr` to an InfluxQL duration and returns a DataFusion interval.
///
/// Returns an error if `expr` is not a duration expression.
pub(super) fn expr_to_df_interval_dt(expr: &Expr) -> ExprResult {
    let ns = duration_expr_to_nanoseconds(expr).map_err(error::map::expr_error)?;
    Ok(lit(ScalarValue::new_interval_mdn(0, 0, ns)))
}

fn lower_bound_to_df_expr(v: Option<i64>) -> Option<DFExpr> {
    v.map(|ts| {
        "time"
            .as_expr()
            .gt_eq(lit(ScalarValue::TimestampNanosecond(Some(ts), None)))
    })
}

fn upper_bound_to_df_expr(v: Option<i64>) -> Option<DFExpr> {
    v.map(|ts| {
        "time"
            .as_expr()
            .lt_eq(lit(ScalarValue::TimestampNanosecond(Some(ts), None)))
    })
}

pub(super) fn time_range_to_df_expr(cond: TimeRange) -> Option<DFExpr> {
    match (cond.lower, cond.upper) {
        (Some(lower), Some(upper)) if lower == upper => {
            // Use ts = <lower> if the lower and upper are equal
            Some(
                "time"
                    .as_expr()
                    .eq(lit(ScalarValue::TimestampNanosecond(Some(lower), None))),
            )
        }
        // If lower > upper, then the expression lower < time < upper is always false
        (Some(lower), Some(upper)) if lower > upper => Some(lit(false)),
        (lower, upper) => match (lower_bound_to_df_expr(lower), upper_bound_to_df_expr(upper)) {
            (Some(e), None) | (None, Some(e)) => Some(e),
            (Some(lower), Some(upper)) => Some(lower.and(upper)),
            (None, None) => None,
        },
    }
}
