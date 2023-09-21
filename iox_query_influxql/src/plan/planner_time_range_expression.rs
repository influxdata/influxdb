//! APIs for transforming InfluxQL [expressions][influxdb_influxql_parser::expression::Expr].
use datafusion::common::ScalarValue;
use datafusion::logical_expr::{lit, Expr as DFExpr};
use datafusion_util::AsExpr;
use influxdb_influxql_parser::time_range::TimeRange;

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
