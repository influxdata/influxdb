//! APIs for transforming InfluxQL [expressions][influxdb_influxql_parser::expression::Expr].
use crate::plan::error;
use datafusion::common::{Result, ScalarValue};
use datafusion::logical_expr::{lit, Expr as DFExpr};
use datafusion::optimizer::utils::disjunction;
use datafusion_util::AsExpr;
use influxdb_influxql_parser::expression::Expr;
use influxdb_influxql_parser::time_range::{
    duration_expr_to_nanoseconds, LowerBound, TimeCondition, UpperBound,
};
use std::ops::Bound;

type ExprResult = Result<DFExpr>;

/// Simplifies `expr` to an InfluxQL duration and returns a DataFusion interval.
///
/// Returns an error if `expr` is not a duration expression.
pub(super) fn expr_to_df_interval_dt(expr: &Expr) -> ExprResult {
    let ns = duration_expr_to_nanoseconds(expr).map_err(error::map::expr_error)?;
    Ok(lit(ScalarValue::new_interval_mdn(0, 0, ns)))
}

fn lower_bound_to_df_expr(v: LowerBound) -> Option<DFExpr> {
    match *v {
        Bound::Included(ts) => Some(
            "time"
                .as_expr()
                .gt_eq(lit(ScalarValue::TimestampNanosecond(Some(ts), None))),
        ),
        Bound::Excluded(ts) => Some(
            "time"
                .as_expr()
                .gt(lit(ScalarValue::TimestampNanosecond(Some(ts), None))),
        ),
        Bound::Unbounded => None,
    }
}

fn upper_bound_to_df_expr(v: UpperBound) -> Option<DFExpr> {
    match *v {
        Bound::Included(ts) => Some(
            "time"
                .as_expr()
                .lt_eq(lit(ScalarValue::TimestampNanosecond(Some(ts), None))),
        ),
        Bound::Excluded(ts) => Some(
            "time"
                .as_expr()
                .lt(lit(ScalarValue::TimestampNanosecond(Some(ts), None))),
        ),
        Bound::Unbounded => None,
    }
}

pub(super) fn time_condition_to_df_expr(cond: TimeCondition) -> Option<DFExpr> {
    match cond {
        TimeCondition::Range(tr) => match (
            lower_bound_to_df_expr(tr.lower),
            upper_bound_to_df_expr(tr.upper),
        ) {
            (None, None) => None,
            (Some(e), None) | (None, Some(e)) => Some(e),
            (Some(lower), Some(upper)) => Some(lower.and(upper)),
        },
        TimeCondition::List(timestamps) => disjunction(timestamps.into_iter().map(|ts| {
            "time"
                .as_expr()
                .eq(lit(ScalarValue::TimestampNanosecond(Some(ts), None)))
        })),
    }
}
