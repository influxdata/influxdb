//! APIs for transforming InfluxQL [expressions][influxdb_influxql_parser::expression::Expr].
use crate::plan::error;
use datafusion::common::{Result, ScalarValue};
use datafusion::logical_expr::{lit, Expr as DFExpr};
use influxdb_influxql_parser::expression::Expr;
use influxdb_influxql_parser::time_range::duration_expr_to_nanoseconds;

type ExprResult = Result<DFExpr>;

/// Simplifies `expr` to an InfluxQL duration and returns a DataFusion interval.
///
/// Returns an error if `expr` is not a duration expression.
pub(super) fn expr_to_df_interval_dt(expr: &Expr) -> ExprResult {
    let ns = duration_expr_to_nanoseconds(expr).map_err(error::map::expr_error)?;
    Ok(lit(ScalarValue::new_interval_mdn(0, 0, ns)))
}
