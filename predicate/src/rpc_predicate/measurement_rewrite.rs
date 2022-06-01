use datafusion::error::Result as DataFusionResult;
use datafusion::logical_plan::{lit, Column, Expr, ExprRewritable, ExprRewriter};

use super::MEASUREMENT_COLUMN_NAME;

/// Rewrites all references to the [MEASUREMENT_COLUMN_NAME] column
/// with the actual table name
pub(crate) fn rewrite_measurement_references(
    table_name: &str,
    expr: Expr,
) -> DataFusionResult<Expr> {
    let mut rewriter = MeasurementRewriter { table_name };
    expr.rewrite(&mut rewriter)
}

struct MeasurementRewriter<'a> {
    table_name: &'a str,
}

impl ExprRewriter for MeasurementRewriter<'_> {
    fn mutate(&mut self, expr: Expr) -> DataFusionResult<Expr> {
        Ok(match expr {
            // rewrite col("_measurement") --> "table_name"
            Expr::Column(Column { relation, name }) if name == MEASUREMENT_COLUMN_NAME => {
                // should not have a qualified foo._measurement
                // reference
                assert!(relation.is_none());
                lit(self.table_name)
            }
            // no rewrite needed
            _ => expr,
        })
    }
}
