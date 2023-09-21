use datafusion::common::tree_node::Transformed;
use datafusion::error::Result as DataFusionResult;
use datafusion::prelude::{lit, Column, Expr};

use super::MEASUREMENT_COLUMN_NAME;

/// Rewrites all references to the [MEASUREMENT_COLUMN_NAME] column
/// with the actual table name
pub(crate) fn rewrite_measurement_references(
    table_name: &str,
    expr: Expr,
) -> DataFusionResult<Transformed<Expr>> {
    Ok(match expr {
        // rewrite col("_measurement") --> "table_name"
        Expr::Column(Column { relation, name }) if name == MEASUREMENT_COLUMN_NAME => {
            // should not have a qualified foo._measurement
            // reference
            assert!(relation.is_none());
            Transformed::Yes(lit(table_name))
        }
        // no rewrite needed
        _ => Transformed::No(expr),
    })
}
