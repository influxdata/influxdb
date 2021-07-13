//! This module contains DataFusion utility functions and helpers

use std::collections::HashSet;

use arrow::{compute::SortOptions, datatypes::Schema as ArrowSchema, record_batch::RecordBatch};

use datafusion::{
    error::DataFusionError,
    logical_plan::{Expr, LogicalPlan, LogicalPlanBuilder},
    optimizer::utils::expr_to_columns,
    physical_plan::expressions::{col as physical_col, PhysicalSortExpr},
};
use internal_types::schema::{sort::SortKey, Schema};

/// Create a logical plan that produces the record batch
pub fn make_scan_plan(batch: RecordBatch) -> std::result::Result<LogicalPlan, DataFusionError> {
    let schema = batch.schema();
    let partitions = vec![vec![batch]];
    let projection = None; // scan all columns
    LogicalPlanBuilder::scan_memory(partitions, schema, projection)?.build()
}

/// Returns true if all columns referred to in schema are present, false
/// otherwise
pub fn schema_has_all_expr_columns(schema: &Schema, expr: &Expr) -> bool {
    let mut predicate_columns = HashSet::new();
    expr_to_columns(expr, &mut predicate_columns).unwrap();

    predicate_columns
        .into_iter()
        .all(|col_name| schema.find_index_of(&col_name.name).is_some())
}

/// Returns the pk in arrow's expression used for data sorting
pub fn arrow_pk_sort_exprs(
    key_columns: Vec<&str>,
    input_schema: &ArrowSchema,
) -> Vec<PhysicalSortExpr> {
    let mut sort_exprs = vec![];
    for key in key_columns {
        let expr = physical_col(key, &input_schema).expect("pk in schema");
        sort_exprs.push(PhysicalSortExpr {
            expr,
            options: SortOptions {
                descending: false,
                nulls_first: false,
            },
        });
    }

    sort_exprs
}

pub fn arrow_sort_key_exprs(
    sort_key: SortKey<'_>,
    input_schema: &ArrowSchema,
) -> Vec<PhysicalSortExpr> {
    let mut sort_exprs = vec![];
    for (key, options) in sort_key.iter() {
        let expr = physical_col(key, &input_schema).expect("sort key column in schema");
        sort_exprs.push(PhysicalSortExpr {
            expr,
            options: SortOptions {
                descending: options.descending,
                nulls_first: options.nulls_first,
            },
        });
    }

    sort_exprs
}

#[cfg(test)]
mod tests {
    use datafusion::prelude::*;
    use internal_types::schema::builder::SchemaBuilder;

    use super::*;

    #[test]
    fn test_schema_has_all_exprs_() {
        let schema = SchemaBuilder::new().tag("t1").timestamp().build().unwrap();

        assert!(schema_has_all_expr_columns(
            &schema,
            &col("t1").eq(lit("foo"))
        ));
        assert!(!schema_has_all_expr_columns(
            &schema,
            &col("t2").eq(lit("foo"))
        ));
        assert!(schema_has_all_expr_columns(
            &schema,
            &col("t1").eq(col("time"))
        ));
        assert!(!schema_has_all_expr_columns(
            &schema,
            &col("t1").eq(col("time2"))
        ));
        assert!(!schema_has_all_expr_columns(
            &schema,
            &col("t1").eq(col("time")).and(col("t3").lt(col("time")))
        ));
    }
}
