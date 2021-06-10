//! This module contains DataFusion utility functions and helpers

use std::{collections::HashSet, sync::Arc};

use arrow::{
    compute::SortOptions,
    datatypes::{Schema as ArrowSchema, SchemaRef as ArrowSchemaRef},
    record_batch::RecordBatch,
};

use data_types::partition_metadata::ColumnSummary;
use datafusion::{
    error::DataFusionError,
    logical_plan::{Expr, LogicalPlan, LogicalPlanBuilder},
    optimizer::utils::expr_to_column_names,
    physical_plan::expressions::{col as physical_col, PhysicalSortExpr},
};
use internal_types::schema::Schema;

/// Create a logical plan that produces the record batch
pub fn make_scan_plan(batch: RecordBatch) -> std::result::Result<LogicalPlan, DataFusionError> {
    let schema = batch.schema();
    let partitions = vec![vec![batch]];
    let projection = None; // scan all columns
    LogicalPlanBuilder::scan_memory(partitions, schema, projection)?.build()
}

/// Given the requested projection (set of requested columns),
/// returns the schema of selecting just those columns
///
/// TODO contribute this back upstream in arrow's Schema so we can
/// avoid the copy of fields
pub fn project_schema(
    arrow_schema: ArrowSchemaRef,
    projection: &Option<Vec<usize>>,
) -> ArrowSchemaRef {
    match projection {
        None => arrow_schema,
        Some(projection) => {
            let new_fields = projection
                .iter()
                .map(|&i| arrow_schema.field(i))
                .cloned()
                .collect();
            Arc::new(ArrowSchema::new(new_fields))
        }
    }
}

/// Returns true if all columns referred to in schema are present, false
/// otherwise
pub fn schema_has_all_expr_columns(schema: &Schema, expr: &Expr) -> bool {
    let mut predicate_columns = HashSet::new();
    expr_to_column_names(expr, &mut predicate_columns).unwrap();

    predicate_columns
        .into_iter()
        .all(|col_name| schema.find_index_of(&col_name).is_some())
}

/// Returns the pk in arrow's expression used for data sorting
pub fn arrow_pk_sort_exprs(key_summaries: Vec<&ColumnSummary>) -> Vec<PhysicalSortExpr> {
    let mut sort_exprs = vec![];
    for key in key_summaries {
        sort_exprs.push(PhysicalSortExpr {
            expr: physical_col(key.name.as_str()),
            options: SortOptions {
                descending: false,
                nulls_first: false,
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
