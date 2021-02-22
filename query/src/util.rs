//! This module contains DataFusion utility functions and helpers

use std::{collections::HashSet, sync::Arc};

use arrow_deps::{
    arrow::datatypes::{Schema as ArrowSchema, SchemaRef as ArrowSchemaRef},
    arrow::record_batch::RecordBatch,
    datafusion::{
        error::DataFusionError,
        logical_plan::{binary_expr, Expr, LogicalPlan, LogicalPlanBuilder, Operator},
        optimizer::utils::expr_to_column_names,
        prelude::{col, lit},
    },
};
use data_types::{schema::Schema, TIME_COLUMN_NAME};

/// Creates a single expression representing the conjunction (aka
/// AND'ing) together of a set of expressions
#[derive(Debug, Default)]
pub struct AndExprBuilder {
    cur_expr: Option<Expr>,
}

impl AndExprBuilder {
    /// append `new_expr` to the expression chain being built
    pub fn append_opt_ref(self, new_expr: Option<&Expr>) -> Self {
        match new_expr {
            None => self,
            Some(new_expr) => self.append_expr(new_expr.clone()),
        }
    }

    /// append `new_expr` to the expression chain being built
    pub fn append_opt(self, new_expr: Option<Expr>) -> Self {
        match new_expr {
            None => self,
            Some(new_expr) => self.append_expr(new_expr),
        }
    }

    /// Append `new_expr` to the expression chain being built
    pub fn append_expr(self, new_expr: Expr) -> Self {
        let Self { cur_expr } = self;

        let cur_expr = if let Some(cur_expr) = cur_expr {
            binary_expr(cur_expr, Operator::And, new_expr)
        } else {
            new_expr
        };

        let cur_expr = Some(cur_expr);

        Self { cur_expr }
    }

    /// Creates the new filter expression, consuming Self
    pub fn build(self) -> Option<Expr> {
        self.cur_expr
    }
}

/// Creates expression like:
/// start <= time && time < end
pub fn make_range_expr(start: i64, end: i64) -> Expr {
    let ts_low = arrow_deps::datafusion::prelude::lit(start).lt_eq(col(TIME_COLUMN_NAME));
    let ts_high = col(TIME_COLUMN_NAME).lt(lit(end));

    ts_low.and(ts_high)
}

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

#[cfg(test)]
mod tests {
    use data_types::schema::builder::SchemaBuilder;

    use super::*;

    #[test]
    fn test_make_range_expr() {
        // Test that the generated predicate is correct

        let ts_predicate_expr = make_range_expr(101, 202);
        let expected_string = "Int64(101) LtEq #time And #time Lt Int64(202)";
        let actual_string = format!("{:?}", ts_predicate_expr);

        assert_eq!(actual_string, expected_string);
    }

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
