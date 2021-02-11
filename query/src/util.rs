//! This module contains DataFusion utility functions and helpers

use std::sync::Arc;

use arrow_deps::{
    arrow::datatypes::{Schema as ArrowSchema, SchemaRef as ArrowSchemaRef},
    arrow::record_batch::RecordBatch,
    datafusion::{
        error::DataFusionError,
        logical_plan::{binary_expr, Expr, LogicalPlan, LogicalPlanBuilder, Operator},
    },
};

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
