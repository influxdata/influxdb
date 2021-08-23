use std::sync::Arc;

use crate::exec::context::{IOxExecutionContext, DEFAULT_CATALOG};
use datafusion::{catalog::catalog::CatalogProvider, error::Result, physical_plan::ExecutionPlan};

/// This struct can create plans for running SQL queries against databases
#[derive(Debug, Default)]
pub struct SqlQueryPlanner {}

impl SqlQueryPlanner {
    pub fn new() -> Self {
        Self::default()
    }

    /// Plan a SQL query against the data in `database`, and return a
    /// DataFusion physical execution plan that runs on the query executor.
    ///
    /// When the plan is executed, it will run on the query executor
    /// in a streaming fashion.
    pub fn query<D: CatalogProvider + 'static>(
        &self,
        database: Arc<D>,
        query: &str,
        ctx: &IOxExecutionContext,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        ctx.register_catalog(DEFAULT_CATALOG, database);
        ctx.prepare_sql(query)
    }
}
