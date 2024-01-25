use std::sync::Arc;

use crate::exec::context::IOxSessionContext;
use datafusion::{common::ParamValues, error::Result, physical_plan::ExecutionPlan};

/// This struct can create plans for running SQL queries against databases
#[derive(Debug, Default, Copy, Clone)]
pub struct SqlQueryPlanner {}

impl SqlQueryPlanner {
    pub fn new() -> Self {
        Self::default()
    }

    /// Plan a SQL query against the catalogs registered with `ctx`, and return a
    /// DataFusion physical execution plan that runs on the query executor.
    pub async fn query(
        &self,
        query: &str,
        params: impl Into<ParamValues> + Send,
        ctx: &IOxSessionContext,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let ctx = ctx.child_ctx("SqlQueryPlanner::query");
        ctx.sql_to_physical_plan_with_params(query, params).await
    }
}
