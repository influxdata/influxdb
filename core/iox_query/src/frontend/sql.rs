use std::sync::Arc;

use crate::exec::context::IOxSessionContext;
use datafusion::{
    common::ParamValues, error::Result, logical_expr::LogicalPlan, physical_plan::ExecutionPlan,
};

/// This struct can create plans for running SQL queries against databases
#[derive(Debug, Default, Copy, Clone)]
pub struct SqlQueryPlanner {}

impl SqlQueryPlanner {
    pub fn new() -> Self {
        Self::default()
    }

    /// Logically plan a SQL query against the catalogs registered with `ctx`, and return a
    /// DataFusion logical plan that runs on the query executor.
    pub async fn logical_plan(
        &self,
        sql: &str,
        params: impl Into<ParamValues> + Send,
        ctx: &IOxSessionContext,
    ) -> Result<LogicalPlan> {
        let ctx = ctx.child_ctx("sql_query_planner_logical_plan");
        ctx.sql_to_logical_plan_with_params(sql, params).await
    }

    /// Physically plan a SQL query against the catalogs registered with `ctx`, and return a
    /// DataFusion physical execution plan that runs on the query executor.
    pub async fn physical_plan(
        &self,
        logical_plan: LogicalPlan,
        ctx: &IOxSessionContext,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let ctx = ctx.child_ctx("sql_query_planner_physical_plan");
        ctx.create_physical_plan(&logical_plan).await
    }
}
