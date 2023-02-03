use std::sync::Arc;

use crate::exec::context::IOxSessionContext;
use crate::plan::influxql::InfluxQLToLogicalPlan;
use crate::QueryNamespace;
use datafusion::{
    error::{DataFusionError, Result},
    physical_plan::ExecutionPlan,
};
use influxdb_influxql_parser::parse_statements;
use observability_deps::tracing::debug;

/// This struct can create plans for running SQL queries against databases
#[derive(Debug, Default)]
pub struct InfluxQLQueryPlanner {}

impl InfluxQLQueryPlanner {
    pub fn new() -> Self {
        Self::default()
    }

    /// Plan an InfluxQL query against the catalogs registered with `ctx`, and return a
    /// DataFusion physical execution plan that runs on the query executor.
    pub async fn query(
        &self,
        database: Arc<dyn QueryNamespace>,
        query: &str,
        ctx: &IOxSessionContext,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let ctx = ctx.child_ctx("query");
        debug!(text=%query, "planning InfluxQL query");

        let mut statements = parse_statements(query)
            .map_err(|e| DataFusionError::External(format!("{e}").into()))?;

        if statements.len() != 1 {
            return Err(DataFusionError::NotImplemented(
                "The context currently only supports a single InfluxQL statement".to_string(),
            ));
        }

        let planner = InfluxQLToLogicalPlan::new(&ctx, database);
        let logical_plan = planner.statement_to_plan(statements.pop().unwrap()).await?;
        debug!(plan=%logical_plan.display_graphviz(), "logical plan");

        // This would only work for SELECT statements at the moment, as the schema queries do
        // not return ExecutionPlan
        ctx.create_physical_plan(&logical_plan).await
    }
}
