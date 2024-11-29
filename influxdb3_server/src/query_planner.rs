use std::sync::Arc;

use datafusion::{error::DataFusionError, physical_plan::ExecutionPlan};
use iox_query::{exec::IOxSessionContext, frontend::sql::SqlQueryPlanner};
use iox_query_influxql::frontend::planner::InfluxQLQueryPlanner;
use iox_query_params::StatementParams;

type Result<T, E = DataFusionError> = std::result::Result<T, E>;

/// A query planner for creating physical query plans for SQL/InfluxQL queries made through the REST
/// API using a separate threadpool.
///
/// This is based on the similar implementation for the planner in the flight service [here][ref].
///
/// [ref]: https://github.com/influxdata/influxdb3_core/blob/6fcbb004232738d55655f32f4ad2385523d10696/service_grpc_flight/src/planner.rs#L24-L33
pub(crate) struct Planner {
    ctx: IOxSessionContext,
}

impl Planner {
    /// Create a new `Planner`
    pub(crate) fn new(ctx: &IOxSessionContext) -> Self {
        Self {
            ctx: ctx.child_ctx("rest_api_query_planner"),
        }
    }

    /// Plan a SQL query and return a DataFusion physical plan
    pub(crate) async fn sql(
        &self,
        query: impl AsRef<str> + Send,
        params: StatementParams,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let planner = SqlQueryPlanner::new();
        let query = query.as_ref();
        let ctx = self.ctx.child_ctx("rest_api_query_planner_sql");

        planner.query(query, params, &ctx).await
    }

    /// Plan an InfluxQL query and return a DataFusion physical plan
    pub(crate) async fn influxql(
        &self,
        query: impl AsRef<str> + Send,
        params: impl Into<StatementParams> + Send,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let query = query.as_ref();
        let ctx = self.ctx.child_ctx("rest_api_query_planner_influxql");

        InfluxQLQueryPlanner::query(query, params, &ctx).await
    }
}
