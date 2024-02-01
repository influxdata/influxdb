//! Query planner wrapper for use in IOx services
use std::sync::Arc;

use bytes::Bytes;
use datafusion::{
    arrow::datatypes::SchemaRef, error::DataFusionError, physical_plan::ExecutionPlan,
};
use flightsql::{FlightSQLCommand, FlightSQLPlanner};
use iox_query::{exec::IOxSessionContext, frontend::sql::SqlQueryPlanner, QueryNamespace};

pub(crate) use datafusion::error::{DataFusionError as Error, Result};
use iox_query_influxql::frontend::planner::InfluxQLQueryPlanner;
use iox_query_params::StatementParams;

/// Query planner that plans queries on a separate threadpool.
///
/// Query planning was, at time of writing, a single threaded affair. In order
/// to avoid tying up the tokio executor that is handling API requests, IOx plan
/// queries using a separate thread pool.
#[derive(Debug)]
pub(crate) struct Planner {
    /// Executors (whose threadpool to use)
    ctx: IOxSessionContext,
}

impl Planner {
    /// Create a new planner that will plan queries using the provided context
    pub(crate) fn new(ctx: &IOxSessionContext) -> Self {
        Self {
            ctx: ctx.child_ctx("Planner"),
        }
    }

    /// Plan a SQL query against the data in a namespace, and return a
    /// DataFusion physical execution plan.
    pub(crate) async fn sql(
        &self,
        query: impl AsRef<str> + Send,
        params: StatementParams,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let planner = SqlQueryPlanner::new();
        let query = query.as_ref();
        let ctx = self.ctx.child_ctx("planner sql");
        let params = params.into_df_param_values();

        planner.query(query, params, &ctx).await
    }

    /// Plan an InfluxQL query against the data in `database`, and return a
    /// DataFusion physical execution plan.
    pub(crate) async fn influxql(
        &self,
        query: impl AsRef<str> + Send,
        params: impl Into<StatementParams> + Send,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let planner = InfluxQLQueryPlanner::new();
        let query = query.as_ref();
        let ctx = self.ctx.child_ctx("planner influxql");
        let params = params.into();

        planner.query(query, params, &ctx).await
    }

    /// Creates a plan for a `DoGet` FlightSQL message, as described on
    /// [`FlightSQLPlanner::do_get`], on a separate threadpool
    pub(crate) async fn flight_sql_do_get(
        &self,
        namespace_name: impl AsRef<str> + Send,
        namespace: Arc<dyn QueryNamespace>,
        cmd: FlightSQLCommand,
        params: StatementParams,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let namespace_name = namespace_name.as_ref();
        let ctx = self.ctx.child_ctx("planner flight_sql_do_get");
        let params = params.into_df_param_values();

        FlightSQLPlanner::do_get(namespace_name, namespace, cmd, params, &ctx)
            .await
            .map_err(DataFusionError::from)
    }

    /// Creates a plan for a `DoAction` FlightSQL message, as described on
    /// [`FlightSQLPlanner::do_action`], on a separate threadpool
    pub(crate) async fn flight_sql_do_action(
        &self,
        namespace_name: impl Into<String> + Send,
        namespace: Arc<dyn QueryNamespace>,
        cmd: FlightSQLCommand,
    ) -> Result<Bytes> {
        let namespace_name = namespace_name.into();
        let ctx = self.ctx.child_ctx("planner flight_sql_do_get");

        FlightSQLPlanner::do_action(namespace_name, namespace, cmd, &ctx)
            .await
            .map_err(DataFusionError::from)
    }

    /// Returns the [`SchemaRef`] to be included in the response to a
    /// `GetFlightInfo` FlightSQL message as described on
    /// [`FlightSQLPlanner::get_schema`], on a separate threadpool.
    pub(crate) async fn flight_sql_get_flight_info_schema(
        &self,
        namespace_name: impl Into<String> + Send,
        cmd: FlightSQLCommand,
    ) -> Result<SchemaRef> {
        let namespace_name = namespace_name.into();
        let ctx = self.ctx.child_ctx("planner flight_sql_get_flight_info");

        FlightSQLPlanner::get_schema(namespace_name, cmd, &ctx)
            .await
            .map_err(DataFusionError::from)
    }
}
