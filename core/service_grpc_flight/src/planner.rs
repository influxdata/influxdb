//! Query planner wrapper for use in IOx services
use std::sync::Arc;

use arrow_flight::FlightData;
use bytes::Bytes;
pub(crate) use datafusion::error::{DataFusionError as Error, Result};
use datafusion::{
    arrow::datatypes::SchemaRef, error::DataFusionError, physical_plan::ExecutionPlan,
};
use flightsql::{BaseTableType, FlightSQLCommand, FlightSQLPlanner};
use futures::stream::Peekable;
use generated_types::Streaming;
use iox_query::{
    QueryNamespace,
    exec::{IOxSessionContext, QueryLanguage},
    frontend::sql::SqlQueryPlanner,
};
use iox_query_influxql::frontend::planner::InfluxQLQueryPlanner;
use iox_query_params::StatementParams;

use crate::request::RunQuery;

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
            ctx: ctx.child_ctx("flight_planner"),
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
        let ctx = self.ctx.child_ctx("planner_sql");

        planner.query(query, params, &ctx).await
    }

    /// Plan an InfluxQL query against the data in `database`, and return a
    /// DataFusion physical execution plan.
    pub(crate) async fn influxql(
        &self,
        query: impl AsRef<str> + Send,
        params: impl Into<StatementParams> + Send,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let query = query.as_ref();
        let ctx = self.ctx.child_ctx("planner_influxql");

        InfluxQLQueryPlanner::query(query, params, &ctx).await
    }

    /// Creates a plan for a `DoGet` FlightSQL message, as described on
    /// [`FlightSQLPlanner::do_get`], on a separate threadpool
    pub(crate) async fn flight_sql_do_get(
        &self,
        namespace_name: impl AsRef<str> + Send,
        namespace: Arc<dyn QueryNamespace>,
        cmd: FlightSQLCommand,
        base_table_type: BaseTableType,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let namespace_name = namespace_name.as_ref();
        let ctx = self.ctx.child_ctx("planner_flight_sql_do_get");

        FlightSQLPlanner::do_get(namespace_name, namespace, cmd, &ctx, base_table_type)
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
        let ctx = self.ctx.child_ctx("planner_flight_sql_do_action");

        FlightSQLPlanner::do_action(namespace_name, namespace, cmd, &ctx)
            .await
            .map_err(DataFusionError::from)
    }

    /// Creates a plan for a `DoPut` FlightSQL message, as described on
    /// [`FlightSQLPlanner::do_put`], on a separate threadpool
    pub(crate) async fn flight_sql_do_put(
        &self,
        namespace_name: impl Into<String> + Send,
        namespace: Arc<dyn QueryNamespace>,
        cmd: FlightSQLCommand,
        data: Peekable<Streaming<FlightData>>,
    ) -> Result<Bytes> {
        let namespace_name = namespace_name.into();
        let ctx = self.ctx.child_ctx("planner_flight_sql_do_put");

        FlightSQLPlanner::do_put(namespace_name, namespace, cmd, data, &ctx)
            .await
            .map_err(DataFusionError::from)
    }

    /// Returns the [`SchemaRef`] to be included in the response to a
    /// `GetFlightInfo` FlightSQL message as described on
    /// [`FlightSQLPlanner::get_schema`], on a separate threadpool.
    ///
    /// If query_lang is  Some(InfluxQL), the query is treated as
    /// InfluxQL. Otherwise, the query is treated as SQL.
    pub(crate) async fn flight_sql_get_flight_info_schema(
        &self,
        namespace_name: impl Into<String> + Send,
        cmd: FlightSQLCommand,
        query_lang: Option<QueryLanguage>,
    ) -> Result<(SchemaRef, RunQuery)> {
        let namespace_name = namespace_name.into();
        let ctx = self.ctx.child_ctx("planner_flight_sql_get_flight_info");
        let query_lang = query_lang.unwrap_or(QueryLanguage::Sql);

        match (cmd, query_lang) {
            // We only want to handle queries with an InfluxQL header like they're actually
            // InfluxQL if they come with a CommandStatementQuery cmd, because that's the only
            // variant that could reasonably be processed as InfluxQL and we're not certain that
            // flightsql clients have the capability to switch this header on and off per-request,
            // so instead of just failing on influxql commands that aren't of this variant, we want
            // to let commands kinda do what you'd expect.
            (FlightSQLCommand::CommandStatementQuery(cmd), QueryLanguage::InfluxQL) => self
                .influxql_query_to_schema(&cmd.query)
                .await
                .map(|schema| (schema, RunQuery::InfluxQL(cmd.query))),

            // this seems kinda unnecessary (as it's basically a wildcard but verbose) but we want
            // to make sure nobody misses this in the future if another language is added to this
            // variant - exhaustive patterns are nice and useful
            // but we're matching on all patterns here not because we want the FlightSQLPlanner to
            // handle InfluxQL and all other languages, but rather because (at moment of writing)
            // we've already verified that, up above, we matched the special case(s) where we want to
            // actually process InfluxQL, and here we just want to fallback to normal sql
            // processing.
            (cmd, QueryLanguage::Sql | QueryLanguage::InfluxQL) => {
                FlightSQLPlanner::get_schema(namespace_name, &cmd, &ctx)
                    .await
                    .map_err(DataFusionError::from)
                    .map(|schema| (schema, RunQuery::FlightSQL(cmd)))
            }
        }
    }

    async fn influxql_query_to_schema(&self, query: &str) -> Result<SchemaRef> {
        let ctx = self.ctx.child_ctx("planner_influxql_query_to_schema");

        let statement = InfluxQLQueryPlanner::query_to_statement(query)?;
        let logical_plan =
            InfluxQLQueryPlanner::statement_to_plan(statement, StatementParams::new(), &ctx)
                .await?;

        Ok(FlightSQLPlanner::get_schema_for_plan(&logical_plan))
    }
}
