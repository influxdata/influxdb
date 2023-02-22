//! Query planner wrapper for use in IOx services
use std::sync::Arc;

use bytes::Bytes;
use datafusion::{error::DataFusionError, physical_plan::ExecutionPlan};
use flightsql::{FlightSQLCommand, FlightSQLPlanner};
use iox_query::{
    exec::IOxSessionContext,
    frontend::{influxrpc::InfluxRpcPlanner, sql::SqlQueryPlanner},
    plan::{fieldlist::FieldListPlan, seriesset::SeriesSetPlans, stringset::StringSetPlan},
    Aggregate, QueryNamespace, WindowDuration,
};

pub use datafusion::error::{DataFusionError as Error, Result};
use iox_query::frontend::influxql::InfluxQLQueryPlanner;
use predicate::rpc_predicate::InfluxRpcPredicate;

/// Query planner that plans queries on a separate threadpool.
///
/// Query planning was, at time of writing, a single threaded
/// affair. In order to avoid tying up the tokio executor that is
/// handling API requests, IOx plan queries using a separate thread
/// pool.
pub struct Planner {
    /// Executors (whose threadpool to use)
    ctx: IOxSessionContext,
}

impl Planner {
    /// Create a new planner that will plan queries using the provided context
    pub fn new(ctx: &IOxSessionContext) -> Self {
        Self {
            ctx: ctx.child_ctx("Planner"),
        }
    }

    /// Plan a SQL query against the data in a namespace, and return a
    /// DataFusion physical execution plan.
    pub async fn sql(&self, query: impl Into<String> + Send) -> Result<Arc<dyn ExecutionPlan>> {
        let planner = SqlQueryPlanner::new();
        let query = query.into();
        let ctx = self.ctx.child_ctx("planner sql");

        self.ctx
            .run(async move { planner.query(&query, &ctx).await })
            .await
    }

    /// Plan an InfluxQL query against the data in `database`, and return a
    /// DataFusion physical execution plan.
    pub async fn influxql(
        &self,
        query: impl Into<String> + Send,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let planner = InfluxQLQueryPlanner::new();
        let query = query.into();
        let ctx = self.ctx.child_ctx("planner influxql");

        self.ctx
            .run(async move { planner.query(&query, &ctx).await })
            .await
    }

    /// Creates a plan for a `DoGet` FlightSQL message,
    /// as described on [`FlightSQLPlanner::do_get`], on a
    /// separate threadpool
    pub async fn flight_sql_do_get<N>(
        &self,
        namespace_name: impl Into<String>,
        namespace: Arc<N>,
        cmd: FlightSQLCommand,
    ) -> Result<Arc<dyn ExecutionPlan>>
    where
        N: QueryNamespace + 'static,
    {
        let namespace_name = namespace_name.into();
        let ctx = self.ctx.child_ctx("planner flight_sql_do_get");

        self.ctx
            .run(async move {
                FlightSQLPlanner::do_get(namespace_name, namespace, cmd, &ctx)
                    .await
                    .map_err(DataFusionError::from)
            })
            .await
    }

    /// Creates a plan for a `DoAction` FlightSQL message,
    /// as described on [`FlightSQLPlanner::do_action`], on a
    /// separate threadpool
    pub async fn flight_sql_do_action<N>(
        &self,
        namespace_name: impl Into<String>,
        namespace: Arc<N>,
        cmd: FlightSQLCommand,
    ) -> Result<Bytes>
    where
        N: QueryNamespace + 'static,
    {
        let namespace_name = namespace_name.into();
        let ctx = self.ctx.child_ctx("planner flight_sql_do_get");

        self.ctx
            .run(async move {
                FlightSQLPlanner::do_action(namespace_name, namespace, cmd, &ctx)
                    .await
                    .map_err(DataFusionError::from)
            })
            .await
    }

    /// Creates the response for a `GetFlightInfo`  FlightSQL  message
    /// as described on [`FlightSQLPlanner::get_flight_info`], on a
    /// separate threadpool.
    pub async fn flight_sql_get_flight_info(
        &self,
        namespace_name: impl Into<String>,
        cmd: FlightSQLCommand,
    ) -> Result<Bytes> {
        let namespace_name = namespace_name.into();
        let ctx = self.ctx.child_ctx("planner flight_sql_get_flight_info");

        self.ctx
            .run(async move {
                FlightSQLPlanner::get_flight_info(namespace_name, cmd, &ctx)
                    .await
                    .map_err(DataFusionError::from)
            })
            .await
    }

    /// Creates a plan as described on
    /// [`InfluxRpcPlanner::table_names`], on a separate threadpool
    pub async fn table_names<N>(
        &self,
        namespace: Arc<N>,
        predicate: InfluxRpcPredicate,
    ) -> Result<StringSetPlan>
    where
        N: QueryNamespace + 'static,
    {
        let planner = InfluxRpcPlanner::new(self.ctx.child_ctx("planner table_names"));

        self.ctx
            .run(async move {
                planner
                    .table_names(namespace, predicate)
                    .await
                    .map_err(|e| e.to_df_error("table_names"))
            })
            .await
    }

    /// Creates a plan as described on
    /// [`InfluxRpcPlanner::tag_keys`], on a separate threadpool
    pub async fn tag_keys<N>(
        &self,
        namespace: Arc<N>,
        predicate: InfluxRpcPredicate,
    ) -> Result<StringSetPlan>
    where
        N: QueryNamespace + 'static,
    {
        let planner = InfluxRpcPlanner::new(self.ctx.child_ctx("planner tag_keys"));

        self.ctx
            .run(async move {
                planner
                    .tag_keys(namespace, predicate)
                    .await
                    .map_err(|e| e.to_df_error("tag_keys"))
            })
            .await
    }

    /// Creates a plan as described on
    /// [`InfluxRpcPlanner::tag_values`], on a separate threadpool
    pub async fn tag_values<N>(
        &self,
        namespace: Arc<N>,
        tag_name: impl Into<String> + Send,
        predicate: InfluxRpcPredicate,
    ) -> Result<StringSetPlan>
    where
        N: QueryNamespace + 'static,
    {
        let tag_name = tag_name.into();
        let planner = InfluxRpcPlanner::new(self.ctx.child_ctx("planner tag_values"));

        self.ctx
            .run(async move {
                planner
                    .tag_values(namespace, &tag_name, predicate)
                    .await
                    .map_err(|e| e.to_df_error("tag_values"))
            })
            .await
    }

    /// Creates a plan as described on
    /// [`InfluxRpcPlanner::field_columns`], on a separate threadpool
    pub async fn field_columns<N>(
        &self,
        namespace: Arc<N>,
        predicate: InfluxRpcPredicate,
    ) -> Result<FieldListPlan>
    where
        N: QueryNamespace + 'static,
    {
        let planner = InfluxRpcPlanner::new(self.ctx.child_ctx("planner field_columns"));

        self.ctx
            .run(async move {
                planner
                    .field_columns(namespace, predicate)
                    .await
                    .map_err(|e| e.to_df_error("field_columns"))
            })
            .await
    }

    /// Creates a plan as described on
    /// [`InfluxRpcPlanner::read_filter`], on a separate threadpool
    pub async fn read_filter<N>(
        &self,
        namespace: Arc<N>,
        predicate: InfluxRpcPredicate,
    ) -> Result<SeriesSetPlans>
    where
        N: QueryNamespace + 'static,
    {
        let planner = InfluxRpcPlanner::new(self.ctx.child_ctx("planner read_filter"));

        self.ctx
            .run(async move {
                planner
                    .read_filter(namespace, predicate)
                    .await
                    .map_err(|e| e.to_df_error("read_filter"))
            })
            .await
    }

    /// Creates a plan as described on
    /// [`InfluxRpcPlanner::read_group`], on a separate threadpool
    pub async fn read_group<N>(
        &self,
        namespace: Arc<N>,
        predicate: InfluxRpcPredicate,
        agg: Aggregate,
        group_columns: Vec<String>,
    ) -> Result<SeriesSetPlans>
    where
        N: QueryNamespace + 'static,
    {
        let planner = InfluxRpcPlanner::new(self.ctx.child_ctx("planner read_group"));

        self.ctx
            .run(async move {
                planner
                    .read_group(namespace, predicate, agg, &group_columns)
                    .await
                    .map_err(|e| e.to_df_error("read_group"))
            })
            .await
    }

    /// Creates a plan as described on
    /// [`InfluxRpcPlanner::read_window_aggregate`], on a separate threadpool
    pub async fn read_window_aggregate<N>(
        &self,
        namespace: Arc<N>,
        predicate: InfluxRpcPredicate,
        agg: Aggregate,
        every: WindowDuration,
        offset: WindowDuration,
    ) -> Result<SeriesSetPlans>
    where
        N: QueryNamespace + 'static,
    {
        let planner = InfluxRpcPlanner::new(self.ctx.child_ctx("planner read_window_aggregate"));

        self.ctx
            .run(async move {
                planner
                    .read_window_aggregate(namespace, predicate, agg, every, offset)
                    .await
                    .map_err(|e| e.to_df_error("read_window_aggregate"))
            })
            .await
    }
}
