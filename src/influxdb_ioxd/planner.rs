//! Query planner wrapper for use in IOx services
use std::sync::Arc;

use datafusion::{catalog::catalog::CatalogProvider, physical_plan::ExecutionPlan};
use query::{
    exec::Executor,
    frontend::{influxrpc::InfluxRpcPlanner, sql::SqlQueryPlanner},
    group_by::{Aggregate, WindowDuration},
    plan::{fieldlist::FieldListPlan, seriesset::SeriesSetPlans, stringset::StringSetPlan},
    predicate::Predicate,
    Database,
};
use snafu::{ResultExt, Snafu};

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Error planning sql query {}", source))]
    Sql {
        query: String,
        source: query::frontend::sql::Error,
    },

    #[snafu(display("Error planning InfluxRPC query {}", source))]
    InfluxRpc {
        source: query::frontend::influxrpc::Error,
    },

    #[snafu(display("Internal executor error while planning query: {}", source))]
    InternalExecutionWhilePlanning { source: query::exec::Error },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

/// Query planner that plans queries on a separate threadpool.
///
/// Query planning was, at time of writing, a single threaded
/// affair. In order to avoid tying up the tokio executor that is
/// handling API requests, we plan queries using a separate thread
/// pool.
pub struct Planner {
    /// Executors (whose threadpool to use)
    exec: Arc<Executor>,
}

impl Planner {
    /// Create a new planner that will plan queries using the threadpool of
    /// `exec`
    pub fn new(exec: Arc<Executor>) -> Self {
        Self { exec }
    }

    /// Plan a SQL query against the data in `database`, and return a
    /// DataFusion physical execution plan.
    pub async fn sql<D: CatalogProvider + 'static>(
        &self,
        database: Arc<D>,
        query: impl Into<String> + Send,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let planner = SqlQueryPlanner::new();
        let q_executor = Arc::clone(&self.exec);
        let query = query.into();

        self.exec
            .run(async move {
                planner
                    .query(database, &query, q_executor.as_ref())
                    .context(Sql { query })
            })
            .await
            .context(InternalExecutionWhilePlanning)?
    }

    /// Creates a plan as described on
    /// [`InfluxRpcPlanner::table_names`], on a separate threadpool
    pub async fn table_names<D>(
        &self,
        database: Arc<D>,
        predicate: Predicate,
    ) -> Result<StringSetPlan>
    where
        D: Database + 'static,
    {
        let planner = InfluxRpcPlanner::new();

        self.exec
            .run(async move {
                planner
                    .table_names(database.as_ref(), predicate)
                    .context(InfluxRpc)
            })
            .await
            .context(InternalExecutionWhilePlanning)?
    }

    /// Creates a plan as described on
    /// [`InfluxRpcPlanner::tag_keys`], on a separate threadpool
    pub async fn tag_keys<D>(&self, database: Arc<D>, predicate: Predicate) -> Result<StringSetPlan>
    where
        D: Database + 'static,
    {
        let planner = InfluxRpcPlanner::new();

        self.exec
            .run(async move {
                planner
                    .tag_keys(database.as_ref(), predicate)
                    .context(InfluxRpc)
            })
            .await
            .context(InternalExecutionWhilePlanning)?
    }

    /// Creates a plan as described on
    /// [`InfluxRpcPlanner::tag_values`], on a separate threadpool
    pub async fn tag_values<D>(
        &self,
        database: Arc<D>,
        tag_name: impl Into<String> + Send,
        predicate: Predicate,
    ) -> Result<StringSetPlan>
    where
        D: Database + 'static,
    {
        let tag_name = tag_name.into();
        let planner = InfluxRpcPlanner::new();

        self.exec
            .run(async move {
                planner
                    .tag_values(database.as_ref(), &tag_name, predicate)
                    .context(InfluxRpc)
            })
            .await
            .context(InternalExecutionWhilePlanning)?
    }

    /// Creates a plan as described on
    /// [`InfluxRpcPlanner::field_columns`], on a separate threadpool
    pub async fn field_columns<D>(
        &self,
        database: Arc<D>,
        predicate: Predicate,
    ) -> Result<FieldListPlan>
    where
        D: Database + 'static,
    {
        let planner = InfluxRpcPlanner::new();

        self.exec
            .run(async move {
                planner
                    .field_columns(database.as_ref(), predicate)
                    .context(InfluxRpc)
            })
            .await
            .context(InternalExecutionWhilePlanning)?
    }

    /// Creates a plan as described on
    /// [`InfluxRpcPlanner::read_filter`], on a separate threadpool
    pub async fn read_filter<D>(
        &self,
        database: Arc<D>,
        predicate: Predicate,
    ) -> Result<SeriesSetPlans>
    where
        D: Database + 'static,
    {
        let planner = InfluxRpcPlanner::new();

        self.exec
            .run(async move {
                planner
                    .read_filter(database.as_ref(), predicate)
                    .context(InfluxRpc)
            })
            .await
            .context(InternalExecutionWhilePlanning)?
    }

    /// Creates a plan as described on
    /// [`InfluxRpcPlanner::read_group`], on a separate threadpool
    pub async fn read_group<D>(
        &self,
        database: Arc<D>,
        predicate: Predicate,
        agg: Aggregate,
        group_columns: Vec<String>,
    ) -> Result<SeriesSetPlans>
    where
        D: Database + 'static,
    {
        let planner = InfluxRpcPlanner::new();

        self.exec
            .run(async move {
                planner
                    .read_group(database.as_ref(), predicate, agg, &group_columns)
                    .context(InfluxRpc)
            })
            .await
            .context(InternalExecutionWhilePlanning)?
    }

    /// Creates a plan as described on
    /// [`InfluxRpcPlanner::read_window_aggregate`], on a separate threadpool
    pub async fn read_window_aggregate<D>(
        &self,
        database: Arc<D>,
        predicate: Predicate,
        agg: Aggregate,
        every: WindowDuration,
        offset: WindowDuration,
    ) -> Result<SeriesSetPlans>
    where
        D: Database + 'static,
    {
        let planner = InfluxRpcPlanner::new();

        self.exec
            .run(async move {
                planner
                    .read_window_aggregate(database.as_ref(), predicate, agg, every, offset)
                    .context(InfluxRpc)
            })
            .await
            .context(InternalExecutionWhilePlanning)?
    }
}
