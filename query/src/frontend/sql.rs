use std::sync::Arc;

use snafu::{ResultExt, Snafu};

use crate::exec::{context::DEFAULT_CATALOG, Executor, ExecutorType};
use datafusion::{
    catalog::catalog::CatalogProvider, error::DataFusionError, physical_plan::ExecutionPlan,
};

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Error preparing query {}", source))]
    Preparing { source: crate::exec::context::Error },

    #[snafu(display("Internal Error creating memtable for table {}: {}", table, source))]
    InternalMemTableCreation {
        table: String,
        source: DataFusionError,
    },

    #[snafu(display("Error listing partition keys: {}", source))]
    GettingDatabasePartition {
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    #[snafu(display(
        "Error getting table schema for table '{}' in chunk {}: {}",
        table_name,
        chunk_id,
        source
    ))]
    GettingTableSchema {
        table_name: String,
        chunk_id: u32,
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    #[snafu(display(
        "Error adding chunk to table provider for table '{}' in chunk {}: {}",
        table_name,
        chunk_id,
        source
    ))]
    AddingChunkToProvider {
        table_name: String,
        chunk_id: u32,
        source: crate::provider::Error,
    },

    #[snafu(display("Error creating table provider for table '{}': {}", table_name, source))]
    CreatingTableProvider {
        table_name: String,
        source: crate::provider::Error,
    },

    #[snafu(display(
        "Error registering table provider for table '{}': {}",
        table_name,
        source
    ))]
    RegisteringTableProvider {
        table_name: String,
        source: DataFusionError,
    },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

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
        executor: &Executor,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let mut ctx = executor.new_context(ExecutorType::Query);
        ctx.register_catalog(DEFAULT_CATALOG, database);
        ctx.prepare_sql(query).context(Preparing)
    }
}
