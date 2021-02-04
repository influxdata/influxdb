use std::sync::Arc;

use snafu::{ResultExt, Snafu};

use crate::{exec::Executor, provider::ProviderBuilder, Database, PartitionChunk};
use arrow_deps::datafusion::{error::DataFusionError, physical_plan::ExecutionPlan};
use data_types::selection::Selection;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Error preparing query {}", source))]
    Preparing { source: crate::exec::context::Error },

    #[snafu(display("Invalid sql query: {} : {}", query, source))]
    InvalidSqlQuery {
        query: String,
        source: sqlparser::parser::ParserError,
    },

    #[snafu(display("Unsupported SQL statement in query {}: {}", query, statement))]
    UnsupportedStatement {
        query: String,
        statement: Box<Statement>,
    },

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
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

/// This struct can create plans for running SQL queries against databases
#[derive(Debug, Default)]
pub struct SQLQueryPlanner {}

impl SQLQueryPlanner {
    /// Plan a SQL query against the data in `database`, and return a
    /// DataFusion physical execution plan. The plan can then be
    /// executed using `executor` in a streaming fashion.
    pub async fn query<D: Database + 'static>(
        &self,
        database: &D,
        query: &str,
        executor: &Executor,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let mut ctx = executor.new_context();

        // figure out the table names that appear in the sql
        let table_names = table_names(query)?;

        let partition_keys = database
            .partition_keys()
            .await
            .map_err(|e| Box::new(e) as _)
            .context(GettingDatabasePartition)?;

        // Register a table provider for each table so DataFusion
        // knows what the schema of that table is and how to obtain
        // its data when needed.
        for table_name in &table_names {
            let mut builder = ProviderBuilder::new(table_name);

            for partition_key in &partition_keys {
                for chunk in database.chunks(partition_key).await {
                    if chunk.has_table(table_name).await {
                        let chunk_id = chunk.id();
                        let chunk_table_schema = chunk
                            .table_schema(table_name, Selection::All)
                            .await
                            .map_err(|e| Box::new(e) as _)
                            .context(GettingTableSchema {
                                table_name,
                                chunk_id,
                            })?;

                        builder = builder
                            .add_chunk(chunk, chunk_table_schema.into())
                            .context(AddingChunkToProvider {
                                table_name,
                                chunk_id,
                            })?
                    }
                }
            }
            let provider = builder
                .build()
                .context(CreatingTableProvider { table_name })?;

            ctx.inner_mut()
                .register_table(&table_name, Box::new(provider));
        }

        ctx.prepare_sql(query).await.context(Preparing)
    }
}

use sqlparser::{
    ast::{SetExpr, Statement, TableFactor},
    dialect::GenericDialect,
    parser::Parser,
};

/// return a list of table names that appear in the query
/// TODO find some way to avoid using sql parser direcly here
fn table_names(query: &str) -> Result<Vec<String>> {
    let mut tables = vec![];

    let dialect = GenericDialect {};
    let ast = Parser::parse_sql(&dialect, query).context(InvalidSqlQuery { query })?;

    for statement in ast {
        match statement {
            Statement::Query(q) => {
                if let SetExpr::Select(q) = q.body {
                    for item in q.from {
                        if let TableFactor::Table { name, .. } = item.relation {
                            tables.push(name.to_string());
                        }
                    }
                }
            }
            _ => {
                return UnsupportedStatement {
                    query: query.to_string(),
                    statement,
                }
                .fail()
            }
        }
    }
    Ok(tables)
}
