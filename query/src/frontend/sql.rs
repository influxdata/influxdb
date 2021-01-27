use std::sync::Arc;

use snafu::{ResultExt, Snafu};

use crate::{exec::Executor, selection::Selection, Database, PartitionChunk};
use arrow_deps::datafusion::{
    datasource::MemTable, error::DataFusionError, physical_plan::ExecutionPlan,
};

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

    #[snafu(display("Internal error converting table to arrow {}: {}", table, source))]
    InternalTableConversion {
        table: String,
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    #[snafu(display("No rows found in table {} while executing '{}'", table, query))]
    InternalNoRowsInTable { table: String, query: String },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

/// This struct can create plans for running SQL queries against databases
#[derive(Debug, Default)]
pub struct SQLQueryPlanner {}

impl SQLQueryPlanner {
    /// Plan a SQL query against the data in `database`, and return a
    /// DataFusion physical execution plan. The plan can then be
    /// executed using `executor` in a streaming fashion.
    pub async fn query<D: Database>(
        &self,
        database: &D,
        query: &str,
        executor: &Executor,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let mut ctx = executor.new_context();

        // figure out the table names that appear in the sql
        let table_names = table_names(query)?;

        let partition_keys = database.partition_keys().await.unwrap();

        // Register a table provider for each table so DataFusion
        // knows what the schema of that table is and how to obtain
        // its data when needed.
        for table in &table_names {
            let mut data = Vec::new();
            for partition_key in &partition_keys {
                for chunk in database.chunks(partition_key).await {
                    chunk
                        .table_to_arrow(&mut data, &table, Selection::All)
                        .map_err(|e| Box::new(e) as _)
                        .context(InternalTableConversion { table })?
                }
            }

            // TODO: make our own struct that implements
            // TableProvider, type so we can take advantage of
            // datafusion predicate and selection pushdown. For now,
            // use a Memtable provider (which requires materializing
            // the entire table here)

            // if the table was reported to exist, it should not be empty (eventually we
            // should get the schema and table data separtely)
            if data.is_empty() {
                return InternalNoRowsInTable { table, query }.fail();
            }

            let schema = data[0].schema().clone();
            let provider = Box::new(
                MemTable::try_new(schema, vec![data])
                    .context(InternalMemTableCreation { table })?,
            );

            ctx.inner_mut().register_table(&table, provider);
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
