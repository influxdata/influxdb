//! This module implements the "Observer" functionality of the SQL repl

use influxdb_iox_client::connection::Connection;
use snafu::{ResultExt, Snafu};

use std::{collections::HashMap, sync::Arc, time::Instant};

use arrow::{
    array::{Array, ArrayRef, StringArray},
    datatypes::{Field, Schema},
    record_batch::RecordBatch,
};
use datafusion::{
    datasource::MemTable,
    prelude::{ExecutionConfig, ExecutionContext},
};

use observability_deps::tracing::{debug, info};

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Generic error"))]
    Generic,

    #[snafu(display("Error loading remote state: {}", source))]
    LoadingDatabaseNames {
        source: influxdb_iox_client::management::ListDatabaseError,
    },

    #[snafu(display("Error running remote query: {}", source))]
    RunningRemoteQuery {
        source: influxdb_iox_client::flight::Error,
    },

    #[snafu(display("Error running observer query: {}", source))]
    Query {
        source: datafusion::error::DataFusionError,
    },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

/// The Observer contains a local DataFusion execution engine that has
/// pre-loaded with consolidated system table views.
pub struct Observer {
    /// DataFusion execution context for executing queries
    context: ExecutionContext,
}

impl Observer {
    /// Attempt to create a new observer instance, loading from the remote server
    pub async fn try_new(connection: Connection) -> Result<Self> {
        let mut context =
            ExecutionContext::with_config(ExecutionConfig::new().with_information_schema(true));

        load_remote_system_tables(&mut context, connection).await?;

        Ok(Self { context })
    }

    /// Runs the specified sql query locally against the preloaded context
    pub async fn run_query(&mut self, sql: &str) -> Result<Vec<RecordBatch>> {
        self.context
            .sql(sql)
            .context(Query)?
            .collect()
            .await
            .context(Query)
    }

    pub fn help(&self) -> String {
        r#"You are now in Observer mode.

SQL commands in this mode run against a cached unified view of
remote system tables in all remote databases.

To see the unified tables available to you, try running
SHOW TABLES;

To reload the most recent version of the database system tables, run
OBSERVER;

Example SQL to show the total estimated storage size by database:

SELECT database_name, storage, count(*) as num_chunks,
  sum(estimated_bytes)/1024/1024 as estimated_mb
FROM chunks
GROUP BY database_name, storage
ORDER BY estimated_mb desc;

"#
        .to_string()
    }
}

/// Copies the data from the remote tables across all databases in a
/// remote server into a local copy that also has an extra
/// `database_name` column for the database
async fn load_remote_system_tables(
    context: &mut ExecutionContext,
    connection: Connection,
) -> Result<()> {
    // all prefixed with "system."
    let table_names = vec!["chunks", "columns", "operations"];

    let start = Instant::now();

    let mut management_client = influxdb_iox_client::management::Client::new(connection.clone());

    let db_names = management_client
        .list_databases()
        .await
        .context(LoadingDatabaseNames)?;

    println!("Loading system tables from {} databases", db_names.len());

    let tasks = db_names
        .into_iter()
        .map(|db_name| {
            let table_names = table_names.clone();
            let connection = connection.clone();
            table_names.into_iter().map(move |table_name| {
                let table_name = table_name.to_string();
                let db_name = db_name.to_string();
                let connection = connection.clone();
                let sql = format!("select * from system.{}", table_name);
                tokio::task::spawn(async move {
                    let mut client = influxdb_iox_client::flight::Client::new(connection);
                    let mut query_results = client
                        .perform_query(&db_name, &sql)
                        .await
                        .context(RunningRemoteQuery)?;

                    let mut batches = vec![];

                    while let Some(data) = query_results.next().await.context(RunningRemoteQuery)? {
                        batches.push(data);
                    }

                    let t: Result<RemoteSystemTable> = Ok(RemoteSystemTable {
                        db_name,
                        table_name,
                        batches,
                    });
                    print!("."); // give some indication of progress
                    use std::io::Write;
                    std::io::stdout().flush().unwrap();
                    t
                })
            })
        })
        .flatten()
        .collect::<Vec<_>>();

    // now, get the results and combine them
    let results = futures::future::join_all(tasks).await;

    let mut builder = AggregatedTableBuilder::new();
    results.into_iter().for_each(|result| {
        match result {
            Ok(Ok(table)) => {
                builder.append(table);
            }
            // This is not a fatal error so log it and keep going
            Ok(Err(e)) => {
                println!("WARNING: Error running query: {}", e);
            }
            // This is not a fatal error so log it and keep going
            Err(e) => {
                println!("WARNING: Error running task: {}", e);
            }
        }
    });

    println!();
    println!(" Completed in {:?}", Instant::now() - start);

    builder.build(context);

    Ok(())
}

#[derive(Debug)]
/// Contains the results from a system table query for a specific database
struct RemoteSystemTable {
    db_name: String,
    table_name: String,
    batches: Vec<RecordBatch>,
}

#[derive(Debug, Default)]
/// Aggregates several table responses into a unified view
struct AggregatedTableBuilder {
    tables: HashMap<String, VirtualTableBuilder>,
}

impl AggregatedTableBuilder {
    fn new() -> Self {
        Self::default()
    }

    /// Appends a table response to the aggregated tables being built
    fn append(&mut self, t: RemoteSystemTable) {
        let RemoteSystemTable {
            db_name,
            table_name,
            batches,
        } = t;

        let num_rows: usize = batches.iter().map(|b| b.num_rows()).sum();

        info!(%table_name, %db_name, num_batches=batches.len(), %num_rows, "Aggregating results");

        let table_builder = self
            .tables
            .entry(table_name.clone())
            .or_insert_with(|| VirtualTableBuilder::new(table_name));

        table_builder.append_batches(&db_name, batches);
    }

    /// register a table provider for this system table
    fn build(self, ctx: &mut ExecutionContext) {
        let Self { tables } = self;

        for (table_name, table_builder) in tables {
            debug!(%table_name, "registering system table");
            table_builder.build(ctx);
        }
    }
}

/// Creates a "virtual" version of `select * from <table>` which has a
/// "database_name" column pre-pended to all actual record batches
#[derive(Debug)]
struct VirtualTableBuilder {
    table_name: String,
    batches: Vec<RecordBatch>,
}

impl VirtualTableBuilder {
    pub fn new(table_name: impl Into<String>) -> Self {
        let table_name = table_name.into();
        Self {
            table_name,
            batches: Vec::new(),
        }
    }

    /// Append batches from `select * from <system table>` to the
    /// results being created
    fn append_batches(&mut self, db_name: &str, new_batches: Vec<RecordBatch>) {
        self.batches.extend(new_batches.into_iter().map(|batch| {
            use std::iter::once;

            let array =
                StringArray::from_iter_values(std::iter::repeat(db_name).take(batch.num_rows()));
            let data_type = array.data_type().clone();
            let array = Arc::new(array) as ArrayRef;

            let new_columns = once(array)
                .chain(batch.columns().iter().cloned())
                .collect::<Vec<ArrayRef>>();

            let new_fields = once(Field::new("database_name", data_type, false))
                .chain(batch.schema().fields().iter().cloned())
                .collect::<Vec<Field>>();
            let new_schema = Arc::new(Schema::new(new_fields));

            RecordBatch::try_new(new_schema, new_columns).expect("Creating new record batch")
        }))
    }

    /// register a table provider  for this sytem table
    fn build(self, ctx: &mut ExecutionContext) {
        let Self {
            table_name,
            batches,
        } = self;

        let schema = if batches.is_empty() {
            panic!("No batches for ChunksTableBuilder");
        } else {
            batches[0].schema()
        };

        let partitions = batches
            .into_iter()
            .map(|batch| vec![batch])
            .collect::<Vec<_>>();

        let memtable = MemTable::try_new(schema, partitions).expect("creating memtable");

        ctx.register_table(table_name.as_str(), Arc::new(memtable))
            .ok();
    }
}
