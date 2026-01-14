use async_trait::async_trait;
use datafusion::arrow::array::{Array, BooleanArray, RecordBatch, StringArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::arrow::error::ArrowError;
use datafusion::common::DataFusionError;
use datafusion::execution::SendableRecordBatchStream;
use datafusion::physical_plan::memory::MemoryStream;
use influxdb_influxql_parser::statement::Statement;
use influxdb3_catalog::catalog::Catalog;
use iox_query::query_log::QueryLogEntries;
use iox_query::{QueryDatabase, QueryNamespace};
use iox_query_influxql::show_databases::{InfluxQlShowDatabases, generate_metadata};
use iox_query_influxql::show_retention_policies::InfluxQlShowRetentionPolicies;
use iox_query_params::StatementParams;
use schema::INFLUXQL_MEASUREMENT_COLUMN_NAME;
use std::fmt::Debug;
use std::sync::Arc;
use trace::ctx::SpanContext;
use trace::span::{Span, SpanExt};
use trace_http::ctx::RequestLogContext;
use tracker::InstrumentedAsyncOwnedSemaphorePermit;

#[derive(Debug, thiserror::Error)]
pub enum QueryExecutorError {
    #[error("database not found: {db_name}")]
    DatabaseNotFound { db_name: String },
    #[error("error while planning query: {0}")]
    QueryPlanning(#[source] DataFusionError),
    #[error("error while executing plan: {0}")]
    ExecuteStream(#[source] DataFusionError),
    #[error("unable to compose record batches from databases: {0}")]
    DatabasesToRecordBatch(#[source] ArrowError),
    #[error("unable to compose record batches from retention policies: {0}")]
    RetentionPoliciesToRecordBatch(#[source] ArrowError),
    #[error("invokded a method that is not implemented: {0}")]
    MethodNotImplemented(&'static str),
    #[error(transparent)]
    Anyhow(#[from] anyhow::Error),
}

#[async_trait]
pub trait QueryExecutor: QueryDatabase + Debug + Send + Sync + 'static {
    async fn get_db_namespace(
        &self,
        database_name: &str,
        span_ctx: &Option<SpanContext>,
    ) -> Result<Arc<dyn QueryNamespace>, QueryExecutorError> {
        self.namespace(
            database_name,
            span_ctx.child_span("get_db_namespace"),
            false,
        )
        .await
        .map_err(|_| QueryExecutorError::DatabaseNotFound {
            db_name: database_name.to_string(),
        })?
        .ok_or_else(|| QueryExecutorError::DatabaseNotFound {
            db_name: database_name.to_string(),
        })
    }

    async fn query_sql(
        &self,
        database: &str,
        q: &str,
        params: Option<StatementParams>,
        span_ctx: Option<SpanContext>,
        external_span_ctx: Option<RequestLogContext>,
    ) -> Result<SendableRecordBatchStream, QueryExecutorError>;

    async fn query_influxql(
        &self,
        database_name: &str,
        query_str: &str,
        influxql_statement: Statement,
        params: Option<StatementParams>,
        span_ctx: Option<SpanContext>,
        external_span_ctx: Option<RequestLogContext>,
    ) -> Result<SendableRecordBatchStream, QueryExecutorError>;

    fn show_databases(
        &self,
        include_deleted: bool,
    ) -> Result<SendableRecordBatchStream, QueryExecutorError>;

    async fn show_retention_policies(
        &self,
        database: Option<&str>,
        span_ctx: Option<SpanContext>,
    ) -> Result<SendableRecordBatchStream, QueryExecutorError>;

    fn upcast(&self) -> Arc<dyn QueryDatabase + 'static>;
}

#[derive(Debug)]
pub struct ShowDatabases(Arc<Catalog>);

impl ShowDatabases {
    pub fn new(catalog: Arc<Catalog>) -> Arc<Self> {
        Arc::new(Self(catalog))
    }
}

#[async_trait::async_trait]
impl InfluxQlShowDatabases for ShowDatabases {
    /// Produce the Arrow schema for the `SHOW DATABASES` InfluxQL query
    fn schema(&self) -> SchemaRef {
        Arc::new(
            Schema::new(vec![
                Field::new(INFLUXQL_MEASUREMENT_COLUMN_NAME, DataType::Utf8, false),
                Field::new("name", DataType::Utf8, false),
            ])
            .with_metadata(generate_metadata(0)),
        )
    }

    /// Produce a record batch stream containing the results for the `SHOW DATABASES` query
    async fn show_databases(
        &self,
        database_names: Vec<String>,
    ) -> Result<SendableRecordBatchStream, DataFusionError> {
        let databases = self
            .0
            .db_names()
            .into_iter()
            .filter(|db| database_names.contains(db))
            .collect::<Vec<_>>();
        let measurement_array: StringArray = vec!["databases"; databases.len()].into();
        let names_array: StringArray = databases.into();
        let arrays = vec![
            Arc::new(measurement_array) as Arc<dyn Array>,
            Arc::new(names_array) as Arc<dyn Array>,
        ];
        let batch = RecordBatch::try_new(self.schema(), arrays)?;
        Ok(Box::pin(MemoryStream::try_new(
            vec![batch],
            self.schema(),
            None,
        )?))
    }
}

#[cfg(test)]
mod show_databases_tests {
    use std::sync::Arc;

    use datafusion::{
        arrow::array::RecordBatch, assert_batches_eq, execution::SendableRecordBatchStream,
    };
    use futures::StreamExt;
    use influxdb3_catalog::catalog::Catalog;
    use iox_query_influxql::show_databases::InfluxQlShowDatabases;

    use crate::query_executor::ShowDatabases;

    #[tokio::test]
    async fn test_show_databases() {
        let catalog = Catalog::new_in_memory("test").await.map(Arc::new).unwrap();
        catalog.create_database("foo").await.unwrap();
        catalog.create_database("bar").await.unwrap();
        catalog.create_database("mop").await.unwrap();
        let show_databases = ShowDatabases::new(catalog);
        for (dbs, expected) in [
            (
                vec!["foo"],
                vec![
                    "+------------------+------+",
                    "| iox::measurement | name |",
                    "+------------------+------+",
                    "| databases        | foo  |",
                    "+------------------+------+",
                ],
            ),
            (
                vec!["foo", "bar"],
                vec![
                    "+------------------+------+",
                    "| iox::measurement | name |",
                    "+------------------+------+",
                    "| databases        | foo  |",
                    "| databases        | bar  |",
                    "+------------------+------+",
                ],
            ),
            (
                vec!["foo", "bar", "mop"],
                vec![
                    "+------------------+------+",
                    "| iox::measurement | name |",
                    "+------------------+------+",
                    "| databases        | foo  |",
                    "| databases        | bar  |",
                    "| databases        | mop  |",
                    "+------------------+------+",
                ],
            ),
        ] {
            let stream = show_databases
                .show_databases(dbs.into_iter().map(|s| s.to_string()).collect())
                .await
                .unwrap();
            let batches = collect_stream(stream).await;
            assert_batches_eq!(expected, &batches);
        }
    }

    async fn collect_stream(mut stream: SendableRecordBatchStream) -> Vec<RecordBatch> {
        let mut batches = Vec::new();
        while let Some(batch) = stream.next().await {
            batches.push(batch.unwrap());
        }
        batches
    }
}

#[derive(Debug)]
pub struct ShowRetentionPolicies(Arc<Catalog>);

impl ShowRetentionPolicies {
    pub fn new(catalog: Arc<Catalog>) -> Arc<Self> {
        Arc::new(Self(catalog))
    }
}

/// Implementation of `SHOW RETENTION POLICIES` for the `/query` API.
///
/// # Note
///
/// The original v1 /query API reports the following fields:
///
/// - `shardGroupDuration`
/// - `replicaN`
/// - `futureWriteLimit`
/// - `pastWriteLimit`
///
/// These are not reported in this implementation since they do not represent anything in the
/// underlying database in v3.
#[async_trait::async_trait]
impl InfluxQlShowRetentionPolicies for ShowRetentionPolicies {
    fn schema(&self) -> SchemaRef {
        Arc::new(
            Schema::new(vec![
                Field::new(INFLUXQL_MEASUREMENT_COLUMN_NAME, DataType::Utf8, false),
                Field::new("name", DataType::Utf8, false),
                // NOTE(tjh): duration is allowed to be nullable for databases that have `Indefinite`
                // retention periods.
                Field::new("duration", DataType::Utf8, true),
                Field::new("default", DataType::Boolean, false),
            ])
            .with_metadata(generate_metadata(0)),
        )
    }

    async fn show_retention_policies(
        &self,
        db_name: String,
    ) -> Result<SendableRecordBatchStream, DataFusionError> {
        let Some(db) = self.0.db_schema(&db_name) else {
            return Err(DataFusionError::Plan(format!(
                "database not found: {db_name}"
            )));
        };
        let measurement_array: StringArray = vec!["retention_policies"].into();
        let names_array: StringArray = vec![db.name().as_ref()].into();
        let rp = db.retention_period.format_v1();
        let durations_array = StringArray::from(vec![rp.as_ref()]);
        let default_array: BooleanArray = vec![true].into();

        let arrays = vec![
            Arc::new(measurement_array) as Arc<dyn Array>,
            Arc::new(names_array) as Arc<dyn Array>,
            Arc::new(durations_array) as Arc<dyn Array>,
            Arc::new(default_array) as Arc<dyn Array>,
        ];

        let batch = RecordBatch::try_new(self.schema(), arrays)?;
        Ok(Box::pin(MemoryStream::try_new(
            vec![batch],
            self.schema(),
            None,
        )?))
    }
}

#[cfg(test)]
mod show_retention_policies_tests {
    use std::{sync::Arc, time::Duration};

    use datafusion::{
        arrow::array::RecordBatch, assert_batches_eq, execution::SendableRecordBatchStream,
    };
    use futures::StreamExt;
    use influxdb3_catalog::catalog::{Catalog, CreateDatabaseOptions};
    use iox_query_influxql::show_retention_policies::InfluxQlShowRetentionPolicies;

    use crate::query_executor::ShowRetentionPolicies;

    #[tokio::test]
    async fn test_show_retention_policies() {
        let catalog = Catalog::new_in_memory("test").await.map(Arc::new).unwrap();
        let days = |n_days: u64| Duration::from_secs(n_days * 24 * 60 * 60);
        catalog
            .create_database_opts(
                "foo",
                CreateDatabaseOptions::default().retention_period(days(7)),
            )
            .await
            .unwrap();
        catalog
            .create_database_opts(
                "bar",
                CreateDatabaseOptions::default().retention_period(days(30)),
            )
            .await
            .unwrap();
        catalog
            .create_database_opts(
                "baz",
                CreateDatabaseOptions::default().retention_period(Duration::from_secs(90)),
            )
            .await
            .unwrap();
        catalog.create_database("mop").await.unwrap();
        let show_retention_policies = ShowRetentionPolicies::new(catalog);
        for (db, expected) in [
            (
                "foo",
                vec![
                    "+--------------------+------+----------+---------+",
                    "| iox::measurement   | name | duration | default |",
                    "+--------------------+------+----------+---------+",
                    "| retention_policies | foo  | 168h0m0s | true    |",
                    "+--------------------+------+----------+---------+",
                ],
            ),
            (
                "bar",
                vec![
                    "+--------------------+------+----------+---------+",
                    "| iox::measurement   | name | duration | default |",
                    "+--------------------+------+----------+---------+",
                    "| retention_policies | bar  | 720h0m0s | true    |",
                    "+--------------------+------+----------+---------+",
                ],
            ),
            (
                "baz",
                vec![
                    "+--------------------+------+----------+---------+",
                    "| iox::measurement   | name | duration | default |",
                    "+--------------------+------+----------+---------+",
                    "| retention_policies | baz  | 1m30s    | true    |",
                    "+--------------------+------+----------+---------+",
                ],
            ),
            (
                "mop",
                vec![
                    "+--------------------+------+----------+---------+",
                    "| iox::measurement   | name | duration | default |",
                    "+--------------------+------+----------+---------+",
                    "| retention_policies | mop  | 0s       | true    |",
                    "+--------------------+------+----------+---------+",
                ],
            ),
        ] {
            let stream = show_retention_policies
                .show_retention_policies(db.to_string())
                .await
                .unwrap();
            let batches = collect_stream(stream).await;
            assert_batches_eq!(expected, &batches);
        }
    }

    async fn collect_stream(mut stream: SendableRecordBatchStream) -> Vec<RecordBatch> {
        let mut batches = Vec::new();
        while let Some(batch) = stream.next().await {
            batches.push(batch.unwrap());
        }
        batches
    }
}

#[derive(Debug, Copy, Clone)]
pub struct UnimplementedQueryExecutor;

#[async_trait]
impl QueryDatabase for UnimplementedQueryExecutor {
    async fn namespace(
        &self,
        _name: &str,
        _span: Option<Span>,
        _include_debug_info_tables: bool,
    ) -> Result<Option<Arc<dyn QueryNamespace>>, DataFusionError> {
        unimplemented!()
    }

    async fn acquire_semaphore(
        &self,
        _span: Option<Span>,
    ) -> InstrumentedAsyncOwnedSemaphorePermit {
        unimplemented!()
    }

    fn query_log(&self) -> QueryLogEntries {
        unimplemented!()
    }
}

#[async_trait]
impl QueryExecutor for UnimplementedQueryExecutor {
    async fn query_sql(
        &self,
        _database: &str,
        _q: &str,
        _params: Option<StatementParams>,
        _span_ctx: Option<SpanContext>,
        _external_span_ctx: Option<RequestLogContext>,
    ) -> Result<SendableRecordBatchStream, QueryExecutorError> {
        Err(QueryExecutorError::MethodNotImplemented("query_sql"))
    }

    async fn query_influxql(
        &self,
        _database_name: &str,
        _query_str: &str,
        _influxql_statement: Statement,
        _params: Option<StatementParams>,
        _span_ctx: Option<SpanContext>,
        _external_span_ctx: Option<RequestLogContext>,
    ) -> Result<SendableRecordBatchStream, QueryExecutorError> {
        Err(QueryExecutorError::MethodNotImplemented("query_influxql"))
    }

    fn show_databases(
        &self,
        _include_deleted: bool,
    ) -> Result<SendableRecordBatchStream, QueryExecutorError> {
        Err(QueryExecutorError::MethodNotImplemented("show_databases"))
    }

    async fn show_retention_policies(
        &self,
        _database: Option<&str>,
        _span_ctx: Option<SpanContext>,
    ) -> Result<SendableRecordBatchStream, QueryExecutorError> {
        Err(QueryExecutorError::MethodNotImplemented(
            "show_retention_policies",
        ))
    }

    fn upcast(&self) -> Arc<dyn QueryDatabase + 'static> {
        Arc::new(UnimplementedQueryExecutor) as _
    }
}
