//! module for query executor
use crate::system_tables::{SystemSchemaProvider, SYSTEM_SCHEMA_NAME};
use crate::{QueryExecutor, QueryKind};
use arrow::array::{ArrayRef, Int64Builder, StringBuilder, StructArray};
use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use arrow_schema::ArrowError;
use async_trait::async_trait;
use data_types::NamespaceId;
use datafusion::catalog::{CatalogProvider, SchemaProvider, Session};
use datafusion::common::arrow::array::StringArray;
use datafusion::common::arrow::datatypes::{DataType, Field, Schema as DatafusionSchema};
use datafusion::datasource::{TableProvider, TableType};
use datafusion::error::DataFusionError;
use datafusion::execution::SendableRecordBatchStream;
use datafusion::logical_expr::TableProviderFilterPushDown;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::prelude::Expr;
use datafusion_util::config::DEFAULT_SCHEMA;
use datafusion_util::MemoryStream;
use influxdb3_catalog::catalog::{Catalog, DatabaseSchema};
use influxdb3_write::last_cache::LastCacheFunction;
use influxdb3_write::WriteBuffer;
use iox_query::exec::{Executor, IOxSessionContext, QueryConfig};
use iox_query::frontend::sql::SqlQueryPlanner;
use iox_query::provider::ProviderBuilder;
use iox_query::query_log::QueryLog;
use iox_query::query_log::QueryText;
use iox_query::query_log::StateReceived;
use iox_query::query_log::{QueryCompletedToken, QueryLogEntries};
use iox_query::{Extension, QueryDatabase};
use iox_query::{QueryChunk, QueryNamespace};
use iox_query_influxql::frontend::planner::InfluxQLQueryPlanner;
use iox_query_params::StatementParams;
use metric::Registry;
use observability_deps::tracing::{debug, info};
use schema::Schema;
use std::any::Any;
use std::collections::HashMap;
use std::fmt::Debug;
use std::sync::Arc;
use trace::ctx::SpanContext;
use trace::span::{Span, SpanExt, SpanRecorder};
use trace_http::ctx::RequestLogContext;
use tracker::{
    AsyncSemaphoreMetrics, InstrumentedAsyncOwnedSemaphorePermit, InstrumentedAsyncSemaphore,
};

#[derive(Debug)]
pub struct QueryExecutorImpl {
    catalog: Arc<Catalog>,
    write_buffer: Arc<dyn WriteBuffer>,
    exec: Arc<Executor>,
    datafusion_config: Arc<HashMap<String, String>>,
    query_execution_semaphore: Arc<InstrumentedAsyncSemaphore>,
    query_log: Arc<QueryLog>,
}

impl QueryExecutorImpl {
    pub fn new(
        catalog: Arc<Catalog>,
        write_buffer: Arc<dyn WriteBuffer>,
        exec: Arc<Executor>,
        metrics: Arc<Registry>,
        datafusion_config: Arc<HashMap<String, String>>,
        concurrent_query_limit: usize,
        query_log_size: usize,
    ) -> Self {
        let semaphore_metrics = Arc::new(AsyncSemaphoreMetrics::new(
            &metrics,
            &[("semaphore", "query_execution")],
        ));
        let query_execution_semaphore =
            Arc::new(semaphore_metrics.new_semaphore(concurrent_query_limit));
        let query_log = Arc::new(QueryLog::new(
            query_log_size,
            Arc::new(iox_time::SystemProvider::new()),
        ));
        Self {
            catalog,
            write_buffer,
            exec,
            datafusion_config,
            query_execution_semaphore,
            query_log,
        }
    }
}

#[async_trait]
impl QueryExecutor for QueryExecutorImpl {
    type Error = Error;

    async fn query(
        &self,
        database: &str,
        query: &str,
        params: Option<StatementParams>,
        kind: QueryKind,
        span_ctx: Option<SpanContext>,
        external_span_ctx: Option<RequestLogContext>,
    ) -> Result<SendableRecordBatchStream, Self::Error> {
        info!(%database, %query, ?params, ?kind, "QueryExecutorImpl as QueryExecutor::query");
        let db = self
            .namespace(database, span_ctx.child_span("get database"), false)
            .await
            .map_err(|_| Error::DatabaseNotFound {
                db_name: database.to_string(),
            })?
            .ok_or_else(|| Error::DatabaseNotFound {
                db_name: database.to_string(),
            })?;

        // TODO - configure query here?
        let ctx = db.new_query_context(span_ctx, Default::default());

        let params = params.unwrap_or_default();

        debug!("create query plan");
        let (plan, query_type) = match kind {
            QueryKind::Sql => {
                let planner = SqlQueryPlanner::new();
                (planner.query(query, params.clone(), &ctx).await, "sql")
            }
            QueryKind::InfluxQl => (
                InfluxQLQueryPlanner::query(query, params.clone(), &ctx).await,
                "influxql",
            ),
        };
        let token = db.record_query(
            external_span_ctx.as_ref().map(RequestLogContext::ctx),
            query_type,
            Box::new(query.to_string()),
            params,
        );
        let plan = match plan.map_err(Error::QueryPlanning) {
            Ok(plan) => plan,
            Err(e) => {
                token.fail();
                return Err(e);
            }
        };
        let token = token.planned(&ctx, Arc::clone(&plan));

        // TODO: Enforce concurrency limit here
        let token = token.permit();

        debug!("execute stream of query results");
        match ctx.execute_stream(Arc::clone(&plan)).await {
            Ok(query_results) => {
                token.success();
                Ok(query_results)
            }
            Err(err) => {
                token.fail();
                Err(Error::ExecuteStream(err))
            }
        }
    }

    fn show_databases(&self) -> Result<SendableRecordBatchStream, Self::Error> {
        let mut databases = self.catalog.list_databases();
        // sort them to ensure consistent order:
        databases.sort_unstable();
        let databases = StringArray::from(databases);
        let schema =
            DatafusionSchema::new(vec![Field::new("iox::database", DataType::Utf8, false)]);
        let batch = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(databases)])
            .map_err(Error::DatabasesToRecordBatch)?;
        Ok(Box::pin(MemoryStream::new(vec![batch])))
    }

    async fn show_retention_policies(
        &self,
        database: Option<&str>,
        span_ctx: Option<SpanContext>,
    ) -> Result<SendableRecordBatchStream, Self::Error> {
        let mut databases = if let Some(db) = database {
            vec![db.to_owned()]
        } else {
            self.catalog.list_databases()
        };
        // sort them to ensure consistent order:
        databases.sort_unstable();

        let mut rows = Vec::with_capacity(databases.len());
        for database in databases {
            let db = self
                .namespace(&database, span_ctx.child_span("get database"), false)
                .await
                .map_err(|_| Error::DatabaseNotFound {
                    db_name: database.to_string(),
                })?
                .ok_or_else(|| Error::DatabaseNotFound {
                    db_name: database.to_string(),
                })?;
            let duration = db.retention_time_ns();
            let (db_name, rp_name) = split_database_name(&database);
            rows.push(RetentionPolicyRow {
                database: db_name,
                name: rp_name,
                duration,
            });
        }

        let batch = retention_policy_rows_to_batch(&rows);
        Ok(Box::pin(MemoryStream::new(vec![batch])))
    }
}

#[derive(Debug)]
struct RetentionPolicyRow {
    database: String,
    name: String,
    duration: Option<i64>,
}

#[derive(Debug, Default)]
struct RetentionPolicyRowBuilder {
    database: StringBuilder,
    name: StringBuilder,
    duration: Int64Builder,
}

impl RetentionPolicyRowBuilder {
    fn append(&mut self, row: &RetentionPolicyRow) {
        self.database.append_value(row.database.as_str());
        self.name.append_value(row.name.as_str());
        self.duration.append_option(row.duration);
    }

    // Note: may be able to use something simpler than StructArray here, this is just based
    // directly on the arrow docs: https://docs.rs/arrow/latest/arrow/array/builder/index.html
    fn finish(&mut self) -> StructArray {
        StructArray::from(vec![
            (
                Arc::new(Field::new("iox::database", DataType::Utf8, false)),
                Arc::new(self.database.finish()) as ArrayRef,
            ),
            (
                Arc::new(Field::new("name", DataType::Utf8, false)),
                Arc::new(self.name.finish()) as ArrayRef,
            ),
            (
                Arc::new(Field::new("duration", DataType::Int64, true)),
                Arc::new(self.duration.finish()) as ArrayRef,
            ),
        ])
    }
}

impl<'a> Extend<&'a RetentionPolicyRow> for RetentionPolicyRowBuilder {
    fn extend<T: IntoIterator<Item = &'a RetentionPolicyRow>>(&mut self, iter: T) {
        iter.into_iter().for_each(|row| self.append(row));
    }
}

fn retention_policy_rows_to_batch(rows: &[RetentionPolicyRow]) -> RecordBatch {
    let mut builder = RetentionPolicyRowBuilder::default();
    builder.extend(rows);
    RecordBatch::from(&builder.finish())
}

const AUTOGEN_RETENTION_POLICY: &str = "autogen";

fn split_database_name(db_name: &str) -> (String, String) {
    let mut split = db_name.split('/');
    (
        split.next().unwrap().to_owned(),
        split.next().unwrap_or(AUTOGEN_RETENTION_POLICY).to_owned(),
    )
}

#[derive(Debug, thiserror::Error)]
pub enum Error {
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
}

// This implementation is for the Flight service
#[async_trait]
impl QueryDatabase for QueryExecutorImpl {
    async fn namespace(
        &self,
        name: &str,
        span: Option<Span>,
        // We expose the `system` tables by default in the monolithic versions of InfluxDB 3
        _include_debug_info_tables: bool,
    ) -> Result<Option<Arc<dyn QueryNamespace>>, DataFusionError> {
        let _span_recorder = SpanRecorder::new(span);

        let db_schema = self.catalog.db_schema(name).ok_or_else(|| {
            DataFusionError::External(Box::new(Error::DatabaseNotFound {
                db_name: name.into(),
            }))
        })?;

        Ok(Some(Arc::new(Database::new(
            db_schema,
            Arc::clone(&self.write_buffer),
            Arc::clone(&self.exec),
            Arc::clone(&self.datafusion_config),
            Arc::clone(&self.query_log),
        ))))
    }

    async fn acquire_semaphore(&self, span: Option<Span>) -> InstrumentedAsyncOwnedSemaphorePermit {
        Arc::clone(&self.query_execution_semaphore)
            .acquire_owned(span)
            .await
            .expect("Semaphore should not be closed by anyone")
    }

    fn query_log(&self) -> QueryLogEntries {
        self.query_log.entries()
    }
}

#[derive(Debug, Clone)]
pub struct Database {
    db_schema: Arc<DatabaseSchema>,
    write_buffer: Arc<dyn WriteBuffer>,
    exec: Arc<Executor>,
    datafusion_config: Arc<HashMap<String, String>>,
    query_log: Arc<QueryLog>,
    system_schema_provider: Arc<SystemSchemaProvider>,
}

impl Database {
    pub fn new(
        db_schema: Arc<DatabaseSchema>,
        write_buffer: Arc<dyn WriteBuffer>,
        exec: Arc<Executor>,
        datafusion_config: Arc<HashMap<String, String>>,
        query_log: Arc<QueryLog>,
    ) -> Self {
        let system_schema_provider = Arc::new(SystemSchemaProvider::new(
            Arc::clone(&db_schema.name),
            Arc::clone(&query_log),
            Arc::clone(&write_buffer),
        ));
        Self {
            db_schema,
            write_buffer,
            exec,
            datafusion_config,
            query_log,
            system_schema_provider,
        }
    }

    fn from_namespace(db: &Self) -> Self {
        Self {
            db_schema: Arc::clone(&db.db_schema),
            write_buffer: Arc::clone(&db.write_buffer),
            exec: Arc::clone(&db.exec),
            datafusion_config: Arc::clone(&db.datafusion_config),
            query_log: Arc::clone(&db.query_log),
            system_schema_provider: Arc::clone(&db.system_schema_provider),
        }
    }

    async fn query_table(&self, table_name: &str) -> Option<Arc<QueryTable>> {
        self.db_schema.get_table_schema(table_name).map(|schema| {
            Arc::new(QueryTable {
                db_schema: Arc::clone(&self.db_schema),
                name: table_name.into(),
                schema: schema.clone(),
                write_buffer: Arc::clone(&self.write_buffer),
            })
        })
    }
}

#[async_trait]
impl QueryNamespace for Database {
    async fn chunks(
        &self,
        table_name: &str,
        filters: &[Expr],
        projection: Option<&Vec<usize>>,
        ctx: IOxSessionContext,
    ) -> Result<Vec<Arc<dyn QueryChunk>>, DataFusionError> {
        let _span_recorder = SpanRecorder::new(ctx.child_span("QueryDatabase::chunks"));
        debug!(%table_name, ?filters, "Database as QueryNamespace::chunks");

        let Some(table) = self.query_table(table_name).await else {
            debug!(%table_name, "No entry for table");
            return Ok(vec![]);
        };

        table.chunks(&ctx.inner().state(), projection, filters, None)
    }

    fn retention_time_ns(&self) -> Option<i64> {
        None
    }

    fn record_query(
        &self,
        span_ctx: Option<&SpanContext>,
        query_type: &'static str,
        query_text: QueryText,
        query_params: StatementParams,
    ) -> QueryCompletedToken<StateReceived> {
        let trace_id = span_ctx.map(|ctx| ctx.trace_id);
        let namespace_name: Arc<str> = Arc::from("influxdb3 edge");
        self.query_log.push(
            NamespaceId::new(0),
            namespace_name,
            query_type,
            query_text,
            query_params,
            trace_id,
        )
    }

    fn new_query_context(
        &self,
        span_ctx: Option<SpanContext>,
        _config: Option<&QueryConfig>,
    ) -> IOxSessionContext {
        let mut cfg = self
            .exec
            .new_session_config()
            .with_default_catalog(Arc::new(Self::from_namespace(self)))
            .with_span_context(span_ctx);

        for (k, v) in self.datafusion_config.as_ref() {
            cfg = cfg.with_config_option(k, v);
        }

        let ctx = cfg.build();
        ctx.inner().register_udtf(
            LAST_CACHE_UDTF_NAME,
            Arc::new(LastCacheFunction::new(
                self.db_schema.name.to_string(),
                self.write_buffer.last_cache_provider(),
            )),
        );
        ctx
    }

    fn new_extended_query_context(
        &self,
        _extension: Arc<dyn Extension>,
        _span_ctx: Option<SpanContext>,
        _query_config: Option<&QueryConfig>,
    ) -> IOxSessionContext {
        unimplemented!();
    }
}

const LAST_CACHE_UDTF_NAME: &str = "last_cache";

impl CatalogProvider for Database {
    fn as_any(&self) -> &dyn Any {
        self as &dyn Any
    }

    fn schema_names(&self) -> Vec<String> {
        debug!("Database as CatalogProvider::schema_names");
        vec![DEFAULT_SCHEMA.to_string(), SYSTEM_SCHEMA_NAME.to_string()]
    }

    fn schema(&self, name: &str) -> Option<Arc<dyn SchemaProvider>> {
        debug!(schema_name = %name, "Database as CatalogProvider::schema");
        match name {
            DEFAULT_SCHEMA => Some(Arc::new(Self::from_namespace(self))),
            SYSTEM_SCHEMA_NAME => Some(Arc::clone(&self.system_schema_provider) as _),
            _ => None,
        }
    }
}

#[async_trait]
impl SchemaProvider for Database {
    fn as_any(&self) -> &dyn Any {
        self as &dyn Any
    }

    fn table_names(&self) -> Vec<String> {
        self.db_schema
            .table_names()
            .iter()
            .map(|t| t.to_string())
            .collect()
    }

    async fn table(&self, name: &str) -> Result<Option<Arc<dyn TableProvider>>, DataFusionError> {
        Ok(self.query_table(name).await.map(|qt| qt as _))
    }

    fn table_exist(&self, name: &str) -> bool {
        self.db_schema.table_exists(name)
    }
}

#[derive(Debug)]
pub struct QueryTable {
    db_schema: Arc<DatabaseSchema>,
    name: Arc<str>,
    schema: Schema,
    write_buffer: Arc<dyn WriteBuffer>,
}

impl QueryTable {
    fn chunks(
        &self,
        ctx: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        _limit: Option<usize>,
    ) -> Result<Vec<Arc<dyn QueryChunk>>, DataFusionError> {
        self.write_buffer.get_table_chunks(
            &self.db_schema.name,
            self.name.as_ref(),
            filters,
            projection,
            ctx,
        )
    }
}

#[async_trait]
impl TableProvider for QueryTable {
    fn as_any(&self) -> &dyn Any {
        self as &dyn Any
    }

    fn schema(&self) -> SchemaRef {
        self.schema.as_arrow()
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> datafusion::common::Result<Vec<TableProviderFilterPushDown>> {
        Ok(vec![TableProviderFilterPushDown::Inexact; filters.len()])
    }

    async fn scan(
        &self,
        ctx: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> datafusion::common::Result<Arc<dyn ExecutionPlan>> {
        let filters = filters.to_vec();
        debug!(
            ?projection,
            ?filters,
            ?limit,
            "QueryTable as TableProvider::scan"
        );
        let mut builder = ProviderBuilder::new(Arc::clone(&self.name), self.schema.clone());

        let chunks = self.chunks(ctx, projection, &filters, limit)?;
        for chunk in chunks {
            builder = builder.add_chunk(chunk);
        }

        let provider = match builder.build() {
            Ok(provider) => provider,
            Err(e) => panic!("unexpected error: {e:?}"),
        };

        provider.scan(ctx, projection, &filters, limit).await
    }
}
#[cfg(test)]
mod tests {
    use std::{num::NonZeroUsize, sync::Arc, time::Duration};

    use arrow::array::RecordBatch;
    use data_types::NamespaceName;
    use datafusion::{assert_batches_sorted_eq, error::DataFusionError};
    use futures::TryStreamExt;
    use influxdb3_catalog::catalog::Catalog;
    use influxdb3_wal::{Level0Duration, WalConfig};
    use influxdb3_write::{
        last_cache::LastCacheProvider, persister::Persister, write_buffer::WriteBufferImpl,
        WriteBuffer,
    };
    use iox_query::exec::{DedicatedExecutor, Executor, ExecutorConfig};
    use iox_time::{MockProvider, Time};
    use metric::Registry;
    use object_store::{local::LocalFileSystem, ObjectStore};
    use parquet_file::storage::{ParquetStorage, StorageId};

    use crate::{
        query_executor::QueryExecutorImpl, system_tables::table_name_predicate_error, QueryExecutor,
    };

    fn make_exec(object_store: Arc<dyn ObjectStore>) -> Arc<Executor> {
        let metrics = Arc::new(metric::Registry::default());

        let parquet_store = ParquetStorage::new(
            Arc::clone(&object_store),
            StorageId::from("test_exec_storage"),
        );
        Arc::new(Executor::new_with_config_and_executor(
            ExecutorConfig {
                target_query_partitions: NonZeroUsize::new(1).unwrap(),
                object_stores: [&parquet_store]
                    .into_iter()
                    .map(|store| (store.id(), Arc::clone(store.object_store())))
                    .collect(),
                metric_registry: Arc::clone(&metrics),
                // Default to 1gb
                mem_pool_size: 1024 * 1024 * 1024, // 1024 (b/kb) * 1024 (kb/mb) * 1024 (mb/gb)
            },
            DedicatedExecutor::new_testing(),
        ))
    }

    async fn setup() -> (Arc<dyn WriteBuffer>, QueryExecutorImpl, Arc<MockProvider>) {
        // Set up QueryExecutor
        let object_store: Arc<dyn ObjectStore> =
            Arc::new(LocalFileSystem::new_with_prefix(test_helpers::tmp_dir().unwrap()).unwrap());
        let persister = Arc::new(Persister::new(Arc::clone(&object_store), "test_host"));
        let time_provider = Arc::new(MockProvider::new(Time::from_timestamp_nanos(0)));
        let executor = make_exec(object_store);
        let write_buffer: Arc<dyn WriteBuffer> = Arc::new(
            WriteBufferImpl::new(
                Arc::clone(&persister),
                Arc::new(Catalog::new()),
                Arc::new(LastCacheProvider::new()),
                Arc::<MockProvider>::clone(&time_provider),
                Arc::clone(&executor),
                WalConfig {
                    level_0_duration: Level0Duration::new_1m(),
                    max_write_buffer_size: 100,
                    flush_interval: Duration::from_millis(10),
                    snapshot_size: 1,
                },
            )
            .await
            .unwrap(),
        );
        let metrics = Arc::new(Registry::new());
        let df_config = Arc::new(Default::default());
        let query_executor = QueryExecutorImpl::new(
            write_buffer.catalog(),
            Arc::clone(&write_buffer),
            executor,
            metrics,
            df_config,
            10,
            10,
        );

        (write_buffer, query_executor, time_provider)
    }

    #[test_log::test(tokio::test)]
    async fn system_parquet_files_success() {
        let (write_buffer, query_executor, time_provider) = setup().await;
        // Perform some writes to multiple tables
        let db_name = "test_db";
        // perform writes over time to generate WAL files and some snapshots
        // the time provider is bumped to trick the system into persisting files:
        for i in 0..10 {
            let time = i * 10;
            let _ = write_buffer
                .write_lp(
                    NamespaceName::new(db_name).unwrap(),
                    "\
                cpu,host=a,region=us-east usage=250\n\
                mem,host=a,region=us-east usage=150000\n\
                ",
                    Time::from_timestamp_nanos(time),
                    false,
                    influxdb3_write::Precision::Nanosecond,
                )
                .await
                .unwrap();

            time_provider.set(Time::from_timestamp(time + 1, 0).unwrap());
        }

        // bump time again and sleep briefly to ensure time to persist things
        time_provider.set(Time::from_timestamp(20, 0).unwrap());
        tokio::time::sleep(Duration::from_millis(500)).await;

        struct TestCase<'a> {
            query: &'a str,
            expected: &'a [&'a str],
        }

        let test_cases = [
            TestCase {
                query: "SELECT * FROM system.parquet_files WHERE table_name = 'cpu'",
                expected: &[
                    "+------------+-----------------------------------------------------+------------+-----------+----------+----------+",
                    "| table_name | path                                                | size_bytes | row_count | min_time | max_time |",
                    "+------------+-----------------------------------------------------+------------+-----------+----------+----------+",
                    "| cpu        | dbs/test_db/cpu/1970-01-01/00-00/0000000003.parquet | 2142       | 2         | 0        | 10       |",
                    "| cpu        | dbs/test_db/cpu/1970-01-01/00-00/0000000005.parquet | 2142       | 2         | 20       | 30       |",
                    "| cpu        | dbs/test_db/cpu/1970-01-01/00-00/0000000007.parquet | 2142       | 2         | 40       | 50       |",
                    "| cpu        | dbs/test_db/cpu/1970-01-01/00-00/0000000009.parquet | 2142       | 2         | 60       | 70       |",
                    "+------------+-----------------------------------------------------+------------+-----------+----------+----------+",
                ],
            },
            TestCase {
                query: "SELECT * FROM system.parquet_files WHERE table_name = 'mem'",
                expected: &[
                    "+------------+-----------------------------------------------------+------------+-----------+----------+----------+",
                    "| table_name | path                                                | size_bytes | row_count | min_time | max_time |",
                    "+------------+-----------------------------------------------------+------------+-----------+----------+----------+",
                    "| mem        | dbs/test_db/mem/1970-01-01/00-00/0000000003.parquet | 2142       | 2         | 0        | 10       |",
                    "| mem        | dbs/test_db/mem/1970-01-01/00-00/0000000005.parquet | 2142       | 2         | 20       | 30       |",
                    "| mem        | dbs/test_db/mem/1970-01-01/00-00/0000000007.parquet | 2142       | 2         | 40       | 50       |",
                    "| mem        | dbs/test_db/mem/1970-01-01/00-00/0000000009.parquet | 2142       | 2         | 60       | 70       |",
                    "+------------+-----------------------------------------------------+------------+-----------+----------+----------+",
                ],
            },
        ];

        for t in test_cases {
            let batch_stream = query_executor
                .query(db_name, t.query, None, crate::QueryKind::Sql, None, None)
                .await
                .unwrap();
            let batches: Vec<RecordBatch> = batch_stream.try_collect().await.unwrap();
            assert_batches_sorted_eq!(t.expected, &batches);
        }
    }

    #[tokio::test]
    async fn system_parquet_files_predicate_error() {
        let (write_buffer, query_executor, time_provider) = setup().await;
        // make some writes, so that we have a database that we can query against:
        let db_name = "test_db";
        let _ = write_buffer
            .write_lp(
                NamespaceName::new(db_name).unwrap(),
                "cpu,host=a,region=us-east usage=0.1 1",
                Time::from_timestamp_nanos(0),
                false,
                influxdb3_write::Precision::Nanosecond,
            )
            .await
            .unwrap();

        // Bump time to trick the persister into persisting to parquet:
        time_provider.set(Time::from_timestamp(60 * 10, 0).unwrap());

        // query without the `WHERE table_name =` clause to trigger the error:
        let query = "SELECT * FROM system.parquet_files";
        let stream = query_executor
            .query(db_name, query, None, crate::QueryKind::Sql, None, None)
            .await
            .unwrap();
        let error: DataFusionError = stream.try_collect::<Vec<RecordBatch>>().await.unwrap_err();
        assert_eq!(error.message(), table_name_predicate_error().message());
    }
}
