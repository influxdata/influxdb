//! module for query executor
use crate::{QueryExecutor, QueryKind};
use arrow::array::{
    ArrayRef, BooleanArray, DurationNanosecondArray, Int64Array, Int64Builder, StringBuilder,
    StructArray, TimestampNanosecondArray, UInt64Array,
};
use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use arrow_schema::{ArrowError, TimeUnit};
use async_trait::async_trait;
use data_types::NamespaceId;
use datafusion::catalog::schema::SchemaProvider;
use datafusion::catalog::CatalogProvider;
use datafusion::common::arrow::array::StringArray;
use datafusion::common::arrow::datatypes::{DataType, Field, Schema as DatafusionSchema};
use datafusion::datasource::{TableProvider, TableType};
use datafusion::error::DataFusionError;
use datafusion::execution::context::SessionState;
use datafusion::execution::SendableRecordBatchStream;
use datafusion::logical_expr::{col, BinaryExpr, Operator, TableProviderFilterPushDown};
use datafusion::physical_plan::ExecutionPlan;
use datafusion::prelude::Expr;
use datafusion::scalar::ScalarValue;
use datafusion_util::config::DEFAULT_SCHEMA;
use datafusion_util::MemoryStream;
use influxdb3_write::{
    catalog::{Catalog, DatabaseSchema},
    WriteBuffer,
};
use influxdb3_write::{ParquetFile, Persister};
use iox_query::exec::{Executor, IOxSessionContext, QueryConfig};
use iox_query::frontend::sql::SqlQueryPlanner;
use iox_query::provider::ProviderBuilder;
use iox_query::query_log::StateReceived;
use iox_query::query_log::{QueryCompletedToken, QueryLogEntries};
use iox_query::query_log::{QueryLog, QueryLogEntryState};
use iox_query::query_log::{QueryPhase, QueryText};
use iox_query::QueryDatabase;
use iox_query::{QueryChunk, QueryNamespace};
use iox_query_influxql::frontend::planner::InfluxQLQueryPlanner;
use iox_query_params::StatementParams;
use iox_system_tables::{IoxSystemTable, SystemTableProvider};
use metric::Registry;
use observability_deps::tracing::{debug, info, trace};
use schema::Schema;
use std::any::Any;
use std::collections::HashMap;
use std::fmt::Debug;
use std::ops::Deref;
use std::sync::Arc;
use trace::ctx::SpanContext;
use trace::span::{Span, SpanExt, SpanRecorder};
use trace_http::ctx::RequestLogContext;
use tracker::{
    AsyncSemaphoreMetrics, InstrumentedAsyncOwnedSemaphorePermit, InstrumentedAsyncSemaphore,
};

#[derive(Debug)]
pub struct QueryExecutorImpl<W> {
    catalog: Arc<Catalog>,
    write_buffer: Arc<W>,
    exec: Arc<Executor>,
    datafusion_config: Arc<HashMap<String, String>>,
    query_execution_semaphore: Arc<InstrumentedAsyncSemaphore>,
    query_log: Arc<QueryLog>,
}

impl<W: WriteBuffer> QueryExecutorImpl<W> {
    pub fn new(
        catalog: Arc<Catalog>,
        write_buffer: Arc<W>,
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
impl<W: WriteBuffer> QueryExecutor for QueryExecutorImpl<W> {
    type Error = Error;

    async fn query(
        &self,
        database: &str,
        q: &str,
        params: Option<StatementParams>,
        kind: QueryKind,
        span_ctx: Option<SpanContext>,
        external_span_ctx: Option<RequestLogContext>,
    ) -> Result<SendableRecordBatchStream, Self::Error> {
        info!("query in executor {}", database);
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
        let token = db.record_query(
            external_span_ctx.as_ref().map(RequestLogContext::ctx),
            "sql",
            Box::new(q.to_string()),
            params.clone(),
        );

        info!("plan");
        let plan = match kind {
            QueryKind::Sql => {
                let planner = SqlQueryPlanner::new();
                planner.query(q, params, &ctx).await
            }
            QueryKind::InfluxQl => {
                let planner = InfluxQLQueryPlanner::new();
                planner.query(q, params, &ctx).await
            }
        }
        .map_err(Error::QueryPlanning);
        let plan = match plan {
            Ok(plan) => plan,
            Err(e) => {
                token.fail();
                return Err(e);
            }
        };
        let token = token.planned(&ctx, Arc::clone(&plan));

        // TODO: Enforce concurrency limit here
        let token = token.permit();

        info!("execute_stream");
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
impl<W: WriteBuffer> QueryDatabase for QueryExecutorImpl<W> {
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
            name,
            db_schema,
            Arc::clone(&self.write_buffer) as _,
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
pub struct Database<B> {
    db_schema: Arc<DatabaseSchema>,
    write_buffer: Arc<B>,
    exec: Arc<Executor>,
    datafusion_config: Arc<HashMap<String, String>>,
    query_log: Arc<QueryLog>,
    system_schema_provider: Arc<SystemSchemaProvider>,
}

impl<B: WriteBuffer> Database<B> {
    pub fn new<S: Into<String>>(
        db_name: S,
        db_schema: Arc<DatabaseSchema>,
        write_buffer: Arc<B>,
        exec: Arc<Executor>,
        datafusion_config: Arc<HashMap<String, String>>,
        query_log: Arc<QueryLog>,
    ) -> Self {
        let system_schema_provider = Arc::new(SystemSchemaProvider::new(
            db_name.into(),
            write_buffer.persister(),
            Arc::clone(&query_log),
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

    async fn query_table(&self, table_name: &str) -> Option<Arc<QueryTable<B>>> {
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
impl<B: WriteBuffer> QueryNamespace for Database<B> {
    async fn chunks(
        &self,
        table_name: &str,
        filters: &[Expr],
        projection: Option<&Vec<usize>>,
        ctx: IOxSessionContext,
    ) -> Result<Vec<Arc<dyn QueryChunk>>, DataFusionError> {
        let _span_recorder = SpanRecorder::new(ctx.child_span("QueryDatabase::chunks"));
        debug!(%table_name, ?filters, "Finding chunks for table");

        let Some(table) = self.query_table(table_name).await else {
            trace!(%table_name, "No entry for table");
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

        cfg.build()
    }
}

impl<B: WriteBuffer> CatalogProvider for Database<B> {
    fn as_any(&self) -> &dyn Any {
        self as &dyn Any
    }

    fn schema_names(&self) -> Vec<String> {
        info!("CatalogProvider schema_names");
        vec![DEFAULT_SCHEMA.to_string(), SYSTEM_SCHEMA.to_string()]
    }

    fn schema(&self, name: &str) -> Option<Arc<dyn SchemaProvider>> {
        info!("CatalogProvider schema {}", name);
        match name {
            DEFAULT_SCHEMA => Some(Arc::new(Self::from_namespace(self))),
            SYSTEM_SCHEMA => Some(Arc::clone(&self.system_schema_provider) as _),
            _ => None,
        }
    }
}

#[async_trait]
impl<B: WriteBuffer> SchemaProvider for Database<B> {
    fn as_any(&self) -> &dyn Any {
        self as &dyn Any
    }

    fn table_names(&self) -> Vec<String> {
        self.db_schema.table_names()
    }

    async fn table(&self, name: &str) -> Result<Option<Arc<dyn TableProvider>>, DataFusionError> {
        Ok(self.query_table(name).await.map(|qt| qt as _))
    }

    fn table_exist(&self, name: &str) -> bool {
        self.db_schema.table_exists(name)
    }
}

#[derive(Debug)]
pub struct QueryTable<B> {
    db_schema: Arc<DatabaseSchema>,
    name: Arc<str>,
    schema: Schema,
    write_buffer: Arc<B>,
}

impl<B: WriteBuffer> QueryTable<B> {
    fn chunks(
        &self,
        ctx: &SessionState,
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
impl<B: WriteBuffer> TableProvider for QueryTable<B> {
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
        ctx: &SessionState,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> datafusion::common::Result<Arc<dyn ExecutionPlan>> {
        let filters = filters.to_vec();
        info!(
            "TableProvider scan {:?} {:?} {:?}",
            projection, filters, limit
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

pub const SYSTEM_SCHEMA: &str = "system";

const QUERIES_TABLE: &str = "queries";
const PARQUET_FILES_TABLE: &str = "parquet_files";

struct SystemSchemaProvider {
    tables: HashMap<&'static str, Arc<dyn TableProvider>>,
}

impl std::fmt::Debug for SystemSchemaProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut keys = self.tables.keys().copied().collect::<Vec<_>>();
        keys.sort_unstable();

        f.debug_struct("SystemSchemaProvider")
            .field("tables", &keys.join(", "))
            .finish()
    }
}

impl SystemSchemaProvider {
    fn new(db_name: String, persister: Arc<dyn Persister>, query_log: Arc<QueryLog>) -> Self {
        let mut tables = HashMap::<&'static str, Arc<dyn TableProvider>>::new();
        let parquet_files = Arc::new(SystemTableProvider::new(Arc::new(ParquetFilesTable::new(
            db_name, persister,
        ))));
        let queries = Arc::new(SystemTableProvider::new(Arc::new(QueriesTable::new(
            query_log,
        ))));
        tables.insert(QUERIES_TABLE, queries);
        tables.insert(PARQUET_FILES_TABLE, parquet_files);
        Self { tables }
    }
}

#[async_trait]
impl SchemaProvider for SystemSchemaProvider {
    fn as_any(&self) -> &dyn Any {
        self as &dyn Any
    }

    fn table_names(&self) -> Vec<String> {
        let mut names = self
            .tables
            .keys()
            .map(|s| (*s).to_owned())
            .collect::<Vec<_>>();
        names.sort();
        names
    }

    async fn table(&self, name: &str) -> Result<Option<Arc<dyn TableProvider>>, DataFusionError> {
        Ok(self.tables.get(name).cloned())
    }

    fn table_exist(&self, name: &str) -> bool {
        self.tables.contains_key(name)
    }
}

struct ParquetFilesTable {
    db_name: String,
    schema: SchemaRef,
    persister: Arc<dyn Persister>,
}

impl ParquetFilesTable {
    fn new(db_name: String, persister: Arc<dyn Persister>) -> Self {
        Self {
            db_name,
            schema: parquet_files_schema(),
            persister: Arc::clone(&persister),
        }
    }
}

// TODO - make this configurable:
const MAX_SEGMENTS_FOR_PARQUET_FILES_TABLE: usize = 1_000;
/// Used in queries to the system.parquet_files table
///
/// # Example
/// ```sql
/// SELECT * FROM system.parquet_files WHERE table_name = 'foo'
/// ```
const TABLE_NAME_PREDICATE: &str = "table_name";

fn table_name_predicate_error() -> DataFusionError {
    DataFusionError::Plan(format!(
        "must provide a {TABLE_NAME_PREDICATE} = '<table_name>' predicate in queries to \
            {SYSTEM_SCHEMA}.{PARQUET_FILES_TABLE}"
    ))
}

#[async_trait::async_trait]
impl IoxSystemTable for ParquetFilesTable {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    async fn scan(
        &self,
        filters: Option<Vec<Expr>>,
        _limit: Option<usize>,
    ) -> Result<RecordBatch, DataFusionError> {
        let schema = self.schema();

        // extract `table_name` from filters
        let table_name = filters
            .ok_or_else(table_name_predicate_error)?
            .iter()
            .find_map(|f| match f {
                Expr::BinaryExpr(BinaryExpr { left, op, right }) => {
                    if left.deref() == &col(TABLE_NAME_PREDICATE) && op == &Operator::Eq {
                        match right.deref() {
                            Expr::Literal(
                                ScalarValue::Utf8(Some(s)) | ScalarValue::LargeUtf8(Some(s)),
                            ) => Some(s.to_owned()),
                            _ => None,
                        }
                    } else {
                        None
                    }
                }
                _ => None,
            })
            .ok_or_else(table_name_predicate_error)?;

        let parquet_files: Vec<ParquetFile> = self
            .persister
            .load_segments(MAX_SEGMENTS_FOR_PARQUET_FILES_TABLE)
            .await?
            .into_iter()
            .filter_map(|mut s| s.databases.remove(&self.db_name))
            .filter_map(|mut d| d.tables.remove(&table_name))
            // NOTE - we can grab the sort key here too, if we want that displayed:
            .flat_map(|t| t.parquet_files)
            .collect();

        from_parquet_files(&table_name, schema, parquet_files)
    }
}

fn parquet_files_schema() -> SchemaRef {
    let columns = vec![
        Field::new("table_name", DataType::Utf8, false),
        Field::new("path", DataType::Utf8, false),
        Field::new("size_bytes", DataType::UInt64, false),
        Field::new("row_count", DataType::UInt64, false),
        Field::new("min_time", DataType::Int64, false),
        Field::new("max_time", DataType::Int64, false),
    ];
    Arc::new(DatafusionSchema::new(columns))
}

fn from_parquet_files(
    table_name: &str,
    schema: SchemaRef,
    parquet_files: Vec<ParquetFile>,
) -> Result<RecordBatch, DataFusionError> {
    let columns: Vec<ArrayRef> = vec![
        Arc::new(
            vec![table_name; parquet_files.len()]
                .iter()
                .map(|s| Some(s.to_string()))
                .collect::<StringArray>(),
        ),
        Arc::new(
            parquet_files
                .iter()
                .map(|f| Some(f.path.to_string()))
                .collect::<StringArray>(),
        ),
        Arc::new(
            parquet_files
                .iter()
                .map(|f| Some(f.size_bytes))
                .collect::<UInt64Array>(),
        ),
        Arc::new(
            parquet_files
                .iter()
                .map(|f| Some(f.row_count))
                .collect::<UInt64Array>(),
        ),
        Arc::new(
            parquet_files
                .iter()
                .map(|f| Some(f.min_time))
                .collect::<Int64Array>(),
        ),
        Arc::new(
            parquet_files
                .iter()
                .map(|f| Some(f.max_time))
                .collect::<Int64Array>(),
        ),
    ];

    Ok(RecordBatch::try_new(schema, columns)?)
}

struct QueriesTable {
    schema: SchemaRef,
    query_log: Arc<QueryLog>,
}

impl QueriesTable {
    fn new(query_log: Arc<QueryLog>) -> Self {
        Self {
            schema: queries_schema(),
            query_log,
        }
    }
}

#[async_trait::async_trait]
impl IoxSystemTable for QueriesTable {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    async fn scan(
        &self,
        _filters: Option<Vec<Expr>>,
        _limit: Option<usize>,
    ) -> Result<RecordBatch, DataFusionError> {
        let schema = self.schema();

        let entries = self
            .query_log
            .entries()
            .entries
            .into_iter()
            .map(|e| e.state())
            .collect::<Vec<_>>();

        from_query_log_entries(Arc::clone(&schema), &entries)
    }
}

fn queries_schema() -> SchemaRef {
    let columns = vec![
        Field::new("id", DataType::Utf8, false),
        Field::new("phase", DataType::Utf8, false),
        Field::new(
            "issue_time",
            DataType::Timestamp(TimeUnit::Nanosecond, None),
            false,
        ),
        Field::new("query_type", DataType::Utf8, false),
        Field::new("query_text", DataType::Utf8, false),
        Field::new("partitions", DataType::Int64, true),
        Field::new("parquet_files", DataType::Int64, true),
        Field::new(
            "plan_duration",
            DataType::Duration(TimeUnit::Nanosecond),
            true,
        ),
        Field::new(
            "permit_duration",
            DataType::Duration(TimeUnit::Nanosecond),
            true,
        ),
        Field::new(
            "execute_duration",
            DataType::Duration(TimeUnit::Nanosecond),
            true,
        ),
        Field::new(
            "end2end_duration",
            DataType::Duration(TimeUnit::Nanosecond),
            true,
        ),
        Field::new(
            "compute_duration",
            DataType::Duration(TimeUnit::Nanosecond),
            true,
        ),
        Field::new("max_memory", DataType::Int64, true),
        Field::new("success", DataType::Boolean, false),
        Field::new("running", DataType::Boolean, false),
        Field::new("cancelled", DataType::Boolean, false),
        Field::new("trace_id", DataType::Utf8, true),
    ];

    Arc::new(DatafusionSchema::new(columns))
}

fn from_query_log_entries(
    schema: SchemaRef,
    entries: &[Arc<QueryLogEntryState>],
) -> Result<RecordBatch, DataFusionError> {
    let mut columns: Vec<ArrayRef> = vec![];

    columns.push(Arc::new(
        entries
            .iter()
            .map(|e| Some(e.id.to_string()))
            .collect::<StringArray>(),
    ));

    columns.push(Arc::new(
        entries
            .iter()
            .map(|e| Some(e.phase.name()))
            .collect::<StringArray>(),
    ));

    columns.push(Arc::new(
        entries
            .iter()
            .map(|e| e.issue_time)
            .map(|ts| Some(ts.timestamp_nanos()))
            .collect::<TimestampNanosecondArray>(),
    ));

    columns.push(Arc::new(
        entries
            .iter()
            .map(|e| Some(&e.query_type))
            .collect::<StringArray>(),
    ));

    columns.push(Arc::new(
        entries
            .iter()
            .map(|e| Some(e.query_text.to_string()))
            .collect::<StringArray>(),
    ));

    columns.push(Arc::new(
        entries.iter().map(|e| e.partitions).collect::<Int64Array>(),
    ));

    columns.push(Arc::new(
        entries
            .iter()
            .map(|e| e.parquet_files)
            .collect::<Int64Array>(),
    ));

    columns.push(Arc::new(
        entries
            .iter()
            .map(|e| e.plan_duration.map(|d| d.as_nanos() as i64))
            .collect::<DurationNanosecondArray>(),
    ));

    columns.push(Arc::new(
        entries
            .iter()
            .map(|e| e.permit_duration.map(|d| d.as_nanos() as i64))
            .collect::<DurationNanosecondArray>(),
    ));

    columns.push(Arc::new(
        entries
            .iter()
            .map(|e| e.execute_duration.map(|d| d.as_nanos() as i64))
            .collect::<DurationNanosecondArray>(),
    ));

    columns.push(Arc::new(
        entries
            .iter()
            .map(|e| e.end2end_duration.map(|d| d.as_nanos() as i64))
            .collect::<DurationNanosecondArray>(),
    ));

    columns.push(Arc::new(
        entries
            .iter()
            .map(|e| e.compute_duration.map(|d| d.as_nanos() as i64))
            .collect::<DurationNanosecondArray>(),
    ));

    columns.push(Arc::new(
        entries.iter().map(|e| e.max_memory).collect::<Int64Array>(),
    ));

    columns.push(Arc::new(
        entries
            .iter()
            .map(|e| Some(e.success))
            .collect::<BooleanArray>(),
    ));

    columns.push(Arc::new(
        entries
            .iter()
            .map(|e| Some(e.running))
            .collect::<BooleanArray>(),
    ));

    columns.push(Arc::new(
        entries
            .iter()
            .map(|e| Some(e.phase == QueryPhase::Cancel))
            .collect::<BooleanArray>(),
    ));

    columns.push(Arc::new(
        entries
            .iter()
            .map(|e| e.trace_id.map(|x| format!("{:x}", x.0)))
            .collect::<StringArray>(),
    ));

    let batch = RecordBatch::try_new(schema, columns)?;
    Ok(batch)
}

#[cfg(test)]
mod tests {
    use std::{num::NonZeroUsize, sync::Arc, time::Duration};

    use arrow::array::RecordBatch;
    use data_types::NamespaceName;
    use datafusion::{assert_batches_sorted_eq, error::DataFusionError};
    use futures::TryStreamExt;
    use influxdb3_write::{
        persister::PersisterImpl, wal::WalImpl, write_buffer::WriteBufferImpl, Bufferer,
        SegmentDuration,
    };
    use iox_query::exec::{DedicatedExecutor, Executor, ExecutorConfig};
    use iox_time::{MockProvider, Time};
    use metric::Registry;
    use object_store::{local::LocalFileSystem, ObjectStore};
    use parquet_file::storage::{ParquetStorage, StorageId};

    use crate::{
        query_executor::{table_name_predicate_error, QueryExecutorImpl},
        QueryExecutor,
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

    type TestWriteBuffer = WriteBufferImpl<WalImpl, MockProvider>;
    async fn setup() -> (
        Arc<TestWriteBuffer>,
        QueryExecutorImpl<TestWriteBuffer>,
        Arc<MockProvider>,
    ) {
        // Set up QueryExecutor
        let object_store =
            Arc::new(LocalFileSystem::new_with_prefix(test_helpers::tmp_dir().unwrap()).unwrap());
        let persister = Arc::new(PersisterImpl::new(Arc::clone(&object_store) as _));
        let time_provider = Arc::new(MockProvider::new(Time::from_timestamp_nanos(0)));
        let executor = make_exec(object_store);
        let write_buffer = Arc::new(
            WriteBufferImpl::new(
                Arc::clone(&persister) as _,
                Option::<Arc<WalImpl>>::None,
                Arc::clone(&time_provider),
                SegmentDuration::new_5m(),
                Arc::clone(&executor),
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
    async fn system_parquet_files() {
        let (write_buffer, query_executor, time_provider) = setup().await;
        // Perform some writes to multiple tables
        let db_name = "test_db";
        let _ = write_buffer
            .write_lp(
                NamespaceName::new(db_name).unwrap(),
                "cpu,host=a,region=us-east usage=0.1 1\n\
                cpu,host=b,region=us-east usage=0.2 1\n\
                cpu,host=c,region=us-west usage=0.3 1\n\
                cpu,host=d,region=us-west usage=0.4 1\n\
                cpu,host=e,region=us-cent usage=0.5 1\n\
                cpu,host=f,region=us-cent usage=0.6 1\n\
                cpu,host=g,region=ca-east usage=0.7 1\n\
                cpu,host=h,region=ca-west usage=0.8 1\n\
                mem,host=a,region=us-east usage=1000 1\n\
                mem,host=b,region=us-east usage=2000 1\n\
                mem,host=c,region=us-west usage=3000 1\n\
                mem,host=d,region=us-west usage=4000 1\n\
                mem,host=e,region=us-cent usage=5000 1\n\
                mem,host=f,region=us-cent usage=6000 1\n\
                mem,host=g,region=ca-east usage=7000 1\n\
                mem,host=h,region=ca-west usage=8000 1\n\
                ",
                Time::from_timestamp_nanos(0),
                false,
                influxdb3_write::Precision::Nanosecond,
            )
            .await
            .unwrap();

        // Bump time to trick the persister into persisting to parquet:
        time_provider.set(Time::from_timestamp(60 * 10, 0).unwrap());

        let mut remaining_attempts = 10;
        let batches = loop {
            // wait for persister to do its thing:
            tokio::time::sleep(Duration::from_millis(500)).await;

            // query the system.parquet_files table
            let query = "SELECT * FROM system.parquet_files WHERE table_name = 'cpu'";
            let batch_stream = query_executor
                .query(db_name, query, None, crate::QueryKind::Sql, None, None)
                .await
                .unwrap();
            let batches: Vec<RecordBatch> = batch_stream.try_collect().await.unwrap();
            if batches.is_empty() {
                if remaining_attempts == 0 {
                    panic!("query never returned result, which means data never was persisted");
                } else {
                    remaining_attempts -= 1;
                    continue;
                }
            } else {
                break batches;
            }
        };
        assert_batches_sorted_eq!(
            [
                "+------------+-----------------------------------------------------+------------+-----------+----------+----------+",
                "| table_name | path                                                | size_bytes | row_count | min_time | max_time |",
                "+------------+-----------------------------------------------------+------------+-----------+----------+----------+",
                "| cpu        | dbs/test_db/cpu/1970-01-01T00-00/4294967294.parquet | 2265       | 8         | 1        | 1        |",
                "+------------+-----------------------------------------------------+------------+-----------+----------+----------+",
            ],
            &batches
        );
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
