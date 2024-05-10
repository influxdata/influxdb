//! module for query executor
use crate::{QueryExecutor, QueryKind};
use arrow::array::{ArrayRef, Int64Builder, StringBuilder, StructArray};
use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use arrow_schema::ArrowError;
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
use datafusion::logical_expr::TableProviderFilterPushDown;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::prelude::Expr;
use datafusion_util::config::DEFAULT_SCHEMA;
use datafusion_util::MemoryStream;
use influxdb3_write::{
    catalog::{Catalog, DatabaseSchema},
    WriteBuffer,
};
use iox_query::exec::{Executor, IOxSessionContext, QueryConfig};
use iox_query::frontend::sql::SqlQueryPlanner;
use iox_query::provider::ProviderBuilder;
use iox_query::query_log::QueryLog;
use iox_query::query_log::QueryText;
use iox_query::query_log::StateReceived;
use iox_query::query_log::{QueryCompletedToken, QueryLogEntries};
use iox_query::QueryDatabase;
use iox_query::{QueryChunk, QueryNamespace};
use iox_query_influxql::frontend::planner::InfluxQLQueryPlanner;
use iox_query_params::StatementParams;
use metric::Registry;
use observability_deps::tracing::{debug, info, trace};
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
    ) -> Self {
        let semaphore_metrics = Arc::new(AsyncSemaphoreMetrics::new(
            &metrics,
            &[("semaphore", "query_execution")],
        ));
        let query_execution_semaphore =
            Arc::new(semaphore_metrics.new_semaphore(concurrent_query_limit));
        // TODO Fine tune this number or make configurable
        const QUERY_LOG_LIMIT: usize = 1_000;
        let query_log = Arc::new(QueryLog::new(
            QUERY_LOG_LIMIT,
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

        let token = db.record_query(
            external_span_ctx.as_ref().map(RequestLogContext::ctx),
            "sql",
            Box::new(q.to_string()),
            // TODO - ignoring params for now:
            StatementParams::default(),
        );

        info!("plan");
        let params = params.unwrap_or_default();
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
        .map_err(Error::QueryPlanning)?;
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
        include_debug_info_tables: bool,
    ) -> Result<Option<Arc<dyn QueryNamespace>>, DataFusionError> {
        let _span_recorder = SpanRecorder::new(span);

        let db_schema = self.catalog.db_schema(name).ok_or_else(|| {
            DataFusionError::External(Box::new(Error::DatabaseNotFound {
                db_name: name.into(),
            }))
        })?;

        Ok(Some(Arc::new(Database::new(
            db_schema,
            Arc::clone(&self.write_buffer) as _,
            Arc::clone(&self.exec),
            Arc::clone(&self.datafusion_config),
            Arc::clone(&self.query_log),
            include_debug_info_tables,
        ))))
    }

    async fn acquire_semaphore(&self, span: Option<Span>) -> InstrumentedAsyncOwnedSemaphorePermit {
        Arc::clone(&self.query_execution_semaphore)
            .acquire_owned(span)
            .await
            .expect("Semaphore should not be closed by anyone")
    }

    fn query_log(&self) -> QueryLogEntries {
        todo!();
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
    pub fn new(
        db_schema: Arc<DatabaseSchema>,
        write_buffer: Arc<B>,
        exec: Arc<Executor>,
        datafusion_config: Arc<HashMap<String, String>>,
        query_log: Arc<QueryLog>,
        include_debug_info_tables: bool,
    ) -> Self {
        let system_schema_provider = Arc::new(SystemSchemaProvider::new(
            write_buffer.catalog(),
            Arc::clone(&query_log),
            include_debug_info_tables,
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
            .with_default_catalog(Arc::new(Database::from_namespace(self)))
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
        vec![DEFAULT_SCHEMA.to_string()]
    }

    fn schema(&self, name: &str) -> Option<Arc<dyn SchemaProvider>> {
        info!("CatalogProvider schema {}", name);
        match name {
            DEFAULT_SCHEMA => Some(Arc::new(Database::from_namespace(self))),
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

const _QUERIES_TABLE: &str = "queries";
const _PARQUET_FILES_TABLE: &str = "parquet_files";

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
    fn new(_catalog: Arc<Catalog>, _query_log: Arc<QueryLog>, include_debug_info: bool) -> Self {
        let tables = HashMap::new();
        if include_debug_info {
            // Using todo!() here causes gRPC integration tests to fail, likely because they
            // enable debug mode by default, thus entering this if block. So, just leaving this
            // here in lieu of todo!().
            //
            // Eventually, we will implement the queries and parquet_files tables and they will
            // be injected to the provider's table hashmap here...
            info!("TODO - gather system tables");
        }
        Self { tables }
    }
}
