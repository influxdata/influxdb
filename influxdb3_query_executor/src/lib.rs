//! module for query executor
mod query_planner;

use crate::query_planner::Planner;
use arrow::array::{ArrayRef, StringBuilder, StringViewBuilder, StructArray};
use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use arrow_array::{Array, BooleanArray};
use async_trait::async_trait;
use data_types::{Namespace, NamespaceId};
use datafusion::catalog::{CatalogProvider, SchemaProvider, Session};
use datafusion::common::arrow::array::StringArray;
use datafusion::common::arrow::datatypes::{DataType, Field, Schema as DatafusionSchema};
use datafusion::datasource::{TableProvider, TableType};
use datafusion::error::DataFusionError;
use datafusion::execution::SendableRecordBatchStream;
use datafusion::logical_expr::TableProviderFilterPushDown;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::prelude::Expr;
use datafusion_util::MemoryStream;
use datafusion_util::config::DEFAULT_SCHEMA;
use influxdb_influxql_parser::statement::Statement;
use influxdb3_cache::distinct_cache::{DISTINCT_CACHE_UDTF_NAME, DistinctCacheFunction};
use influxdb3_cache::last_cache::{LAST_CACHE_UDTF_NAME, LastCacheFunction};
use influxdb3_catalog::catalog::{Catalog, DatabaseSchema, TableDefinition};
use influxdb3_internal_api::query_executor::{QueryExecutor, QueryExecutorError};
use influxdb3_processing_engine::ProcessingEngineManagerImpl;
use influxdb3_sys_events::SysEventStore;
use influxdb3_system_tables::{
    AllSystemSchemaTablesProvider, SYSTEM_SCHEMA_NAME, SystemSchemaProvider,
};
use influxdb3_telemetry::store::TelemetryStore;
use influxdb3_write::{ChunkFilter, WriteBuffer};
use iox_query::QueryDatabase;
use iox_query::exec::{Executor, IOxSessionContext, QueryConfig};
use iox_query::provider::ProviderBuilder;
use iox_query::query_log::QueryLog;
use iox_query::query_log::QueryText;
use iox_query::query_log::StateReceived;
use iox_query::query_log::{QueryCompletedToken, QueryLogEntries};
use iox_query::{QueryChunk, QueryNamespace};
use iox_query_params::StatementParams;
use iox_time::TimeProvider;
use metric::Registry;
use observability_deps::tracing::{debug, info};
use std::any::Any;
use std::borrow::Cow;
use std::cmp::Ordering;
use std::collections::HashMap;
use std::fmt::Debug;
use std::sync::{Arc, RwLock};
use tokio::sync::Semaphore;
use trace::span::{Span, SpanRecorder};
use trace::{ctx::SpanContext, span::MetaValue};
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
    telemetry_store: Arc<TelemetryStore>,
    sys_events_store: Arc<SysEventStore>,
    started_with_auth: bool,
    processing_engine: Arc<RwLock<Option<Arc<ProcessingEngineManagerImpl>>>>,
}

impl Clone for QueryExecutorImpl {
    fn clone(&self) -> Self {
        Self {
            catalog: Arc::clone(&self.catalog),
            write_buffer: Arc::clone(&self.write_buffer),
            exec: Arc::clone(&self.exec),
            datafusion_config: Arc::clone(&self.datafusion_config),
            query_execution_semaphore: Arc::clone(&self.query_execution_semaphore),
            query_log: Arc::clone(&self.query_log),
            telemetry_store: Arc::clone(&self.telemetry_store),
            sys_events_store: Arc::clone(&self.sys_events_store),
            started_with_auth: self.started_with_auth,
            processing_engine: Arc::clone(&self.processing_engine),
        }
    }
}

/// Arguments for [`QueryExecutorImpl::new`]
#[derive(Debug)]
pub struct CreateQueryExecutorArgs {
    pub catalog: Arc<Catalog>,
    pub write_buffer: Arc<dyn WriteBuffer>,
    pub exec: Arc<Executor>,
    pub metrics: Arc<Registry>,
    pub time_provider: Arc<dyn TimeProvider>,
    pub datafusion_config: Arc<HashMap<String, String>>,
    pub query_log_size: usize,
    pub telemetry_store: Arc<TelemetryStore>,
    pub sys_events_store: Arc<SysEventStore>,
    pub started_with_auth: bool,
    pub processing_engine: Option<Arc<ProcessingEngineManagerImpl>>,
}

impl QueryExecutorImpl {
    pub fn new(
        CreateQueryExecutorArgs {
            catalog,
            write_buffer,
            exec,
            metrics,
            datafusion_config,
            query_log_size,
            telemetry_store,
            sys_events_store,
            started_with_auth,
            time_provider,
            processing_engine,
        }: CreateQueryExecutorArgs,
    ) -> Self {
        let semaphore_metrics = Arc::new(AsyncSemaphoreMetrics::new(
            &metrics,
            &[("semaphore", "query_execution")],
        ));
        let query_execution_semaphore =
            Arc::new(semaphore_metrics.new_semaphore(Semaphore::MAX_PERMITS));
        let query_log = Arc::new(QueryLog::new(query_log_size, time_provider, &metrics, None));
        Self {
            catalog,
            write_buffer,
            exec,
            datafusion_config,
            query_execution_semaphore,
            query_log,
            telemetry_store,
            sys_events_store,
            started_with_auth,
            processing_engine: Arc::new(RwLock::new(processing_engine)),
        }
    }

    pub fn set_processing_engine(&self, processing_engine: Arc<ProcessingEngineManagerImpl>) {
        *self.processing_engine.write().unwrap() = Some(processing_engine);
    }
}

#[async_trait]
impl QueryExecutor for QueryExecutorImpl {
    async fn query_sql(
        &self,
        database: &str,
        query: &str,
        params: Option<StatementParams>,
        span_ctx: Option<SpanContext>,
        external_span_ctx: Option<RequestLogContext>,
    ) -> Result<SendableRecordBatchStream, QueryExecutorError> {
        info!(%database, %query, ?params, "executing sql query");
        let db = self.get_db_namespace(database, &span_ctx).await?;
        let span_ctxt = span_ctx
            .clone()
            .map(|span| span.child("query_database_sql"));
        let mut recorder = SpanRecorder::new(span_ctxt);
        recorder.set_metadata("query", MetaValue::String(query.to_string().into()));

        query_database_sql(
            db,
            query,
            params,
            span_ctx,
            external_span_ctx,
            Arc::clone(&self.telemetry_store),
        )
        .await
    }

    async fn query_influxql(
        &self,
        database: &str,
        query: &str,
        influxql_statement: Statement,
        params: Option<StatementParams>,
        span_ctx: Option<SpanContext>,
        external_span_ctx: Option<RequestLogContext>,
    ) -> Result<SendableRecordBatchStream, QueryExecutorError> {
        info!(database, query, ?params, "executing influxql query");
        let db = self.get_db_namespace(database, &span_ctx).await?;
        query_database_influxql(
            db,
            query,
            influxql_statement,
            params,
            span_ctx,
            external_span_ctx,
            Arc::clone(&self.telemetry_store),
        )
        .await
    }

    fn show_databases(
        &self,
        include_deleted: bool,
    ) -> Result<SendableRecordBatchStream, QueryExecutorError> {
        let mut databases = self.catalog.list_db_schema();
        // sort them to ensure consistent order, first by deleted, then by name:
        databases.sort_unstable_by(|a, b| match a.deleted.cmp(&b.deleted) {
            Ordering::Equal => a.name.cmp(&b.name),
            ordering => ordering,
        });
        if !include_deleted {
            databases.retain(|db| !db.deleted);
        }
        let mut fields = Vec::with_capacity(2);
        fields.push(Field::new("iox::database", DataType::Utf8, false));
        let mut arrays = Vec::with_capacity(2);
        let names: StringArray = databases
            .iter()
            .map(|db| db.name.as_ref())
            .collect::<Vec<&str>>()
            .into();
        let names = Arc::new(names) as Arc<dyn Array>;
        arrays.push(names);
        if include_deleted {
            fields.push(Field::new("deleted", DataType::Boolean, false));
            let deleted: BooleanArray = databases
                .iter()
                .map(|db| db.deleted)
                .collect::<Vec<bool>>()
                .into();
            let deleted = Arc::new(deleted);
            arrays.push(deleted);
        }
        let schema = DatafusionSchema::new(fields);
        let batch = RecordBatch::try_new(Arc::new(schema), arrays)
            .map_err(QueryExecutorError::DatabasesToRecordBatch)?;
        Ok(Box::pin(MemoryStream::new(vec![batch])))
    }

    async fn show_retention_policies(
        &self,
        database: Option<&str>,
        _span_ctx: Option<SpanContext>,
    ) -> Result<SendableRecordBatchStream, QueryExecutorError> {
        let mut databases = if let Some(db) = database {
            vec![db.to_owned()]
        } else {
            self.catalog.db_names()
        };
        // sort them to ensure consistent order:
        databases.sort_unstable();

        let mut rows = Vec::with_capacity(databases.len());
        for database in databases {
            let db_schema = self.catalog.db_schema(&database).ok_or_else(|| {
                QueryExecutorError::DatabaseNotFound {
                    db_name: database.to_string(),
                }
            })?;
            let duration = db_schema.retention_period.format_v1();
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

    fn upcast(&self) -> Arc<dyn QueryDatabase + 'static> {
        // NB: This clone is required to get compiler to be happy
        //     to convert `self` to dyn QueryDatabase. This wasn't
        //     possible without getting owned value of self.
        //     TODO: see if this clone can be removed satisfying
        //           grpc setup in `make_flight_server`
        let cloned_self = (*self).clone();
        Arc::new(cloned_self) as _
    }
}

// NOTE: this method is separated out as it is called from a separate query executor
// implementation in Enterprise
async fn query_database_sql(
    db: Arc<dyn QueryNamespace>,
    query: &str,
    params: Option<StatementParams>,
    span_ctx: Option<SpanContext>,
    external_span_ctx: Option<RequestLogContext>,
    telemetry_store: Arc<TelemetryStore>,
) -> Result<SendableRecordBatchStream, QueryExecutorError> {
    let params = params.unwrap_or_default();

    let token = db.record_query(
        external_span_ctx.as_ref().map(RequestLogContext::ctx),
        "sql",
        Box::new(query.to_string()),
        params.clone(),
        // NB: do we need to provide an auth ID?
        None,
    );

    // NOTE - we use the default query configuration on the IOxSessionContext here:
    let ctx = db.new_query_context(span_ctx, Default::default());
    let planner = Planner::new(&ctx);
    let query = query.to_string();

    // Perform query planning on a separate threadpool than the IO runtime that is servicing
    // this request by using `IOxSessionContext::run`.
    let plan = ctx
        .run(async move { planner.sql(query, params).await })
        .await;

    let plan = match plan.map_err(QueryExecutorError::QueryPlanning) {
        Ok(plan) => plan,
        Err(e) => {
            token.fail();
            return Err(e);
        }
    };
    let token = token.planned(&ctx, Arc::clone(&plan));

    // TODO: Enforce concurrency limit here
    let token = token.permit();

    telemetry_store.update_num_queries();

    match ctx.execute_stream(Arc::clone(&plan)).await {
        Ok(query_results) => {
            token.success();
            Ok(query_results)
        }
        Err(err) => {
            token.fail();
            Err(QueryExecutorError::ExecuteStream(err))
        }
    }
}

async fn query_database_influxql(
    db: Arc<dyn QueryNamespace>,
    query_str: &str,
    statement: Statement,
    params: Option<StatementParams>,
    span_ctx: Option<SpanContext>,
    external_span_ctx: Option<RequestLogContext>,
    telemetry_store: Arc<TelemetryStore>,
) -> Result<SendableRecordBatchStream, QueryExecutorError> {
    let params = params.unwrap_or_default();
    let token = db.record_query(
        external_span_ctx.as_ref().map(RequestLogContext::ctx),
        "influxql",
        Box::new(query_str.to_string()),
        params.clone(),
        // NB: do we need to provide an auth ID?
        None,
    );

    let ctx = db.new_query_context(span_ctx, Default::default());
    let planner = Planner::new(&ctx);
    let plan = ctx
        .run(async move { planner.influxql(statement, params).await })
        .await;

    let plan = match plan.map_err(QueryExecutorError::QueryPlanning) {
        Ok(plan) => plan,
        Err(e) => {
            token.fail();
            return Err(e);
        }
    };

    let token = token.planned(&ctx, Arc::clone(&plan));

    let token = token.permit();

    telemetry_store.update_num_queries();

    match ctx.execute_stream(Arc::clone(&plan)).await {
        Ok(query_results) => {
            token.success();
            Ok(query_results)
        }
        Err(err) => {
            token.fail();
            Err(QueryExecutorError::ExecuteStream(err))
        }
    }
}

#[derive(Debug)]
struct RetentionPolicyRow {
    database: String,
    name: String,
    duration: Cow<'static, str>,
}

#[derive(Debug, Default)]
struct RetentionPolicyRowBuilder {
    database: StringBuilder,
    name: StringBuilder,
    duration: StringViewBuilder,
}

impl RetentionPolicyRowBuilder {
    fn append(&mut self, row: &RetentionPolicyRow) {
        self.database.append_value(row.database.as_str());
        self.name.append_value(row.name.as_str());
        self.duration.append_value(row.duration.as_ref());
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
                Arc::new(Field::new("duration", DataType::Utf8View, true)),
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
            DataFusionError::External(Box::new(QueryExecutorError::DatabaseNotFound {
                db_name: name.into(),
            }))
        })?;
        let system_schema_provider = Arc::new(SystemSchemaProvider::AllSystemSchemaTables(
            AllSystemSchemaTablesProvider::new(
                Arc::clone(&db_schema),
                Arc::clone(&self.query_log),
                Arc::clone(&self.write_buffer),
                Arc::clone(&self.sys_events_store),
                Arc::clone(&self.write_buffer.catalog()),
                self.started_with_auth,
                self.processing_engine.read().unwrap().clone(),
            ),
        ));
        Ok(Some(Arc::new(Database::new(CreateDatabaseArgs {
            db_schema,
            write_buffer: Arc::clone(&self.write_buffer),
            exec: Arc::clone(&self.exec),
            datafusion_config: Arc::clone(&self.datafusion_config),
            query_log: Arc::clone(&self.query_log),
            system_schema_provider,
        }))))
    }

    async fn list_namespaces(
        &self,
        _span: Option<Span>,
    ) -> Result<Vec<Namespace>, DataFusionError> {
        Ok(self.catalog.list_namespaces())
    }

    async fn acquire_semaphore(&self, span: Option<Span>) -> InstrumentedAsyncOwnedSemaphorePermit {
        acquire_semaphore(Arc::clone(&self.query_execution_semaphore), span).await
    }

    fn query_log(&self) -> QueryLogEntries {
        self.query_log.entries()
    }
}

async fn acquire_semaphore(
    semaphore: Arc<InstrumentedAsyncSemaphore>,
    span: Option<Span>,
) -> InstrumentedAsyncOwnedSemaphorePermit {
    semaphore
        .acquire_owned(span)
        .await
        .expect("Semaphore should not be closed by anyone")
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

/// Arguments for [`Database::new`]
#[derive(Debug)]
pub struct CreateDatabaseArgs {
    db_schema: Arc<DatabaseSchema>,
    write_buffer: Arc<dyn WriteBuffer>,
    exec: Arc<Executor>,
    datafusion_config: Arc<HashMap<String, String>>,
    query_log: Arc<QueryLog>,
    system_schema_provider: Arc<SystemSchemaProvider>,
}

impl Database {
    pub fn new(
        CreateDatabaseArgs {
            db_schema,
            write_buffer,
            exec,
            datafusion_config,
            query_log,
            system_schema_provider,
        }: CreateDatabaseArgs,
    ) -> Self {
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
        let table_name: Arc<str> = table_name.into();
        self.db_schema
            .table_definition(Arc::clone(&table_name))
            .map(|table_def| {
                Arc::new(QueryTable {
                    db_schema: Arc::clone(&self.db_schema),
                    table_def,
                    write_buffer: Arc::clone(&self.write_buffer),
                })
            })
    }
}

#[async_trait]
impl QueryNamespace for Database {
    fn retention_time_ns(&self) -> Option<i64> {
        None
    }

    fn record_query(
        &self,
        span_ctx: Option<&SpanContext>,
        query_type: &'static str,
        query_text: QueryText,
        query_params: StatementParams,
        auth_id: Option<String>,
    ) -> QueryCompletedToken<StateReceived> {
        let trace_id = span_ctx.map(|ctx| ctx.trace_id);
        let namespace_id = NamespaceId::new(self.db_schema.id.get().into());
        let namespace_name = self.db_schema.name();
        self.query_log.push(
            namespace_id,
            namespace_name,
            query_type,
            query_text,
            query_params,
            auth_id,
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
                self.db_schema.id,
                self.write_buffer.last_cache_provider(),
            )),
        );
        ctx.inner().register_udtf(
            DISTINCT_CACHE_UDTF_NAME,
            Arc::new(DistinctCacheFunction::new(
                self.db_schema.id,
                self.write_buffer.distinct_cache_provider(),
            )),
        );
        ctx
    }

    fn new_extended_query_context(
        &self,
        _extension: std::option::Option<std::sync::Arc<dyn iox_query::Extension + 'static>>,
        _span_ctx: Option<SpanContext>,
        _query_config: Option<&QueryConfig>,
    ) -> IOxSessionContext {
        unimplemented!();
    }
}

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
        let mut names = self
            .db_schema
            .table_names()
            .iter()
            .map(|t| t.to_string())
            .collect::<Vec<_>>();
        names.sort();
        names
    }

    async fn table(
        &self,
        table_name: &str,
    ) -> Result<Option<Arc<dyn TableProvider>>, DataFusionError> {
        Ok(self.query_table(table_name).await.map(|qt| qt as _))
    }

    fn table_exist(&self, name: &str) -> bool {
        self.db_schema.table_name_to_id(name).is_some()
    }
}

#[derive(Debug)]
pub struct QueryTable {
    db_schema: Arc<DatabaseSchema>,
    table_def: Arc<TableDefinition>,
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
        let mut buffer_filter = ChunkFilter::new(&self.table_def, filters)
            .map_err(|error| DataFusionError::External(Box::new(error)))?;

        let catalog = self.write_buffer.catalog();
        let retention_period_cutoff = match self
            .db_schema
            .get_retention_period_cutoff_ts_nanos(catalog.time_provider())
        {
            Some(time) => time,
            None => {
                return self.write_buffer.get_table_chunks(
                    Arc::clone(&self.db_schema),
                    Arc::clone(&self.table_def),
                    &buffer_filter,
                    projection,
                    ctx,
                );
            }
        };

        buffer_filter.time_lower_bound_ns = buffer_filter
            .time_lower_bound_ns
            .map(|lb| lb.max(retention_period_cutoff))
            .or(Some(retention_period_cutoff));

        self.write_buffer.get_table_chunks(
            Arc::clone(&self.db_schema),
            Arc::clone(&self.table_def),
            &buffer_filter,
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
        self.table_def.schema.as_arrow()
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
        let mut builder = ProviderBuilder::new(
            Arc::clone(&self.table_def.table_name),
            self.table_def.schema.clone(),
        );

        let chunks = self.chunks(ctx, projection, &filters, limit)?;
        for chunk in chunks {
            builder = builder.add_chunk(chunk);
        }

        // NOTE: this build method is, at time of writing, infallible, but handle the error anyway.
        let provider = builder
            .build()
            .map_err(|e| DataFusionError::Internal(format!("unexpected error: {e:?}")))?;

        provider.scan(ctx, projection, &filters, limit).await
    }
}

#[cfg(test)]
mod tests;
