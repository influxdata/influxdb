//! module for query executor
use crate::QueryExecutor;
use arrow::datatypes::SchemaRef;
use async_trait::async_trait;
use data_types::NamespaceId;
use data_types::{ChunkId, ChunkOrder, TransitionPartitionId};
use datafusion::catalog::schema::SchemaProvider;
use datafusion::catalog::CatalogProvider;
use datafusion::common::ParamValues;
use datafusion::common::Statistics;
use datafusion::datasource::{TableProvider, TableType};
use datafusion::error::DataFusionError;
use datafusion::execution::context::SessionState;
use datafusion::execution::SendableRecordBatchStream;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::prelude::Expr;
use datafusion_util::config::DEFAULT_SCHEMA;
use influxdb3_write::{
    catalog::{Catalog, DatabaseSchema},
    WriteBuffer,
};
use iox_query::exec::{Executor, ExecutorType, IOxSessionContext};
use iox_query::frontend::sql::SqlQueryPlanner;
use iox_query::provider::ProviderBuilder;
use iox_query::query_log::QueryCompletedToken;
use iox_query::query_log::QueryLog;
use iox_query::query_log::QueryText;
use iox_query::query_log::StateReceived;
use iox_query::QueryNamespaceProvider;
use iox_query::{QueryChunk, QueryChunkData, QueryNamespace};
use metric::Registry;
use observability_deps::tracing::info;
use schema::sort::SortKey;
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
        Self {
            catalog,
            write_buffer,
            exec,
            datafusion_config,
            query_execution_semaphore,
        }
    }
}

#[async_trait]
impl<W: WriteBuffer> QueryExecutor for QueryExecutorImpl<W> {
    async fn query(
        &self,
        database: &str,
        q: &str,
        span_ctx: Option<SpanContext>,
        external_span_ctx: Option<RequestLogContext>,
    ) -> crate::Result<SendableRecordBatchStream> {
        info!("query in executor {}", database);
        let db = self
            .db(database, span_ctx.child_span("get database"), false)
            .await
            .ok_or_else(|| crate::Error::DatabaseNotFound {
                db_name: database.to_string(),
            })?;

        let ctx = db.new_query_context(span_ctx);

        let token = db.record_query(
            external_span_ctx.as_ref().map(RequestLogContext::ctx),
            "sql",
            Box::new(q.to_string()),
        );

        info!("plan");
        let planner = SqlQueryPlanner::new();
        // TODO: Figure out if we want to support parameter values in SQL
        // queries
        let params = ParamValues::List(Vec::new());
        let plan = planner.query(q, params, &ctx).await?;
        let token = token.planned(Arc::clone(&plan));

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
                Err(err.into())
            }
        }
    }
}

// This implementation is for the Flight service
#[async_trait]
impl<W: WriteBuffer> QueryNamespaceProvider for QueryExecutorImpl<W> {
    async fn db(
        &self,
        name: &str,
        span: Option<Span>,
        _include_debug_info_tables: bool,
    ) -> Option<Arc<dyn QueryNamespace>> {
        let _span_recorder = SpanRecorder::new(span);

        let db_schema = self.catalog.db_schema(name)?;

        Some(Arc::new(QueryDatabase::new(
            db_schema,
            Arc::clone(&self.write_buffer) as _,
            Arc::clone(&self.exec),
            Arc::clone(&self.datafusion_config),
        )))
    }

    async fn acquire_semaphore(&self, span: Option<Span>) -> InstrumentedAsyncOwnedSemaphorePermit {
        Arc::clone(&self.query_execution_semaphore)
            .acquire_owned(span)
            .await
            .expect("Semaphore should not be closed by anyone")
    }
}

#[derive(Debug)]
pub struct QueryDatabase<B> {
    db_schema: Arc<DatabaseSchema>,
    write_buffer: Arc<B>,
    exec: Arc<Executor>,
    datafusion_config: Arc<HashMap<String, String>>,
    query_log: Arc<QueryLog>,
}

impl<B: WriteBuffer> QueryDatabase<B> {
    pub fn new(
        db_schema: Arc<DatabaseSchema>,
        write_buffer: Arc<B>,
        exec: Arc<Executor>,
        datafusion_config: Arc<HashMap<String, String>>,
    ) -> Self {
        // TODO Fine tune this number
        const QUERY_LOG_LIMIT: usize = 10;

        let query_log = Arc::new(QueryLog::new(
            QUERY_LOG_LIMIT,
            Arc::new(iox_time::SystemProvider::new()),
        ));
        Self {
            db_schema,
            write_buffer,
            exec,
            datafusion_config,
            query_log,
        }
    }
}

#[async_trait]
impl<B: WriteBuffer> QueryNamespace for QueryDatabase<B> {
    async fn chunks(
        &self,
        _table_name: &str,
        _filters: &[Expr],
        _projection: Option<&Vec<usize>>,
        _ctx: IOxSessionContext,
    ) -> Result<Vec<Arc<dyn QueryChunk>>, DataFusionError> {
        info!("called chunks on querydatabase");
        todo!()
    }

    fn retention_time_ns(&self) -> Option<i64> {
        None
    }

    fn record_query(
        &self,
        span_ctx: Option<&SpanContext>,
        query_type: &'static str,
        query_text: QueryText,
    ) -> QueryCompletedToken<StateReceived> {
        let trace_id = span_ctx.map(|ctx| ctx.trace_id);
        let namespace_name: Arc<str> = Arc::from("influxdb3 edge");
        self.query_log.push(
            NamespaceId::new(0),
            namespace_name,
            query_type,
            query_text,
            trace_id,
        )
    }

    fn new_query_context(&self, span_ctx: Option<SpanContext>) -> IOxSessionContext {
        let qdb = Self::new(
            Arc::clone(&self.db_schema),
            Arc::clone(&self.write_buffer),
            Arc::clone(&self.exec),
            Arc::clone(&self.datafusion_config),
        );

        let mut cfg = self
            .exec
            .new_execution_config(ExecutorType::Query)
            .with_default_catalog(Arc::new(qdb))
            .with_span_context(span_ctx);

        for (k, v) in self.datafusion_config.as_ref() {
            cfg = cfg.with_config_option(k, v);
        }

        cfg.build()
    }
}

impl<B: WriteBuffer> CatalogProvider for QueryDatabase<B> {
    fn as_any(&self) -> &dyn Any {
        self as &dyn Any
    }

    fn schema_names(&self) -> Vec<String> {
        info!("CatalogProvider schema_names");
        vec![DEFAULT_SCHEMA.to_string()]
    }

    fn schema(&self, name: &str) -> Option<Arc<dyn SchemaProvider>> {
        info!("CatalogProvider schema {}", name);
        let qdb = Self::new(
            Arc::clone(&self.db_schema),
            Arc::clone(&self.write_buffer),
            Arc::clone(&self.exec),
            Arc::clone(&self.datafusion_config),
        );

        match name {
            DEFAULT_SCHEMA => Some(Arc::new(qdb)),
            _ => None,
        }
    }
}

#[async_trait]
impl<B: WriteBuffer> SchemaProvider for QueryDatabase<B> {
    fn as_any(&self) -> &dyn Any {
        self as &dyn Any
    }

    fn table_names(&self) -> Vec<String> {
        self.db_schema.table_names()
    }

    async fn table(&self, name: &str) -> Option<Arc<dyn TableProvider>> {
        info!("table {}", name);

        let schema = self.db_schema.get_table_schema(name).unwrap();

        info!("return QueryTable");
        let name: Arc<str> = name.into();
        Some(Arc::new(QueryTable {
            db_schema: Arc::clone(&self.db_schema),
            name,
            schema,
            write_buffer: Arc::clone(&self.write_buffer),
        }))
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

#[derive(Debug)]
pub struct ParquetChunk {}

impl QueryChunk for ParquetChunk {
    fn stats(&self) -> Arc<Statistics> {
        todo!()
    }

    fn schema(&self) -> &Schema {
        todo!()
    }

    fn partition_id(&self) -> &TransitionPartitionId {
        todo!()
    }

    fn sort_key(&self) -> Option<&SortKey> {
        todo!()
    }

    fn id(&self) -> ChunkId {
        todo!()
    }

    fn may_contain_pk_duplicates(&self) -> bool {
        todo!()
    }

    fn data(&self) -> QueryChunkData {
        todo!()
    }

    fn chunk_type(&self) -> &str {
        todo!()
    }

    fn order(&self) -> ChunkOrder {
        todo!()
    }

    fn as_any(&self) -> &dyn Any {
        todo!()
    }
}
