use async_trait::async_trait;
use datafusion::arrow::error::ArrowError;
use datafusion::common::DataFusionError;
use datafusion::execution::SendableRecordBatchStream;
use iox_query::query_log::QueryLogEntries;
use iox_query::{QueryDatabase, QueryNamespace};
use iox_query_params::StatementParams;
use std::fmt::Debug;
use std::sync::Arc;
use trace::ctx::SpanContext;
use trace::span::Span;
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
}

#[async_trait]
pub trait QueryExecutor: QueryDatabase + Debug + Send + Sync + 'static {
    async fn query(
        &self,
        database: &str,
        q: &str,
        params: Option<StatementParams>,
        kind: QueryKind,
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

    fn upcast(&self) -> Arc<(dyn QueryDatabase + 'static)>;
}

#[derive(Debug, Clone, Copy)]
pub enum QueryKind {
    Sql,
    InfluxQl,
}

impl QueryKind {
    pub fn query_type(&self) -> &'static str {
        match self {
            Self::Sql => "sql",
            Self::InfluxQl => "influxql",
        }
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
    async fn query(
        &self,
        _database: &str,
        _q: &str,
        _params: Option<StatementParams>,
        _kind: QueryKind,
        _span_ctx: Option<SpanContext>,
        _external_span_ctx: Option<RequestLogContext>,
    ) -> Result<SendableRecordBatchStream, QueryExecutorError> {
        Err(QueryExecutorError::DatabaseNotFound {
            db_name: "unimplemented".to_string(),
        })
    }

    fn show_databases(
        &self,
        _include_deleted: bool,
    ) -> Result<SendableRecordBatchStream, QueryExecutorError> {
        Err(QueryExecutorError::DatabaseNotFound {
            db_name: "unimplemented".to_string(),
        })
    }

    async fn show_retention_policies(
        &self,
        _database: Option<&str>,
        _span_ctx: Option<SpanContext>,
    ) -> Result<SendableRecordBatchStream, QueryExecutorError> {
        Err(QueryExecutorError::DatabaseNotFound {
            db_name: "unimplemented".to_string(),
        })
    }

    fn upcast(&self) -> Arc<(dyn QueryDatabase + 'static)> {
        Arc::new(UnimplementedQueryExecutor) as _
    }
}
