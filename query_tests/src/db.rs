use std::{any::Any, fmt::Debug, sync::Arc};

use async_trait::async_trait;
use datafusion::catalog::catalog::CatalogProvider;
use db::Db;
use predicate::rpc_predicate::QueryDatabaseMeta;
use query::{
    exec::{ExecutionContextProvider, IOxExecutionContext},
    QueryChunk, QueryDatabase,
};

use self::sealed::AbstractDbInterface;

/// Abstract database used during testing.
///
/// This is required to make all the involved traits object-safe.
#[derive(Debug)]
pub struct AbstractDb(Box<dyn AbstractDbInterface>);

impl AbstractDb {
    pub fn create_old(db: Arc<Db>) -> Self {
        Self(Box::new(OldDb(db)))
    }

    pub fn old_db(&self) -> Option<Arc<Db>> {
        self.0
            .as_any()
            .downcast_ref::<OldDb>()
            .map(|o| Arc::clone(&o.0))
    }
}

impl ExecutionContextProvider for AbstractDb {
    fn new_query_context(&self, span_ctx: Option<trace::ctx::SpanContext>) -> IOxExecutionContext {
        self.0.new_query_context(span_ctx)
    }
}

impl CatalogProvider for AbstractDb {
    fn as_any(&self) -> &dyn Any {
        self as &dyn Any
    }

    fn schema_names(&self) -> Vec<String> {
        self.0.catalog_provider().schema_names()
    }

    fn schema(&self, name: &str) -> Option<Arc<dyn datafusion::catalog::schema::SchemaProvider>> {
        self.0.catalog_provider().schema(name)
    }
}

#[async_trait]
impl QueryDatabase for AbstractDb {
    async fn chunks(
        &self,
        table_name: &str,
        predicate: &predicate::Predicate,
    ) -> Vec<Arc<dyn QueryChunk>> {
        self.0.chunks(table_name, predicate).await
    }

    fn record_query(
        &self,
        ctx: &IOxExecutionContext,
        query_type: &str,
        query_text: query::QueryText,
    ) -> query::QueryCompletedToken {
        self.0.record_query(ctx, query_type, query_text)
    }
}

impl QueryDatabaseMeta for AbstractDb {
    fn table_names(&self) -> Vec<String> {
        self.0.table_names()
    }

    fn table_schema(&self, table_name: &str) -> Option<Arc<schema::Schema>> {
        self.0.table_schema(table_name)
    }
}

mod sealed {
    use super::*;

    #[async_trait]
    pub trait AbstractDbInterface: Debug + Send + Sync + 'static {
        fn as_any(&self) -> &dyn Any;

        fn new_query_context(
            &self,
            span_ctx: Option<trace::ctx::SpanContext>,
        ) -> IOxExecutionContext;

        fn catalog_provider(&self) -> Arc<dyn CatalogProvider>;

        async fn chunks(
            &self,
            table_name: &str,
            predicate: &predicate::Predicate,
        ) -> Vec<Arc<dyn QueryChunk>>;

        fn record_query(
            &self,
            ctx: &IOxExecutionContext,
            query_type: &str,
            query_text: query::QueryText,
        ) -> query::QueryCompletedToken;

        fn table_names(&self) -> Vec<String>;

        fn table_schema(&self, table_name: &str) -> Option<Arc<schema::Schema>>;
    }
}

#[derive(Debug)]
struct OldDb(Arc<Db>);

#[async_trait]
impl AbstractDbInterface for OldDb {
    fn as_any(&self) -> &dyn Any {
        self as &dyn Any
    }

    fn new_query_context(&self, span_ctx: Option<trace::ctx::SpanContext>) -> IOxExecutionContext {
        self.0.new_query_context(span_ctx)
    }

    fn catalog_provider(&self) -> Arc<dyn CatalogProvider> {
        Arc::clone(&self.0) as _
    }

    async fn chunks(
        &self,
        table_name: &str,
        predicate: &predicate::Predicate,
    ) -> Vec<Arc<dyn QueryChunk>> {
        self.0.chunks(table_name, predicate).await
    }

    fn record_query(
        &self,
        ctx: &IOxExecutionContext,
        query_type: &str,
        query_text: query::QueryText,
    ) -> query::QueryCompletedToken {
        self.0.record_query(ctx, query_type, query_text)
    }

    fn table_names(&self) -> Vec<String> {
        self.0.table_names()
    }

    fn table_schema(&self, table_name: &str) -> Option<Arc<schema::Schema>> {
        self.0.table_schema(table_name)
    }
}
