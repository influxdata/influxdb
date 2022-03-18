use std::{
    any::Any,
    fmt::{Debug, Display},
    sync::Arc,
};

use async_trait::async_trait;
use data_types::chunk_metadata::{ChunkAddr, ChunkId, ChunkOrder};
use datafusion::catalog::catalog::CatalogProvider;
use db::{chunk::DbChunk, Db};
use predicate::rpc_predicate::QueryDatabaseMeta;
use query::{
    exec::{ExecutionContextProvider, IOxExecutionContext},
    QueryChunk, QueryChunkMeta, QueryDatabase,
};

use self::sealed::{AbstractChunkInterface, AbstractDbInterface};

pub struct Error(String);

impl Error {
    fn new<E>(e: E) -> Self
    where
        E: std::error::Error,
    {
        Self(e.to_string())
    }
}

impl Debug for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("Error").field(&self.0).finish()
    }
}

impl Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Display::fmt(&self.0, f)
    }
}

impl std::error::Error for Error {}

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
    fn new_query_context(
        self: &Arc<Self>,
        span_ctx: Option<trace::ctx::SpanContext>,
    ) -> IOxExecutionContext {
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
    type Chunk = AbstractChunk;

    async fn chunks(
        &self,
        table_name: &str,
        predicate: &predicate::Predicate,
    ) -> Vec<Arc<Self::Chunk>> {
        self.0.chunks(table_name, predicate).await
    }

    fn record_query(
        &self,
        ctx: &IOxExecutionContext,
        query_type: impl Into<String>,
        query_text: query::QueryText,
    ) -> query::QueryCompletedToken {
        self.0.record_query(ctx, query_type.into(), query_text)
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

#[derive(Debug)]
pub struct AbstractChunk(Box<dyn AbstractChunkInterface>);

impl AbstractChunk {
    fn create_old(chunk: Arc<DbChunk>) -> Self {
        Self(Box::new(OldChunk(chunk)))
    }
}

impl QueryChunk for AbstractChunk {
    type Error = Error;

    fn id(&self) -> ChunkId {
        self.0.id()
    }

    fn addr(&self) -> ChunkAddr {
        self.0.addr()
    }

    fn table_name(&self) -> &str {
        self.0.table_name()
    }

    fn may_contain_pk_duplicates(&self) -> bool {
        self.0.may_contain_pk_duplicates()
    }

    fn apply_predicate_to_metadata(
        &self,
        predicate: &predicate::Predicate,
    ) -> Result<predicate::PredicateMatch, Self::Error> {
        self.0.apply_predicate_to_metadata(predicate)
    }

    fn column_names(
        &self,
        ctx: IOxExecutionContext,
        predicate: &predicate::Predicate,
        columns: schema::selection::Selection<'_>,
    ) -> Result<Option<query::exec::stringset::StringSet>, Self::Error> {
        self.0.column_names(ctx, predicate, columns)
    }

    fn column_values(
        &self,
        ctx: IOxExecutionContext,
        column_name: &str,
        predicate: &predicate::Predicate,
    ) -> Result<Option<query::exec::stringset::StringSet>, Self::Error> {
        self.0.column_values(ctx, column_name, predicate)
    }

    fn read_filter(
        &self,
        ctx: IOxExecutionContext,
        predicate: &predicate::Predicate,
        selection: schema::selection::Selection<'_>,
    ) -> Result<datafusion::physical_plan::SendableRecordBatchStream, Self::Error> {
        self.0.read_filter(ctx, predicate, selection)
    }

    fn chunk_type(&self) -> &str {
        self.0.chunk_type()
    }

    fn order(&self) -> ChunkOrder {
        self.0.order()
    }
}

impl QueryChunkMeta for AbstractChunk {
    fn summary(&self) -> Option<&data_types::partition_metadata::TableSummary> {
        self.0.summary()
    }

    fn schema(&self) -> Arc<schema::Schema> {
        self.0.schema()
    }

    fn sort_key(&self) -> Option<&schema::sort::SortKey> {
        self.0.sort_key()
    }

    fn delete_predicates(&self) -> &[Arc<data_types::delete_predicate::DeletePredicate>] {
        self.0.delete_predicates()
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
        ) -> Vec<Arc<AbstractChunk>>;

        fn record_query(
            &self,
            ctx: &IOxExecutionContext,
            query_type: String,
            query_text: query::QueryText,
        ) -> query::QueryCompletedToken;

        fn table_names(&self) -> Vec<String>;

        fn table_schema(&self, table_name: &str) -> Option<Arc<schema::Schema>>;
    }

    pub trait AbstractChunkInterface: Debug + Send + Sync + 'static {
        fn id(&self) -> ChunkId;

        fn addr(&self) -> ChunkAddr;

        fn table_name(&self) -> &str;

        fn may_contain_pk_duplicates(&self) -> bool;

        fn apply_predicate_to_metadata(
            &self,
            predicate: &predicate::Predicate,
        ) -> Result<predicate::PredicateMatch, Error>;

        fn column_names(
            &self,
            ctx: IOxExecutionContext,
            predicate: &predicate::Predicate,
            columns: schema::selection::Selection<'_>,
        ) -> Result<Option<query::exec::stringset::StringSet>, Error>;

        fn column_values(
            &self,
            ctx: IOxExecutionContext,
            column_name: &str,
            predicate: &predicate::Predicate,
        ) -> Result<Option<query::exec::stringset::StringSet>, Error>;

        fn read_filter(
            &self,
            ctx: IOxExecutionContext,
            predicate: &predicate::Predicate,
            selection: schema::selection::Selection<'_>,
        ) -> Result<datafusion::physical_plan::SendableRecordBatchStream, Error>;

        fn chunk_type(&self) -> &str;

        fn order(&self) -> ChunkOrder;

        fn summary(&self) -> Option<&data_types::partition_metadata::TableSummary>;

        fn schema(&self) -> Arc<schema::Schema>;

        fn sort_key(&self) -> Option<&schema::sort::SortKey>;

        fn delete_predicates(&self) -> &[Arc<data_types::delete_predicate::DeletePredicate>];
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
    ) -> Vec<Arc<AbstractChunk>> {
        self.0
            .chunks(table_name, predicate)
            .await
            .into_iter()
            .map(|c| Arc::new(AbstractChunk::create_old(c)))
            .collect()
    }

    fn record_query(
        &self,
        ctx: &IOxExecutionContext,
        query_type: String,
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

#[derive(Debug)]
struct OldChunk(Arc<DbChunk>);

impl AbstractChunkInterface for OldChunk {
    fn id(&self) -> ChunkId {
        self.0.id()
    }

    fn addr(&self) -> ChunkAddr {
        self.0.addr().clone()
    }

    fn table_name(&self) -> &str {
        self.0.table_name()
    }

    fn may_contain_pk_duplicates(&self) -> bool {
        self.0.may_contain_pk_duplicates()
    }

    fn apply_predicate_to_metadata(
        &self,
        predicate: &predicate::Predicate,
    ) -> Result<predicate::PredicateMatch, Error> {
        self.0
            .apply_predicate_to_metadata(predicate)
            .map_err(Error::new)
    }

    fn column_names(
        &self,
        ctx: IOxExecutionContext,
        predicate: &predicate::Predicate,
        columns: schema::selection::Selection<'_>,
    ) -> Result<Option<query::exec::stringset::StringSet>, Error> {
        self.0
            .column_names(ctx, predicate, columns)
            .map_err(Error::new)
    }

    fn column_values(
        &self,
        ctx: IOxExecutionContext,
        column_name: &str,
        predicate: &predicate::Predicate,
    ) -> Result<Option<query::exec::stringset::StringSet>, Error> {
        self.0
            .column_values(ctx, column_name, predicate)
            .map_err(Error::new)
    }

    fn read_filter(
        &self,
        ctx: IOxExecutionContext,
        predicate: &predicate::Predicate,
        selection: schema::selection::Selection<'_>,
    ) -> Result<datafusion::physical_plan::SendableRecordBatchStream, Error> {
        self.0
            .read_filter(ctx, predicate, selection)
            .map_err(Error::new)
    }

    fn chunk_type(&self) -> &str {
        self.0.chunk_type()
    }

    fn order(&self) -> ChunkOrder {
        self.0.order()
    }

    fn summary(&self) -> Option<&data_types::partition_metadata::TableSummary> {
        self.0.summary()
    }

    fn schema(&self) -> Arc<schema::Schema> {
        self.0.schema()
    }

    fn sort_key(&self) -> Option<&schema::sort::SortKey> {
        self.0.sort_key()
    }

    fn delete_predicates(&self) -> &[Arc<data_types::delete_predicate::DeletePredicate>] {
        self.0.delete_predicates()
    }
}
