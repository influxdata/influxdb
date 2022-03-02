//! Namespace within the whole database.
use std::{any::Any, sync::Arc};

use async_trait::async_trait;
use data_types::{chunk_metadata::ChunkSummary, partition_metadata::PartitionAddr};
use datafusion::catalog::{catalog::CatalogProvider, schema::SchemaProvider};
use db::{access::QueryCatalogAccess, catalog::Catalog as DbCatalog, chunk::DbChunk};
use iox_catalog::interface::NamespaceId;
use job_registry::JobRegistry;
use object_store::ObjectStore;
use predicate::{rpc_predicate::QueryDatabaseMeta, Predicate};
use query::{
    exec::{ExecutionContextProvider, Executor, ExecutorType, IOxExecutionContext},
    QueryCompletedToken, QueryDatabase, QueryText,
};
use schema::Schema;
use time::TimeProvider;
use trace::ctx::SpanContext;

use crate::{cache::CatalogCache, chunk::ParquetChunkAdapter};

/// Maps a catalog namespace to all the in-memory resources and sync-state that the querier needs.
#[derive(Debug)]
pub struct QuerierNamespace {
    /// Old-gen DB catalog.
    db_catalog: Arc<DbCatalog>,

    /// Adapter to create old-gen chunks.
    chunk_adapter: ParquetChunkAdapter,

    /// ID of this namespace.
    id: NamespaceId,

    /// Name of this namespace.
    name: Arc<str>,

    /// Catalog interface for query
    catalog_access: Arc<QueryCatalogAccess>,

    /// Executor for queries.
    exec: Arc<Executor>,
}

impl QuerierNamespace {
    /// Create new, empty namespace.
    ///
    /// You may call [`sync`](Self::sync) to fill the namespace with chunks.
    pub fn new(
        catalog_cache: Arc<CatalogCache>,
        name: Arc<str>,
        id: NamespaceId,
        metric_registry: Arc<metric::Registry>,
        object_store: Arc<ObjectStore>,
        time_provider: Arc<dyn TimeProvider>,
        exec: Arc<Executor>,
    ) -> Self {
        let db_catalog = Arc::new(DbCatalog::new(
            Arc::clone(&name),
            Arc::clone(&metric_registry),
            Arc::clone(&time_provider),
        ));

        // no real job registration system
        let jobs = Arc::new(JobRegistry::new(
            Arc::clone(&metric_registry),
            Arc::clone(&time_provider),
        ));
        let catalog_access = Arc::new(QueryCatalogAccess::new(
            name.to_string(),
            Arc::clone(&db_catalog),
            jobs,
            Arc::clone(&time_provider),
            &metric_registry,
        ));

        Self {
            db_catalog,
            chunk_adapter: ParquetChunkAdapter::new(
                catalog_cache,
                object_store,
                metric_registry,
                time_provider,
            ),
            id,
            name,
            catalog_access,
            exec,
        }
    }

    /// Namespace name.
    pub fn name(&self) -> Arc<str> {
        Arc::clone(&self.name)
    }

    /// Sync tables and chunks.
    ///
    /// Should be called regularly.
    pub async fn sync(&self) {
        // TODO: implement this
    }
}

impl QueryDatabaseMeta for QuerierNamespace {
    fn table_names(&self) -> Vec<String> {
        self.catalog_access.table_names()
    }

    fn table_schema(&self, table_name: &str) -> Option<Arc<Schema>> {
        self.catalog_access.table_schema(table_name)
    }
}

#[async_trait]
impl QueryDatabase for QuerierNamespace {
    type Chunk = DbChunk;

    fn partition_addrs(&self) -> Vec<PartitionAddr> {
        self.catalog_access.partition_addrs()
    }

    fn chunks(&self, table_name: &str, predicate: &Predicate) -> Vec<Arc<Self::Chunk>> {
        self.catalog_access.chunks(table_name, predicate)
    }

    fn chunk_summaries(&self) -> Vec<ChunkSummary> {
        self.catalog_access.chunk_summaries()
    }

    fn record_query(
        &self,
        query_type: impl Into<String>,
        query_text: QueryText,
    ) -> QueryCompletedToken {
        self.catalog_access.record_query(query_type, query_text)
    }
}

impl CatalogProvider for QuerierNamespace {
    fn as_any(&self) -> &dyn Any {
        self as &dyn Any
    }

    fn schema_names(&self) -> Vec<String> {
        self.catalog_access.schema_names()
    }

    fn schema(&self, name: &str) -> Option<Arc<dyn SchemaProvider>> {
        self.catalog_access.schema(name)
    }
}

impl ExecutionContextProvider for QuerierNamespace {
    fn new_query_context(self: &Arc<Self>, span_ctx: Option<SpanContext>) -> IOxExecutionContext {
        self.exec
            .new_execution_config(ExecutorType::Query)
            .with_default_catalog(Arc::<Self>::clone(self))
            .with_span_context(span_ctx)
            .build()
    }
}
