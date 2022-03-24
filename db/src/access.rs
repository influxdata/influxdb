//! This module contains the interface to the Catalog / Chunks used by
//! the query engine

use super::{catalog::Catalog, chunk::DbChunk, query_log::QueryLog};
use crate::system_tables;
use async_trait::async_trait;
use datafusion::{
    catalog::{catalog::CatalogProvider, schema::SchemaProvider},
    datasource::TableProvider,
};
use hashbrown::HashMap;
use job_registry::JobRegistry;
use metric::{Attributes, DurationCounter, Metric, U64Counter};
use observability_deps::tracing::debug;
use parking_lot::Mutex;
use predicate::{rpc_predicate::QueryDatabaseMeta, Predicate};
use query::{exec::IOxSessionContext, QueryChunk};
use query::{
    provider::{ChunkPruner, ProviderBuilder},
    pruning::{prune_chunks, PruningObserver},
    QueryCompletedToken, QueryDatabase, QueryText, DEFAULT_SCHEMA,
};
use schema::Schema;
use std::time::Instant;
use std::{any::Any, sync::Arc};
use system_tables::{SystemSchemaProvider, SYSTEM_SCHEMA};
use time::TimeProvider;

/// The number of entries to store in the circular query buffer log
const QUERY_LOG_SIZE: usize = 10_000;

/// Metrics related to chunk access (pruning specifically)
#[derive(Debug)]
struct AccessMetrics {
    /// The database name
    db_name: Arc<str>,

    /// Total time spent snapshotting catalog
    catalog_snapshot_duration: DurationCounter,
    /// Total number of catalog snapshots
    catalog_snapshot_count: U64Counter,

    /// Total time spent pruning chunks
    prune_duration: DurationCounter,
    /// Total number of times pruned chunks
    prune_count: U64Counter,

    /// Total number of chunks pruned via statistics
    pruned_chunks: Metric<U64Counter>,
    /// Total number of rows pruned using statistics
    pruned_rows: Metric<U64Counter>,

    /// Keyed by table name
    tables: Mutex<HashMap<String, Arc<TableAccessMetrics>>>,
}

impl AccessMetrics {
    fn new(registry: &metric::Registry, db_name: Arc<str>) -> Self {
        let attributes = Attributes::from([("db_name", db_name.to_string().into())]);
        let catalog_snapshot_duration = registry
            .register_metric::<DurationCounter>(
                "query_access_catalog_snapshot_duration",
                "Total time spent snapshotting catalog",
            )
            .recorder(attributes.clone());

        let catalog_snapshot_count = registry
            .register_metric::<U64Counter>(
                "query_access_catalog_snapshot",
                "Total number of catalog snapshots",
            )
            .recorder(attributes.clone());

        let prune_duration = registry
            .register_metric::<DurationCounter>(
                "query_access_prune_duration",
                "Total time spent pruning chunks",
            )
            .recorder(attributes.clone());

        let prune_count = registry
            .register_metric::<U64Counter>(
                "query_access_prune",
                "Total number of times pruned chunks",
            )
            .recorder(attributes);

        let pruned_chunks = registry.register_metric(
            "query_access_pruned_chunks",
            "Number of chunks pruned using metadata",
        );

        let pruned_rows = registry.register_metric(
            "query_access_pruned_rows",
            "Number of rows pruned using metadata",
        );

        Self {
            catalog_snapshot_duration,
            catalog_snapshot_count,
            prune_duration,
            prune_count,
            db_name,
            pruned_chunks,
            pruned_rows,
            tables: Default::default(),
        }
    }

    fn table_metrics(&self, table: &str) -> Arc<TableAccessMetrics> {
        let mut tables = self.tables.lock();
        let (_, metrics) = tables.raw_entry_mut().from_key(table).or_insert_with(|| {
            let attributes = Attributes::from([("db_name", self.db_name.to_string().into())]);

            let metrics = TableAccessMetrics {
                pruned_chunks: self.pruned_chunks.recorder(attributes.clone()),
                pruned_rows: self.pruned_rows.recorder(attributes),
            };

            (table.to_string(), Arc::new(metrics))
        });
        Arc::clone(metrics)
    }
}

/// Metrics related to chunk access for a specific table
#[derive(Debug)]
struct TableAccessMetrics {
    /// Total number of chunks pruned via statistics
    pruned_chunks: U64Counter,
    /// Total number of rows pruned using statistics
    pruned_rows: U64Counter,
}

/// `QueryCatalogAccess` implements traits that allow the query engine
/// (and DataFusion) to access the contents of the IOx catalog.
#[derive(Debug)]
pub struct QueryCatalogAccess {
    /// The catalog to have access to
    catalog: Arc<Catalog>,

    /// Handles finding / pruning chunks based on predicates
    chunk_access: Arc<ChunkAccess>,

    /// Stores queries which have been executed
    query_log: Arc<QueryLog>,

    /// Provides access to system tables
    system_tables: Arc<SystemSchemaProvider>,

    /// Provides access to "normal" user tables
    user_tables: Arc<DbSchemaProvider>,
}

impl QueryCatalogAccess {
    pub fn new(
        db_name: impl Into<String>,
        catalog: Arc<Catalog>,
        jobs: Arc<JobRegistry>,
        time_provider: Arc<dyn TimeProvider>,
        metric_registry: &metric::Registry,
    ) -> Self {
        let db_name = Arc::from(db_name.into());
        let access_metrics = AccessMetrics::new(metric_registry, Arc::clone(&db_name));
        let chunk_access = Arc::new(ChunkAccess::new(Arc::clone(&catalog), access_metrics));
        let query_log = Arc::new(QueryLog::new(QUERY_LOG_SIZE, time_provider));

        let system_tables = Arc::new(SystemSchemaProvider::new(
            db_name.as_ref(),
            Arc::clone(&catalog),
            jobs,
            Arc::clone(&query_log),
        ));
        let user_tables = Arc::new(DbSchemaProvider::new(
            Arc::clone(&catalog),
            Arc::clone(&chunk_access),
        ));
        Self {
            catalog,
            chunk_access,
            query_log,
            system_tables,
            user_tables,
        }
    }
}

/// Encapsulates everything needed to find candidate chunks for
/// queries (including pruning based on metadata)
#[derive(Debug)]
struct ChunkAccess {
    /// The catalog to have access to
    catalog: Arc<Catalog>,

    /// Metrics about query processing
    access_metrics: AccessMetrics,
}

impl ChunkAccess {
    fn new(catalog: Arc<Catalog>, access_metrics: AccessMetrics) -> Self {
        Self {
            catalog,
            access_metrics,
        }
    }

    /// Returns all chunks from `table_name` that may have data that passes the
    /// specified predicates. The chunks are pruned as aggressively as
    /// possible based on metadata.
    fn candidate_chunks(
        &self,
        table_name: &str,
        predicate: &Predicate,
    ) -> Vec<Arc<dyn QueryChunk>> {
        let start = Instant::now();

        // Get chunks and schema as a single transaction
        let (chunks, schema) = {
            let table = match self.catalog.table(table_name).ok() {
                Some(table) => table,
                None => return vec![],
            };

            let schema = Arc::clone(&table.schema().read());
            let chunks = table.filtered_chunks(
                predicate.partition_key.as_deref(),
                predicate.range,
                DbChunk::snapshot,
            );

            (chunks, schema)
        };

        let mut chunks: Vec<_> = chunks
            .into_iter()
            .map(|c| c as Arc<dyn QueryChunk>)
            .collect();

        self.access_metrics.catalog_snapshot_count.inc(1);
        self.access_metrics
            .catalog_snapshot_duration
            .inc(start.elapsed());

        // if there is a field restriction on the predicate, only
        // chunks with that field should be returned. If the chunk has
        // none of the fields specified, then it doesn't match
        if let Some(field_columns) = &predicate.field_columns {
            chunks.retain(|chunk| {
                let schema = chunk.schema();
                // keep chunk if it has any of the columns requested
                field_columns
                    .iter()
                    .any(|col| schema.find_index_of(col).is_some())
            })
        }

        self.prune_chunks(table_name, schema, chunks, predicate)
    }
}

impl ChunkPruner for ChunkAccess {
    fn prune_chunks(
        &self,
        table_name: &str,
        table_schema: Arc<Schema>,
        chunks: Vec<Arc<dyn QueryChunk>>,
        predicate: &Predicate,
    ) -> Vec<Arc<dyn QueryChunk>> {
        let start = Instant::now();

        debug!(num_chunks=chunks.len(), %predicate, "Attempting to prune chunks");
        let observer = self.access_metrics.table_metrics(table_name);
        let pruned = prune_chunks(observer.as_ref(), table_schema, chunks, predicate);

        self.access_metrics.prune_count.inc(1);
        self.access_metrics.prune_duration.inc(start.elapsed());

        pruned
    }
}

impl PruningObserver for TableAccessMetrics {
    fn was_pruned(&self, chunk: &dyn QueryChunk) {
        let chunk_summary = chunk.summary().expect("Chunk should have summary");
        self.pruned_chunks.inc(1);
        self.pruned_rows.inc(chunk_summary.total_count())
    }
}

#[async_trait]
impl QueryDatabase for QueryCatalogAccess {
    /// Return a covering set of chunks for a particular table and predicate
    async fn chunks(&self, table_name: &str, predicate: &Predicate) -> Vec<Arc<dyn QueryChunk>> {
        self.chunk_access.candidate_chunks(table_name, predicate)
    }

    fn record_query(
        &self,
        ctx: &IOxSessionContext,
        query_type: &str,
        query_text: QueryText,
    ) -> QueryCompletedToken {
        // When the query token is dropped the query entry's completion time
        // will be set.
        let query_log = Arc::clone(&self.query_log);
        let trace_id = ctx.span().map(|s| s.ctx.trace_id);
        let entry = query_log.push(query_type, query_text, trace_id);
        QueryCompletedToken::new(move |success| query_log.set_completed(entry, success))
    }

    fn as_meta(&self) -> &dyn QueryDatabaseMeta {
        self
    }
}

impl QueryDatabaseMeta for QueryCatalogAccess {
    fn table_names(&self) -> Vec<String> {
        self.catalog.table_names()
    }

    fn table_schema(&self, table_name: &str) -> Option<Arc<Schema>> {
        self.catalog
            .table(table_name)
            .ok()
            .map(|table| Arc::clone(&table.schema().read()))
    }
}

// Datafusion catalog provider interface
impl CatalogProvider for QueryCatalogAccess {
    fn as_any(&self) -> &dyn Any {
        self as &dyn Any
    }

    fn schema_names(&self) -> Vec<String> {
        vec![
            DEFAULT_SCHEMA.to_string(),
            system_tables::SYSTEM_SCHEMA.to_string(),
        ]
    }

    fn schema(&self, name: &str) -> Option<Arc<dyn SchemaProvider>> {
        match name {
            DEFAULT_SCHEMA => Some(Arc::clone(&self.user_tables) as Arc<dyn SchemaProvider>),
            SYSTEM_SCHEMA => Some(Arc::clone(&self.system_tables) as Arc<dyn SchemaProvider>),
            _ => None,
        }
    }

    fn register_schema(
        &self,
        _name: &str,
        _schema: Arc<dyn SchemaProvider>,
    ) -> Option<Arc<dyn SchemaProvider>> {
        // https://github.com/apache/arrow-datafusion/issues/2051
        unimplemented!("Schemas can not be registered in IOx");
    }
}

/// Implement the DataFusion schema provider API
#[derive(Debug)]
struct DbSchemaProvider {
    /// The catalog to have access to
    catalog: Arc<Catalog>,

    /// Handles finding / pruning chunks based on predicates
    chunk_access: Arc<ChunkAccess>,
}

impl DbSchemaProvider {
    fn new(catalog: Arc<Catalog>, chunk_access: Arc<ChunkAccess>) -> Self {
        Self {
            catalog,
            chunk_access,
        }
    }
}

impl SchemaProvider for DbSchemaProvider {
    fn as_any(&self) -> &dyn Any {
        self as &dyn Any
    }

    fn table_names(&self) -> Vec<String> {
        self.catalog.table_names()
    }

    /// Create a table provider for the named table
    fn table(&self, table_name: &str) -> Option<Arc<dyn TableProvider>> {
        let schema = {
            let table = self.catalog.table(table_name).ok()?;
            let schema = Arc::clone(&table.schema().read());
            schema
        };

        let mut builder = ProviderBuilder::new(table_name, schema);
        builder = builder.add_pruner(Arc::clone(&self.chunk_access) as Arc<dyn ChunkPruner>);

        // TODO: Better chunk pruning (#3570)
        for chunk in self
            .chunk_access
            .candidate_chunks(table_name, &Default::default())
        {
            builder = builder.add_chunk(chunk);
        }

        match builder.build() {
            Ok(provider) => Some(Arc::new(provider)),
            Err(e) => panic!("unexpected error: {:?}", e),
        }
    }

    fn table_exist(&self, name: &str) -> bool {
        self.catalog.table(name).is_ok()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_helpers::write_lp;
    use crate::utils::make_db;
    use predicate::PredicateBuilder;

    #[tokio::test]
    async fn test_filtered_chunks() {
        let partition_key = "1970-01-01T00";
        let db = make_db().await.db;

        write_lp(&db, "cpu foo=1 1");
        write_lp(&db, "cpu foo=1 2");

        db.compact_partition("cpu", partition_key).await.unwrap();

        write_lp(&db, "cpu foo=1 3");

        db.compact_chunks("cpu", partition_key, |c| c.storage().1.has_mutable_buffer())
            .await
            .unwrap();

        write_lp(&db, "cpu foo=1 4");

        let predicate = Default::default();
        assert_eq!(db.catalog_access.chunks("cpu", &predicate).await.len(), 3);

        let predicate = PredicateBuilder::new().timestamp_range(0, 1).build();
        assert_eq!(db.catalog_access.chunks("cpu", &predicate).await.len(), 0);

        let predicate = PredicateBuilder::new().timestamp_range(0, 2).build();
        assert_eq!(db.catalog_access.chunks("cpu", &predicate).await.len(), 1);

        let predicate = PredicateBuilder::new().timestamp_range(2, 3).build();
        assert_eq!(db.catalog_access.chunks("cpu", &predicate).await.len(), 1);

        let predicate = PredicateBuilder::new().timestamp_range(2, 4).build();
        assert_eq!(db.catalog_access.chunks("cpu", &predicate).await.len(), 2);

        let predicate = PredicateBuilder::new().timestamp_range(2, 5).build();
        assert_eq!(db.catalog_access.chunks("cpu", &predicate).await.len(), 3);
    }
}
