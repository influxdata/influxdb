//! This module contains the interface to the Catalog / Chunks used by
//! the query engine

use crate::{db::system_tables, JobRegistry};
use std::{any::Any, sync::Arc};

use super::{
    catalog::{Catalog, TableNameFilter},
    chunk::DbChunk,
    Error, Result,
};

use async_trait::async_trait;
use data_types::chunk_metadata::ChunkSummary;
use datafusion::{
    catalog::{catalog::CatalogProvider, schema::SchemaProvider},
    datasource::TableProvider,
};
use metric::{Attributes, Metric, U64Counter};
use observability_deps::tracing::debug;
use predicate::predicate::{Predicate, PredicateBuilder};
use query::{
    provider::{ChunkPruner, ProviderBuilder},
    QueryChunk, QueryChunkMeta, DEFAULT_SCHEMA,
};
use schema::Schema;
use system_tables::{SystemSchemaProvider, SYSTEM_SCHEMA};

use hashbrown::HashMap;
use parking_lot::{MappedMutexGuard, Mutex, MutexGuard};
use query::{
    pruning::{prune_chunks, PruningObserver},
    QueryDatabase,
};

/// Metrics related to chunk access (pruning specifically)
#[derive(Debug)]
struct AccessMetrics {
    /// The database name
    db_name: Arc<str>,
    /// Total number of chunks pruned via statistics
    pruned_chunks: Metric<U64Counter>,
    /// Total number of rows pruned using statistics
    pruned_rows: Metric<U64Counter>,
    /// Keyed by table name
    tables: Mutex<HashMap<Arc<str>, TableAccessMetrics>>,
}

impl AccessMetrics {
    fn new(registry: &metric::Registry, db_name: Arc<str>) -> Self {
        let pruned_chunks = registry.register_metric(
            "query_access_pruned_chunks",
            "Number of chunks pruned using metadata",
        );

        let pruned_rows = registry.register_metric(
            "query_access_pruned_rows",
            "Number of rows pruned using metadata",
        );

        Self {
            db_name,
            pruned_chunks,
            pruned_rows,
            tables: Default::default(),
        }
    }

    fn table_metrics(&self, table: &Arc<str>) -> MappedMutexGuard<'_, TableAccessMetrics> {
        MutexGuard::map(self.tables.lock(), |tables| {
            let (_, metrics) = tables.raw_entry_mut().from_key(table).or_insert_with(|| {
                let attributes = Attributes::from([
                    ("db_name", self.db_name.to_string().into()),
                    ("table_name", table.to_string().into()),
                ]);

                let metrics = TableAccessMetrics {
                    pruned_chunks: self.pruned_chunks.recorder(attributes.clone()),
                    pruned_rows: self.pruned_rows.recorder(attributes),
                };

                (Arc::clone(table), metrics)
            });
            metrics
        })
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
pub(crate) struct QueryCatalogAccess {
    /// The catalog to have access to
    catalog: Arc<Catalog>,

    /// Handles finding / pruning chunks based on predicates
    chunk_access: Arc<ChunkAccess>,

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
        metric_registry: &metric::Registry,
    ) -> Self {
        let db_name = Arc::from(db_name.into());
        let access_metrics = AccessMetrics::new(metric_registry, Arc::clone(&db_name));
        let chunk_access = Arc::new(ChunkAccess::new(Arc::clone(&catalog), access_metrics));

        let system_tables = Arc::new(SystemSchemaProvider::new(
            db_name.as_ref(),
            Arc::clone(&catalog),
            jobs,
        ));
        let user_tables = Arc::new(DbSchemaProvider::new(
            Arc::clone(&catalog),
            Arc::clone(&chunk_access),
        ));

        Self {
            catalog,
            chunk_access,
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

    /// Returns all chunks that may have data that passes the
    /// specified predicates. The chunks are pruned as aggressively as
    /// possible based on metadata.
    fn candidate_chunks(&self, predicate: &Predicate) -> Vec<Arc<DbChunk>> {
        let partition_key = predicate.partition_key.as_deref();
        let table_names: TableNameFilter<'_> = predicate.table_names.as_ref().into();

        // Apply initial partition key / table name pruning
        let chunks = self
            .catalog
            .filtered_chunks(table_names, partition_key, DbChunk::snapshot);

        self.prune_chunks(chunks, predicate)
    }
}

impl ChunkPruner<DbChunk> for ChunkAccess {
    fn prune_chunks(&self, chunks: Vec<Arc<DbChunk>>, predicate: &Predicate) -> Vec<Arc<DbChunk>> {
        // TODO: call "apply_predicate" here too for additional
        // metadata based pruning

        debug!(num_chunks=chunks.len(), %predicate, "Attempting to prune chunks");
        prune_chunks(self, chunks, predicate)
    }
}

impl PruningObserver for ChunkAccess {
    type Observed = DbChunk;

    fn was_pruned(&self, chunk: &Self::Observed) {
        let metrics = self.access_metrics.table_metrics(chunk.table_name());
        metrics.pruned_chunks.inc(1);
        metrics.pruned_rows.inc(chunk.summary().total_count())
    }

    fn could_not_prune_chunk(&self, chunk: &Self::Observed, reason: &str) {
        debug!(
            chunk_id=%chunk.id().get(),
            reason,
            "could not prune chunk from query",
        )
    }
}

#[async_trait]
impl QueryDatabase for QueryCatalogAccess {
    type Error = Error;
    type Chunk = DbChunk;

    /// Return a covering set of chunks for a particular partition
    fn chunks(&self, predicate: &Predicate) -> Vec<Arc<Self::Chunk>> {
        self.chunk_access.candidate_chunks(predicate)
    }

    fn partition_keys(&self) -> Result<Vec<String>, Self::Error> {
        Ok(self.catalog.partition_keys().into_iter().collect())
    }

    fn chunk_summaries(&self) -> Result<Vec<ChunkSummary>> {
        Ok(self.catalog.chunk_summaries())
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
            Arc::clone(&table.schema().read())
        };

        let mut builder = ProviderBuilder::new(table_name, schema);
        builder =
            builder.add_pruner(Arc::clone(&self.chunk_access) as Arc<dyn ChunkPruner<DbChunk>>);

        let predicate = PredicateBuilder::new().table(table_name).build();

        for chunk in self.chunk_access.candidate_chunks(&predicate) {
            builder = builder.add_chunk(chunk);
        }

        match builder.build() {
            Ok(provider) => Some(Arc::new(provider)),
            Err(e) => panic!("unexpected error: {:?}", e),
        }
    }
}
