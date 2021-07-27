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
use internal_types::schema::Schema;
use metrics::{Counter, KeyValue, MetricRegistry};
use observability_deps::tracing::debug;
use query::{
    predicate::{Predicate, PredicateBuilder},
    provider::{ChunkPruner, ProviderBuilder},
    QueryChunk, QueryChunkMeta, DEFAULT_SCHEMA,
};
use system_tables::{SystemSchemaProvider, SYSTEM_SCHEMA};

use query::{
    pruning::{prune_chunks, PruningObserver},
    QueryDatabase,
};

/// Metrics related to chunk access (pruning specifically)
#[derive(Debug)]
struct AccessMetrics {
    /// Total number of chunks pruned via statistics
    pruned_chunks: Counter,
    /// Total number of rows pruned using statistics
    pruned_rows: Counter,
}

impl AccessMetrics {
    fn new(metrics_registry: Arc<metrics::MetricRegistry>, metric_labels: Vec<KeyValue>) -> Self {
        let pruning_domain =
            metrics_registry.register_domain_with_labels("query_access", metric_labels);

        let pruned_chunks = pruning_domain.register_counter_metric(
            "pruned_chunks",
            None,
            "Number of chunks pruned using metadata",
        );

        let pruned_rows = pruning_domain.register_counter_metric(
            "pruned_rows",
            None,
            "Number of rows pruned using metadata",
        );

        Self {
            pruned_chunks,
            pruned_rows,
        }
    }
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
        metrics_registry: Arc<MetricRegistry>,
        metric_labels: Vec<KeyValue>,
    ) -> Self {
        let access_metrics = AccessMetrics::new(metrics_registry, metric_labels);
        let chunk_access = Arc::new(ChunkAccess::new(Arc::clone(&catalog), access_metrics));

        let system_tables = Arc::new(SystemSchemaProvider::new(
            db_name,
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
        let labels = vec![KeyValue::new("table_name", chunk.table_name().to_string())];
        let num_rows = chunk.summary().total_count();
        self.access_metrics
            .pruned_chunks
            .add_with_labels(1, &labels);
        self.access_metrics
            .pruned_rows
            .add_with_labels(num_rows, &labels);
        debug!(chunk_id = chunk.id(), num_rows, "pruned chunk from query")
    }

    fn could_not_prune_chunk(&self, chunk: &Self::Observed, reason: &str) {
        debug!(
            chunk_id = chunk.id(),
            reason, "could not prune chunk from query"
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
