//! Implementation of a DataFusion `TableProvider` in terms of `PartitionChunk`s

use std::sync::Arc;

use arrow::datatypes::SchemaRef as ArrowSchemaRef;
use datafusion::{
    datasource::{
        datasource::{Statistics, TableProviderFilterPushDown},
        TableProvider,
    },
    error::{DataFusionError, Result as DataFusionResult},
    logical_plan::Expr,
    physical_plan::ExecutionPlan,
};
use internal_types::schema::{builder::SchemaMerger, Schema};
use observability_deps::tracing::debug;

use crate::{
    predicate::{Predicate, PredicateBuilder},
    util::project_schema,
    PartitionChunk,
};

use snafu::{ResultExt, Snafu};

mod adapter;
mod physical;
use self::physical::IOxReadFilterNode;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Chunk schema not compatible for table '{}': {}", table_name, source))]
    ChunkSchemaNotCompatible {
        table_name: String,
        source: internal_types::schema::builder::Error,
    },

    #[snafu(display(
        "Internal error: no chunks found in builder for table '{}': {}",
        table_name,
        source,
    ))]
    InternalNoChunks {
        table_name: String,
        source: internal_types::schema::builder::Error,
    },

    #[snafu(display(
        "Internal error: no chunk pruner provided to builder for {}",
        table_name,
    ))]
    InternalNoChunkPruner { table_name: String },

    #[snafu(display("Internal error: No rows found in table '{}'", table_name))]
    InternalNoRowsInTable { table_name: String },

    #[snafu(display("Internal error: Cannot verify the push-down predicate '{}'", source,))]
    InternalPushdownPredicate {
        source: datafusion::error::DataFusionError,
    },
}
pub type Result<T, E = Error> = std::result::Result<T, E>;

/// Something that can prune chunks based on their metadata
pub trait ChunkPruner<C: PartitionChunk>: Sync + Send + std::fmt::Debug {
    /// prune `chunks`, if possible, based on predicate.
    fn prune_chunks(&self, chunks: Vec<Arc<C>>, predicate: &Predicate) -> Vec<Arc<C>>;
}

/// Builds a `ChunkTableProvider` from a series of `PartitionChunk`s
/// and ensures the schema across the chunks is compatible and
/// consistent.
#[derive(Debug)]
pub struct ProviderBuilder<C: PartitionChunk + 'static> {
    table_name: Arc<str>,
    schema_merger: SchemaMerger,
    chunk_pruner: Option<Arc<dyn ChunkPruner<C>>>,
    chunks: Vec<Arc<C>>,
}

impl<C: PartitionChunk> ProviderBuilder<C> {
    pub fn new(table_name: impl AsRef<str>) -> Self {
        Self {
            table_name: Arc::from(table_name.as_ref()),
            schema_merger: SchemaMerger::new(),
            chunk_pruner: None,
            chunks: Vec::new(),
        }
    }

    /// Add a new chunk to this provider
    pub fn add_chunk(self, chunk: Arc<C>, chunk_table_schema: Schema) -> Result<Self> {
        let Self {
            table_name,
            schema_merger,
            chunk_pruner,
            mut chunks,
        } = self;

        let schema_merger =
            schema_merger
                .merge(chunk_table_schema)
                .context(ChunkSchemaNotCompatible {
                    table_name: table_name.as_ref(),
                })?;

        chunks.push(chunk);

        Ok(Self {
            table_name,
            schema_merger,
            chunk_pruner,
            chunks,
        })
    }

    /// Specify a `ChunkPruner` for the provider that will apply
    /// additional chunk level pruning based on pushed down predicates
    pub fn add_pruner(mut self, chunk_pruner: Arc<dyn ChunkPruner<C>>) -> Self {
        assert!(
            self.chunk_pruner.is_none(),
            "Chunk pruner already specified"
        );
        self.chunk_pruner = Some(chunk_pruner);
        self
    }

    /// Specify a `ChunkPruner` for the provider that does no
    /// additional pruning based on pushed down predicates.
    ///
    /// Some planners, such as InfluxRPC which apply all predicates
    /// when they get the initial list of chunks, do not need an
    /// additional pass.
    pub fn add_no_op_pruner(self) -> Self {
        let chunk_pruner = Arc::new(NoOpPruner {});
        self.add_pruner(chunk_pruner)
    }

    /// Create the Provider
    pub fn build(self) -> Result<ChunkTableProvider<C>> {
        let Self {
            table_name,
            schema_merger,
            chunk_pruner,
            chunks,
        } = self;

        let iox_schema = schema_merger
            .build()
            .context(InternalNoChunks {
                table_name: table_name.as_ref(),
            })?
            // sort so the columns are always in a consistent order
            .sort_fields_by_name();

        // if the table was reported to exist, it should not be empty
        if chunks.is_empty() {
            return InternalNoRowsInTable {
                table_name: table_name.as_ref(),
            }
            .fail();
        }

        let chunk_pruner = match chunk_pruner {
            Some(chunk_pruner) => chunk_pruner,
            None => {
                return InternalNoChunkPruner {
                    table_name: table_name.as_ref(),
                }
                .fail()
            }
        };

        Ok(ChunkTableProvider {
            table_name,
            iox_schema,
            chunk_pruner,
            chunks,
        })
    }
}

/// Implementation of a DataFusion TableProvider in terms of PartitionChunks
///
/// This allows DataFusion to see data from Chunks as a single table, as well as
/// push predicates and selections down to chunks
#[derive(Debug)]
pub struct ChunkTableProvider<C: PartitionChunk + 'static> {
    table_name: Arc<str>,
    /// The IOx schema (wrapper around Arrow Schemaref) for this table
    iox_schema: Schema,
    /// Something that can prune chunks
    chunk_pruner: Arc<dyn ChunkPruner<C>>,
    // The chunks
    chunks: Vec<Arc<C>>,
}

impl<C: PartitionChunk + 'static> ChunkTableProvider<C> {
    /// Return the IOx schema view for the data provided by this provider
    pub fn iox_schema(&self) -> Schema {
        self.iox_schema.clone()
    }

    /// Return the Arrow schema view for the data provided by this provider
    pub fn arrow_schema(&self) -> ArrowSchemaRef {
        self.iox_schema.as_arrow()
    }
}

impl<C: PartitionChunk + 'static> TableProvider for ChunkTableProvider<C> {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    /// Schema with all available columns across all chunks
    fn schema(&self) -> ArrowSchemaRef {
        self.arrow_schema()
    }

    fn scan(
        &self,
        projection: &Option<Vec<usize>>,
        _batch_size: usize,
        filters: &[Expr],
        _limit: Option<usize>,
    ) -> std::result::Result<Arc<dyn ExecutionPlan>, DataFusionError> {
        debug!(?filters, "Input Filters to Scan");

        // Note that `filters` don't actually need to be evaluated in
        // the scan for the plans to be correct, they are an extra
        // optimization for providers which can offer them
        let predicate = PredicateBuilder::default()
            .add_pushdown_exprs(filters)
            .build();

        // Now we have a second attempt to prune out chunks based on
        // metadata using the pushed down predicate (e.g. in SQL).
        let chunks: Vec<Arc<C>> = self.chunks.to_vec();
        let num_initial_chunks = chunks.len();
        let chunks = self.chunk_pruner.prune_chunks(chunks, &predicate);
        debug!(%predicate, num_initial_chunks, num_final_chunks=chunks.len(), "pruned with pushed down predicates");

        // Figure out the schema of the requested output
        let scan_schema = project_schema(self.arrow_schema(), projection);

        let plan =
            IOxReadFilterNode::new(Arc::clone(&self.table_name), scan_schema, chunks, predicate);

        Ok(Arc::new(plan))
    }

    fn statistics(&self) -> Statistics {
        // TODO translate IOx stats to DataFusion statistics
        Statistics::default()
    }

    fn supports_filter_pushdown(
        &self,
        _filter: &Expr,
    ) -> DataFusionResult<TableProviderFilterPushDown> {
        Ok(TableProviderFilterPushDown::Inexact)
    }
}

#[derive(Debug)]
/// A pruner that does not do pruning (suitable if no additional pruning is possible)
struct NoOpPruner {}
impl<C: PartitionChunk> ChunkPruner<C> for NoOpPruner {
    fn prune_chunks(&self, chunks: Vec<Arc<C>>, _predicate: &Predicate) -> Vec<Arc<C>> {
        chunks
    }
}
