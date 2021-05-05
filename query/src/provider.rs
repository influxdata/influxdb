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

use crate::{predicate::Predicate, util::project_schema, PartitionChunk};

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

    #[snafu(display("Internal error: No rows found in table '{}'", table_name))]
    InternalNoRowsInTable { table_name: String },
}
pub type Result<T, E = Error> = std::result::Result<T, E>;

/// Builds a `ChunkTableProvider` from a series of `PartitionChunk`s
/// and ensures the schema across the chunks is compatible and
/// consistent.
#[derive(Debug)]
pub struct ProviderBuilder<C: PartitionChunk + 'static> {
    table_name: Arc<String>,
    schema_merger: SchemaMerger,
    chunk_and_infos: Vec<ChunkInfo<C>>,
}

/// Holds the information needed to generate data for a specific chunk
#[derive(Debug)]
pub(crate) struct ChunkInfo<C>
where
    C: PartitionChunk + 'static,
{
    /// The schema of the table in just this chunk (the overall table
    /// schema may have more columns if this chunk doesn't have
    /// columns that are in other chunks)
    chunk_table_schema: Schema,
    chunk: Arc<C>,
}

// The #[derive(Clone)] clone was complaining about C not implementing
// Clone, which didn't make sense
// Tracked by https://github.com/rust-lang/rust/issues/26925
impl<C> Clone for ChunkInfo<C>
where
    C: PartitionChunk + 'static,
{
    fn clone(&self) -> Self {
        Self {
            chunk_table_schema: self.chunk_table_schema.clone(),
            chunk: Arc::clone(&self.chunk),
        }
    }
}

impl<C: PartitionChunk> ProviderBuilder<C> {
    pub fn new(table_name: impl Into<String>) -> Self {
        Self {
            table_name: Arc::new(table_name.into()),
            schema_merger: SchemaMerger::new(),
            chunk_and_infos: Vec::new(),
        }
    }

    /// Add a new chunk to this provider
    pub fn add_chunk(self, chunk: Arc<C>, chunk_table_schema: Schema) -> Result<Self> {
        let Self {
            table_name,
            schema_merger,
            mut chunk_and_infos,
        } = self;

        let schema_merger =
            schema_merger
                .merge(chunk_table_schema.clone())
                .context(ChunkSchemaNotCompatible {
                    table_name: table_name.as_ref(),
                })?;

        let chunk_info = ChunkInfo {
            chunk_table_schema,
            chunk,
        };
        chunk_and_infos.push(chunk_info);

        Ok(Self {
            table_name,
            schema_merger,
            chunk_and_infos,
        })
    }

    pub fn build(self) -> Result<ChunkTableProvider<C>> {
        let Self {
            table_name,
            schema_merger,
            chunk_and_infos,
        } = self;

        let iox_schema = schema_merger
            .build()
            .context(InternalNoChunks {
                table_name: table_name.as_ref(),
            })?
            // sort so the columns are always in a consistent order
            .sort_fields_by_name();

        // if the table was reported to exist, it should not be empty
        if chunk_and_infos.is_empty() {
            return InternalNoRowsInTable {
                table_name: table_name.as_ref(),
            }
            .fail();
        }

        Ok(ChunkTableProvider {
            table_name,
            iox_schema,
            chunk_and_infos,
        })
    }
}

/// Implementation of a DataFusion TableProvider in terms of PartitionChunks
///
/// This allows DataFusion to see data from Chunks as a single table, as well as
/// push predicates and selections down to chunks
#[derive(Debug)]
pub struct ChunkTableProvider<C: PartitionChunk + 'static> {
    table_name: Arc<String>,
    /// The IOx schema (wrapper around Arrow Schemaref) for this table
    iox_schema: Schema,
    // The chunks and their corresponding schema
    chunk_and_infos: Vec<ChunkInfo<C>>,
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
        _filters: &[Expr],
        _limit: Option<usize>,
    ) -> std::result::Result<Arc<dyn ExecutionPlan>, DataFusionError> {
        // TODO Here is where predicate pushdown will happen.  To make
        // predicate push down happen, the provider need need to
        // create a Predicate from the Expr .
        //
        // Note that `filters` don't actually need to be evaluated in
        // the scan for the plans to be correct, they are an extra
        // optimization for providers which can offer them
        let predicate = Predicate::default();

        // Figure out the schema of the requested output
        let scan_schema = project_schema(self.arrow_schema(), projection);

        let plan = IOxReadFilterNode::new(
            Arc::clone(&self.table_name),
            scan_schema,
            self.chunk_and_infos.clone(),
            predicate,
        );

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
        Ok(TableProviderFilterPushDown::Unsupported)
    }
}
