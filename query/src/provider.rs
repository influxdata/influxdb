//! Implementation of a DataFusion TableProvider in terms of PartitionChunks

use std::sync::Arc;

use arrow_deps::{
    arrow::datatypes::SchemaRef,
    datafusion::{
        datasource::{
            datasource::{Statistics, TableProviderFilterPushDown},
            TableProvider,
        },
        error::{DataFusionError, Result as DataFusionResult},
        logical_plan::Expr,
        physical_plan::ExecutionPlan,
    },
};

use crate::{predicate::Predicate, PartitionChunk};

use snafu::{OptionExt, Snafu};

mod physical;
use self::physical::IOxReadFilterNode;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display(
        "Chunk schema not compatible for table '{}'. They must be identical. Existing: {:?}, New: {:?}",
        table_name,
        existing_schema,
        chunk_schema
    ))]
    ChunkSchemaNotCompatible {
        table_name: String,
        existing_schema: SchemaRef,
        chunk_schema: SchemaRef,
    },

    #[snafu(display(
        "Internal error: no chunks found in builder for table {:?}",
        table_name,
    ))]
    NoChunks { table_name: String },

    #[snafu(display("No rows found in table {}", table_name))]
    InternalNoRowsInTable { table_name: String },
}
pub type Result<T, E = Error> = std::result::Result<T, E>;

/// Builds a ChunkTableProvider from a series of `PartitionChunks`
/// and ensures the schema across the chunks is compatible and
/// consistent.
#[derive(Debug)]
pub struct ProviderBuilder<C: PartitionChunk + 'static> {
    table_name: Arc<String>,
    schema: Option<SchemaRef>,
    chunks: Vec<Arc<C>>,
}

impl<C: PartitionChunk> ProviderBuilder<C> {
    pub fn new(table_name: impl Into<String>) -> Self {
        Self {
            table_name: Arc::new(table_name.into()),
            schema: None,
            chunks: Vec::new(),
        }
    }

    /// Add a new chunk to this provider
    pub fn add_chunk(mut self, chunk: Arc<C>, chunk_table_schema: SchemaRef) -> Result<Self> {
        self.schema = Some(if let Some(existing_schema) = self.schema.take() {
            self.check_schema(existing_schema, chunk_table_schema)?
        } else {
            chunk_table_schema
        });
        self.chunks.push(chunk);
        Ok(self)
    }

    /// returns Ok(combined_schema) if the schema of chunk is compatible with
    /// `existing_schema`, Err() with why otherwise
    fn check_schema(
        &self,
        existing_schema: SchemaRef,
        chunk_schema: SchemaRef,
    ) -> Result<SchemaRef> {
        // For now, use strict equality. Eventually should union the schema
        if existing_schema != chunk_schema {
            ChunkSchemaNotCompatible {
                table_name: self.table_name.as_ref(),
                existing_schema,
                chunk_schema,
            }
            .fail()
        } else {
            Ok(chunk_schema)
        }
    }

    pub fn build(self) -> Result<ChunkTableProvider<C>> {
        let Self {
            table_name,
            schema,
            chunks,
        } = self;

        let schema = schema.context(NoChunks {
            table_name: table_name.as_ref(),
        })?;

        // if the table was reported to exist, it should not be empty
        if chunks.is_empty() {
            return InternalNoRowsInTable {
                table_name: table_name.as_ref(),
            }
            .fail();
        }

        Ok(ChunkTableProvider {
            table_name,
            schema,
            chunks,
        })
    }
}

/// Implementation of a DataFusion TableProvider in terms of PartitionChunks
///
/// This allows DataFusion to see data from Chunks as a single table, as well as
/// push predicates and selections down to chunks
#[derive(Debug)]
pub struct ChunkTableProvider<C: PartitionChunk> {
    table_name: Arc<String>,
    schema: SchemaRef,
    chunks: Vec<Arc<C>>,
}

impl<C: PartitionChunk + 'static> TableProvider for ChunkTableProvider<C> {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn scan(
        &self,
        projection: &Option<Vec<usize>>,
        _batch_size: usize,
        _filters: &[Expr],
    ) -> std::result::Result<Arc<dyn ExecutionPlan>, DataFusionError> {
        // TODO Here is where predicate pushdown will happen.  To make
        // predicate push down happen, the provider need need to
        // create a Predicate from the Expr .
        //
        // Note that `filters` don't actually need to be evaluated in
        // the scan for the plans to be correct, they are an extra
        // optimization for providers which can offer them
        let predicate = Predicate::default();

        let plan = IOxReadFilterNode::new(
            self.table_name.clone(),
            self.schema.clone(),
            self.chunks.clone(),
            predicate,
            projection.clone(),
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
