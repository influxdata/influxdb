//! Implementation of a DataFusion PhysicalPlan node across partition chunks

use std::{fmt, sync::Arc};

use arrow::datatypes::SchemaRef;
use datafusion::{
    error::DataFusionError,
    physical_plan::{DisplayFormatType, ExecutionPlan, Partitioning, SendableRecordBatchStream},
};
use internal_types::{schema::Schema, selection::Selection};

use crate::{predicate::Predicate, QueryChunk};

use async_trait::async_trait;

use super::adapter::SchemaAdapterStream;

/// Implements the DataFusion physical plan interface
#[derive(Debug)]
pub(crate) struct IOxReadFilterNode<C: QueryChunk + 'static> {
    table_name: Arc<str>,
    /// The desired output schema (includes selection_
    /// note that the chunk may not have all these columns.
    schema: SchemaRef,
    chunks: Vec<Arc<C>>,
    predicate: Predicate,
}

impl<C: QueryChunk + 'static> IOxReadFilterNode<C> {
    /// Create a execution plan node that reads data from `chunks` producing
    /// output according to schema, while applying `predicate` and
    /// returns
    pub fn new(
        table_name: Arc<str>,
        schema: SchemaRef,
        chunks: Vec<Arc<C>>,
        predicate: Predicate,
    ) -> Self {
        Self {
            table_name,
            schema,
            chunks,
            predicate,
        }
    }
}

#[async_trait]
impl<C: QueryChunk + 'static> ExecutionPlan for IOxReadFilterNode<C> {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn output_partitioning(&self) -> Partitioning {
        Partitioning::UnknownPartitioning(self.chunks.len())
    }

    fn children(&self) -> Vec<Arc<dyn ExecutionPlan>> {
        // no inputs
        vec![]
    }

    fn with_new_children(
        &self,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        assert!(children.is_empty(), "no children expected in iox plan");

        let chunks: Vec<Arc<C>> = self.chunks.to_vec();

        // For some reason when I used an automatically derived `Clone` implementation
        // the compiler didn't recognize the trait implementation
        let new_self = Self {
            table_name: Arc::clone(&self.table_name),
            schema: Arc::clone(&self.schema),
            chunks,
            predicate: self.predicate.clone(),
        };

        Ok(Arc::new(new_self))
    }

    async fn execute(
        &self,
        partition: usize,
    ) -> datafusion::error::Result<SendableRecordBatchStream> {
        let fields = self.schema.fields();
        let selection_cols = fields.iter().map(|f| f.name() as &str).collect::<Vec<_>>();

        let chunk = Arc::clone(&self.chunks[partition]);

        let chunk_table_schema = chunk.schema();

        // The output selection is all the columns in the schema.
        //
        // However, this chunk may not have all those columns. Thus we
        // restrict the requested selection to the actual columns
        // available, and use SchemaAdapterStream to pad the rest of
        // the columns with NULLs if necessary
        let selection_cols = restrict_selection(selection_cols, &chunk_table_schema);
        let selection = Selection::Some(&selection_cols);

        let stream = chunk.read_filter(&self.predicate, selection).map_err(|e| {
            DataFusionError::Execution(format!(
                "Error creating scan for table {} chunk {}: {}",
                self.table_name,
                chunk.id(),
                e
            ))
        })?;

        let adapter = SchemaAdapterStream::try_new(stream, Arc::clone(&self.schema))
            .map_err(|e| DataFusionError::Internal(e.to_string()))?;

        Ok(Box::pin(adapter))
    }

    fn fmt_as(&self, t: DisplayFormatType, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match t {
            DisplayFormatType::Default => {
                write!(
                    f,
                    "IOxReadFilterNode: table_name={}, chunks={} predicate={}",
                    self.table_name,
                    self.chunks.len(),
                    self.predicate,
                )
            }
        }
    }
}

/// Removes any columns that are not present in schema, returning a possibly
/// restricted set of columns
fn restrict_selection<'a>(
    selection_cols: Vec<&'a str>,
    chunk_table_schema: &'a Schema,
) -> Vec<&'a str> {
    let arrow_schema = chunk_table_schema.as_arrow();

    selection_cols
        .into_iter()
        .filter(|col| arrow_schema.fields().iter().any(|f| f.name() == col))
        .collect()
}
