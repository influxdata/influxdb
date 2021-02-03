//! Implementation of a DataFusion PhysicalPlan node across partition chunks

use std::sync::Arc;

use arrow_deps::{
    arrow::datatypes::SchemaRef,
    datafusion::{
        error::DataFusionError,
        physical_plan::{ExecutionPlan, Partitioning, SendableRecordBatchStream},
    },
};
use data_types::selection::Selection;

use crate::{predicate::Predicate, PartitionChunk};

use async_trait::async_trait;

/// Implements the DataFusion physical plan interface
#[derive(Debug)]
pub struct IOxReadFilterNode<C: PartitionChunk + 'static> {
    table_name: Arc<String>,
    schema: SchemaRef,
    chunks: Vec<Arc<C>>,
    predicate: Predicate,
    projection: Option<Vec<usize>>,
}

impl<C: PartitionChunk + 'static> IOxReadFilterNode<C> {
    pub fn new(
        table_name: Arc<String>,
        schema: SchemaRef,
        chunks: Vec<Arc<C>>,
        predicate: Predicate,
        projection: Option<Vec<usize>>,
    ) -> Self {
        Self {
            table_name,
            schema,
            chunks,
            predicate,
            projection,
        }
    }
}

#[async_trait]
impl<C: PartitionChunk + 'static> ExecutionPlan for IOxReadFilterNode<C> {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
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
    ) -> arrow_deps::datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        assert!(children.is_empty(), "no children expected in iox plan");

        // For some reason when I used an automatically derived `Clone` implementation
        // the compiler didn't recognize the trait implementation
        let new_self = Self {
            table_name: self.table_name.clone(),
            schema: self.schema.clone(),
            chunks: self.chunks.clone(),
            predicate: self.predicate.clone(),
            projection: self.projection.clone(),
        };

        Ok(Arc::new(new_self))
    }

    async fn execute(
        &self,
        partition: usize,
    ) -> arrow_deps::datafusion::error::Result<SendableRecordBatchStream> {
        let fields = self.schema.fields();
        let selection_cols = self.projection.as_ref().map(|projection| {
            projection
                .iter()
                .map(|&index| fields[index].name() as &str)
                .collect::<Vec<_>>()
        });

        let selection = if let Some(selection_cols) = selection_cols.as_ref() {
            Selection::Some(&selection_cols)
        } else {
            Selection::All
        };

        let chunk = &self.chunks[partition];
        chunk
            .read_filter(&self.table_name, &self.predicate, selection)
            .await
            .map_err(|e| {
                DataFusionError::Execution(format!(
                    "Error creating scan for table {} chunk {}: {}",
                    self.table_name,
                    chunk.id(),
                    e
                ))
            })
    }
}
