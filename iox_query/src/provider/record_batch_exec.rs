//! Implementation of a DataFusion PhysicalPlan node across partition chunks

use crate::statistics::build_statistics_for_chunks;
use crate::{QueryChunk, CHUNK_ORDER_COLUMN_NAME};

use super::adapter::SchemaAdapterStream;
use arrow::datatypes::SchemaRef;
use datafusion::physical_plan::display::ProjectSchemaDisplay;
use datafusion::{
    error::DataFusionError,
    execution::context::TaskContext,
    physical_plan::{
        expressions::{Column, PhysicalSortExpr},
        metrics::{BaselineMetrics, ExecutionPlanMetricsSet, MetricsSet},
        DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, SendableRecordBatchStream,
        Statistics,
    },
    scalar::ScalarValue,
};
use observability_deps::tracing::trace;
use schema::sort::SortKey;
use std::{collections::HashMap, fmt, sync::Arc};

/// Implements the DataFusion physical plan interface for [`RecordBatch`]es with automatic projection and NULL-column creation.
///
///
/// [`RecordBatch`]: arrow::record_batch::RecordBatch
#[derive(Debug)]
pub(crate) struct RecordBatchesExec {
    /// Chunks contained in this exec node.
    chunks: Vec<Arc<dyn QueryChunk>>,

    /// Overall schema.
    schema: SchemaRef,

    /// Execution metrics
    metrics: ExecutionPlanMetricsSet,

    /// Statistics over all batches.
    statistics: Statistics,

    /// Sort key that was passed to [`chunks_to_physical_nodes`].
    ///
    /// This is NOT used to set the output ordering. It is only here to recover this information later.
    ///
    ///
    /// [`chunks_to_physical_nodes`]: super::physical::chunks_to_physical_nodes
    output_sort_key_memo: Option<SortKey>,

    /// Output ordering.
    output_ordering: Option<Vec<PhysicalSortExpr>>,
}

impl RecordBatchesExec {
    pub fn new(
        chunks: impl IntoIterator<Item = Arc<dyn QueryChunk>>,
        schema: SchemaRef,
        output_sort_key_memo: Option<SortKey>,
    ) -> Self {
        let chunks: Vec<_> = chunks.into_iter().collect();
        let statistics = build_statistics_for_chunks(&chunks, Arc::clone(&schema));

        let chunk_order_field = schema.field_with_name(CHUNK_ORDER_COLUMN_NAME).ok();
        let output_ordering = if chunk_order_field.is_some() {
            Some(vec![
                // every chunk gets its own partition, so we can claim that the output is ordered
                PhysicalSortExpr {
                    expr: Arc::new(
                        Column::new_with_schema(CHUNK_ORDER_COLUMN_NAME, &schema)
                            .expect("just checked presence of chunk order col"),
                    ),
                    options: Default::default(),
                },
            ])
        } else {
            None
        };

        Self {
            chunks,
            schema,
            statistics,
            output_sort_key_memo,
            output_ordering,
            metrics: ExecutionPlanMetricsSet::new(),
        }
    }

    /// Chunks that make up this node.
    pub fn chunks(&self) -> impl Iterator<Item = &Arc<dyn QueryChunk>> {
        self.chunks.iter()
    }

    /// Sort key that was passed to [`chunks_to_physical_nodes`].
    ///
    /// This is NOT used to set the output ordering. It is only here to recover this information later.
    ///
    ///
    /// [`chunks_to_physical_nodes`]: super::physical::chunks_to_physical_nodes
    pub fn output_sort_key_memo(&self) -> Option<&SortKey> {
        self.output_sort_key_memo.as_ref()
    }
}

impl ExecutionPlan for RecordBatchesExec {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn output_partitioning(&self) -> Partitioning {
        Partitioning::UnknownPartitioning(self.chunks.len())
    }

    fn output_ordering(&self) -> Option<&[PhysicalSortExpr]> {
        self.output_ordering.as_deref()
    }

    fn children(&self) -> Vec<Arc<dyn ExecutionPlan>> {
        // no inputs
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        assert!(children.is_empty(), "no children expected in iox plan");

        Ok(self)
    }

    fn execute(
        &self,
        partition: usize,
        _context: Arc<TaskContext>,
    ) -> datafusion::error::Result<SendableRecordBatchStream> {
        trace!(partition, "Start RecordBatchesExec::execute");

        let baseline_metrics = BaselineMetrics::new(&self.metrics, partition);

        let schema = self.schema();

        let chunk = &self.chunks[partition];

        let stream = match chunk.data() {
            crate::QueryChunkData::RecordBatches(stream) => stream,
            crate::QueryChunkData::Parquet(_) => {
                return Err(DataFusionError::Execution(String::from(
                    "chunk must contain record batches",
                )));
            }
        };
        let virtual_columns = HashMap::from([(
            CHUNK_ORDER_COLUMN_NAME,
            ScalarValue::from(chunk.order().get()),
        )]);
        let adapter = Box::pin(
            SchemaAdapterStream::try_new(stream, schema, &virtual_columns, baseline_metrics)
                .map_err(|e| DataFusionError::External(Box::new(e)))?,
        );

        trace!(partition, "End RecordBatchesExec::execute");
        Ok(adapter)
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }

    fn statistics(&self) -> Result<Statistics, DataFusionError> {
        Ok(self.statistics.clone())
    }
}

impl DisplayAs for RecordBatchesExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(f, "RecordBatchesExec: chunks={}", self.chunks.len(),)?;
                if !self.schema.fields().is_empty() {
                    write!(f, ", projection={}", ProjectSchemaDisplay(&self.schema))?;
                }
                Ok(())
            }
        }
    }
}
