//! Implementation of a DataFusion PhysicalPlan node across partition chunks

use crate::QueryChunk;

use super::adapter::SchemaAdapterStream;
use arrow::{datatypes::SchemaRef, record_batch::RecordBatch};
use data_types::TableSummary;
use datafusion::{
    error::DataFusionError,
    execution::context::TaskContext,
    physical_plan::{
        expressions::PhysicalSortExpr,
        memory::MemoryStream,
        metrics::{BaselineMetrics, ExecutionPlanMetricsSet, MetricsSet},
        DisplayFormatType, ExecutionPlan, Partitioning, SendableRecordBatchStream, Statistics,
    },
};
use observability_deps::tracing::trace;
use std::{collections::HashSet, fmt, sync::Arc};

/// Implements the DataFusion physical plan interface for [`RecordBatch`]es with automatic projection and NULL-column creation.
#[derive(Debug)]
pub(crate) struct RecordBatchesExec {
    /// Chunks contained in this exec node.
    chunks: Vec<(Arc<dyn QueryChunk>, Vec<RecordBatch>)>,

    /// Overall schema.
    schema: SchemaRef,

    /// Execution metrics
    metrics: ExecutionPlanMetricsSet,

    /// Statistics over all batches.
    statistics: Statistics,
}

impl RecordBatchesExec {
    pub fn new(chunks: impl IntoIterator<Item = Arc<dyn QueryChunk>>, schema: SchemaRef) -> Self {
        let mut combined_summary_option: Option<TableSummary> = None;

        let chunks: Vec<_> = chunks
            .into_iter()
            .map(|chunk| {
                let batches = chunk
                    .data()
                    .into_record_batches()
                    .expect("chunk must have record batches");
                let summary = chunk.summary();
                match combined_summary_option.as_mut() {
                    None => {
                        combined_summary_option = Some(summary.as_ref().clone());
                    }
                    Some(combined_summary) => {
                        combined_summary.update_from(&summary);
                    }
                }

                (chunk, batches)
            })
            .collect();

        let statistics = combined_summary_option
            .map(|combined_summary| crate::statistics::df_from_iox(&schema, &combined_summary))
            .unwrap_or_default();

        Self {
            chunks,
            schema,
            statistics,
            metrics: ExecutionPlanMetricsSet::new(),
        }
    }

    /// Chunks that make up this node.
    pub fn chunks(&self) -> impl Iterator<Item = &Arc<dyn QueryChunk>> {
        self.chunks.iter().map(|(chunk, _batches)| chunk)
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
        // TODO ??
        None
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

        let (chunk, batches) = &self.chunks[partition];
        let part_schema = chunk.schema().as_arrow();

        // The output selection is all the columns in the schema.
        //
        // However, this chunk may not have all those columns. Thus we
        // restrict the requested selection to the actual columns
        // available, and use SchemaAdapterStream to pad the rest of
        // the columns with NULLs if necessary
        let final_output_column_names: HashSet<_> =
            schema.fields().iter().map(|f| f.name()).collect();
        let projection: Vec<_> = part_schema
            .fields()
            .iter()
            .enumerate()
            .filter(|(_idx, field)| final_output_column_names.contains(field.name()))
            .map(|(idx, _)| idx)
            .collect();
        let projection = (!((projection.len() == part_schema.fields().len())
            && (projection.iter().enumerate().all(|(a, b)| a == *b))))
        .then_some(projection);
        let incomplete_output_schema = projection
            .as_ref()
            .map(|projection| Arc::new(part_schema.project(projection).expect("projection broken")))
            .unwrap_or(part_schema);

        let stream = Box::pin(MemoryStream::try_new(
            batches.clone(),
            incomplete_output_schema,
            projection,
        )?);
        let adapter = Box::pin(
            SchemaAdapterStream::try_new(stream, schema, baseline_metrics)
                .map_err(|e| DataFusionError::Internal(e.to_string()))?,
        );

        trace!(partition, "End RecordBatchesExec::execute");
        Ok(adapter)
    }

    fn fmt_as(&self, t: DisplayFormatType, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let total_groups = self.chunks.len();

        let total_batches = self
            .chunks
            .iter()
            .map(|(_chunk, batches)| batches.len())
            .sum::<usize>();

        let total_rows = self
            .chunks
            .iter()
            .flat_map(|(_chunk, batches)| batches.iter().map(|batch| batch.num_rows()))
            .sum::<usize>();

        match t {
            DisplayFormatType::Default => {
                write!(
                    f,
                    "RecordBatchesExec: batches_groups={total_groups} batches={total_batches} total_rows={total_rows}",
                )
            }
        }
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }

    fn statistics(&self) -> Statistics {
        self.statistics.clone()
    }
}
