//! Implementation of a DataFusion PhysicalPlan node across partition chunks

use super::adapter::SchemaAdapterStream;
use crate::{exec::IOxSessionContext, QueryChunk};
use arrow::datatypes::SchemaRef;
use data_types::TableSummary;
use datafusion::{
    error::DataFusionError,
    execution::context::TaskContext,
    physical_plan::{
        expressions::PhysicalSortExpr,
        metrics::{BaselineMetrics, ExecutionPlanMetricsSet, MetricsSet},
        DisplayFormatType, ExecutionPlan, Partitioning, SendableRecordBatchStream, Statistics,
    },
};
use observability_deps::tracing::trace;
use predicate::Predicate;
use schema::{selection::Selection, Schema};
use std::{fmt, sync::Arc};

/// Implements the DataFusion physical plan interface
#[derive(Debug)]
pub(crate) struct IOxReadFilterNode {
    table_name: Arc<str>,
    /// The desired output schema (includes selection)
    /// note that the chunk may not have all these columns.
    iox_schema: Arc<Schema>,
    chunks: Vec<Arc<dyn QueryChunk>>,
    predicate: Predicate,
    /// Execution metrics
    metrics: ExecutionPlanMetricsSet,

    // execution context used for tracing
    ctx: IOxSessionContext,
}

impl IOxReadFilterNode {
    /// Create a execution plan node that reads data from `chunks` producing
    /// output according to schema, while applying `predicate` and
    /// returns
    pub fn new(
        ctx: IOxSessionContext,
        table_name: Arc<str>,
        iox_schema: Arc<Schema>,
        chunks: Vec<Arc<dyn QueryChunk>>,
        predicate: Predicate,
    ) -> Self {
        Self {
            ctx,
            table_name,
            iox_schema,
            chunks,
            predicate,
            metrics: ExecutionPlanMetricsSet::new(),
        }
    }
}

impl ExecutionPlan for IOxReadFilterNode {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.iox_schema.as_arrow()
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

        let chunks: Vec<Arc<dyn QueryChunk>> = self.chunks.to_vec();

        // For some reason when I used an automatically derived `Clone` implementation
        // the compiler didn't recognize the trait implementation
        let new_self = Self {
            ctx: self.ctx.child_ctx("with_new_children"),
            table_name: Arc::clone(&self.table_name),
            iox_schema: Arc::clone(&self.iox_schema),
            chunks,
            predicate: self.predicate.clone(),
            metrics: ExecutionPlanMetricsSet::new(),
        };

        Ok(Arc::new(new_self))
    }

    fn execute(
        &self,
        partition: usize,
        _context: Arc<TaskContext>,
    ) -> datafusion::error::Result<SendableRecordBatchStream> {
        trace!(partition, "Start IOxReadFilterNode::execute");

        let baseline_metrics = BaselineMetrics::new(&self.metrics, partition);
        let timer = baseline_metrics.elapsed_compute().timer();

        let schema = self.schema();
        let fields = schema.fields();
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

        let stream = chunk
            .read_filter(
                self.ctx.child_ctx("chunk read_filter"),
                &self.predicate,
                selection,
            )
            .map_err(|e| {
                DataFusionError::Execution(format!(
                    "Error creating scan for table {} chunk {}: {}",
                    self.table_name,
                    chunk.id(),
                    e
                ))
            })?;

        // all CPU time is now done, pass in baseline metrics to adapter
        timer.done();

        let adapter = SchemaAdapterStream::try_new(stream, schema, baseline_metrics)
            .map_err(|e| DataFusionError::Internal(e.to_string()))?;

        trace!(partition, "End IOxReadFilterNode::execute");
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

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }

    fn statistics(&self) -> Statistics {
        let mut combined_summary_option: Option<TableSummary> = None;
        for chunk in &self.chunks {
            combined_summary_option = match combined_summary_option {
                None => Some(
                    chunk
                        .summary()
                        .expect("Chunk should have summary")
                        .as_ref()
                        .clone(),
                ),
                Some(mut combined_summary) => {
                    combined_summary
                        .update_from(&chunk.summary().expect("Chunk should have summary"));
                    Some(combined_summary)
                }
            }
        }

        combined_summary_option
            .map(|combined_summary| {
                crate::statistics::df_from_iox(self.iox_schema.as_ref(), &combined_summary)
            })
            .unwrap_or_default()
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
