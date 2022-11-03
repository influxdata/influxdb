//! Implementation of a DataFusion PhysicalPlan node across partition chunks

use super::adapter::SchemaAdapterStream;
use crate::{exec::IOxSessionContext, QueryChunk, QueryChunkData};
use arrow::datatypes::SchemaRef;
use data_types::TableSummary;
use datafusion::{
    datasource::listing::PartitionedFile,
    error::DataFusionError,
    execution::context::TaskContext,
    physical_plan::{
        execute_stream,
        expressions::PhysicalSortExpr,
        file_format::{FileScanConfig, ParquetExec},
        memory::MemoryStream,
        metrics::{BaselineMetrics, ExecutionPlanMetricsSet, MetricsSet},
        stream::RecordBatchStreamAdapter,
        DisplayFormatType, ExecutionPlan, Partitioning, SendableRecordBatchStream, Statistics,
    },
};
use futures::TryStreamExt;
use observability_deps::tracing::trace;
use parking_lot::Mutex;
use predicate::Predicate;
use schema::Schema;
use std::{collections::HashSet, fmt, sync::Arc};

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

    /// remember all ParquetExecs created by this node so we can pass
    /// along metrics.
    ///
    /// When we use ParquetExec directly (rather
    /// than an IOxReadFilterNode) the metric will be directly
    /// available: <https://github.com/influxdata/influxdb_iox/issues/5897>
    parquet_execs: Mutex<Vec<Arc<ParquetExec>>>,

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
            table_name,
            iox_schema,
            chunks,
            predicate,
            metrics: ExecutionPlanMetricsSet::new(),
            parquet_execs: Mutex::new(vec![]),
            ctx,
        }
    }

    // Meant for testing -- provide input to the inner parquet execs
    // that were created
    fn parquet_execs(&self) -> Vec<Arc<ParquetExec>> {
        self.parquet_execs.lock().to_vec()
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
            parquet_execs: Mutex::new(self.parquet_execs()),
            metrics: ExecutionPlanMetricsSet::new(),
        };

        Ok(Arc::new(new_self))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> datafusion::error::Result<SendableRecordBatchStream> {
        trace!(partition, "Start IOxReadFilterNode::execute");

        let baseline_metrics = BaselineMetrics::new(&self.metrics, partition);

        let schema = self.schema();

        let chunk = Arc::clone(&self.chunks[partition]);

        let chunk_table_schema = chunk.schema();

        // The output selection is all the columns in the schema.
        //
        // However, this chunk may not have all those columns. Thus we
        // restrict the requested selection to the actual columns
        // available, and use SchemaAdapterStream to pad the rest of
        // the columns with NULLs if necessary
        let final_output_column_names: HashSet<_> =
            schema.fields().iter().map(|f| f.name()).collect();
        let projection: Vec<_> = chunk_table_schema
            .iter()
            .enumerate()
            .filter(|(_idx, (_t, field))| final_output_column_names.contains(field.name()))
            .map(|(idx, _)| idx)
            .collect();
        let projection = (!((projection.len() == chunk_table_schema.len())
            && (projection.iter().enumerate().all(|(a, b)| a == *b))))
        .then_some(projection);
        let incomplete_output_schema = projection
            .as_ref()
            .map(|projection| {
                Arc::new(
                    chunk_table_schema
                        .as_arrow()
                        .project(projection)
                        .expect("projection broken"),
                )
            })
            .unwrap_or_else(|| chunk_table_schema.as_arrow());

        let stream = match chunk.data() {
            QueryChunkData::RecordBatches(batches) => {
                let stream = Box::pin(MemoryStream::try_new(
                    batches,
                    incomplete_output_schema,
                    projection,
                )?);
                let adapter = SchemaAdapterStream::try_new(stream, schema, baseline_metrics)
                    .map_err(|e| DataFusionError::Internal(e.to_string()))?;
                Box::pin(adapter) as SendableRecordBatchStream
            }
            QueryChunkData::Parquet(exec_input) => {
                let base_config = FileScanConfig {
                    object_store_url: exec_input.object_store_url,
                    file_schema: Arc::clone(&schema),
                    file_groups: vec![vec![PartitionedFile {
                        object_meta: exec_input.object_meta,
                        partition_values: vec![],
                        range: None,
                        extensions: None,
                    }]],
                    statistics: Statistics::default(),
                    projection: None,
                    limit: None,
                    table_partition_cols: vec![],
                    config_options: context.session_config().config_options(),
                };
                let delete_predicates: Vec<_> = chunk
                    .delete_predicates()
                    .iter()
                    .map(|pred| Arc::new(pred.as_ref().clone().into()))
                    .collect();
                let predicate = self
                    .predicate
                    .clone()
                    .with_delete_predicates(&delete_predicates);
                let metadata_size_hint = None;

                let exec = Arc::new(ParquetExec::new(
                    base_config,
                    predicate.filter_expr(),
                    metadata_size_hint,
                ));

                self.parquet_execs.lock().push(Arc::clone(&exec));

                let stream = RecordBatchStreamAdapter::new(
                    schema,
                    futures::stream::once(execute_stream(exec, context)).try_flatten(),
                );

                // Note: No SchemaAdapterStream required here because `ParquetExec` already creates NULL columns for us.

                Box::pin(stream)
            }
        };

        trace!(partition, "End IOxReadFilterNode::execute");
        Ok(stream)
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
        let mut metrics = self.metrics.clone_inner();

        // copy all metrics from the child parquet_execs
        for exec in self.parquet_execs() {
            if let Some(parquet_metrics) = exec.metrics() {
                for m in parquet_metrics.iter() {
                    metrics.push(Arc::clone(m))
                }
            }
        }

        Some(metrics)
    }

    fn statistics(&self) -> Statistics {
        let mut combined_summary_option: Option<TableSummary> = None;
        for chunk in &self.chunks {
            combined_summary_option = match combined_summary_option {
                None => Some(chunk.summary().as_ref().clone()),
                Some(mut combined_summary) => {
                    combined_summary.update_from(&chunk.summary());
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
