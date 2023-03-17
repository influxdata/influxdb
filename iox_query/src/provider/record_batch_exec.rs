//! Implementation of a DataFusion PhysicalPlan node across partition chunks

use crate::{QueryChunk, CHUNK_ORDER_COLUMN_NAME};

use super::adapter::SchemaAdapterStream;
use arrow::{datatypes::SchemaRef, record_batch::RecordBatch};
use data_types::{ColumnSummary, InfluxDbType, TableSummary};
use datafusion::{
    error::DataFusionError,
    execution::context::TaskContext,
    physical_plan::{
        expressions::{Column, PhysicalSortExpr},
        memory::MemoryStream,
        metrics::{BaselineMetrics, ExecutionPlanMetricsSet, MetricsSet},
        DisplayFormatType, ExecutionPlan, Partitioning, SendableRecordBatchStream, Statistics,
    },
    scalar::ScalarValue,
};
use observability_deps::tracing::trace;
use schema::sort::SortKey;
use std::{
    borrow::Cow,
    collections::{HashMap, HashSet},
    fmt,
    num::NonZeroU64,
    sync::Arc,
};

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
        let has_chunk_order_col = schema.field_with_name(CHUNK_ORDER_COLUMN_NAME).is_ok();

        let chunks: Vec<_> = chunks
            .into_iter()
            .map(|chunk| {
                let batches = chunk
                    .data()
                    .into_record_batches()
                    .expect("chunk must have record batches");

                (chunk, batches)
            })
            .collect();

        let statistics = chunks
            .iter()
            .fold(
                None,
                |mut combined_summary: Option<TableSummary>, (chunk, _batches)| {
                    let summary = chunk.summary();

                    let summary = if has_chunk_order_col {
                        // add chunk order column
                        let order = chunk.order().get();
                        let summary = TableSummary {
                            columns: summary
                                .columns
                                .iter()
                                .cloned()
                                .chain(std::iter::once(ColumnSummary {
                                    name: CHUNK_ORDER_COLUMN_NAME.to_owned(),
                                    influxdb_type: InfluxDbType::Field,
                                    stats: data_types::Statistics::I64(data_types::StatValues {
                                        min: Some(order),
                                        max: Some(order),
                                        total_count: summary.total_count(),
                                        null_count: Some(0),
                                        distinct_count: Some(NonZeroU64::new(1).unwrap()),
                                    }),
                                }))
                                .collect(),
                        };

                        Cow::Owned(summary)
                    } else {
                        Cow::Borrowed(summary.as_ref())
                    };

                    match combined_summary.as_mut() {
                        None => {
                            combined_summary = Some(summary.into_owned());
                        }
                        Some(combined_summary) => {
                            combined_summary.update_from(&summary);
                        }
                    }

                    combined_summary
                },
            )
            .map(|combined_summary| crate::statistics::df_from_iox(&schema, &combined_summary))
            .unwrap_or_default();

        let output_ordering = if has_chunk_order_col {
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
        self.chunks.iter().map(|(chunk, _batches)| chunk)
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
        let virtual_columns = HashMap::from([(
            CHUNK_ORDER_COLUMN_NAME,
            ScalarValue::from(chunk.order().get()),
        )]);
        let adapter = Box::pin(
            SchemaAdapterStream::try_new(stream, schema, &virtual_columns, baseline_metrics)
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
