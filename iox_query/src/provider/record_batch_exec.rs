//! Implementation of a DataFusion PhysicalPlan node across partition chunks

use crate::{statistics::DFStatsAggregator, QueryChunk, CHUNK_ORDER_COLUMN_NAME};

use super::adapter::SchemaAdapterStream;
use arrow::datatypes::{Schema, SchemaRef};
use datafusion::{
    error::DataFusionError,
    execution::context::TaskContext,
    physical_plan::{
        expressions::{Column, PhysicalSortExpr},
        memory::MemoryStream,
        metrics::{BaselineMetrics, ExecutionPlanMetricsSet, MetricsSet},
        ColumnStatistics, DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning,
        SendableRecordBatchStream, Statistics,
    },
    scalar::ScalarValue,
};
use observability_deps::tracing::trace;
use schema::sort::SortKey;
use std::{
    collections::{HashMap, HashSet},
    fmt,
    sync::Arc,
};

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
        let chunk_order_field = schema.field_with_name(CHUNK_ORDER_COLUMN_NAME).ok();
        let chunk_order_only_schema =
            chunk_order_field.map(|field| Schema::new(vec![field.clone()]));

        let chunks: Vec<_> = chunks.into_iter().collect();

        let statistics = chunks
            .iter()
            .fold(DFStatsAggregator::new(&schema), |mut agg, chunk| {
                agg.update(&chunk.stats(), chunk.schema().as_arrow().as_ref());

                if let Some(schema) = chunk_order_only_schema.as_ref() {
                    let order = chunk.order().get();
                    let order = ScalarValue::from(order);
                    agg.update(
                        &Statistics {
                            num_rows: Some(0),
                            total_byte_size: Some(0),
                            column_statistics: Some(vec![ColumnStatistics {
                                null_count: Some(0),
                                max_value: Some(order.clone()),
                                min_value: Some(order),
                                distinct_count: Some(1),
                            }]),
                            is_exact: true,
                        },
                        schema,
                    );
                }

                agg
            })
            .build();

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

        let batches = chunk.data().into_record_batches().ok_or_else(|| {
            DataFusionError::Execution(String::from("chunk must contain record batches"))
        })?;

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

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }

    fn statistics(&self) -> Statistics {
        self.statistics.clone()
    }
}

impl DisplayAs for RecordBatchesExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(f, "RecordBatchesExec: chunks={}", self.chunks.len(),)
            }
        }
    }
}
