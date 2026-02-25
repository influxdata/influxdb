//! Implementation of a DataFusion PhysicalPlan node across partition chunks

use crate::ingester::{IngesterChunk, MetricDecoratorStream};
use crate::statistics::build_statistics_for_chunks;
use crate::statistics::partition_statistics::{
    PartitionStatistics, PartitionedStatistics, project_schema_onto_datasrc_statistics,
};
use crate::{CHUNK_ORDER_COLUMN_NAME, QueryChunk};

use super::adapter::SchemaAdapterStream;
use arrow::datatypes::SchemaRef;
use datafusion::physical_plan::display::ProjectSchemaDisplay;
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType, SchedulingType};
use datafusion::{
    error::DataFusionError,
    execution::context::TaskContext,
    physical_expr::EquivalenceProperties,
    physical_plan::{
        DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PlanProperties,
        SendableRecordBatchStream, Statistics,
        expressions::{Column, PhysicalSortExpr},
        metrics::{BaselineMetrics, ExecutionPlanMetricsSet, MetricsSet},
    },
    scalar::ScalarValue,
};
use itertools::Itertools;
use schema::sort::SortKey;
use std::{collections::HashMap, fmt, sync::Arc};
use tracing::trace;

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

    /// Cache holding plan properties like equivalences, output partitioning, output ordering etc.
    cache: PlanProperties,
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
        let eq_properties = if chunk_order_field.is_some() {
            let output_ordering = [
                // every chunk gets its own partition, so we can claim that the output is ordered
                PhysicalSortExpr {
                    expr: Arc::new(
                        Column::new_with_schema(CHUNK_ORDER_COLUMN_NAME, &schema)
                            .expect("just checked presence of chunk order col"),
                    ),
                    options: Default::default(),
                },
            ];
            EquivalenceProperties::new_with_orderings(Arc::clone(&schema), [output_ordering])
        } else {
            EquivalenceProperties::new(Arc::clone(&schema))
        };

        let cache = Self::compute_properties(eq_properties, &chunks);

        Self {
            chunks,
            schema,
            statistics,
            output_sort_key_memo,
            metrics: ExecutionPlanMetricsSet::new(),
            cache,
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

    /// This function creates the cache object that stores the plan properties such as equivalence properties, partitioning, ordering, etc.
    fn compute_properties(
        eq_properties: EquivalenceProperties,
        chunks: &[Arc<dyn QueryChunk>],
    ) -> PlanProperties {
        let output_partitioning = Partitioning::UnknownPartitioning(chunks.len());

        PlanProperties::new(
            eq_properties,
            output_partitioning,
            EmissionType::Incremental,
            Boundedness::Bounded,
        )
        .with_scheduling_type(SchedulingType::Cooperative)
    }
}

impl PartitionStatistics for RecordBatchesExec {
    fn statistics_by_partition(&self) -> datafusion::common::Result<PartitionedStatistics> {
        // each chunk is a partition
        self.chunks
            .iter()
            .map(|chunk| {
                // some chunks may be missing schema columns
                project_schema_onto_datasrc_statistics(
                    &chunk.stats(),
                    &chunk.schema().as_arrow(),
                    &self.schema,
                )
            })
            .try_collect::<Arc<Statistics>, Vec<_>, _>()
    }
}

impl ExecutionPlan for RecordBatchesExec {
    fn name(&self) -> &str {
        Self::static_name()
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn properties(&self) -> &datafusion::physical_plan::PlanProperties {
        &self.cache
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
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

        // If these are ingester chunks, we want to decorate with the MetricDecorator
        // so we can accumulate ingester <=> querier metrics in the query log/system tables/etc.
        let instrumented_query_chunk = chunk.as_any().downcast_ref::<IngesterChunk>();
        let stream = if let Some(iq_metrics) = instrumented_query_chunk {
            Box::pin(MetricDecoratorStream::new(
                stream,
                iq_metrics.metrics(),
                self.metrics.clone(),
            ))
        } else {
            stream
        };

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

    fn partition_statistics(
        &self,
        partition: Option<usize>,
    ) -> Result<Statistics, DataFusionError> {
        match partition {
            None => Ok(self.statistics.clone()),
            Some(partition) => Ok(self.chunks[partition].stats().as_ref().clone()),
        }
    }
}

impl DisplayAs for RecordBatchesExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match t {
            DisplayFormatType::Default
            | DisplayFormatType::Verbose
            | DisplayFormatType::TreeRender => {
                write!(f, "RecordBatchesExec: chunks={}", self.chunks.len(),)?;
                if !self.schema.fields().is_empty() {
                    write!(f, ", projection={}", ProjectSchemaDisplay(&self.schema))?;
                }
                Ok(())
            }
        }
    }
}
