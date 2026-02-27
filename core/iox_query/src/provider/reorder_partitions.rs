use std::{any::Any, sync::Arc};

use arrow::datatypes::SchemaRef;
use datafusion::{
    common::Statistics,
    error::Result,
    execution::{SendableRecordBatchStream, TaskContext},
    physical_expr::{EquivalenceProperties, LexOrdering, LexRequirement, OrderingRequirements},
    physical_plan::{
        DisplayAs, DisplayFormatType, Distribution, ExecutionPlan, ExecutionPlanProperties,
        PlanProperties,
        metrics::{ExecutionPlanMetricsSet, MetricsSet},
    },
};

/// ReorderPartitionsExec takes a single input with 1 or more partitioned streams,
/// and maps an output partition to a different input partition.
///
/// ```text
/// ┌─────────────────────────┐
/// │ ┌───┬───┬───┬───┐       │
/// │ │ C │ D │ E │ F │       │──┐
/// │ └───┴───┴───┴───┘       │  │
/// └─────────────────────────┘  │  ┌───────────────────┐    ┌───────────────────┐     ┌───────────────┐
///   Partition Stream 0         │  │                   │    │                   │     │ ┌───┬───┐     │
///                              ├─▶│  ReorderPartition │───▶│  ProgressiveEval  │───▶ │ │ A │ B │ ... │
///                              │  │   (partition=1)   │    │   (partition=0)   │     │ └───┴─▲─┘     │
///                              │  │                   │    │                   │     │       │       │
/// ┌─────────────────────────┐  │  └───────────────────┘    └───────────────────┘     └───────┴───────┘
/// │ ┌───┬───┐               │  │
/// │ │ A │ B │               │──┘
/// │ └───┴───┘               │
/// └─────────────────────────┘
///   Partition Stream 1
///
/// ```
#[derive(Debug, Clone)]
pub(crate) struct ReorderPartitionsExec {
    /// Input plan
    input: Arc<dyn ExecutionPlan>,

    /// How the output partition index gets mapped to the input partition index.
    ///
    /// `mapped_partition_indices[out] = in`
    mapped_partition_indices: Vec<usize>,

    /// Output ordering enforced by partition mapping.
    output_ordering: LexOrdering,

    /// Cache holding plan properties like equivalences, output partitioning, output ordering etc.
    cache: PlanProperties,

    /// Execution metrics
    metrics: ExecutionPlanMetricsSet,
}

impl ReorderPartitionsExec {
    /// Create a new reorder partitions execution plan
    pub fn new(
        input: Arc<dyn ExecutionPlan>,
        mapped_partition_indices: Vec<usize>,
        output_ordering: LexOrdering,
    ) -> Result<Self> {
        let cache = Self::compute_properties(&input, &output_ordering)?;
        Ok(Self {
            input,
            mapped_partition_indices,
            output_ordering,
            cache,
            metrics: ExecutionPlanMetricsSet::new(),
        })
    }

    /// Input
    pub fn input(&self) -> &Arc<dyn ExecutionPlan> {
        &self.input
    }

    /// Mapping of indices.
    /// `mapped_partition_indices[out] = in`
    pub fn mapped_partition_indices(&self) -> &[usize] {
        &self.mapped_partition_indices
    }

    /// This function creates the cache object that stores the plan properties such as equivalence properties, partitioning, ordering, etc.
    fn compute_properties(
        input: &Arc<dyn ExecutionPlan>,
        output_ordering: &LexOrdering,
    ) -> Result<PlanProperties> {
        let input_constants = input
            .properties()
            .equivalence_properties()
            .constants()
            .to_owned();
        let mut eq_properties = EquivalenceProperties::new(input.schema());
        eq_properties.add_constants(input_constants)?;
        eq_properties.add_ordering(output_ordering.iter().cloned());

        Ok(PlanProperties::new(
            eq_properties,
            input.output_partitioning().clone(),
            input.pipeline_behavior(),
            input.boundedness(),
        ))
    }
}

impl ExecutionPlan for ReorderPartitionsExec {
    fn name(&self) -> &str {
        Self::static_name()
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.input.schema()
    }

    fn properties(&self) -> &PlanProperties {
        &self.cache
    }

    fn required_input_distribution(&self) -> Vec<Distribution> {
        vec![Distribution::UnspecifiedDistribution]
    }

    fn required_input_ordering(&self) -> Vec<Option<OrderingRequirements>> {
        let input_ordering = self
            .input()
            .properties()
            .output_ordering()
            .cloned()
            .map(LexRequirement::from)
            .map(OrderingRequirements::new);

        vec![input_ordering]
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(Self::new(
            Arc::<dyn ExecutionPlan>::clone(&children[0]),
            self.mapped_partition_indices.clone(),
            self.output_ordering.clone(),
        )?))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let mapped_input_partition = self.mapped_partition_indices[partition];
        self.input.execute(mapped_input_partition, context)
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }

    fn statistics(&self) -> Result<Statistics> {
        self.input.partition_statistics(None)
    }
}

impl DisplayAs for ReorderPartitionsExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default
            | DisplayFormatType::Verbose
            | DisplayFormatType::TreeRender => {
                write!(f, "ReorderPartitionsExec: ")?;
                write!(
                    f,
                    "mapped_partition_indices=[{}]",
                    self.mapped_partition_indices
                        .iter()
                        .map(|r| r.to_string())
                        .collect::<Vec<_>>()
                        .join(", ")
                )?;

                Ok(())
            }
        }
    }
}
