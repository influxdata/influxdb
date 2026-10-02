//! Metrics reported by the run-optimized streaming merge.

use std::{
    any::Any,
    fmt::{Debug, Display, Formatter},
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
};

use datafusion::physical_plan::metrics::{
    Count, CustomMetricValue, ExecutionPlanMetricsSet, Gauge, MetricBuilder, MetricValue, Time,
};

/// The arithmetic mean of a series of observations.
///
/// The value carries the running sum and the number of observations, so that
/// [`aggregate`](CustomMetricValue::aggregate) can combine two instances into
/// the mean over their pooled observations.
#[derive(Debug, Default)]
pub struct MeanMetricValue {
    sum: AtomicUsize,
    count: AtomicUsize,
}

impl MeanMetricValue {
    /// Record one observation.
    pub fn add(&self, value: usize) {
        self.sum.fetch_add(value, Ordering::Relaxed);
        self.count.fetch_add(1, Ordering::Relaxed);
    }

    /// The running sum of all observations.
    pub fn sum(&self) -> usize {
        self.sum.load(Ordering::Relaxed)
    }

    /// The number of observations.
    pub fn count(&self) -> usize {
        self.count.load(Ordering::Relaxed)
    }

    /// The mean of the observations, rounded to nearest, or zero when there
    /// are no observations.
    pub fn mean(&self) -> usize {
        let count = self.count();
        (self.sum() + count / 2).checked_div(count).unwrap_or(0)
    }
}

impl Display for MeanMetricValue {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} ({}/{})", self.mean(), self.sum(), self.count())
    }
}

impl CustomMetricValue for MeanMetricValue {
    fn new_empty(&self) -> Arc<dyn CustomMetricValue> {
        Arc::new(Self::default())
    }

    fn aggregate(&self, other: Arc<dyn CustomMetricValue>) {
        let Some(other) = other.as_any().downcast_ref::<Self>() else {
            return;
        };
        self.sum.fetch_add(other.sum(), Ordering::Relaxed);
        self.count.fetch_add(other.count(), Ordering::Relaxed);
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_usize(&self) -> usize {
        self.mean()
    }

    fn is_eq(&self, other: &Arc<dyn CustomMetricValue>) -> bool {
        let Some(other) = other.as_any().downcast_ref::<Self>() else {
            return false;
        };
        self.sum() == other.sum() && self.count() == other.count()
    }
}

/// The metrics a [`RunOptimizedStreamingMergeBuilder`] records in addition to
/// the `BaselineMetrics` its DataFusion counterpart takes.
///
/// [`RunOptimizedStreamingMergeBuilder`]:
///     super::RunOptimizedStreamingMergeBuilder
///
/// These describe how well the merge's assumption held: how long the blocks it
/// found were, and whether it gave up and handed over to a row-at-a-time merge.
#[derive(Debug, Clone)]
pub struct RunOptimizedMergeMetrics {
    /// The number of streams being merged.
    pub(super) merge_inputs: Count,

    /// The number of rounds of the merge tournament.
    pub(super) merge_rounds: Count,

    /// The mean number of rows in a block emitted by a round.
    pub(super) mean_block_rows: Arc<MeanMetricValue>,

    /// One when the merge degraded and handed its remaining inputs to
    /// DataFusion's row-at-a-time streaming merge.
    pub(super) fell_back_to_streaming_merge: Count,

    /// Time spent in the merge tournament.
    pub(super) merge_time: Time,

    /// Time spent copying rows into output batches.
    pub(super) coalesce_time: Time,

    /// The peak size of the merge's memory reservation.
    pub(super) mem_used: Gauge,
}

impl RunOptimizedMergeMetrics {
    /// Register this partition's metrics against `metrics`.
    pub fn new(metrics: &ExecutionPlanMetricsSet, partition: usize) -> Self {
        let mean_block_rows = Arc::new(MeanMetricValue::default());
        MetricBuilder::new(metrics)
            .with_partition(partition)
            .build(MetricValue::Custom {
                name: "mean_block_rows".into(),
                value: Arc::clone(&mean_block_rows) as Arc<dyn CustomMetricValue>,
            });

        Self {
            merge_inputs: MetricBuilder::new(metrics).counter("merge_inputs", partition),
            merge_rounds: MetricBuilder::new(metrics).counter("merge_rounds", partition),
            mean_block_rows,
            fell_back_to_streaming_merge: MetricBuilder::new(metrics)
                .counter("fell_back_to_streaming_merge", partition),
            merge_time: MetricBuilder::new(metrics).subset_time("merge_time", partition),
            coalesce_time: MetricBuilder::new(metrics).subset_time("coalesce_time", partition),
            mem_used: MetricBuilder::new(metrics).mem_used(partition),
        }
    }
}

impl Default for RunOptimizedMergeMetrics {
    /// Metrics that are recorded but never reported, for a caller that does not
    /// want them.
    fn default() -> Self {
        Self::new(&ExecutionPlanMetricsSet::new(), 0)
    }
}
