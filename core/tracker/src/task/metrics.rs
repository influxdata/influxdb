use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
    time::Duration,
};

use crate::{AbstractTaskRegistry, TaskId, TaskRegistration, TaskTracker};

/// Function that extracts metric attributes from job metadata.
///
/// Note that some attributes like `"status"` will automatically be set/overwritten to ensure a certain consistency.
pub type FAttributes<T> = Box<dyn FnMut(&T) -> metric::Attributes + Send>;

/// Wraps a task registry and adds metrics.
#[derive(Debug)]
pub struct TaskRegistryWithMetrics<T, R>
where
    T: std::fmt::Debug + Send + Sync,
    R: AbstractTaskRegistry<T>,
{
    registry: R,
    metrics: RegistryMetrics<T>,
}

impl<T, R> TaskRegistryWithMetrics<T, R>
where
    T: std::fmt::Debug + Send + Sync,
    R: AbstractTaskRegistry<T>,
{
    pub fn new(
        inner: R,
        metric_registry: Arc<metric::Registry>,
        f_attributes: FAttributes<T>,
    ) -> Self {
        Self {
            registry: inner,
            metrics: RegistryMetrics::new(metric_registry, f_attributes),
        }
    }
}

impl<T, R> AbstractTaskRegistry<T> for TaskRegistryWithMetrics<T, R>
where
    T: std::fmt::Debug + Send + Sync,
    R: AbstractTaskRegistry<T>,
{
    fn register(&mut self, metadata: T) -> (TaskTracker<T>, TaskRegistration) {
        self.registry.register(metadata)
    }

    fn get(&self, id: TaskId) -> Option<TaskTracker<T>> {
        self.registry.get(id)
    }

    fn tracked_len(&self) -> usize {
        self.registry.tracked_len()
    }

    fn tracked(&self) -> Vec<TaskTracker<T>> {
        self.registry.tracked()
    }

    fn running(&self) -> Vec<TaskTracker<T>> {
        self.registry.running()
    }

    fn reclaim(&mut self) -> Vec<TaskTracker<T>> {
        let pruned = self.registry.reclaim();
        self.metrics.update(&self.registry, &pruned);
        pruned
    }
}

struct RegistryMetrics<T>
where
    T: std::fmt::Debug + Send + Sync,
{
    active_gauge: metric::Metric<metric::U64Gauge>,

    // Accumulates jobs that were pruned from the limited job history. This is required to not saturate the completed
    // count after a while.
    completed_accu: BTreeMap<metric::Attributes, u64>,

    cpu_time_histogram: metric::Metric<metric::DurationHistogram>,
    wall_time_histogram: metric::Metric<metric::DurationHistogram>,

    // Set of jobs for which we already accounted data but that are still tracked. We must not account these
    // jobs a second time.
    completed_but_still_tracked: BTreeSet<TaskId>,

    f_attributes: FAttributes<T>,
}

impl<T> std::fmt::Debug for RegistryMetrics<T>
where
    T: std::fmt::Debug + Send + Sync,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RegistryMetrics")
            .field("active_gauge", &self.active_gauge)
            .field("completed_accu", &self.completed_accu)
            .field("cpu_time_histogram", &self.cpu_time_histogram)
            .field("wall_time_histogram", &self.wall_time_histogram)
            .field(
                "completed_but_still_tracked",
                &self.completed_but_still_tracked,
            )
            .finish_non_exhaustive()
    }
}

impl<T> RegistryMetrics<T>
where
    T: std::fmt::Debug + Send + Sync,
{
    fn new(metric_registry: Arc<metric::Registry>, f_attributes: FAttributes<T>) -> Self {
        Self {
            active_gauge: metric_registry
                .register_metric("influxdb_iox_job_count", "Number of known jobs"),
            completed_accu: Default::default(),
            cpu_time_histogram: metric_registry.register_metric_with_options(
                "influxdb_iox_job_completed_cpu",
                "CPU time of of completed jobs",
                Self::duration_histogram_options,
            ),
            wall_time_histogram: metric_registry.register_metric_with_options(
                "influxdb_iox_job_completed_wall",
                "Wall time of of completed jobs",
                Self::duration_histogram_options,
            ),
            completed_but_still_tracked: Default::default(),
            f_attributes,
        }
    }

    fn duration_histogram_options() -> metric::DurationHistogramOptions {
        metric::DurationHistogramOptions::new(vec![
            Duration::from_millis(10),
            Duration::from_millis(100),
            Duration::from_secs(1),
            Duration::from_secs(10),
            Duration::from_secs(100),
            metric::DURATION_MAX,
        ])
    }

    fn update<R>(&mut self, registry: &R, pruned: &[TaskTracker<T>])
    where
        R: AbstractTaskRegistry<T>,
    {
        // scan pruned jobs
        for job in pruned {
            assert!(job.is_complete());
            if self.completed_but_still_tracked.remove(&job.id()) {
                // already accounted
                continue;
            }

            self.process_completed_job(job);
        }

        // scan current completed jobs
        let (tracked_completed, tracked_other): (Vec<_>, Vec<_>) = registry
            .tracked()
            .into_iter()
            .partition(|job| job.is_complete());
        for job in tracked_completed {
            if !self.completed_but_still_tracked.insert(job.id()) {
                // already accounted
                continue;
            }

            self.process_completed_job(&job);
        }

        // scan current not-completed jobs
        let mut accumulator: BTreeMap<metric::Attributes, u64> = self.completed_accu.clone();
        for job in tracked_other {
            let attr = self.job_to_gauge_attributes(&job);
            accumulator
                .entry(attr.clone())
                .and_modify(|x| *x += 1)
                .or_insert(1);
        }

        // emit metric
        for (attr, count) in accumulator {
            self.active_gauge.recorder(attr).set(count);
        }
    }

    fn job_to_gauge_attributes(&mut self, job: &TaskTracker<T>) -> metric::Attributes
    where
        T: Send + Sync,
    {
        let metadata = job.metadata();
        let status = job.get_status();

        let mut attributes = (self.f_attributes)(metadata);
        attributes.insert(
            "status",
            status
                .result()
                .map(|result| result.name())
                .unwrap_or_else(|| status.name()),
        );

        attributes
    }

    fn process_completed_job(&mut self, job: &TaskTracker<T>) {
        let attr = self.job_to_gauge_attributes(job);
        self.completed_accu
            .entry(attr.clone())
            .and_modify(|x| *x += 1)
            .or_insert(1);

        let status = job.get_status();
        if let Some(nanos) = status.cpu_nanos() {
            self.cpu_time_histogram
                .recorder(attr.clone())
                .record(std::time::Duration::from_nanos(nanos as u64));
        }
        if let Some(nanos) = status.wall_nanos() {
            self.wall_time_histogram
                .recorder(attr)
                .record(std::time::Duration::from_nanos(nanos as u64));
        }
    }
}

#[cfg(test)]
mod tests {
    use metric::Observation;

    use crate::{TaskRegistry, TrackedFutureExt};

    use super::*;

    #[test]
    fn test_metrics() {
        let time_provider = Arc::new(iox_time::SystemProvider::new());
        let registry = TaskRegistry::new(time_provider);
        let metric_registry = Arc::new(metric::Registry::new());
        let mut reg = TaskRegistryWithMetrics::new(
            registry,
            Arc::clone(&metric_registry),
            Box::new(extract_attributes),
        );

        fut().track(reg.register(0).1);
        for i in 1..=3 {
            reg.complete(i);
        }

        reg.reclaim();

        let mut reporter = metric::RawReporter::default();
        metric_registry.report(&mut reporter);

        let gauge = reporter
            .metric("influxdb_iox_job_count")
            .unwrap()
            .observation(&[("status", "Dropped"), ("is_even", "true")])
            .unwrap();
        assert_eq!(gauge, &Observation::U64Gauge(1));

        let gauge = reporter
            .metric("influxdb_iox_job_count")
            .unwrap()
            .observation(&[("status", "Success"), ("is_even", "false")])
            .unwrap();
        assert_eq!(gauge, &Observation::U64Gauge(2));
    }

    async fn fut() -> Result<(), ()> {
        Ok(())
    }

    fn extract_attributes(job: &i32) -> metric::Attributes {
        metric::Attributes::from(&[
            ("is_even", if job % 2 == 0 { "true" } else { "false" }),
            ("status", "will be overwritten"),
        ])
    }
}
