use data_types::job::Job;
use parking_lot::Mutex;
use std::{collections::BTreeMap, convert::Infallible, ops::DerefMut, sync::Arc};
use tracker::{TaskId, TaskRegistration, TaskRegistryWithHistory, TaskTracker, TrackedFutureExt};

const JOB_HISTORY_SIZE: usize = 1000;

/// The global job registry
#[derive(Debug)]
pub struct JobRegistry {
    inner: Mutex<JobRegistryInner>,
}

#[derive(Debug)]
pub struct JobRegistryInner {
    registry: TaskRegistryWithHistory<Job>,
    metrics: JobRegistryMetrics,
}

impl JobRegistry {
    pub fn new(metric_registry_v2: Arc<metric::Registry>) -> Self {
        Self {
            inner: Mutex::new(JobRegistryInner {
                registry: TaskRegistryWithHistory::new(JOB_HISTORY_SIZE),
                metrics: JobRegistryMetrics::new(metric_registry_v2),
            }),
        }
    }

    pub fn register(&self, job: Job) -> (TaskTracker<Job>, TaskRegistration) {
        self.inner.lock().registry.register(job)
    }

    /// Returns a list of recent Jobs, including a history of past jobs
    pub fn tracked(&self) -> Vec<TaskTracker<Job>> {
        self.inner.lock().registry.tracked()
    }

    /// Returns the list of running Jobs
    pub fn running(&self) -> Vec<TaskTracker<Job>> {
        self.inner.lock().registry.running()
    }

    pub fn get(&self, id: TaskId) -> Option<TaskTracker<Job>> {
        self.inner.lock().registry.get(id)
    }

    pub fn spawn_dummy_job(&self, nanos: Vec<u64>, db_name: Option<Arc<str>>) -> TaskTracker<Job> {
        let (tracker, registration) = self.register(Job::Dummy {
            nanos: nanos.clone(),
            db_name,
        });

        for duration in nanos {
            tokio::spawn(
                async move {
                    tokio::time::sleep(tokio::time::Duration::from_nanos(duration)).await;
                    Ok::<_, Infallible>(())
                }
                .track(registration.clone()),
            );
        }

        tracker
    }

    /// Reclaims jobs into the historical archive
    ///
    /// Returns the number of remaining jobs
    ///
    /// Should be called periodically
    pub(crate) fn reclaim(&self) -> usize {
        let mut lock = self.inner.lock();
        let mut_ref = lock.deref_mut();

        let reclaimed = mut_ref.registry.reclaim();
        mut_ref.metrics.update(&mut_ref.registry, reclaimed);
        mut_ref.registry.tracked_len()
    }
}

#[derive(Debug)]
struct JobRegistryMetrics {
    active_gauge: metric::Metric<metric::U64Gauge>,
    cpu_time_histogram: metric::Metric<metric::DurationHistogram>,
    wall_time_histogram: metric::Metric<metric::DurationHistogram>,
    reclaimed_accu: BTreeMap<metric::Attributes, u64>,
}

impl JobRegistryMetrics {
    fn new(metric_registry_v2: Arc<metric::Registry>) -> Self {
        Self {
            active_gauge: metric_registry_v2
                .register_metric("influxdb_iox_job_count", "Number of known jobs"),
            cpu_time_histogram: metric_registry_v2.register_metric(
                "influxdb_iox_job_completed_cpu_nanoseconds",
                "CPU time of of completed jobs in nanoseconds",
            ),
            wall_time_histogram: metric_registry_v2.register_metric(
                "influxdb_iox_job_completed_wall_nanoseconds",
                "Wall time of of completed jobs in nanoseconds",
            ),
            reclaimed_accu: Default::default(),
        }
    }

    fn update(
        &mut self,
        registry: &TaskRegistryWithHistory<Job>,
        reclaimed: Vec<TaskTracker<Job>>,
    ) {
        // scan reclaimed jobs
        for job in reclaimed {
            let attr = Self::job_to_gauge_attributes(&job);
            self.reclaimed_accu
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

        // scan current jobs
        let mut accumulator: BTreeMap<metric::Attributes, u64> = self.reclaimed_accu.clone();
        for job in registry.tracked() {
            // completed jobs are passed in explicitely
            if job.is_complete() {
                continue;
            }

            let attr = Self::job_to_gauge_attributes(&job);
            accumulator.entry(attr).and_modify(|x| *x += 1).or_insert(1);
        }

        // emit metric
        for (attr, count) in accumulator {
            self.active_gauge.recorder(attr).set(count);
        }
    }

    fn job_to_gauge_attributes(job: &TaskTracker<Job>) -> metric::Attributes {
        let metadata = job.metadata();
        let status = job.get_status();

        metric::Attributes::from(&[
            ("description", metadata.description()),
            (
                "status",
                status
                    .result()
                    .map(|result| result.name())
                    .unwrap_or_else(|| status.name()),
            ),
        ])
    }
}
