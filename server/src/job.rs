use data_types::job::Job;
use metric::{Attributes, MetricKind, Observation};
use parking_lot::Mutex;
use std::{
    any::Any,
    collections::{BTreeMap, BTreeSet},
    convert::Infallible,
    sync::Arc,
};
use tracker::{TaskId, TaskRegistration, TaskRegistryWithHistory, TaskTracker, TrackedFutureExt};

const JOB_HISTORY_SIZE: usize = 1000;

/// The global job registry
#[derive(Debug)]
pub struct JobRegistry {
    inner: Mutex<TaskRegistryWithHistory<Job>>,
}

impl Default for JobRegistry {
    fn default() -> Self {
        Self {
            inner: Mutex::new(TaskRegistryWithHistory::new(JOB_HISTORY_SIZE)),
        }
    }
}

impl JobRegistry {
    pub fn new() -> Self {
        Default::default()
    }

    pub fn register(&self, job: Job) -> (TaskTracker<Job>, TaskRegistration) {
        self.inner.lock().register(job)
    }

    /// Returns a list of recent Jobs, including a history of past jobs
    pub fn tracked(&self) -> Vec<TaskTracker<Job>> {
        self.inner.lock().tracked()
    }

    /// Returns the list of running Jobs
    pub fn running(&self) -> Vec<TaskTracker<Job>> {
        self.inner.lock().running()
    }

    pub fn get(&self, id: TaskId) -> Option<TaskTracker<Job>> {
        self.inner.lock().get(id)
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
        lock.reclaim();
        lock.tracked_len()
    }
}

#[derive(Debug, Clone)]
pub struct JobRegistryMetrics {
    registry: Arc<JobRegistry>,
    known: Arc<Mutex<BTreeSet<Attributes>>>,
}

impl JobRegistryMetrics {
    pub fn new(registry: Arc<JobRegistry>) -> Self {
        Self {
            registry,
            known: Default::default(),
        }
    }
}

impl metric::Instrument for JobRegistryMetrics {
    fn report(&self, reporter: &mut dyn metric::Reporter) {
        // get known attributes from last round
        let mut accumulator: BTreeMap<Attributes, u64> = self
            .known
            .lock()
            .iter()
            .cloned()
            .map(|attr| (attr, 0))
            .collect();

        // scan current jobs
        for job in self.registry.tracked() {
            let metadata = job.metadata();
            let status = job.get_status();

            let attr = Attributes::from(&[
                ("description", metadata.description()),
                (
                    "status",
                    status
                        .result()
                        .map(|result| result.name())
                        .unwrap_or_else(|| status.name()),
                ),
            ]);

            accumulator.entry(attr).and_modify(|x| *x += 1).or_insert(1);
        }

        // remember known attributes
        {
            let mut known = self.known.lock();
            known.extend(accumulator.keys().cloned());
        }

        // report metrics
        reporter.start_metric(
            "influxdb_iox_job_count",
            "Number of known jobs",
            MetricKind::U64Gauge,
        );
        for (attr, count) in accumulator {
            reporter.report_observation(&attr, Observation::U64Gauge(count));
        }
        reporter.finish_metric();
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}
