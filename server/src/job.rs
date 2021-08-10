use data_types::job::Job;
use parking_lot::Mutex;
use std::convert::Infallible;
use std::sync::Arc;
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
