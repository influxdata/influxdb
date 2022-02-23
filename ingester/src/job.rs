use std::sync::Arc;

use iox_catalog::interface::PartitionId;
use parking_lot::Mutex;
use time::TimeProvider;
use tracker::{
    AbstractTaskRegistry, TaskRegistration, TaskRegistry, TaskRegistryWithHistory,
    TaskRegistryWithMetrics, TaskTracker,
};

const JOB_HISTORY_SIZE: usize = 1000;

#[derive(Debug)]
pub enum Job {
    Persist { partition_id: PartitionId },
}

impl Job {
    fn name(&self) -> &'static str {
        match self {
            Self::Persist { .. } => "persist",
        }
    }

    fn partition_id(&self) -> Option<PartitionId> {
        match self {
            Self::Persist { partition_id, .. } => Some(*partition_id),
        }
    }
}

/// The global job registry
#[derive(Debug)]
pub struct JobRegistry {
    inner: Mutex<TaskRegistryWithMetrics<Job, TaskRegistryWithHistory<Job, TaskRegistry<Job>>>>,
}

impl JobRegistry {
    pub fn new(
        metric_registry: Arc<metric::Registry>,
        time_provider: Arc<dyn TimeProvider>,
    ) -> Self {
        let registry = TaskRegistry::new(time_provider);
        let registry = TaskRegistryWithHistory::new(registry, JOB_HISTORY_SIZE);
        let registry =
            TaskRegistryWithMetrics::new(registry, metric_registry, Box::new(f_attributes));
        Self {
            inner: Mutex::new(registry),
        }
    }

    pub fn register(&self, job: Job) -> (TaskTracker<Job>, TaskRegistration) {
        self.inner.lock().register(job)
    }

    /// Reclaims jobs into the historical archive
    ///
    /// Returns the number of remaining jobs
    ///
    /// Should be called periodically
    pub fn reclaim(&self) -> usize {
        let mut lock = self.inner.lock();
        lock.reclaim();
        lock.tracked_len()
    }
}

fn f_attributes(job: &Job) -> metric::Attributes {
    let mut attributes = metric::Attributes::from(&[("name", job.name())]);

    if let Some(partition_id) = job.partition_id() {
        attributes.insert("partition_id", partition_id.get().to_string());
    }

    attributes
}
