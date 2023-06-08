use std::str::FromStr;
use std::sync::Arc;

use hashbrown::HashMap;

use iox_time::TimeProvider;

use super::{TaskRegistration, TaskTracker};

/// Every future registered with a `TaskRegistry` is assigned a unique
/// `TaskId`
#[derive(Clone, Copy, Debug, Hash, PartialEq, Eq, PartialOrd, Ord)]
pub struct TaskId(pub usize);

impl FromStr for TaskId {
    type Err = std::num::ParseIntError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(Self(FromStr::from_str(s)?))
    }
}

impl ToString for TaskId {
    fn to_string(&self) -> String {
        self.0.to_string()
    }
}

pub trait AbstractTaskRegistry<T>
where
    T: std::fmt::Debug + Send + Sync,
{
    /// Register a new tracker in the registry
    fn register(&mut self, metadata: T) -> (TaskTracker<T>, TaskRegistration);

    /// Returns a complete tracker
    fn complete(&mut self, metadata: T) -> TaskTracker<T> {
        self.register(metadata).0
    }

    /// Get the tracker associated with a given id
    fn get(&self, id: TaskId) -> Option<TaskTracker<T>>;

    /// Returns the number of tracked tasks
    fn tracked_len(&self) -> usize;

    /// Returns a list of trackers, including those that are no longer running
    fn tracked(&self) -> Vec<TaskTracker<T>>;

    /// Returns a list of active trackers
    fn running(&self) -> Vec<TaskTracker<T>>;

    /// Removes completed tasks from the registry and returns a vector of
    /// those removed.
    ///
    /// Should be called periodically.
    fn reclaim(&mut self) -> Vec<TaskTracker<T>>;
}

/// Allows tracking the lifecycle of futures registered by
/// `TrackedFutureExt::track` with an accompanying metadata payload of type T
///
/// Additionally can trigger graceful cancellation of registered futures
#[derive(Debug)]
pub struct TaskRegistry<T>
where
    T: Send + Sync,
{
    next_id: usize,
    tasks: HashMap<TaskId, TaskTracker<T>>,
    time_provider: Arc<dyn TimeProvider>,
}

impl<T> TaskRegistry<T>
where
    T: Send + Sync,
{
    pub fn new(time_provider: Arc<dyn TimeProvider>) -> Self {
        Self {
            next_id: 0,
            tasks: Default::default(),
            time_provider,
        }
    }
}

impl<T> AbstractTaskRegistry<T> for TaskRegistry<T>
where
    T: std::fmt::Debug + Send + Sync,
{
    fn register(&mut self, metadata: T) -> (TaskTracker<T>, TaskRegistration) {
        let id = TaskId(self.next_id);
        self.next_id += 1;

        let registration = TaskRegistration::new(Arc::clone(&self.time_provider));
        let tracker = TaskTracker::new(id, &registration, metadata);

        self.tasks.insert(id, tracker.clone());

        (tracker, registration)
    }

    fn get(&self, id: TaskId) -> Option<TaskTracker<T>> {
        self.tasks.get(&id).cloned()
    }

    fn tracked_len(&self) -> usize {
        self.tasks.len()
    }

    fn tracked(&self) -> Vec<TaskTracker<T>> {
        self.tasks.values().cloned().collect()
    }

    fn running(&self) -> Vec<TaskTracker<T>> {
        self.tasks
            .values()
            .filter_map(|v| {
                if !v.is_complete() {
                    return Some(v.clone());
                }
                None
            })
            .collect()
    }

    /// Removes completed tasks from the registry and returns an iterator of
    /// those removed
    fn reclaim(&mut self) -> Vec<TaskTracker<T>> {
        self.tasks
            .extract_if(|_, v| v.is_complete())
            .map(|(_, v)| v)
            .collect()
    }
}
