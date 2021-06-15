use super::{TaskRegistration, TaskTracker};
use hashbrown::HashMap;
use std::str::FromStr;

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
}

impl<T> Default for TaskRegistry<T>
where
    T: Send + Sync,
{
    fn default() -> Self {
        Self {
            next_id: 0,
            tasks: Default::default(),
        }
    }
}

impl<T> TaskRegistry<T>
where
    T: Send + Sync,
{
    pub fn new() -> Self {
        Default::default()
    }

    /// Register a new tracker in the registry
    pub fn register(&mut self, metadata: T) -> (TaskTracker<T>, TaskRegistration) {
        let id = TaskId(self.next_id);
        self.next_id += 1;

        let registration = TaskRegistration::new();
        let tracker = TaskTracker::new(id, &registration, metadata);

        self.tasks.insert(id, tracker.clone());

        (tracker, registration)
    }

    /// Removes completed tasks from the registry and returns an iterator of
    /// those removed
    pub fn reclaim(&mut self) -> impl Iterator<Item = TaskTracker<T>> + '_ {
        self.tasks
            .drain_filter(|_, v| v.is_complete())
            .map(|(_, v)| v)
    }

    pub fn get(&self, id: TaskId) -> Option<TaskTracker<T>> {
        self.tasks.get(&id).cloned()
    }

    /// Returns the number of tracked tasks
    pub fn tracked_len(&self) -> usize {
        self.tasks.len()
    }

    /// Returns a list of trackers, including those that are no longer running
    pub fn tracked(&self) -> Vec<TaskTracker<T>> {
        self.tasks.values().cloned().collect()
    }

    /// Returns a list of active trackers
    pub fn running(&self) -> Vec<TaskTracker<T>> {
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
}
