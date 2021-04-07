use super::{TaskRegistration, TaskTracker};
use hashbrown::HashMap;
use observability_deps::tracing::debug;
use std::str::FromStr;
use std::sync::Arc;

/// Every future registered with a `TaskRegistry` is assigned a unique
/// `TaskId`
#[derive(Clone, Copy, Debug, Hash, PartialEq, Eq, PartialOrd, Ord)]
pub struct TaskId(pub(super) usize);

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

/// Internal data stored by TrackerRegistry
#[derive(Debug)]
struct TaskSlot<T> {
    tracker: TaskTracker<T>,
    watch: tokio::sync::watch::Sender<bool>,
}

/// Allows tracking the lifecycle of futures registered by
/// `TrackedFutureExt::track` with an accompanying metadata payload of type T
///
/// Additionally can trigger graceful cancellation of registered futures
#[derive(Debug)]
pub struct TaskRegistry<T> {
    next_id: usize,
    tasks: HashMap<TaskId, TaskSlot<T>>,
}

impl<T> Default for TaskRegistry<T> {
    fn default() -> Self {
        Self {
            next_id: 0,
            tasks: Default::default(),
        }
    }
}

impl<T> TaskRegistry<T> {
    pub fn new() -> Self {
        Default::default()
    }

    /// Register a new tracker in the registry
    pub fn register(&mut self, metadata: T) -> (TaskTracker<T>, TaskRegistration) {
        let id = TaskId(self.next_id);
        self.next_id += 1;

        let (sender, receiver) = tokio::sync::watch::channel(false);
        let registration = TaskRegistration::new(receiver);

        let tracker = TaskTracker {
            id,
            metadata: Arc::new(metadata),
            state: Arc::clone(&registration.state),
        };

        self.tasks.insert(
            id,
            TaskSlot {
                tracker: tracker.clone(),
                watch: sender,
            },
        );

        (tracker, registration)
    }

    /// Removes completed tasks from the registry and returns an iterator of
    /// those removed
    pub fn reclaim(&mut self) -> impl Iterator<Item = TaskTracker<T>> + '_ {
        self.tasks
            .drain_filter(|_, v| v.tracker.is_complete())
            .map(|(_, v)| {
                if let Err(error) = v.watch.send(true) {
                    // As we hold a reference to the Tracker here, this should be impossible
                    debug!(?error, "failed to publish tracker completion")
                }
                v.tracker
            })
    }

    pub fn get(&self, id: TaskId) -> Option<TaskTracker<T>> {
        self.tasks.get(&id).map(|x| x.tracker.clone())
    }

    /// Returns the number of tracked tasks
    pub fn tracked_len(&self) -> usize {
        self.tasks.len()
    }

    /// Returns a list of trackers, including those that are no longer running
    pub fn tracked(&self) -> Vec<TaskTracker<T>> {
        self.tasks.iter().map(|(_, v)| v.tracker.clone()).collect()
    }

    /// Returns a list of active trackers
    pub fn running(&self) -> Vec<TaskTracker<T>> {
        self.tasks
            .iter()
            .filter_map(|(_, v)| {
                if !v.tracker.is_complete() {
                    return Some(v.tracker.clone());
                }
                None
            })
            .collect()
    }
}
