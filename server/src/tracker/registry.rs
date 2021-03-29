use super::{Tracker, TrackerRegistration};
use hashbrown::HashMap;
use std::str::FromStr;
use std::sync::Arc;
use tracing::debug;

/// Every future registered with a `TrackerRegistry` is assigned a unique
/// `TrackerId`
#[derive(Clone, Copy, Debug, Hash, PartialEq, Eq, PartialOrd, Ord)]
pub struct TrackerId(pub(super) usize);

impl FromStr for TrackerId {
    type Err = std::num::ParseIntError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(Self(FromStr::from_str(s)?))
    }
}

impl ToString for TrackerId {
    fn to_string(&self) -> String {
        self.0.to_string()
    }
}

/// Internal data stored by TrackerRegistry
#[derive(Debug)]
struct TrackerSlot<T> {
    tracker: Tracker<T>,
    watch: tokio::sync::watch::Sender<bool>,
}

/// Allows tracking the lifecycle of futures registered by
/// `TrackedFutureExt::track` with an accompanying metadata payload of type T
///
/// Additionally can trigger graceful cancellation of registered futures
#[derive(Debug)]
pub struct TrackerRegistry<T> {
    next_id: usize,
    trackers: HashMap<TrackerId, TrackerSlot<T>>,
}

impl<T> Default for TrackerRegistry<T> {
    fn default() -> Self {
        Self {
            next_id: 0,
            trackers: Default::default(),
        }
    }
}

impl<T> TrackerRegistry<T> {
    pub fn new() -> Self {
        Default::default()
    }

    /// Register a new tracker in the registry
    pub fn register(&mut self, metadata: T) -> (Tracker<T>, TrackerRegistration) {
        let id = TrackerId(self.next_id);
        self.next_id += 1;

        let (sender, receiver) = tokio::sync::watch::channel(false);
        let registration = TrackerRegistration::new(receiver);

        let tracker = Tracker {
            id,
            metadata: Arc::new(metadata),
            state: Arc::clone(&registration.state),
        };

        self.trackers.insert(
            id,
            TrackerSlot {
                tracker: tracker.clone(),
                watch: sender,
            },
        );

        (tracker, registration)
    }

    /// Removes completed tasks from the registry and returns an iterator of
    /// those removed
    pub fn reclaim(&mut self) -> impl Iterator<Item = Tracker<T>> + '_ {
        self.trackers
            .drain_filter(|_, v| v.tracker.is_complete())
            .map(|(_, v)| {
                if let Err(error) = v.watch.send(true) {
                    // As we hold a reference to the Tracker here, this should be impossible
                    debug!(?error, "failed to publish tracker completion")
                }
                v.tracker
            })
    }

    pub fn get(&self, id: TrackerId) -> Option<Tracker<T>> {
        self.trackers.get(&id).map(|x| x.tracker.clone())
    }

    /// Returns the number of tracked tasks
    pub fn tracked_len(&self) -> usize {
        self.trackers.len()
    }

    /// Returns a list of trackers, including those that are no longer running
    pub fn tracked(&self) -> Vec<Tracker<T>> {
        self.trackers
            .iter()
            .map(|(_, v)| v.tracker.clone())
            .collect()
    }

    /// Returns a list of active trackers
    pub fn running(&self) -> Vec<Tracker<T>> {
        self.trackers
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
