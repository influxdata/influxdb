use super::{TaskId, TaskRegistration, TaskRegistry, TaskTracker};
use hashbrown::hash_map::Entry;
use hashbrown::HashMap;
use observability_deps::tracing::info;
use std::hash::Hash;
use std::sync::Arc;
use time::TimeProvider;

/// A wrapper around a TaskRegistry that automatically retains a history
#[derive(Debug)]
pub struct TaskRegistryWithHistory<T>
where
    T: Send + Sync,
{
    registry: TaskRegistry<T>,
    history: SizeLimitedHashMap<TaskId, TaskTracker<T>>,
}

impl<T: std::fmt::Display> TaskRegistryWithHistory<T>
where
    T: Send + Sync,
{
    pub fn new(time_provider: Arc<dyn TimeProvider>, capacity: usize) -> Self {
        Self {
            history: SizeLimitedHashMap::new(capacity),
            registry: TaskRegistry::new(time_provider),
        }
    }

    /// Register a new tracker in the registry
    pub fn register(&mut self, metadata: T) -> (TaskTracker<T>, TaskRegistration) {
        self.registry.register(metadata)
    }

    /// Get the tracker associated with a given id
    pub fn get(&self, id: TaskId) -> Option<TaskTracker<T>> {
        match self.history.get(&id) {
            Some(x) => Some(x.clone()),
            None => self.registry.get(id),
        }
    }

    pub fn tracked_len(&self) -> usize {
        self.registry.tracked_len()
    }

    /// Returns a list of trackers, including those that are no longer running
    pub fn tracked(&self) -> Vec<TaskTracker<T>> {
        let mut tracked = self.registry.tracked();
        tracked.extend(self.history.values().cloned());
        tracked
    }

    /// Returns a list of running trackers
    pub fn running(&self) -> Vec<TaskTracker<T>> {
        self.registry.running()
    }

    /// Reclaims jobs into the historical archive
    ///
    /// Returns list of reclaimed jobs as well as a list of jobs that were pruned from the history.
    pub fn reclaim(&mut self) -> (Vec<TaskTracker<T>>, Vec<TaskTracker<T>>) {
        let mut reclaimed = vec![];
        let mut pruned = vec![];

        for job in self.registry.reclaim() {
            info!(%job, "job finished");
            reclaimed.push(job.clone());
            if let Some((_pruned_id, pruned_job)) = self.history.push(job.id(), job) {
                pruned.push(pruned_job);
            }
        }

        (reclaimed, pruned)
    }
}

/// A size limited hashmap that maintains a finite number
/// of key value pairs providing O(1) key lookups
///
/// Inserts over the capacity will overwrite previous values
#[derive(Debug)]
struct SizeLimitedHashMap<K, V> {
    values: HashMap<K, V>,
    ring: Vec<K>,
    start_idx: usize,
    capacity: usize,
}

impl<K: Copy + Hash + Eq + Ord, V> SizeLimitedHashMap<K, V> {
    pub fn new(capacity: usize) -> Self {
        Self {
            values: HashMap::with_capacity(capacity),
            ring: Vec::with_capacity(capacity),
            start_idx: 0,
            capacity,
        }
    }

    /// Get the value associated with a specific key
    pub fn get(&self, key: &K) -> Option<&V> {
        self.values.get(key)
    }

    /// Returns an iterator to all values stored within the ring buffer
    ///
    /// Note: the order is not guaranteed
    pub fn values(&self) -> impl Iterator<Item = &V> + '_ {
        self.values.values()
    }

    /// Push a new value into the ring buffer
    ///
    /// If a value with the given key already exists, it will replace the value
    /// Otherwise it will add the key and value to the buffer.
    ///
    /// If there is insufficient capacity it will drop the oldest key value pair
    /// from the buffer.
    ///
    /// This returns the replaced value (if any).
    pub fn push(&mut self, key: K, value: V) -> Option<(K, V)> {
        if let Entry::Occupied(occupied) = self.values.entry(key) {
            // If already exists - replace existing value
            occupied.replace_entry(value);

            return None;
        }

        if self.ring.len() < self.capacity {
            // Still populating the ring
            assert_eq!(self.start_idx, 0);
            self.ring.push(key);
            self.values.insert(key, value);

            return None;
        }

        // Need to swap something out of the ring
        let mut old_key = key;
        std::mem::swap(&mut self.ring[self.start_idx], &mut old_key);

        self.start_idx += 1;
        if self.start_idx == self.capacity {
            self.start_idx = 0;
        }

        let old_value = self.values.remove(&old_key).unwrap();
        self.values.insert(key, value);

        Some((old_key, old_value))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_hashmap() {
        let expect = |ring: &SizeLimitedHashMap<i32, i32>, expected: &[i32]| {
            let mut values: Vec<_> = ring.values().cloned().collect();
            values.sort_unstable();
            assert_eq!(&values, expected);
        };

        let mut ring = SizeLimitedHashMap::new(5);
        for i in 0..=4 {
            assert_eq!(ring.push(i, i), None);
        }

        expect(&ring, &[0, 1, 2, 3, 4]);

        // Expect rollover
        assert_eq!(ring.push(5, 5), Some((0, 0)));
        expect(&ring, &[1, 2, 3, 4, 5]);

        for i in 6..=9 {
            assert!(ring.push(i, i).is_some());
        }
        expect(&ring, &[5, 6, 7, 8, 9]);

        for i in 10..=52 {
            assert!(ring.push(i + 10, i).is_some());
        }
        expect(&ring, &[48, 49, 50, 51, 52]);
        assert_eq!(*ring.get(&60).unwrap(), 50);
    }

    #[test]
    fn test_registry_archive() {
        let compare = |expected_ids: &[TaskId], archive: &TaskRegistryWithHistory<i32>| {
            let mut collected: Vec<_> = archive.history.values().map(|x| x.id()).collect();
            collected.sort();
            assert_eq!(&collected, expected_ids);
        };

        let time_provider = Arc::new(time::SystemProvider::new());
        let mut archive = TaskRegistryWithHistory::new(time_provider, 4);

        for i in 0..=3 {
            archive.register(i);
        }

        archive.reclaim();

        compare(&[TaskId(0), TaskId(1), TaskId(2), TaskId(3)], &archive);

        for i in 4..=7 {
            archive.register(i);
        }

        compare(&[TaskId(0), TaskId(1), TaskId(2), TaskId(3)], &archive);

        archive.reclaim();

        compare(&[TaskId(4), TaskId(5), TaskId(6), TaskId(7)], &archive);
    }
}
