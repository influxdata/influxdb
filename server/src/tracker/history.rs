use super::{Tracker, TrackerId, TrackerRegistration, TrackerRegistry};
use hashbrown::hash_map::Entry;
use hashbrown::HashMap;
use std::hash::Hash;
use tracing::info;

/// A wrapper around a TrackerRegistry that automatically retains a history
#[derive(Debug)]
pub struct TrackerRegistryWithHistory<T> {
    registry: TrackerRegistry<T>,
    history: SizeLimitedHashMap<TrackerId, Tracker<T>>,
}

impl<T: std::fmt::Debug> TrackerRegistryWithHistory<T> {
    pub fn new(capacity: usize) -> Self {
        Self {
            history: SizeLimitedHashMap::new(capacity),
            registry: TrackerRegistry::new(),
        }
    }

    /// Register a new tracker in the registry
    pub fn register(&mut self, metadata: T) -> (Tracker<T>, TrackerRegistration) {
        self.registry.register(metadata)
    }

    /// Get the tracker associated with a given id
    pub fn get(&self, id: TrackerId) -> Option<Tracker<T>> {
        match self.history.get(&id) {
            Some(x) => Some(x.clone()),
            None => self.registry.get(id),
        }
    }

    pub fn tracked_len(&self) -> usize {
        self.registry.tracked_len()
    }

    /// Returns a list of trackers, including those that are no longer running
    pub fn tracked(&self) -> Vec<Tracker<T>> {
        let mut tracked = self.registry.tracked();
        tracked.extend(self.history.values().cloned());
        tracked
    }

    /// Returns a list of active trackers
    pub fn running(&self) -> Vec<Tracker<T>> {
        self.registry.running()
    }

    /// Reclaims jobs into the historical archive
    pub fn reclaim(&mut self) {
        for job in self.registry.reclaim() {
            info!(?job, "job finished");
            self.history.push(job.id(), job)
        }
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
    /// Otherwise it will add the key and value to the buffer
    ///
    /// If there is insufficient capacity it will drop the oldest key value pair
    /// from the buffer
    pub fn push(&mut self, key: K, value: V) {
        if let Entry::Occupied(occupied) = self.values.entry(key) {
            // If already exists - replace existing value
            occupied.replace_entry(value);

            return;
        }

        if self.ring.len() < self.capacity {
            // Still populating the ring
            assert_eq!(self.start_idx, 0);
            self.ring.push(key);
            self.values.insert(key, value);

            return;
        }

        // Need to swap something out of the ring
        let mut old = key;
        std::mem::swap(&mut self.ring[self.start_idx], &mut old);

        self.start_idx += 1;
        if self.start_idx == self.capacity {
            self.start_idx = 0;
        }

        self.values.remove(&old);
        self.values.insert(key, value);
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
            ring.push(i, i);
        }

        expect(&ring, &[0, 1, 2, 3, 4]);

        // Expect rollover
        ring.push(5, 5);
        expect(&ring, &[1, 2, 3, 4, 5]);

        for i in 6..=9 {
            ring.push(i, i);
        }
        expect(&ring, &[5, 6, 7, 8, 9]);

        for i in 10..=52 {
            ring.push(i + 10, i);
        }
        expect(&ring, &[48, 49, 50, 51, 52]);
        assert_eq!(*ring.get(&60).unwrap(), 50);
    }

    #[test]
    fn test_tracker_archive() {
        let compare = |expected_ids: &[TrackerId], archive: &TrackerRegistryWithHistory<i32>| {
            let mut collected: Vec<_> = archive.history.values().map(|x| x.id()).collect();
            collected.sort();
            assert_eq!(&collected, expected_ids);
        };

        let mut archive = TrackerRegistryWithHistory::new(4);

        for i in 0..=3 {
            archive.register(i);
        }

        archive.reclaim();

        compare(
            &[TrackerId(0), TrackerId(1), TrackerId(2), TrackerId(3)],
            &archive,
        );

        for i in 4..=7 {
            archive.register(i);
        }

        compare(
            &[TrackerId(0), TrackerId(1), TrackerId(2), TrackerId(3)],
            &archive,
        );

        archive.reclaim();

        compare(
            &[TrackerId(4), TrackerId(5), TrackerId(6), TrackerId(7)],
            &archive,
        );
    }
}
