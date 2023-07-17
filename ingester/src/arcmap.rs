//! A map key-value map where values are always wrapped in an [`Arc`], with
//! helper methods for exactly-once initialisation.

#![allow(dead_code)]

use std::{
    borrow::Borrow,
    hash::{BuildHasher, Hash, Hasher},
    sync::Arc,
};

use hashbrown::{
    hash_map::{DefaultHashBuilder, RawEntryMut},
    HashMap,
};
use parking_lot::RwLock;

/// A key-value map where all values are wrapped in [`Arc`]'s and shared across
/// all readers of a given key.
///
/// Each key in an [`ArcMap`] is initialised exactly once, with subsequent
/// lookups being handed an [`Arc`] handle to the same instance.
#[derive(Debug)]
pub(crate) struct ArcMap<K, V, S = DefaultHashBuilder> {
    map: RwLock<HashMap<K, Arc<V>, S>>,
    hasher: S,
}

impl<K, V, S> std::ops::Deref for ArcMap<K, V, S> {
    type Target = RwLock<HashMap<K, Arc<V>, S>>;

    fn deref(&self) -> &Self::Target {
        &self.map
    }
}

impl<K, V> Default for ArcMap<K, V> {
    fn default() -> Self {
        // The same hasher should be used by everything that hashes for a
        // consistent result.
        //
        // See https://github.com/influxdata/influxdb_iox/pull/6086.
        let map: HashMap<K, Arc<V>> = Default::default();
        let hasher = map.hasher().clone();
        Self {
            map: RwLock::new(map),
            hasher,
        }
    }
}

impl<K, V, S> ArcMap<K, V, S>
where
    K: Hash + Eq,
    S: BuildHasher,
{
    /// Fetch an [`Arc`]-wrapped `V` for `key`, or if this is the first lookup
    /// for `key`, initialise the value with the provided `init` closure.
    ///
    /// # Concurrency
    ///
    /// This call is thread-safe - if two calls race, a value will be
    /// initialised exactly once (one arbitrary caller's `init` closure will be
    /// executed) and both callers will obtain a handle to the same instance of
    /// `V`. Both threads will eagerly initialise V and race to "win" storing V
    /// in the map.
    ///
    /// # Performance
    ///
    /// This method is biased towards read-heavy workloads, with many readers
    /// progressing in parallel. If the value for `key` must be initialised, all
    /// readers are blocked while `init` executes and the resulting `V` is
    /// memorised.
    pub(crate) fn get_or_insert_with<Q, F>(&self, key: &Q, init: F) -> Arc<V>
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ToOwned<Owned = K> + ?Sized,
        F: FnOnce() -> Arc<V>,
    {
        // Memorise the hash outside of the lock.
        //
        // This allows the hash to be re-used (and not recomputed) if the value
        // has to be inserted into the map after the existence check. It also
        // obviously keeps the hashing outside of the lock.
        let hash = self.compute_hash(key);

        // First check if the entry exists already.
        //
        // This does NOT use an upgradable read lock, as readers waiting for an
        // upgradeable read lock block other readers wanting an upgradeable read
        // lock. If all readers do that, it's effectively an exclusive lock.
        if let Some((_, v)) = self
            .map
            .read()
            .raw_entry()
            .from_hash(hash, Self::key_equal(key))
        {
            return Arc::clone(v);
        }

        // Otherwise acquire a write lock and insert the value if necessary (it
        // is possible another thread initialised the value after the read check
        // above, but before this write lock was granted).
        let mut guard = self.map.write();
        match guard.raw_entry_mut().from_hash(hash, Self::key_equal(key)) {
            RawEntryMut::Occupied(v) => Arc::clone(v.get()),
            RawEntryMut::Vacant(v) => {
                Arc::clone(v.insert_hashed_nocheck(hash, key.to_owned(), init()).1)
            }
        }
    }

    /// A convenience method over [`Self::get_or_insert_with()`] that
    /// initialises `V` to the default value when `key` has no entry.
    pub(crate) fn get_or_default<Q>(&self, key: &Q) -> Arc<V>
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ToOwned<Owned = K> + ?Sized,
        V: Default,
    {
        self.get_or_insert_with(key, Default::default)
    }

    /// A getter for `key` that returns an [`Arc`]-wrapped `V`, or [`None`] if
    /// `key` has not yet been initialised.
    ///
    /// # Concurrency
    ///
    /// This method is cheap, and multiple callers progress in parallel. Callers
    /// are blocked by a call to [`Self::get_or_insert_with()`] only when a `V`
    /// needs to be initialised.
    pub(crate) fn get<Q>(&self, key: &Q) -> Option<Arc<V>>
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        let hash = self.compute_hash(key);
        self.map
            .read()
            .raw_entry()
            .from_hash(hash, Self::key_equal(key))
            .map(|(_k, v)| Arc::clone(v))
    }

    /// Insert `value` indexed by `key`.
    ///
    /// # Panics
    ///
    /// This method panics if a value already exists for `key`.
    pub(crate) fn insert(&self, key: K, value: Arc<V>) {
        let hash = self.compute_hash(&key);

        match self
            .map
            .write()
            .raw_entry_mut()
            .from_hash(hash, Self::key_equal(&key))
        {
            RawEntryMut::Occupied(_) => panic!("inserting existing key into ArcMap"),
            RawEntryMut::Vacant(view) => {
                view.insert_hashed_nocheck(hash, key, value);
            }
        }
    }

    /// Return a state snapshot of all the values in this [`ArcMap`] in
    /// arbitrary order.
    ///
    /// # Concurrency
    ///
    /// The snapshot generation is serialised w.r.t concurrent calls to mutate
    /// `self` (that is, a new entry may appear immediately after the snapshot
    /// is generated). Calls to [`Self::values`] and other "read" methods
    /// proceed in parallel.
    pub(crate) fn values(&self) -> Vec<Arc<V>> {
        self.map.read().values().map(Arc::clone).collect()
    }

    #[inline]
    fn compute_hash<Q: Hash + ?Sized>(&self, key: &Q) -> u64 {
        let mut state = self.hasher.build_hasher();
        key.hash(&mut state);
        state.finish()
    }

    #[inline]
    fn key_equal<Q>(q: &Q) -> impl FnMut(&'_ K) -> bool + '_
    where
        K: Borrow<Q>,
        Q: ?Sized + Eq,
    {
        move |k| q.eq(k.borrow())
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{
        atomic::{AtomicUsize, Ordering},
        Arc, Barrier,
    };

    use super::*;

    #[test]
    fn test_get() {
        let map = ArcMap::<String, usize>::default();

        let key: &str = "bananas";

        assert!(map.get(key).is_none());

        // Assert the value is initialised from the closure
        let got: Arc<usize> = map.get_or_insert_with(key, || Arc::new(42));
        assert_eq!(*got, 42);

        // Assert the same Arc is returned later.
        let other = map.get(key).expect("should have been initialised");
        assert!(Arc::ptr_eq(&got, &other));
    }

    #[test]
    fn test_init_once() {
        let map = ArcMap::<String, usize>::default();

        let key: &str = "bananas";

        // Assert the value is initialised from the closure
        let got = map.get_or_insert_with(key, || Arc::new(42));
        assert_eq!(*got, 42);

        // And subsequent calls observe the same value, regardless of the init
        // closure
        let got = map.get_or_insert_with(key, || Arc::new(13));
        assert_eq!(*got, 42);

        let got = map.get_or_default(key);
        assert_eq!(*got, 42);
    }

    #[test]
    fn test_insert() {
        let map = ArcMap::<String, usize>::default();

        let key: &str = "bananas";

        assert!(map.get(key).is_none());

        // Assert the value is initialised from the closure
        map.insert(key.to_owned(), Arc::new(42));
        let got = map.get(key).unwrap();
        assert_eq!(*got, 42);

        // Assert the same Arc is returned later.
        let other = map.get(key).expect("should have been initialised");
        assert_eq!(*other, 42);
        assert!(Arc::ptr_eq(&got, &other));

        // And subsequent calls observe the same value, regardless of the init
        // closure
        let got = map.get_or_insert_with(key, || Arc::new(13));
        assert_eq!(*got, 42);
        assert!(Arc::ptr_eq(&got, &other));
    }

    #[test]
    fn test_values() {
        let map = ArcMap::<usize, String>::default();

        map.insert(1, Arc::new("bananas".to_string()));
        map.insert(2, Arc::new("platanos".to_string()));

        let mut got = map
            .values()
            .into_iter()
            .map(|v| String::clone(&*v))
            .collect::<Vec<_>>();
        got.sort_unstable();

        assert_eq!(got, &["bananas", "platanos"]);
    }

    #[test]
    #[should_panic = "inserting existing key"]
    fn test_insert_existing() {
        let map = ArcMap::<String, usize>::default();

        let key: &str = "bananas";
        map.insert(key.to_owned(), Arc::new(42));
        map.insert(key.to_owned(), Arc::new(42));
    }

    #[test]
    #[allow(clippy::needless_collect)] // Only needless if you like deadlocks.
    fn test_init_once_parallel() {
        let map = Arc::new(ArcMap::<String, usize>::default());

        const NUM_THREADS: usize = 10;

        let barrier = Arc::new(Barrier::new(NUM_THREADS));
        let init_count = Arc::new(AtomicUsize::new(0));
        let key: &str = "bananas";

        // Spawn NUM_THREADS and have all of them wait until all the threads
        // have initialised before racing to initialise a V for key.
        //
        // Each thread tries to initialise V to a unique per-thread value, and
        // this test asserts only one thread successfully initialises V to it's
        // unique value.
        let handles = (0..NUM_THREADS)
            .map(|i| {
                let map = Arc::clone(&map);
                let barrier = Arc::clone(&barrier);
                let init_count = Arc::clone(&init_count);

                std::thread::spawn(move || {
                    // Rendezvous with all threads before continuing to maximise
                    // the racy-ness.
                    barrier.wait();

                    let got = map.get_or_insert_with(key, || {
                        init_count.fetch_add(1, Ordering::SeqCst);
                        Arc::new(i)
                    });

                    *got == i
                })
            })
            .collect::<Vec<_>>();

        let winners = handles
            .into_iter()
            .fold(0, |acc, h| if h.join().unwrap() { acc + 1 } else { acc });

        assert_eq!(winners, 1); // Number of threads that observed their unique value
        assert_eq!(init_count.load(Ordering::SeqCst), 1); // Number of init() calls
    }

    #[test]
    fn test_cross_thread_visibility() {
        let refs = Arc::new(ArcMap::default());

        const N_THREADS: i64 = 10;

        let handles = (0..N_THREADS)
            .map(|i| {
                let refs = Arc::clone(&refs);
                std::thread::spawn(move || {
                    refs.insert(i, Arc::new(i));
                })
            })
            .collect::<Vec<_>>();

        for h in handles {
            h.join().unwrap();
        }

        for i in 0..N_THREADS {
            let v = refs.get(&i).unwrap();
            assert_eq!(i, *v);
        }
    }

    // Assert values can be "moved" due to FnOnce being used, vs. Fn.
    //
    // This is a compile-time assertion more than a runtime test.
    #[test]
    fn test_fn_once() {
        let map = ArcMap::<String, String>::default();

        // A non-copy value that is moved into the FnOnce
        let v = "bananas".to_owned();
        let v = map.get_or_insert_with("platanos", move || Arc::new(v));
        assert_eq!(*v, "bananas")
    }

    #[test]
    fn test_key_equal() {
        let k = 42;
        assert!(ArcMap::<_, ()>::key_equal(&k)(&k));
        assert!(!ArcMap::<_, ()>::key_equal(&24)(&k));
    }
}
