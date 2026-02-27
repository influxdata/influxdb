use bincode::{Decode, Encode};
use dashmap::DashMap;
use std::{
    collections::{HashSet, VecDeque},
    fmt::{Debug, Formatter},
    hash::Hash,
    sync::{
        Arc, RwLock, RwLockReadGuard,
        atomic::{AtomicU8, Ordering},
    },
};
use tracker::{LockMetrics, Mutex};

use crate::cache_system::{
    AsyncDrop, DynError, HasSize, InUse,
    hook::{EvictResult, Hook},
};

use super::{fifo::Fifo, ordered_set::OrderedSet};

/// Entry within [`S3Fifo`].
#[derive(Debug)]
pub struct S3FifoEntry<K, V>
where
    K: ?Sized,
{
    key: Arc<K>,
    value: RwLock<V>,
    generation: u64,
    freq: AtomicU8,
}

impl<K, V> S3FifoEntry<K, V>
where
    K: ?Sized,
{
    pub(crate) fn value(&self) -> RwLockReadGuard<'_, V> {
        self.value.read().expect("not poisoned")
    }
}

impl<K, V> HasSize for S3FifoEntry<K, V>
where
    K: HasSize + ?Sized,
    V: HasSize,
{
    fn size(&self) -> usize {
        let Self {
            key,
            value,
            generation: _,
            freq: _,
        } = self;
        key.size() + value.read().expect("not poisoned").size()
    }
}

impl<K, V> InUse for S3FifoEntry<K, V>
where
    K: Send + Sync + ?Sized,
    V: InUse,
{
    fn in_use(&mut self) -> bool
    where
        Self: Sized,
    {
        self.value.write().expect("not poisoned").in_use()
    }
}

/// This arc is shared between the `S3Fifo.entries` and the locked state.
/// Therefore, we want to not consider the arc's ref count
/// in determining in-use status.
fn entry_likely_in_use<K, V>(entry: &Arc<S3FifoEntry<K, V>>) -> bool
where
    K: Send + Sync + ?Sized,
    V: InUse,
{
    // NOTE: try the exclusive lock AFTER performing the cheap `Arc` check
    Arc::strong_count(entry) > 2 || entry.value.write().expect("not poisoned").in_use()
}

impl<K, V> Encode for S3FifoEntry<K, V>
where
    K: Encode + Sized + HasSize,
    V: Encode + HasSize,
{
    fn encode<E: bincode::enc::Encoder>(
        &self,
        encoder: &mut E,
    ) -> Result<(), bincode::error::EncodeError> {
        let Self {
            key,
            value,
            generation,
            freq,
        } = self;

        key.encode(encoder)?;
        value.read().expect("not poisoned").encode(encoder)?;
        generation.encode(encoder)?;
        freq.load(Ordering::SeqCst).encode(encoder)?;

        Ok(())
    }
}

// bincode Decode implementation for S3FifoEntry
impl<K, V, Q> Decode<Q> for S3FifoEntry<K, V>
where
    K: Decode<Q> + Encode + Sized + HasSize,
    V: Decode<Q> + Encode + HasSize,
{
    fn decode<D: bincode::de::Decoder<Context = Q>>(
        decoder: &mut D,
    ) -> Result<Self, bincode::error::DecodeError> {
        let key: Arc<K> = Decode::decode(decoder)?;
        let value: V = Decode::decode(decoder)?;
        let generation: u64 = Decode::decode(decoder)?;
        let freq_val: u8 = Decode::decode(decoder)?;

        Ok(Self {
            key,
            value: RwLock::new(value),
            generation,
            freq: AtomicU8::new(freq_val),
        })
    }
}

impl<K, V> AsyncDrop for S3FifoEntry<K, V>
where
    K: Send + Sync + ?Sized,
    V: AsyncDrop,
{
    fn async_drop(self) -> impl Future<Output = ()> + Send {
        let Self {
            key: _,
            value,
            generation: _,
            freq: _,
        } = self;

        // perform the async drop on the value
        value.into_inner().expect("not poisoned").async_drop()
    }
}

/// Returns the overhead size of the [`S3FifoEntry`]
/// placed into the S3 Fifo cache manager.
///
/// This is useful for testing, since it's the size used
/// for eviction decisions.
pub fn s3_fifo_entry_overhead_size() -> usize {
    // The overhead size is the size of the S3FifoEntry<V> struct,
    // which is used to store the cache entry in the S3 FIFO cache manager.
    Arc::new(S3FifoEntry {
        key: Arc::new(()),
        value: RwLock::new(Arc::new(())),
        generation: 0,
        freq: AtomicU8::new(0),
    })
    .size()
}

pub(crate) type CacheEntry<K, V> = Arc<S3FifoEntry<K, V>>;
type Entries<K, V> = DashMap<Arc<K>, CacheEntry<K, V>>;
pub(crate) type Evicted<K, V> = Vec<Arc<S3FifoEntry<K, V>>>;

/// Implementation of the [S3-FIFO] cache algorithm.
///
/// # Hook Interaction
/// This calls SOME callbacks of the provided [`Hook`]. The caller is expected to call [`Hook::insert`] though. The only
/// method that can trigger hook callbacks is [`get_or_put`](Self::get_or_put). See method docs for more details.
///
///
/// [S3-FIFO]: https://s3fifo.com/
pub struct S3Fifo<K, V>
where
    K: Debug + Eq + Hash + HasSize + Send + Sync + 'static + ?Sized,
    V: HasSize + InUse + Send + Sync + 'static,
{
    locked_state: Mutex<LockedState<K, V>>,
    entries: Entries<K, V>,
    config: S3Config<K>,
}

impl<K, V> Debug for S3Fifo<K, V>
where
    K: Debug + Eq + Hash + HasSize + Send + Sync + 'static + ?Sized,
    V: HasSize + InUse + Send + Sync + 'static,
{
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("S3Fifo")
            .field("entries", &self.entries.len())
            .field("config", &self.config)
            .finish()
    }
}

impl<K, V> S3Fifo<K, V>
where
    K: Debug + Eq + Hash + HasSize + Send + Sync + 'static + ?Sized,
    V: HasSize + InUse + Send + Sync + 'static,
{
    /// Create new, empty set.
    pub fn new(config: S3Config<K>, metric_registry: &metric::Registry) -> Self {
        let lock_metrics = Arc::new(LockMetrics::new(
            metric_registry,
            &[("lock", "s3fifo"), ("cache", config.cache_name)],
        ));
        Self {
            locked_state: lock_metrics.new_mutex(LockedState {
                main: Default::default(),
                small: Default::default(),
                ghost: Default::default(),
            }),
            entries: Default::default(),
            config,
        }
    }

    /// Create new S3Fifo from a deserialized snapshot.
    ///
    /// This function deserializes a snapshot and creates a new S3Fifo instance
    /// with the restored state, including rebuilding the entries map.
    ///
    /// # Parameters
    /// - `config`: The S3 FIFO configuration. See [`S3Config`] for details.
    /// - `snapshot_data`: The serialized cache state data, typically created by [`snapshot`](Self::snapshot).
    /// - `shared_seed`: See [`deserialize_snapshot`](Self::deserialize_snapshot) for
    ///   an explanation of how this context parameter is used during deserialization.
    ///
    /// # Returns
    /// A new S3Fifo instance with the restored state.
    ///
    /// # Errors
    /// Returns a `DynError` if deserialization fails.
    pub fn new_from_snapshot<Q>(
        config: S3Config<K>,
        metric_registry: &metric::Registry,
        snapshot_data: &[u8],
        shared_seed: &Q,
    ) -> Result<Self, DynError>
    where
        Q: Clone,
        K: Decode<Q> + Eq + Hash + Send + Sync + 'static + HasSize + Encode,
        // the value (`V`) should be bincode encoded and decoded
        V: Decode<Q> + HasSize + Send + Sync + 'static + Encode,
    {
        // Deserialize the locked state - all migration and legacy handling is in VersionedState
        let locked_state = Self::deserialize_snapshot(snapshot_data, shared_seed.clone())?;

        // Create the entries map from the deserialized state
        let entries = DashMap::new();

        // Rebuild entries from main queue
        for entry in locked_state.main.iter() {
            entries.insert(Arc::clone(&entry.key), Arc::clone(entry));
        }

        // Rebuild entries from small queue
        for entry in locked_state.small.iter() {
            entries.insert(Arc::clone(&entry.key), Arc::clone(entry));
        }

        // Create the S3Fifo instance
        let lock_metrics = Arc::new(LockMetrics::new(
            metric_registry,
            &[("lock", "s3fifo"), ("cache", config.cache_name)],
        ));
        Ok(Self {
            locked_state: lock_metrics.new_mutex(locked_state),
            entries,
            config,
        })
    }

    /// Gets entry from the set, or inserts it if it does not exist yet.
    /// Returns the entry and any evicted entries.
    ///
    /// # Hook Interaction
    /// If the key already exists, this calls [`Hook::evict`] w/ [`EvictResult::Unfetched`] on the provided new data
    /// since the new data is rejected.
    ///
    /// If the key is new, it calls [`Hook::fetched`].
    ///
    /// If inserting a new key leads to eviction of existing data, [`Hook::evict`] w/ [`EvictResult::Fetched`] is
    /// called. [`EvictResult::Failed`] is NOT used since we also account for the size of errrors.
    ///
    /// # Concurrency
    /// Acquires a lock and blocks other calls to [`get_or_put`](Self::get_or_put).
    ///
    /// Does NOT block read methods like [`get`](Self::get), [`len`](Self::len), and [`is_empty`](Self::is_empty),
    /// except for short-lived internal locks within [`DashMap`].
    pub fn get_or_put(
        &self,
        key: Arc<K>,
        value: V,
        generation: u64,
    ) -> (CacheEntry<K, V>, Evicted<K, V>) {
        // Lock the state BEFORE checking `self.entries`. We won't prevent concurrent reads with it but we prevent that
        // concurrent writes could check `entries`, find the key absent and then double-insert the data into the locked state.
        let mut guard = self.locked_state.lock();

        // Cache hit
        if let Some(entry) = self.entries.get(&key) {
            entry
                .freq
                .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |f| Some(3.min(f + 1)))
                .unwrap(); // Safe unwrap since we are always returning Some in fetch_update
            self.config
                .hook
                .evict(generation, &key, EvictResult::Unfetched);
            let entry = Arc::clone(&entry);

            // drop guard before we drop key & value so that the work to
            // deallocate the key/value is not done while holding the lock
            // and preventing other operations from proceeding
            drop(guard);

            return (
                entry,
                vec![
                    S3FifoEntry {
                        key,
                        value: RwLock::new(value),
                        generation: Default::default(),
                        freq: Default::default(),
                    }
                    .into(),
                ],
            );
        }

        let entry = Arc::new(S3FifoEntry {
            key,
            value: RwLock::new(value),
            generation,
            freq: 0.into(),
        });

        self.config
            .hook
            .fetched(generation, &entry.key, Ok(entry.size()));
        self.entries
            .insert(Arc::clone(&entry.key), Arc::clone(&entry));
        let (evicted_entries, evicted_keys) = if guard.ghost.remove(&entry.key) {
            let evicted = guard.evict(&self.entries, &self.config);
            guard.main.push_back(Arc::clone(&entry));
            evicted
        } else {
            let evicted = guard.evict(&self.entries, &self.config);
            guard.small.push_back(Arc::clone(&entry));
            evicted
        };

        drop(guard);
        drop_it(evicted_keys);

        (entry, evicted_entries)
    }

    /// Gets entry from the set, returns `None` if the key is NOT stored.
    ///
    /// Note that even when the key is NOT stored as an entry, it may be known as a "ghost" and stored within the
    /// internal "ghost set".
    ///
    /// # Concurrency
    /// This method is mostly lockless, except for internal locks within [`DashMap`].
    pub fn get(&self, key: &K) -> Option<CacheEntry<K, V>> {
        let entry = self.entries.get(key).map(|v| Arc::clone(&v));
        if let Some(entry) = entry {
            entry
                .freq
                .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |f| Some(3.min(f + 1)))
                .unwrap(); // Safe unwrap since we are always returning Some in fetch_update
            Some(entry)
        } else {
            None
        }
    }

    /// Number of stored entries.
    ///
    /// # Concurrency
    /// This method is mostly lockless, except for internal locks within [`DashMap`].
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// Returns `true` if there are NO entries within the set.
    ///
    /// # Concurrency
    /// This method is mostly lockless, except for internal locks within [`DashMap`].
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Returns an iterator of all keys currently in the cache.
    pub fn keys(&self) -> impl Iterator<Item = Arc<K>> {
        self.entries.iter().map(|entry| Arc::clone(entry.key()))
    }

    /// Remove multiple keys from the cache, in a blocking manner.
    ///
    /// This method directly removes entries from the cache without going through
    /// the normal eviction process. This is useful for cache management operations
    /// like repair/validation.
    ///
    /// Returns the number of keys that were successfully removed. If a key does not
    /// exist in the cache and cannot be removed, it will be ignored (and the returned count
    /// of removed items will be lower).
    pub fn remove_keys(&self, keys: impl Iterator<Item = K>) -> usize
    where
        K: Sized + Clone + Debug,
    {
        let mut guard = self.locked_state.lock();

        // Remove keys from the entries map
        let to_remove_from_state: HashSet<K> = keys
            .filter_map(|k| self.entries.remove(&k).map(|_| k))
            .collect();

        // Remove from locked state.
        let count_removed = guard.remove_keys(&to_remove_from_state);
        drop(guard);

        count_removed
    }

    /// Create a snapshot of the locked state.
    ///
    /// This function serializes the [`S3Fifo`] inner state using bincode, allowing for
    /// persistence and recovery of the cache state.
    ///
    /// The state serialized will be versioned as [`Version::latest`], corresponding to the
    /// current [`S3Fifo`] inner state implementation ([`S3Fifo::locked_state`]).
    ///
    /// # Returns
    /// A `Vec<u8>` containing the serialized locked state.
    ///
    /// # Errors
    /// Returns a [`DynError`] if serialization fails.
    pub fn snapshot(&self) -> Result<Vec<u8>, DynError>
    where
        K: Encode + Sized + Eq + Hash + HasSize + Send + Sync + 'static,
        V: Encode + HasSize + Send + Sync + 'static,
    {
        // Serialize the versioned snapshot while holding the lock
        // This is safe because bincode serialization is typically fast
        let guard = self.locked_state.lock();
        let versioned_snapshot = VersionedSnapshot {
            version: Version::latest(),
            state: &*guard,
        };

        bincode::encode_to_vec(versioned_snapshot, bincode::config::standard())
            .map_err(|e| crate::cache_system::utils::str_err(&e.to_string()))
    }

    /// Deserialize a snapshot to the locked state.
    ///
    /// The `shared_seed` parameter provides context for bincode's
    /// [`Decode<Context>`](bincode::Decode) trait implementation. This is useful
    /// when the deserialization process needs additional information beyond what's
    /// stored in the serialized data itself. For example, it could provide lookup
    /// tables, configuration, or other contextual data that types need during
    /// deserialization.
    ///
    /// # Errors
    /// Returns a [`DynError`] if deserialization fails.
    fn deserialize_snapshot<Q>(
        snapshot_data: &[u8],
        shared_seed: Q,
    ) -> Result<LockedState<K, V>, DynError>
    where
        Q: Clone,
        K: Decode<Q> + Eq + Hash + Send + Sync + 'static + HasSize + Encode,
        V: Decode<Q> + HasSize + Send + Sync + 'static + Encode,
    {
        // Use bincode's decode
        let (versioned_snapshot, _): (VersionedSnapshot<LockedState<K, V>>, usize) =
            bincode::decode_from_slice_with_context(
                snapshot_data,
                bincode::config::standard(),
                shared_seed,
            )
            .map_err(|e| crate::cache_system::utils::str_err(&e.to_string()))?;

        Ok(versioned_snapshot.state)
    }

    #[cfg(test)]
    pub(crate) fn ghost_len(&self) -> usize {
        let guard = self.locked_state.lock();
        guard.ghost.len()
    }

    #[cfg(test)]
    pub(crate) fn small_queue_keys(&self) -> Vec<Arc<K>> {
        let guard = self.locked_state.lock();
        guard
            .small
            .iter()
            .map(|entry| Arc::clone(&entry.key))
            .collect()
    }

    #[cfg(test)]
    pub(crate) fn main_queue_keys(&self) -> Vec<Arc<K>> {
        let guard = self.locked_state.lock();
        guard
            .main
            .iter()
            .map(|entry| Arc::clone(&entry.key))
            .collect()
    }

    #[cfg(test)]
    pub(crate) fn contains_key_in_entries(&self, key: &K) -> bool {
        self.entries.contains_key(key)
    }

    #[cfg(test)]
    pub(crate) fn contains_key_in_ghost(&self, key: &Arc<K>) -> bool {
        let guard = self.locked_state.lock();
        // The ghost stores Arc<K>, so we need to check by content
        guard.ghost.contains(key)
    }
}

/// Calls [`drop`] but isn't inlined, so it is easier to see on profiles.
#[inline(never)]
pub(crate) fn drop_it<T>(t: T) {
    drop(t);
}

/// Immutable config state of [`S3Fifo`]
pub struct S3Config<K>
where
    K: ?Sized,
{
    pub cache_name: &'static str,
    pub max_memory_size: usize,
    pub max_ghost_memory_size: usize,

    pub hook: Arc<dyn Hook<K>>,

    /// Controls when we start evicting from S in an attempt to move more items to M.
    pub move_to_main_threshold: f64,

    /// Maximum amount of in-flight data in bytes.
    pub inflight_bytes: usize,
}

impl<K> std::fmt::Debug for S3Config<K>
where
    K: ?Sized,
{
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("S3Config")
            .field("max_memory_size", &self.max_memory_size)
            .field("max_ghost_memory_size", &self.max_ghost_memory_size)
            .field("hook", &self.hook)
            .field("move_to_main_threshold", &self.move_to_main_threshold)
            .field("inflight_bytes", &self.inflight_bytes)
            .finish()
    }
}

const VERSION_V1: u8 = 1;
#[cfg(test)]
const VERSION_TEST: u8 = 42;

#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq)]
/// Version of S3Fifo inner state (e.g. [`S3Fifo::locked_state`]).
pub(crate) enum Version {
    V1 = VERSION_V1,

    #[cfg(test)]
    Test = VERSION_TEST,
}

impl Version {
    /// Returns the latest version.
    fn latest() -> Self {
        Self::V1
    }
}

/// Handles the versioning of snapshot data.
pub(crate) struct VersionedSnapshot<S> {
    /// The [`Version`] of this S3Fifo inner state.
    version: Version,

    /// The inner state (`S`), which may be different from the current S3Fifo state
    /// when migrating across versions. (e.g. deserializing older state `S` snapshots).
    state: S,
}

impl<S> Encode for VersionedSnapshot<S>
where
    S: Encode,
{
    fn encode<E: bincode::enc::Encoder>(
        &self,
        encoder: &mut E,
    ) -> Result<(), bincode::error::EncodeError> {
        let version: u8 = self.version as u8;
        version.encode(encoder)?;
        Encode::encode(&self.state, encoder)?;
        Ok(())
    }
}

impl<K, V, Q> Decode<Q> for VersionedSnapshot<LockedState<K, V>>
where
    Q: Clone,
    K: Decode<Q> + Encode + Eq + Hash + HasSize + Sized + Send + Sync + 'static,
    V: Decode<Q> + Encode + HasSize + Send + Sync + 'static,
{
    fn decode<D: bincode::de::Decoder<Context = Q>>(
        decoder: &mut D,
    ) -> Result<Self, bincode::error::DecodeError> {
        let version: u8 = Decode::decode(decoder)?;

        // In the future, we can add version-specific deserialization logic
        // with other `S` implementations for VersionedSnapshot<S>.
        // Then the migration would be done in the match arm.
        match version {
            VERSION_V1 => {
                let locked_state: LockedState<K, V> = Decode::decode(decoder)?;

                Ok(Self {
                    version: Version::V1,
                    state: locked_state,
                })
            }
            _ => unreachable!("Unsupported version: {}", version),
        }
    }
}

/// Mutable part of [`S3Fifo`] that is locked for [`get_or_put`](S3Fifo::get_or_put).
struct LockedState<K, V>
where
    K: Eq + Hash + HasSize + Send + Sync + 'static + ?Sized,
    V: HasSize + Send + Sync + 'static,
{
    main: Fifo<CacheEntry<K, V>>,
    small: Fifo<CacheEntry<K, V>>,
    ghost: OrderedSet<Arc<K>>,
}

impl<K, V> Debug for LockedState<K, V>
where
    K: Eq + Hash + HasSize + Send + Sync + 'static + ?Sized,
    V: HasSize + Send + Sync + 'static,
{
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LockedState")
            .field("main", &self.main)
            .field("small", &self.small)
            .field("ghost", &self.ghost)
            .finish()
    }
}

/// Encode implementation, with trait bounds for `<K, V>`.
impl<K, V> Encode for LockedState<K, V>
where
    K: Encode + Sized + Eq + Hash + HasSize + Send + Sync + 'static,
    V: Encode + HasSize + Send + Sync + 'static,
{
    fn encode<E: bincode::enc::Encoder>(
        &self,
        encoder: &mut E,
    ) -> Result<(), bincode::error::EncodeError> {
        Encode::encode(&self.main, encoder)?;
        Encode::encode(&self.small, encoder)?;
        Encode::encode(&self.ghost, encoder)?;
        Ok(())
    }
}

// bincode Decode implementation for LockedState using Context
// This handles the locked state decoding with bincode's native decoder
impl<K, V, Q> Decode<Q> for LockedState<K, V>
where
    Q: Clone,
    K: Decode<Q> + Encode + Eq + Hash + HasSize + Sized + Send + Sync + 'static,
    V: Decode<Q> + Encode + HasSize + Send + Sync + 'static,
{
    fn decode<D: bincode::de::Decoder<Context = Q>>(
        decoder: &mut D,
    ) -> Result<Self, bincode::error::DecodeError> {
        // using bincode encoding & decoding
        let main: Fifo<CacheEntry<K, V>> = Decode::decode(decoder)?;
        let small: Fifo<CacheEntry<K, V>> = Decode::decode(decoder)?;

        // Use bincode::serde::Compat for OrderedSet since it uses serde derives
        let ghost: OrderedSet<Arc<K>> = Decode::decode(decoder)?;

        Ok(Self { main, small, ghost })
    }
}

impl<K, V> LockedState<K, V>
where
    K: Debug + Eq + Hash + HasSize + Send + Sync + 'static + ?Sized,
    V: HasSize + InUse + Send + Sync + 'static,
{
    fn insert_ghost(&mut self, key: Arc<K>, config: &S3Config<K>, evicted_keys: &mut Vec<Arc<K>>) {
        while self.ghost.memory_size() >= config.max_ghost_memory_size {
            evicted_keys.push(self.ghost.pop_front().expect("ghost queue is NOT empty"));
        }

        self.ghost.push_back(key);
    }

    /// Evict entries, returning any evicted entries.
    fn evict(
        &mut self,
        entries: &Entries<K, V>,
        config: &S3Config<K>,
    ) -> (Vec<CacheEntry<K, V>>, Vec<Arc<K>>) {
        let small_queue_threshold =
            (config.move_to_main_threshold * config.max_memory_size as f64) as usize;
        let mut evicted_entries = Vec::with_capacity(8);
        let mut evicted_keys = Vec::with_capacity(8);

        while self.small.memory_size() + self.main.memory_size() >= config.max_memory_size {
            if self.small.memory_size() >= small_queue_threshold {
                self.evict_from_small_queue(
                    entries,
                    config,
                    &mut evicted_entries,
                    &mut evicted_keys,
                );
            } else {
                self.evict_from_main_queue(entries, config, &mut evicted_entries);
            }
        }

        (evicted_entries, evicted_keys)
    }

    /// Evict at most one entry from the "small" queue.
    ///
    /// This scans through the "small" queue and for every entry it either:
    ///
    /// - **move to "main" queue:** If the entry was used, move it to the back of the "main" queue
    /// - **move to "ghost" set:** If the entry was NOT used, remove it from the cache and add its key to the "ghost" set.
    ///
    /// The method returns if an unused entry was removed or if there are no entries left.
    ///
    /// See [S3-FIFO] for the defintion of the different queue/set types.
    ///
    ///
    /// [S3-FIFO]: https://s3fifo.com/
    fn evict_from_small_queue(
        &mut self,
        entries: &Entries<K, V>,
        config: &S3Config<K>,
        evicted_entries: &mut Vec<CacheEntry<K, V>>,
        evicted_keys: &mut Vec<Arc<K>>,
    ) {
        while let Some(mut tail) = self.small.pop_front() {
            if tail.freq.load(Ordering::SeqCst) > 0 || entry_likely_in_use(&tail) {
                self.main.push_back(tail);
            } else {
                let size = tail.size();

                // cache GETs are still served by the S3Fifo::entries, which is not behind the lock
                // therefore, need to check again
                entries.remove(&tail.key);
                if tail.in_use() {
                    self.main.push_back(Arc::clone(&tail));
                    entries.insert(Arc::clone(&tail.key), tail);
                    continue;
                }

                self.insert_ghost(Arc::clone(&tail.key), config, evicted_keys);
                config
                    .hook
                    .evict(tail.generation, &tail.key, EvictResult::Fetched { size });
                evicted_entries.push(tail);
                break;
            }
        }
    }

    /// Evict at most one entry from the "main" queue.
    ///
    /// This scans through the "main" queue and for every entry it either:
    ///
    /// - **move to to back:** If the entry was used, move it to the back of the "main" queue. Decrease its usage
    ///   counter by one.
    /// - **move to "ghost" set:** If the entry was NOT used, remove it from the cache and add its key to the "ghost" set.
    ///
    /// The method returns if an unused entry was removed or if there are no entries left.
    ///
    /// See [S3-FIFO] for the defintion of the different queue/set types.
    ///
    ///
    /// [S3-FIFO]: https://s3fifo.com/
    fn evict_from_main_queue(
        &mut self,
        entries: &Entries<K, V>,
        config: &S3Config<K>,
        evicted_entries: &mut Vec<CacheEntry<K, V>>,
    ) {
        while let Some(mut tail) = self.main.pop_front() {
            let was_not_zero = tail
                .freq
                .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |f| f.checked_sub(1))
                .is_ok();

            if was_not_zero || entry_likely_in_use(&tail) {
                self.main.push_back(tail);
            } else {
                let size = tail.size();

                // cache GETs are still served by the S3Fifo::entries, which is not behind the lock
                // therefore, need to check again
                entries.remove(&tail.key);
                if tail.in_use() {
                    self.main.push_back(Arc::clone(&tail));
                    entries.insert(Arc::clone(&tail.key), tail);
                    continue;
                }

                config
                    .hook
                    .evict(tail.generation, &tail.key, EvictResult::Fetched { size });
                evicted_entries.push(tail);
                break;
            }
        }
    }

    /// Remove multiple keys from the small and main queues.
    ///
    /// This method efficiently removes multiple keys by iterating through each queue once.
    /// It first checks the small queue for all keys, then checks the main queue for any
    /// remaining keys that weren't found in the small queue.
    ///
    /// Returns the number of keys that were successfully removed. If a key does not
    /// exist in the cache and cannot be removed, it will be ignored (and the returned count
    /// of removed items will be lower).
    fn remove_keys(&mut self, keys_to_remove: &HashSet<K>) -> usize
    where
        K: Sized + Clone + Debug,
    {
        let initial_count = self.small.len() + self.main.len();

        // Remove from small queue
        let filtered_small: VecDeque<_> = self
            .small
            .drain()
            .filter(|entry| !keys_to_remove.contains(entry.key.as_ref()))
            .collect();
        self.small = Fifo::new(filtered_small);

        // Remove from main queue
        let filtered_main: VecDeque<_> = self
            .main
            .drain()
            .filter(|entry| !keys_to_remove.contains(entry.key.as_ref()))
            .collect();
        self.main = Fifo::new(filtered_main);

        // Return the number of keys that were actually removed
        initial_count - (self.small.len() + self.main.len())
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Barrier;

    use crate::cache_system::hook::test_utils::NoOpHook;

    use super::*;

    #[test]
    // ensure the drop (and deallocation) is done outside the critical section
    fn test_get_or_put_known_drops_key_outside_critical_section() {
        std::thread::scope(|s| {
            let s3 = s3_fifo();

            // prime S3-FIFO with an entry
            let barrier_a = Arc::new(Barrier::new(2));
            let (_, evicted) = s3.get_or_put(DropBarrier::new("k", &barrier_a), Arc::new("v"), 0);
            drop_it(evicted);

            let barrier_b = Arc::new(Barrier::new(3));
            let handle_1 =
                s.get_or_put_handle(&s3, DropBarrier::new("k", &barrier_b), Arc::new("v"));
            let handle_2 =
                s.get_or_put_handle(&s3, DropBarrier::new("k", &barrier_b), Arc::new("v"));
            let handle_3 = s.spawn(move || barrier_b.wait());
            handle_1.join().unwrap();
            handle_2.join().unwrap();
            handle_3.join().unwrap();

            // drop data
            s.drop_val(s3, barrier_a);
        });
    }

    #[test]
    fn test_get_or_put_known_drops_val_outside_critical_section() {
        std::thread::scope(|s| {
            let s3 = s3_fifo();

            // prime S3-FIFO with an entry
            let barrier_a = Arc::new(Barrier::new(2));
            let (_, evicted) = s3.get_or_put(Arc::new("k"), DropBarrier::new("v", &barrier_a), 0);
            drop_it(evicted);

            let barrier_b = Arc::new(Barrier::new(3));
            let handle_1 =
                s.get_or_put_handle(&s3, Arc::new("k"), DropBarrier::new("v", &barrier_b));
            let handle_2 =
                s.get_or_put_handle(&s3, Arc::new("k"), DropBarrier::new("v", &barrier_b));
            let handle_3 = s.spawn(move || barrier_b.wait());
            handle_1.join().unwrap();
            handle_2.join().unwrap();
            handle_3.join().unwrap();

            // drop data
            s.drop_val(s3, barrier_a);
        });
    }

    #[derive(Debug)]
    struct DropBarrier<T> {
        payload: T,
        barrier: Arc<Barrier>,
    }

    impl<T> DropBarrier<T> {
        fn new(payload: T, barrier: &Arc<Barrier>) -> Arc<Self> {
            Arc::new(Self {
                payload,
                barrier: Arc::clone(barrier),
            })
        }
    }

    impl<T> PartialEq for DropBarrier<T>
    where
        T: PartialEq,
    {
        fn eq(&self, other: &Self) -> bool {
            self.payload == other.payload
        }
    }

    impl<T> Eq for DropBarrier<T> where T: Eq {}

    impl<T> Hash for DropBarrier<T>
    where
        T: Hash,
    {
        fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
            self.payload.hash(state);
        }
    }

    impl<T> HasSize for DropBarrier<T>
    where
        T: HasSize,
    {
        fn size(&self) -> usize {
            self.payload.size()
        }
    }

    impl<T> InUse for DropBarrier<T> {
        fn in_use(&mut self) -> bool {
            false
        }
    }

    impl<T> Drop for DropBarrier<T> {
        fn drop(&mut self) {
            self.barrier.wait();
        }
    }

    impl<T> AsyncDrop for DropBarrier<T>
    where
        T: AsyncDrop,
    {
        async fn async_drop(self) {
            drop(self);
        }
    }

    fn s3_fifo<K, V>() -> Arc<S3Fifo<K, V>>
    where
        K: std::fmt::Debug + Eq + Hash + HasSize + Send + Sync + 'static,
        V: HasSize + InUse + Send + Sync + 'static,
    {
        Arc::new(S3Fifo::new(
            S3Config {
                cache_name: "test",
                max_memory_size: 10,
                max_ghost_memory_size: 10_000,
                hook: Arc::new(NoOpHook::default()),
                move_to_main_threshold: 0.5,
                inflight_bytes: 10,
            },
            &metric::Registry::new(),
        ))
    }

    trait ScopeExt {
        type Handle;

        fn drop_val<T>(self, val: T, barrier: Arc<Barrier>);

        fn get_or_put_handle<K, V>(&self, s3: &Arc<S3Fifo<K, V>>, k: Arc<K>, v: V) -> Self::Handle
        where
            K: Debug + Eq + Hash + HasSize + Send + Sync + 'static,
            V: Clone + HasSize + InUse + AsyncDrop + Send + Sync + 'static;
    }

    impl<'scope> ScopeExt for &'scope std::thread::Scope<'scope, '_> {
        type Handle = std::thread::ScopedJoinHandle<'scope, ()>;

        fn drop_val<T>(self, val: T, barrier: Arc<Barrier>) {
            let handle = self.spawn(move || barrier.wait());
            drop(val);
            handle.join().unwrap();
        }

        fn get_or_put_handle<K, V>(&self, s3: &Arc<S3Fifo<K, V>>, k: Arc<K>, v: V) -> Self::Handle
        where
            K: Debug + Eq + Hash + HasSize + Send + Sync + 'static,
            V: Clone + HasSize + InUse + AsyncDrop + Send + Sync + 'static,
        {
            let s3_captured = Arc::clone(s3);
            self.spawn(move || {
                let (_, evicted) = s3_captured.get_or_put(k, v, 0);
                drop_it(evicted);
            })
        }
    }
}

#[cfg(test)]
pub(crate) mod test_migration {
    use super::*;

    #[derive(Debug, Encode, Decode)]
    pub(crate) struct TestNewLockedState<K, V> {
        entries: Vec<(K, V)>,
    }

    impl<K, V> TestNewLockedState<K, V>
    where
        K: Clone + Eq + Hash + HasSize + Send + Sync + 'static + Sized + Debug,
        V: Clone + HasSize + Send + Sync + 'static + Debug,
    {
        /// This migrate from the old `LockedState` to the new `TestNewLockedState`.
        fn migrate_from_locked_state(locked_state: LockedState<K, V>) -> Self {
            let entries = locked_state
                .main
                .iter()
                .chain(locked_state.small.iter())
                .map(|entry| {
                    let key = Arc::unwrap_or_clone(Arc::clone(&entry.key));
                    let value = entry.value.read().unwrap().clone();
                    (key, value)
                })
                .collect::<Vec<_>>();

            Self { entries }
        }
    }

    /// How to implement the migration during deserialization.
    impl<K, V, Q> Decode<Q> for VersionedSnapshot<TestNewLockedState<K, V>>
    where
        K: Decode<Q> + Encode + Clone + Eq + Hash + HasSize + Sized + Send + Sync + 'static + Debug,
        V: Decode<Q> + Encode + Clone + HasSize + Send + Sync + 'static + Debug,
        Q: Clone,
    {
        fn decode<D: bincode::de::Decoder<Context = Q>>(
            decoder: &mut D,
        ) -> Result<Self, bincode::error::DecodeError> {
            let version: u8 = Decode::decode(decoder)?;

            match version {
                VERSION_V1 => {
                    // Found the older version, with an older State.
                    //
                    // This does require us to keep the older types & deserialization code,
                    // for Version::latest - 1.
                    let locked_state: LockedState<K, V> = Decode::decode(decoder)?;
                    let new_state = TestNewLockedState::migrate_from_locked_state(locked_state);

                    Ok(Self {
                        version: Version::Test,
                        state: new_state,
                    })
                }
                #[cfg(test)]
                VERSION_TEST => {
                    // Found current versioned state => deserialize.
                    let new_state: TestNewLockedState<K, V> = Decode::decode(decoder)?;

                    Ok(Self {
                        version: Version::Test,
                        state: new_state,
                    })
                }

                _ => unreachable!("Unsupported version: {}", version),
            }
        }
    }

    pub(crate) fn assert_versioned_snapshot<K, V>(
        snapshot: &VersionedSnapshot<TestNewLockedState<K, V>>,
        entries: &[(K, V)],
        expected_version: Version,
    ) where
        K: Decode<()> + Encode + Eq + Hash + HasSize + Sized + Send + Sync + 'static + Debug,
        V: Decode<()> + Encode + PartialEq + HasSize + Send + Sync + 'static + Debug,
    {
        assert_eq!(snapshot.version, expected_version, "Unexpected version");
        assert!(
            !snapshot.state.entries.is_empty(),
            "Snapshot should not be empty"
        );
        assert_eq!(
            &snapshot.state.entries[..],
            entries,
            "Snapshot entries do not match"
        );
    }
}
