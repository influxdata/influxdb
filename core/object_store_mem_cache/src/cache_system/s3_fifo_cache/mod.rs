use async_trait::async_trait;
use object_store_metrics::cache_state::CacheState;
use std::{
    fmt::Debug,
    future::Future,
    hash::Hash,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
};
use tracing::warn;
use tracker::{
    AsyncSemaphoreMetrics, InstrumentedAsyncOwnedSemaphorePermit, InstrumentedAsyncSemaphore,
};

// for benchmarks and tests
pub use s3_fifo::{S3Config, S3Fifo, s3_fifo_entry_overhead_size};

use crate::cache_system::{AsyncDrop, DynError, InUse};

use super::{
    Cache, CacheFn, CacheRequestResult, HasSize,
    hook::{EvictResult, Hook},
    loader::Loader,
    utils::CatchUnwindDynErrorExt,
};

mod fifo;
mod ordered_set;
mod s3_fifo;

/// A cache based upon the [`S3Fifo`] algorithm.
///
/// Caching is based upon a key (`K`) and return a value (`V`).
#[derive(Debug)]
pub struct S3FifoCache<K, V, D>
where
    K: Clone + Eq + Hash + HasSize + Send + Sync + Debug + 'static,
    V: Clone + HasSize + InUse + AsyncDrop + Send + Sync + Debug + 'static,
    D: Clone + Send + Sync + 'static,
{
    cache: Arc<S3Fifo<K, V>>,
    gen_counter: Arc<AtomicU64>,
    loader: Loader<K, V, D>,
    hook: Arc<dyn Hook<K>>,
    inflight_semaphore: Arc<InstrumentedAsyncSemaphore>,
}

impl<K, V, D> S3FifoCache<K, V, D>
where
    K: Clone + Eq + Hash + HasSize + Send + Sync + Debug + 'static,
    V: Clone + HasSize + InUse + AsyncDrop + Send + Sync + Debug + 'static,
    D: Clone + Send + Sync + 'static,
{
    /// Create a new S3FifoCache from an [`S3Config`].
    pub fn new(config: S3Config<K>, metric_registry: &metric::Registry) -> Self {
        let hook = Arc::clone(&config.hook);

        let semaphore_metrics = Arc::new(AsyncSemaphoreMetrics::new(
            metric_registry,
            &[("semaphore", "s3_fifo_cache"), ("cache", config.cache_name)],
        ));
        let inflight_semaphore = Arc::new(semaphore_metrics.new_semaphore(config.inflight_bytes));

        Self {
            cache: Arc::new(S3Fifo::new(config, metric_registry)),
            gen_counter: Default::default(),
            loader: Default::default(),
            hook,
            inflight_semaphore,
        }
    }

    /// Create a new S3FifoCache from a snapshot.
    ///
    /// This function deserializes a snapshot and creates a new cache instance
    /// with the restored state.
    ///
    /// # Parameters
    /// - `config`: The S3 FIFO configuration. See [`S3Config`] for details.
    /// - `snapshot_data`: The serialized cache state data, typically created by [`snapshot`](Self::snapshot).
    /// - `shared_seed`: Provides context for bincode's deserialization process.
    ///   See [`S3Fifo::deserialize_snapshot`] for details on how this parameter
    ///   is used with bincode's [`Decode<Context>`](bincode::Decode) trait.
    ///   It's recommended to use a seed (`Q`) which is cheap to clone.
    ///
    /// # Returns
    /// A new [`S3FifoCache`] instance with the restored state.
    ///
    /// # Errors
    /// Returns a [`DynError`] if deserialization fails.
    pub fn new_from_snapshot<Q>(
        config: S3Config<K>,
        metric_registry: &metric::Registry,
        snapshot_data: &[u8],
        shared_seed: &Q,
    ) -> Result<Self, DynError>
    where
        Q: Clone,
        K: bincode::Encode + bincode::Decode<Q>,
        V: bincode::Encode + bincode::Decode<Q>,
    {
        let hook = Arc::clone(&config.hook);

        let semaphore_metrics = Arc::new(AsyncSemaphoreMetrics::new(
            metric_registry,
            &[("semaphore", "s3_fifo_cache"), ("cache", config.cache_name)],
        ));
        let inflight_semaphore = Arc::new(semaphore_metrics.new_semaphore(config.inflight_bytes));

        let cache = Arc::new(S3Fifo::new_from_snapshot::<Q>(
            config,
            metric_registry,
            snapshot_data,
            shared_seed,
        )?);

        Ok(Self {
            cache,
            gen_counter: Default::default(),
            loader: Default::default(),
            hook,
            inflight_semaphore,
        })
    }

    /// Create a snapshot of the S3Fifo cache state.
    ///
    /// This function serializes the locked_state using bincode, allowing for
    /// persistence and recovery of the cache state.
    ///
    /// # Returns
    /// A `Vec<u8>` containing the serialized cache state.
    ///
    /// # Errors
    /// Returns a [`DynError`] if serialization fails.
    pub fn snapshot(&self) -> Result<Vec<u8>, DynError>
    where
        K: bincode::Encode,
        V: bincode::Encode,
    {
        self.cache.snapshot()
    }

    /// Get the value (`V`) from the cache.
    /// If the entry does not exist, run the future (`F`) that returns a value (`Arc<V>`).
    ///
    /// The value (`V`) is inserted after completion of a future. Some data (`D`) may be accessable earlier,
    /// before the future finishes.
    fn get_or_fetch_impl<F, Fut>(&self, k: &K, f: F, d: D, size_hint: usize) -> CacheState<V, D>
    where
        F: FnOnce() -> Fut + Send + 'static,
        Fut: Future<Output = CacheRequestResult<V>> + Send + 'static,
    {
        // fast path to already-featched value
        if let Some(entry) = self.cache.get(k) {
            return CacheState::WasCached(Ok(entry.value().clone()));
        }

        // slow path
        let gen_counter_captured = Arc::clone(&self.gen_counter);
        let k_captured = Arc::new(k.clone());
        let hook_captured = Arc::clone(&self.hook);
        let cache_captured = Arc::downgrade(&self.cache);
        let semaphore_captured = Arc::clone(&self.inflight_semaphore);
        let fut = move || async move {
            // Acquire insertion permit if size_hint is provided
            let insertion_permit =
                try_acquire_permit(size_hint as u32, &semaphore_captured).await?;

            // now we inform the hook of insertion
            let generation = gen_counter_captured.fetch_add(1, Ordering::SeqCst);
            hook_captured.insert(generation, &k_captured);

            // get the actual value (`V`)
            let fut = f();
            let fetch_res = fut.catch_unwind_dyn_error().await;

            // insert into cache
            let fetch_res = match fetch_res {
                Ok(v) => {
                    if let Some(cache) = cache_captured.upgrade() {
                        // NOTES:
                        // - Don't involve hook here because the cache is doing that for us correctly, even if the key is
                        //   already stored.
                        // - Tell tokio that this is potentially expensive. This is due to the fact that inserting new values
                        //   may free existing ones and the relevant allocator accounting can be rather pricey.
                        // - We pass `v` by value because there is the small chance that between checking the S3-FIFO
                        //   and creating this loader future, the S3-FIFO might have been updated. In that case
                        //   `S3Fifo::get_or_put` will return the exiting entry, but also the to-be-inserted (that we
                        //   originally fetched here) one as "to be evicted" (so we don't have two copies).
                        let k = Arc::clone(&k_captured);
                        let (entry, evicted) = tokio::task::spawn_blocking(move || {
                            let (entry, evicted) = cache.get_or_put(k, v, generation);
                            (entry, evicted)
                        })
                        .await
                        .expect("never fails");

                        evicted.async_drop().await;
                        Ok(entry.value().clone())
                    } else {
                        // notify "fetched" and instantly evict, because underlying cache is gone (during shutdown)
                        let size = v.size();
                        hook_captured.fetched(generation, &k_captured, Ok(size));
                        hook_captured.evict(generation, &k_captured, EvictResult::Fetched { size });

                        Ok(v)
                    }
                }
                Err(e) => {
                    hook_captured.fetched(generation, &k_captured, Err(&e));
                    hook_captured.evict(generation, &k_captured, EvictResult::Failed);

                    Err(e)
                }
            };

            drop(insertion_permit);

            fetch_res
        };
        let load = self.loader.load(k.clone(), fut, d);

        // if already loading => then we don't have to spawn a task for insertion into cache (once loading is done).
        if load.already_loading() {
            let early_access = load.data().clone();
            let fut = load.into_future();
            CacheState::AlreadyLoading(Box::pin(fut), early_access)
        } else {
            let early_access = load.data().clone();
            let fut = load.into_future();
            CacheState::NewEntry(Box::pin(fut), early_access)
        }
    }

    /// Returns an iterator of all keys currently in the [`S3Fifo`] cache.
    ///
    /// Note that the keys listed in the cache are those which have returned from the
    /// [`CacheFn`] function, i.e. they are the keys that have been successfully fetched.
    ///
    /// These keys do not have any guaranteed ordering.
    pub fn list(&self) -> impl Iterator<Item = Arc<K>> {
        self.cache.keys()
    }

    /// Evict multiple keys from the S3FifoCache, in a blocking manner.
    ///
    /// This method directly removes entries from the cache without going through
    /// the normal eviction process, where the S3-Fifo algorthim decides what to evict.
    /// This is useful for cache management operations like repair/validation.
    ///
    /// This method is blocking, and holds a mutex in order to replace the [`S3Fifo`] cache
    /// at once.
    ///
    /// Returns the number of keys that were successfully evicted. If a key does not
    /// exist in the cache and cannot be evicted, it will be ignored (and the
    /// returned count of evicted items will be lower).
    pub fn evict_keys(&self, keys: impl Iterator<Item = K>) -> usize {
        self.cache.remove_keys(keys)
    }
}

#[async_trait]
impl<K, V, D> Cache<K, V, D> for S3FifoCache<K, V, D>
where
    K: Clone + Debug + Eq + Hash + HasSize + Send + Sync + 'static,
    V: Clone + Debug + HasSize + InUse + AsyncDrop + Send + Sync + 'static,
    D: Clone + Debug + Send + Sync + 'static,
{
    /// Get an existing key or start a new fetch process.
    ///
    /// Returns a [`CacheState`] that contains either the cached value or a future that resolves to the value.
    /// If data is loading, the early access data (`D`) is included in the [`CacheState`].
    ///
    /// For a given key, this function will return a value immediately if it is already cached.
    /// If it is not cached and not already loading, it will start a new fetch process
    /// using the early access data (`D`) provided by the caller.
    ///
    /// If a fetch process is already in progress, it will return a different early access (`D`)
    /// which is tied to the ongoing fetch process.
    fn get_or_fetch(&self, k: &K, f: CacheFn<V>, d: D, size_hint: usize) -> CacheState<V, D> {
        self.get_or_fetch_impl(k, f, d, size_hint)
    }

    fn get(&self, k: &K) -> Option<CacheRequestResult<V>> {
        self.cache.get(k).map(|entry| Ok(entry.value().clone()))
    }

    fn len(&self) -> usize {
        self.cache.len()
    }

    fn is_empty(&self) -> bool {
        self.cache.is_empty()
    }

    fn prune(&self) {
        // Intentionally unimplemented, S3Fifo handles its own pruning
    }
}

async fn try_acquire_permit(
    size: u32,
    semaphore: &Arc<InstrumentedAsyncSemaphore>,
) -> Result<InstrumentedAsyncOwnedSemaphorePermit, DynError> {
    if let Some(delta) = semaphore.ensure_total_permits(size as usize) {
        warn!(
            size,
            delta = delta.get(),
            "encountered overlarge file that would be blocked by the semaphore, bumping total semaphore permits",
        );
    }

    semaphore
        .acquire_many_owned(size, None)
        .await
        .map_err(|e| Arc::new(e) as DynError)
}

#[cfg(test)]
mod tests {
    use core::panic;
    use std::sync::atomic::AtomicBool;

    use super::*;
    use object_store_metrics::cache_state::CacheStateKind;

    use crate::cache_system::{
        AsyncDrop,
        hook::test_utils::{NoOpHook, TestHook, TestHookRecord},
        s3_fifo_cache::s3_fifo::{
            Version, VersionedSnapshot,
            test_migration::{TestNewLockedState, assert_versioned_snapshot},
        },
        test_utils::{
            TestSetup, TestValue, extract_full_state, extract_future_and_state, gen_cache_tests,
            runtime_shutdown,
        },
    };

    use futures::future::FutureExt;
    use futures_test_utils::AssertFutureExt;
    use tokio::sync::Barrier;

    #[test]
    fn test_runtime_shutdown() {
        runtime_shutdown(setup());
    }

    #[tokio::test]
    async fn test_ghost_set_is_limited() {
        let cache = S3FifoCache::<Arc<str>, Arc<str>, ()>::new(
            S3Config {
                cache_name: "test",
                max_memory_size: 10,
                max_ghost_memory_size: 10,
                move_to_main_threshold: 0.1,
                hook: Arc::new(NoOpHook::default()),
                inflight_bytes: 10,
            },
            &metric::Registry::new(),
        );
        let k1 = Arc::from("x");
        let v1: Arc<str> = Arc::from("value");
        let size_hint = v1.size();

        let cache_state_k1 = cache.get_or_fetch(
            &k1,
            Box::new(|| futures::future::ready(Ok(v1)).boxed()),
            (),
            size_hint,
        );
        assert_eq!(cache_state_k1.kind(), CacheStateKind::NewEntry);
        cache_state_k1.await_inner().await.unwrap();

        assert_ne!(Arc::strong_count(&k1), 1);

        for i in 0..100 {
            let k = Arc::from(i.to_string());
            let v: Arc<str> = Arc::from("value");
            let size_hint = v.size();
            let cache_state = cache.get_or_fetch(
                &k,
                Box::new(|| futures::future::ready(Ok(v)).boxed()),
                (),
                size_hint,
            );
            assert_eq!(cache_state.kind(), CacheStateKind::NewEntry);
            cache_state.await_inner().await.unwrap();
        }

        assert_eq!(Arc::strong_count(&k1), 1);
    }

    #[tokio::test]
    async fn test_evict_previously_heavy_used_key() {
        let hook = Arc::new(TestHook::default());
        let cache = S3FifoCache::<Arc<str>, Arc<TestValue>, ()>::new(
            S3Config {
                cache_name: "test",
                max_memory_size: 10,
                max_ghost_memory_size: 10,
                move_to_main_threshold: 0.5,
                hook: Arc::clone(&hook) as _,
                inflight_bytes: 10, // this is the same as the max_memory_size, and smaller than the entry sizes (71 & 67)
            },
            &metric::Registry::new(),
        );

        let k_heavy = Arc::from("heavy");
        let size_hint = Arc::new(TestValue(5)).size();

        // make it heavy
        for _ in 0..2 {
            let cache_state = cache.get_or_fetch(
                &k_heavy,
                Box::new(|| futures::future::ready(Ok(Arc::new(TestValue(5)))).boxed()),
                (),
                size_hint,
            );
            cache_state.await_inner().await.unwrap();
        }

        // add new keys
        let k_new = Arc::from("new");
        let size_hint = Arc::new(TestValue(5)).size();

        // make them heavy enough to evict old data
        for _ in 0..2 {
            let cache_state = cache.get_or_fetch(
                &k_new,
                Box::new(|| futures::future::ready(Ok(Arc::new(TestValue(5)))).boxed()),
                (),
                size_hint,
            );
            cache_state.await_inner().await.unwrap();
        }

        // old heavy key is gone
        assert!(cache.get(&k_heavy).is_none());

        assert_eq!(
            hook.records(),
            vec![
                TestHookRecord::Insert(0, Arc::clone(&k_heavy)),
                TestHookRecord::Fetched(0, Arc::clone(&k_heavy), Ok(87)),
                TestHookRecord::Insert(1, Arc::clone(&k_new)),
                TestHookRecord::Fetched(1, Arc::clone(&k_new), Ok(83)),
                TestHookRecord::Evict(0, Arc::clone(&k_heavy), EvictResult::Fetched { size: 87 }),
            ],
        );
    }

    gen_cache_tests!(setup);

    fn setup() -> TestSetup {
        let observer = Arc::new(TestHook::default());
        TestSetup {
            cache: Arc::new(S3FifoCache::<_, _, ()>::new(
                S3Config {
                    cache_name: "test",
                    max_memory_size: 10_000,
                    max_ghost_memory_size: 10_000,
                    move_to_main_threshold: 0.25,
                    hook: Arc::clone(&observer) as _,
                    inflight_bytes: 2_500,
                },
                &metric::Registry::new(),
            )),
            observer,
        }
    }

    // Snapshot functionality tests
    #[derive(Debug, Clone, PartialEq, Eq, Hash, bincode::Encode, bincode::Decode)]
    struct SnapshotTestKey(String);

    impl HasSize for SnapshotTestKey {
        fn size(&self) -> usize {
            self.0.len()
        }
    }

    #[derive(Debug, Clone, PartialEq, bincode::Encode)]
    struct SnapshotTestValue(Vec<u8>);

    impl HasSize for SnapshotTestValue {
        fn size(&self) -> usize {
            self.0.len()
        }
    }

    impl InUse for SnapshotTestValue {
        fn in_use(&mut self) -> bool {
            false
        }
    }

    // bincode Decode implementation for SnapshotTestValue
    impl<Q> bincode::Decode<Q> for SnapshotTestValue {
        fn decode<D: bincode::de::Decoder<Context = Q>>(
            decoder: &mut D,
        ) -> Result<Self, bincode::error::DecodeError> {
            let value: Vec<u8> = bincode::Decode::decode(decoder)?;
            Ok(Self(value))
        }
    }

    impl AsyncDrop for SnapshotTestValue {
        async fn async_drop(self) {
            drop(self);
        }
    }

    #[tokio::test]
    async fn test_snapshot_serialization_roundtrip() {
        let metric_registry = metric::Registry::new();
        let hook: Arc<dyn Hook<SnapshotTestKey>> = Arc::new(NoOpHook::default());

        // Test with multiple entries to ensure comprehensive serialization
        let test_data = vec![
            (
                SnapshotTestKey("key1".to_string()),
                SnapshotTestValue(vec![10, 20, 30]),
            ),
            (
                SnapshotTestKey("key2".to_string()),
                SnapshotTestValue(vec![40, 50]),
            ),
            (
                SnapshotTestKey("key3".to_string()),
                SnapshotTestValue(vec![60, 70, 80, 90]),
            ),
            (
                SnapshotTestKey("key4".to_string()),
                SnapshotTestValue(vec![100, 110, 120]),
            ),
        ];

        // Create the cache with size limits such that we populate the ghost set too
        // such that the OrderedSet (de)serialization is tested.
        let key_size = test_data[0].0.size();
        let value_size = Arc::new(test_data[0].1.clone()).size();
        let max_memory_size = ((key_size + value_size) as f32 * 3.5).round() as usize;
        let cache: S3FifoCache<SnapshotTestKey, SnapshotTestValue, ()> = S3FifoCache::new(
            S3Config {
                cache_name: "test",
                max_memory_size,
                max_ghost_memory_size: (key_size as f32 * 1.9).round() as usize,
                move_to_main_threshold: 0.1,
                hook: Arc::clone(&hook),
                inflight_bytes: max_memory_size.div_ceil(4), // 25%
            },
            &metric_registry,
        );

        // Insert all test data
        for (key, value) in &test_data {
            let size_hint = value.size();
            let value_clone = value.clone();
            let cache_state = cache.get_or_fetch(
                key,
                Box::new(move || async move { Ok(value_clone) }.boxed()),
                (),
                size_hint,
            );
            cache_state.await_inner().await.unwrap();
        }

        // Create snapshot
        let snapshot = cache.snapshot().unwrap();
        assert!(!snapshot.is_empty(), "Snapshot should contain data");

        // Verify that the last inserted entries is still accessible in the original cache
        assert_eq!(cache.len(), 2, "Cache should contain 2 entries");
        for (key, expected_value) in &test_data[2..] {
            let retrieved_value = cache.get(key).unwrap().unwrap();
            assert_eq!(
                &retrieved_value, expected_value,
                "Value mismatch for key: {key:?}",
            );
        }

        // assert that we had 1 ghost entry (& therefore these tests cover the OrderedSet serialization)
        assert_eq!(
            cache.cache.ghost_len(),
            1,
            "Cache should have 1 ghost entry"
        );

        // Create a new cache from the snapshot
        let restored_cache: S3FifoCache<SnapshotTestKey, SnapshotTestValue, ()> =
            S3FifoCache::new_from_snapshot(
                S3Config {
                    cache_name: "test",
                    max_memory_size: 1000,
                    max_ghost_memory_size: 500,
                    move_to_main_threshold: 0.1,
                    hook: Arc::clone(&hook),
                    inflight_bytes: 250,
                },
                &metric_registry,
                &snapshot,
                &(),
            )
            .unwrap();

        // Verify last two entries are preserved in the restored cache
        assert_eq!(
            restored_cache.len(),
            2,
            "Restored cache should contain 2 entries"
        );
        for (key, expected_value) in &test_data[2..] {
            let retrieved_value = restored_cache.get(key).unwrap().unwrap();
            assert_eq!(
                &retrieved_value, expected_value,
                "Value mismatch in restored cache for key: {key:?}"
            );
        }

        // Verify the cache lengths match
        assert_eq!(
            cache.len(),
            restored_cache.len(),
            "Cache lengths should match after restore"
        );

        // assert that we had 1 ghost entry in restored cache (OrderedSet deserialization)
        assert_eq!(
            restored_cache.cache.ghost_len(),
            1,
            "Restored cache should have 1 ghost entry"
        );
    }

    #[tokio::test]
    async fn test_snapshot_versioning() {
        let metric_registry = metric::Registry::new();
        let hook: Arc<dyn Hook<SnapshotTestKey>> = Arc::new(NoOpHook::default());

        // Test with multiple entries to ensure comprehensive serialization
        let test_data = vec![
            (
                SnapshotTestKey("key1".to_string()),
                SnapshotTestValue(vec![10, 20, 30]),
            ),
            (
                SnapshotTestKey("key2".to_string()),
                SnapshotTestValue(vec![40, 50]),
            ),
            (
                SnapshotTestKey("key3".to_string()),
                SnapshotTestValue(vec![60, 70, 80, 90]),
            ),
            (
                SnapshotTestKey("key4".to_string()),
                SnapshotTestValue(vec![100, 110, 120]),
            ),
        ];

        // Create the cache with all entries.
        let cache: S3FifoCache<SnapshotTestKey, SnapshotTestValue, ()> = S3FifoCache::new(
            S3Config {
                cache_name: "test",
                max_memory_size: 10_000,
                max_ghost_memory_size: 10_000,
                move_to_main_threshold: 0.1,
                hook: Arc::clone(&hook),
                inflight_bytes: 2_500,
            },
            &metric_registry,
        );

        // Insert all test data
        for (key, value) in &test_data {
            let size_hint = value.size();
            let value_clone = value.clone();
            let cache_state = cache.get_or_fetch(
                key,
                Box::new(move || async move { Ok(value_clone) }.boxed()),
                (),
                size_hint,
            );
            cache_state.await_inner().await.unwrap();
        }

        // Create snapshot
        let snapshot = cache.snapshot().unwrap();
        assert!(!snapshot.is_empty(), "Snapshot should contain data");

        // Snapshot can be deserialized into new version.
        // (This would be the code used in the S3Fifo::deserialize_snapshot).
        let (versioned_snapshot, _): (
            VersionedSnapshot<TestNewLockedState<SnapshotTestKey, SnapshotTestValue>>,
            usize,
        ) = bincode::decode_from_slice_with_context(&snapshot, bincode::config::standard(), ())
            .unwrap();

        // Assert that we migrated to the new/updated Test version.
        assert_versioned_snapshot(&versioned_snapshot, &test_data, Version::Test);
    }

    #[tokio::test]
    async fn test_snapshot_restore_from_v1_static_data() {
        let snapshot_data = &[
            // oldest entry is in the ghost queue (a.k.a. OrderedSet)
            (
                SnapshotTestKey("key2".to_string()),
                SnapshotTestValue(vec![40, 50]),
            ),
            // two entries are in the Fifo cache queue
            (
                SnapshotTestKey("key3".to_string()),
                SnapshotTestValue(vec![60, 70, 80, 90]),
            ),
            (
                SnapshotTestKey("key4".to_string()),
                SnapshotTestValue(vec![100, 110, 120]),
            ),
        ];

        // Static snapshot data representing a Version::V1 snapshot
        // of the above `snapshot_data`.
        //
        // The snapshot format for V1 is:
        // [version_byte(1), serialized_locked_state...]
        let v1_snapshot_data: &[u8] = &[
            1, 0, 0, 2, 4, 107, 101, 121, 51, 4, 60, 70, 80, 90, 2, 0, 4, 107, 101, 121, 52, 3,
            100, 110, 120, 3, 0, 159, 1, 1, 4, 107, 101, 121, 50, 28, 0, 0,
        ];

        // Verify that the V1 snapshot data starts with VERSION_V1 (1)
        assert_eq!(
            v1_snapshot_data[0], 1,
            "V1 snapshot should start with VERSION_V1 (1)"
        );

        // Create a new cache from the static V1 snapshot data
        // This tests that S3FifoCache::new_from_snapshot can restore from V1 format
        let metric_registry = metric::Registry::new();
        let hook: Arc<dyn Hook<SnapshotTestKey>> = Arc::new(NoOpHook::default());
        let restored_cache: S3FifoCache<SnapshotTestKey, SnapshotTestValue, ()> =
            S3FifoCache::new_from_snapshot(
                S3Config {
                    cache_name: "test",
                    max_memory_size: 10_000,
                    max_ghost_memory_size: 10_000,
                    move_to_main_threshold: 0.1,
                    hook: Arc::clone(&hook),
                    inflight_bytes: 2_500,
                },
                &metric_registry,
                v1_snapshot_data,
                &(),
            )
            .unwrap();

        // Verify last two entries are preserved in the restored cache
        assert_eq!(
            restored_cache.len(),
            2,
            "Restored cache should contain 2 entries"
        );
        for (key, expected_value) in &snapshot_data[1..] {
            let retrieved_value = restored_cache.get(key).unwrap().unwrap();
            assert_eq!(
                &retrieved_value, expected_value,
                "Value mismatch in restored cache for key: {key:?}"
            );
        }

        // assert that we had 1 ghost entry in restored cache (OrderedSet deserialization)
        assert_eq!(
            restored_cache.cache.ghost_len(),
            1,
            "Restored cache should have 1 ghost entry"
        );
    }

    #[tokio::test]
    async fn test_get_or_fetch_with_early_access() {
        let cache = S3FifoCache::<Arc<str>, Arc<str>, Arc<str>>::new(
            S3Config {
                cache_name: "test",
                max_memory_size: 1000,
                max_ghost_memory_size: 500,
                move_to_main_threshold: 0.1,
                hook: Arc::new(NoOpHook::default()),
                inflight_bytes: 250,
            },
            &metric::Registry::new(),
        );

        let key = Arc::from("test_key");
        let early_access_data = Arc::from("early_data");
        let final_value: Arc<str> = Arc::from("final_value");

        /* Test case 1: New entry - should return future and early access data */
        let final_value_clone = Arc::clone(&final_value);
        let size_hint = final_value_clone.size();
        let cache_state = cache.get_or_fetch(
            &key,
            Box::new(move || {
                let value = Arc::clone(&final_value_clone);
                async move {
                    // Simulate some async work
                    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
                    Ok(value)
                }
                .boxed()
            }),
            Arc::clone(&early_access_data),
            size_hint,
        );
        let (got_fut, got_early_access, got_state) = extract_full_state(cache_state);

        // Verify return signature for new entry
        assert_eq!(got_state, CacheStateKind::NewEntry);
        assert!(matches!(got_early_access, Some(e) if Arc::ptr_eq(&e, &early_access_data)));

        // Await the future and verify it returns the expected value
        let result = got_fut.await;
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), final_value);

        /* Test case 2: Already cached - should return immediate result with no early access */
        let cache_state = cache.get_or_fetch(
            &key,
            Box::new(|| async { panic!("should not be called") }.boxed()),
            Arc::from("unused_early_data"),
            size_hint,
        );

        // Verify return signature for cached entry
        assert_eq!(cache_state.kind(), CacheStateKind::WasCached);
        assert!(cache_state.early_access_data().is_none());

        // For cached entries, we should use await_inner directly
        let result = cache_state.await_inner().await;
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), final_value);

        /* Test case 3: Already loading - simulate concurrent access */
        let key = Arc::from("new_test_key");
        let early_access_data = Arc::from("new_early_data");
        let final_value: Arc<str> = Arc::from("new_final_value");

        // Start first request (this will be loading)
        let final_value_clone = Arc::clone(&final_value);
        let size_hint = final_value_clone.size();
        let cache_state_1 = cache.get_or_fetch(
            &key,
            Box::new(move || {
                let value = Arc::clone(&final_value_clone);
                async move {
                    // Simulate longer async work
                    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
                    Ok(value)
                }
                .boxed()
            }),
            Arc::clone(&early_access_data),
            size_hint,
        );
        let (got_fut_1, got_early_access_1, got_state_1) = extract_full_state(cache_state_1);

        // Start second request while first is still loading
        let different_early_access = Arc::from("different_early_data");
        let cache_state_2 = cache.get_or_fetch(
            &key,
            Box::new(|| async { panic!("should not be called") }.boxed()),
            different_early_access,
            size_hint,
        );
        let (got_fut_2, got_early_access_2, state_2) = extract_full_state(cache_state_2);

        // Verify return signatures
        assert_eq!(got_state_1, CacheStateKind::NewEntry);
        assert!(matches!(got_early_access_1, Some(e) if Arc::ptr_eq(&e, &early_access_data)));

        assert_eq!(state_2, CacheStateKind::AlreadyLoading);
        // The second request should get the early access data from the ongoing load
        assert!(matches!(got_early_access_2, Some(e) if Arc::ptr_eq(&e, &early_access_data)));

        // Both futures should resolve to the same value
        let (result1, result2) = tokio::join!(got_fut_1, got_fut_2);
        assert!(result1.is_ok());
        assert!(result2.is_ok());
        assert_eq!(result1.unwrap(), final_value);
        assert_eq!(result2.unwrap(), final_value);
    }

    #[tokio::test]
    async fn test_list_fn_only_includes_fully_loaded_entries() {
        let cache = S3FifoCache::<Arc<str>, Arc<str>, ()>::new(
            S3Config {
                cache_name: "test",
                max_memory_size: 1000,
                max_ghost_memory_size: 500,
                move_to_main_threshold: 0.1,
                hook: Arc::new(NoOpHook::default()),
                inflight_bytes: 250,
            },
            &metric::Registry::new(),
        );

        let key1 = Arc::from("key1".to_string());
        let key2 = Arc::from("key2".to_string());

        // Add first entry
        let cache_state1 = cache.get_or_fetch(
            &key1,
            Box::new(|| futures::future::ready(Ok(Arc::from("value1"))).boxed()),
            (),
            (Arc::from("value1") as Arc<str>).size(),
        );
        cache_state1.await_inner().await.unwrap();

        // Add second entry
        let cache_state2 = cache.get_or_fetch(
            &key2,
            Box::new(|| {
                async move {
                    tokio::time::sleep(std::time::Duration::from_secs(1)).await;
                    Ok(Arc::from("value2"))
                }
                .boxed()
            }),
            (),
            (Arc::from("value2") as Arc<str>).size(),
        );

        // List keys in the cache
        let keys: Vec<Arc<str>> = cache.list().map(Arc::unwrap_or_clone).collect();
        assert_eq!(keys.len(), 1);
        assert!(keys.contains(&key1), "key1 should be in the cache");
        assert!(
            !keys.contains(&key2),
            "key2 should not be in the cache::list() yet, since still loading"
        );

        // wait until second entry is loaded
        cache_state2.await_inner().await.unwrap();

        // Now both keys should be in the cache::list()
        let keys: Vec<Arc<str>> = cache.list().map(Arc::unwrap_or_clone).collect();
        assert_eq!(keys.len(), 2);
        assert!(keys.contains(&key1), "key1 should be in the cache");
        assert!(keys.contains(&key2), "key2 should be in the cache");
    }

    #[tokio::test]
    async fn test_in_use_prevents_eviction() {
        // Create cache with very limited space - only enough for 1 entry (size 10)
        let cache = S3FifoCache::<Arc<str>, Arc<str>, ()>::new(
            S3Config {
                cache_name: "test",
                max_memory_size: 20, // Only enough space for 1 entry
                max_ghost_memory_size: 100,
                move_to_main_threshold: 0.1,
                hook: Arc::new(NoOpHook::default()),
                inflight_bytes: 100,
            },
            &metric::Registry::new(),
        );

        let key1 = Arc::from("key1");
        let key2 = Arc::from("key2");

        // Insert key1 with Arc<V>
        let value1 = Arc::<str>::from("value1");
        let size_hint1 = value1.size();
        let cache_state = cache.get_or_fetch(
            &key1,
            Box::new(move || futures::future::ready(Ok(value1)).boxed()),
            (),
            size_hint1,
        );
        assert_eq!(cache_state.kind(), CacheStateKind::NewEntry);
        let value1 = cache_state.await_inner().await.unwrap();

        // Verify key1 is in cache
        assert!(cache.get(&key1).is_some(), "key1 should be in cache");

        // Get key1 Arc<V> and hold a reference to it
        let held_value = cache.get(&key1).unwrap().unwrap();

        // Insert key2 with Arc<V> - this should try to evict key1 but fail because it's in use
        let value2 = Arc::<str>::from("value2");
        let size_hint2 = value2.size();
        let cache_state = cache.get_or_fetch(
            &key2,
            Box::new(move || futures::future::ready(Ok(value2)).boxed()),
            (),
            size_hint2,
        );
        assert_eq!(cache_state.kind(), CacheStateKind::NewEntry);
        let (mut res2, _) = extract_future_and_state(cache_state);

        // Give some time for the cache to try eviction
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        // key1 should still be in cache because it's in use
        assert!(
            cache.get(&key1).is_some(),
            "key1 should still be in cache due to InUse protection"
        );

        // res2 should still be pending because key1 couldn't be evicted to make space
        res2.assert_pending().await;

        // Drop the reference to key1's value - this should allow eviction
        drop(held_value);
        drop(value1);

        // Small delay to allow eviction to proceed
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;

        // res2 should now complete successfully
        let value2 = res2.await.unwrap();
        assert_eq!(value2, Arc::from("value2"));

        // key1 should now be evicted and key2 should be in cache
        assert!(
            cache.get(&key1).is_none(),
            "key1 should be evicted after reference dropped"
        );
        assert!(cache.get(&key2).is_some(), "key2 should be in cache");
    }

    #[tokio::test]
    async fn test_inflight_semaphore_blocks_when_limit_exceeded() {
        use tokio::sync::Barrier;

        // Create cache with very limited inflight bytes (only 100 bytes)
        let cache = S3FifoCache::<Arc<str>, Arc<str>, ()>::new(
            S3Config {
                cache_name: "test",
                max_memory_size: 10000,
                max_ghost_memory_size: 5000,
                move_to_main_threshold: 0.1,
                hook: Arc::new(NoOpHook::default()),
                inflight_bytes: 100, // 100 bytes allowed inflight
            },
            &metric::Registry::new(),
        );

        let key1 = Arc::from("key1");
        let key2 = Arc::from("key2");

        // Start first request with size hint of 80 bytes (within limit)
        let barrier = Arc::new(Barrier::new(2));
        let barrier_clone1 = Arc::clone(&barrier);
        let cache_state = cache.get_or_fetch(
            &key1,
            Box::new(move || {
                async move {
                    // halt download (in flight bytes) while barrier is held
                    barrier_clone1.wait().await;
                    Ok(Arc::from("value1"))
                }
                .boxed()
            }),
            (),
            80,
        );
        assert_eq!(cache_state.kind(), CacheStateKind::NewEntry);
        let (mut res1, _) = extract_future_and_state(cache_state);
        res1.assert_pending().await;

        // Confirm first put is still loading (holding semaphore)
        let cache_state = cache.get_or_fetch(
            &key1,
            Box::new(move || {
                panic!("should not be called");
            }),
            (),
            80,
        );
        assert_eq!(cache_state.kind(), CacheStateKind::AlreadyLoading);
        let (mut res1_again, _) = extract_future_and_state(cache_state);
        res1_again.assert_pending().await;

        // Start second request with size hint of 50 bytes (would exceed limit)
        let was_called = Arc::new(AtomicBool::new(false));
        let was_called_captured = Arc::clone(&was_called);
        let cache_state_putfirst = cache.get_or_fetch(
            &key2,
            Box::new(move || {
                was_called_captured.store(true, Ordering::SeqCst);
                futures::future::ready(Ok(Arc::from("value2"))).boxed()
            }),
            (),
            50,
        );
        assert_eq!(cache_state_putfirst.kind(), CacheStateKind::NewEntry);
        let (mut res2, _) = extract_future_and_state(cache_state_putfirst);
        res2.assert_pending().await;

        // Confirm second put is seen as loading
        let cache_state_putagain = cache.get_or_fetch(
            &key2,
            Box::new(move || {
                panic!("should not be called");
            }),
            (),
            50,
        );
        assert_eq!(cache_state_putagain.kind(), CacheStateKind::AlreadyLoading);
        let (mut res2_again, _) = extract_future_and_state(cache_state_putagain);
        res2_again.assert_pending().await;
        // although the fetch has not begun yet
        assert!(!was_called.load(Ordering::SeqCst));

        // Release first request by unblocking its barrier
        barrier.wait().await;

        // Wait for first request to complete and release semaphore
        let result1 = res1.await.unwrap();
        assert_eq!(result1, Arc::from("value1"));

        // Wait for second request to complete
        let result2 = res2.await.unwrap();
        assert_eq!(result2, Arc::from("value2"));
    }

    #[tokio::test]
    async fn test_evict_keys_small_queue() {
        let hook = Arc::new(TestHook::default());
        let cache = S3FifoCache::<Arc<str>, Arc<str>, ()>::new(
            S3Config {
                cache_name: "test",
                max_memory_size: 1000,
                max_ghost_memory_size: 500,
                move_to_main_threshold: 0.1,
                hook: Arc::clone(&hook) as _,
                inflight_bytes: 250,
            },
            &metric::Registry::new(),
        );

        // Insert 5 keys in order: key1, key2, key3, key4, key5
        let keys = vec!["key1", "key2", "key3", "key4", "key5"];
        let mut inserted_keys = Vec::new();

        for key_str in &keys {
            let key = Arc::from(*key_str);
            let value = Arc::from(format!("value_{}", key_str));
            inserted_keys.push(Arc::clone(&key));

            let cache_state = cache.get_or_fetch(
                &key,
                Box::new({
                    let value = Arc::clone(&value);
                    move || futures::future::ready(Ok(value)).boxed()
                }),
                (),
                value.size(),
            );
            assert_eq!(cache_state.kind(), CacheStateKind::NewEntry);
            cache_state.await_inner().await.unwrap();
        }

        // Verify all keys are in the cache
        assert_eq!(cache.len(), 5);
        for key in &inserted_keys {
            assert!(cache.get(key).is_some(), "Key {key:?} should be in cache");
        }

        // Get list of keys before eviction to verify ordering preservation
        let keys_before: Vec<Arc<str>> = cache
            .cache
            .small_queue_keys()
            .into_iter()
            .map(Arc::unwrap_or_clone)
            .collect();
        assert_eq!(keys_before.len(), 5, "Should have 5 keys before eviction");

        // Confirm have empty ghost queue
        assert_eq!(cache.cache.ghost_len(), 0, "Ghost queue should be empty");

        // Evict only key2 and key4 (selective eviction)
        let keys_to_evict = vec![
            Arc::clone(&inserted_keys[1]), // key2
            Arc::clone(&inserted_keys[3]), // key4
        ];
        let evicted_count = cache.evict_keys(keys_to_evict.clone().into_iter());
        assert_eq!(evicted_count, 2, "Should have evicted exactly 2 keys");

        // Verify cache size is reduced
        assert_eq!(
            cache.len(),
            3,
            "Cache should contain 3 entries after eviction"
        );

        // Get list of keys after eviction
        let keys_after: Vec<Arc<str>> = cache
            .cache
            .small_queue_keys()
            .into_iter()
            .map(Arc::unwrap_or_clone)
            .collect();
        let expected_remaining_keys: Vec<Arc<str>> = keys_before
            .into_iter()
            .filter(|key| !keys_to_evict.contains(key))
            .collect();
        assert_eq!(
            keys_after, expected_remaining_keys,
            "Remaining keys should match expected keys, and retain the same ordering"
        );

        // Check that evicted keys are removed from S3Fifo::entries
        for evicted_key in &keys_to_evict {
            assert!(
                !cache.cache.contains_key_in_entries(evicted_key),
                "Evicted key {evicted_key:?} should be removed from entries"
            );
        }

        // Check that remaining keys are still in S3Fifo::entries
        for remaining_key in &expected_remaining_keys {
            assert!(
                cache.cache.contains_key_in_entries(remaining_key),
                "Remaining key {remaining_key:?} should still be in entries"
            );
        }

        // Check ghost queue is still empty
        assert_eq!(
            cache.cache.ghost_len(),
            0,
            "Ghost queue should remain empty"
        );
    }

    #[tokio::test]
    async fn test_evict_keys_main_queue_and_ghost() {
        let hook = Arc::new(TestHook::default());
        let cache = S3FifoCache::<Arc<str>, Arc<str>, ()>::new(
            S3Config {
                cache_name: "test",
                max_memory_size: 150 + 100,
                max_ghost_memory_size: 150,
                move_to_main_threshold: 0.3,
                hook: Arc::clone(&hook) as _,
                inflight_bytes: 50,
            },
            &metric::Registry::new(),
        );

        // Insert 6 keys in order: key1, key2, key3, key4, key5, key6
        let keys = vec!["key1", "key2", "key3", "key4", "key5", "key6"];
        let mut inserted_keys = Vec::new();

        for key_str in &keys {
            let key = Arc::from(*key_str);
            let value = Arc::from(format!("value_{}", key_str));
            inserted_keys.push(Arc::clone(&key));

            let cache_state = cache.get_or_fetch(
                &key,
                Box::new({
                    let value = Arc::clone(&value);
                    move || futures::future::ready(Ok(value)).boxed()
                }),
                (),
                value.size(),
            );
            assert_eq!(cache_state.kind(), CacheStateKind::NewEntry);
            cache_state.await_inner().await.unwrap();
        }

        // Verify cache only has the last 3 keys (key4, key5, key6) due to eviction
        assert_eq!(cache.len(), 3, "Cache should contain exactly 3 entries");

        // The first 3 keys should have been evicted and logged in the ghost
        assert_eq!(cache.cache.ghost_len(), 3, "Ghost should have 3 entries");

        // Re-insert the first 3 keys (key1, key2, key3)
        for i in 0..3 {
            let key = Arc::clone(&inserted_keys[i]);
            let value = Arc::from(format!("value_{}", keys[i]));

            let cache_state = cache.get_or_fetch(
                &key,
                Box::new({
                    let value = Arc::clone(&value);
                    move || futures::future::ready(Ok(value)).boxed()
                }),
                (),
                value.size(),
            );
            assert_eq!(cache_state.kind(), CacheStateKind::NewEntry);
            cache_state.await_inner().await.unwrap();
        }

        // Verify the first 3 keys are now in the main queue (since they were in ghost)
        let main_queue_keys = cache.cache.main_queue_keys();
        assert_eq!(
            main_queue_keys,
            vec![
                Arc::new(Arc::clone(&inserted_keys[0])),
                Arc::new(Arc::clone(&inserted_keys[1])),
                Arc::new(Arc::clone(&inserted_keys[2])),
            ]
        );

        // Verify they are no longer in the ghost
        assert!(
            !cache
                .cache
                .contains_key_in_ghost(&Arc::new(Arc::clone(&inserted_keys[0]))),
            "key1 should no longer be in ghost"
        );
        assert!(
            !cache
                .cache
                .contains_key_in_ghost(&Arc::new(Arc::clone(&inserted_keys[1]))),
            "key2 should no longer be in ghost"
        );
        assert!(
            !cache
                .cache
                .contains_key_in_ghost(&Arc::new(Arc::clone(&inserted_keys[2]))),
            "key3 should no longer be in ghost"
        );
        // Instead, we have key4 & key5 & key6 in the ghost
        assert_eq!(
            cache.cache.ghost_len(),
            3,
            "Ghost should have 3 NEW entries"
        );
        assert!(
            cache
                .cache
                .contains_key_in_ghost(&Arc::new(Arc::clone(&inserted_keys[3]))),
            "key4 should be in ghost"
        );

        // Evict key1 (main queue) and key4 (ghost) from the cache
        let keys_to_evict = vec![Arc::clone(&inserted_keys[0]), Arc::clone(&inserted_keys[3])]; // key1, key4
        let evicted_count = cache.evict_keys(keys_to_evict.clone().into_iter());
        assert_eq!(
            evicted_count, 1,
            "Should have evicted exactly 1 key -- since only 1 is currently in the queue"
        );

        // Verify key1 is removed from main queue
        let main_queue_keys_after = cache.cache.main_queue_keys();
        assert!(
            !main_queue_keys_after.contains(&Arc::new(Arc::clone(&inserted_keys[0]))),
            "key1 should be removed from main queue"
        );

        // Verify key1 is removed from entries (should not be in cache anymore)
        assert!(
            !cache.cache.contains_key_in_entries(&inserted_keys[0]),
            "key1 should be removed from entries"
        );

        // Verify key2 & key 3 are still in main queue, as well as the ordering is retained.
        assert_eq!(
            main_queue_keys_after,
            vec![
                Arc::new(Arc::clone(&inserted_keys[1])),
                Arc::new(Arc::clone(&inserted_keys[2])),
            ],
            "key2 & key3 should still be in main queue"
        );

        // Verify key4 is still in ghost queue (should remain there)
        assert!(
            cache
                .cache
                .contains_key_in_ghost(&Arc::new(Arc::clone(&inserted_keys[3]))),
            "key4 should still be in ghost after eviction"
        );
    }

    #[tokio::test]
    async fn insert_race_condition() {
        #[derive(Debug)]
        struct V;

        impl HasSize for V {
            fn size(&self) -> usize {
                1
            }
        }

        impl InUse for V {
            fn in_use(&mut self) -> bool {
                false
            }
        }

        impl AsyncDrop for V {
            async fn async_drop(self) {}
        }

        let hook = Arc::new(TestHook::default());
        let cache = S3FifoCache::<Arc<str>, Arc<V>, ()>::new(
            S3Config {
                cache_name: "test",
                max_memory_size: usize::MAX,
                max_ghost_memory_size: usize::MAX,
                move_to_main_threshold: 0.3,
                hook: Arc::clone(&hook) as _,
                inflight_bytes: 1_000_000,
            },
            &metric::Registry::new(),
        );

        let barrier_1 = Arc::new(Barrier::new(2));
        let barrier_2 = Arc::new(Barrier::new(2));

        let k = Arc::from("k");

        let barrier_1_captured = Arc::clone(&barrier_1);
        let barrier_2_captured = Arc::clone(&barrier_2);
        let state = cache.get_or_fetch(
            &k,
            Box::new(move || {
                Box::pin(async move {
                    barrier_1_captured.wait().await;
                    barrier_2_captured.wait().await;
                    Ok(Arc::new(V))
                })
            }),
            (),
            V.size(),
        );
        let CacheState::NewEntry(mut fut, _data) = state else {
            unreachable!()
        };

        tokio::select! {
            _ = barrier_1.wait() => {},
            _ = &mut fut => unreachable!(),
        };

        // Simulate the following race condition, that is unlikely but can still happen under high load:
        //
        // | Thread 1                          | Thread 2                          |
        // | --------------------------------- | --------------------------------- |
        // | S3-FIFO: get: MISS                |                                   |
        // |                                   | S3-FIFO: get: MISS                |
        // |                                   | loader: get_or_put: MISS & insert |
        // |                                   | S3-FIFO: get_or_put: insert       |
        // |                                   | loader: done                      |
        // | loader: get_or_put: MISS & insert |                                   |
        // | S3-FIFO: get_or_put: reject       |                                   |
        // | async drop to-be-inserted value   |                                   |
        let v = Arc::new(V);
        let (_entry, evicted) = cache
            .cache
            .get_or_put(Arc::new(Arc::clone(&k)), Arc::clone(&v), 0);
        assert!(evicted.is_empty());

        let (_, res) = tokio::join!(barrier_2.wait(), fut);
        let v2 = res.unwrap();
        assert!(Arc::ptr_eq(&v, &v2));
    }
}
