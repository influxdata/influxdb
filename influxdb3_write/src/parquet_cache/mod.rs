//! An in-memory cache of Parquet files that are persisted to object storage
use std::{
    cmp::Reverse,
    collections::BinaryHeap,
    fmt::Debug,
    ops::Range,
    sync::{
        atomic::{AtomicI32, AtomicI64, AtomicUsize, Ordering},
        Arc,
    },
    time::Duration,
};

use anyhow::bail;
use async_trait::async_trait;
use bytes::Bytes;
use futures::{StreamExt, TryStreamExt};
use futures_util::stream::BoxStream;
use hashbrown::{hash_map::Entry, HashMap};
use iox_time::TimeProvider;
use object_store::{
    path::Path, Error, GetOptions, GetResult, GetResultPayload, ListResult, MultipartUpload,
    ObjectMeta, ObjectStore, PutMultipartOpts, PutOptions, PutPayload, PutResult,
    Result as ObjectStoreResult,
};
use observability_deps::tracing::{error, info, warn};
use tokio::sync::{
    mpsc::{channel, Receiver, Sender},
    oneshot, watch, RwLock,
};

/// A request to fetch an item at the given `path` from an object store
///
/// Contains a notifier to notify the caller that registers the cache request when the item
/// has been cached successfully (or if the cache request failed in some way)
pub struct CacheRequest {
    path: Path,
    notifier: oneshot::Sender<()>,
}

impl CacheRequest {
    /// Create a new [`CacheRequest`] along with a receiver to catch the notify message when
    /// the cache request has been fulfilled.
    pub fn create(path: Path) -> (Self, oneshot::Receiver<()>) {
        let (notifier, receiver) = oneshot::channel();
        (Self { path, notifier }, receiver)
    }
}

/// An interface for interacting with a Parquet Cache by registering [`CacheRequest`]s to it.
pub trait ParquetCacheOracle: Send + Sync + Debug {
    /// Register a cache request with the oracle
    fn register(&self, cache_request: CacheRequest);
}

/// Concrete implementation of the [`ParquetCacheOracle`]
///
/// This implementation sends all requests registered to be cached.
#[derive(Debug, Clone)]
pub struct MemCacheOracle {
    cache_request_tx: Sender<CacheRequest>,
}

// TODO(trevor): make this configurable with reasonable default
const CACHE_REQUEST_BUFFER_SIZE: usize = 1_000_000;

impl MemCacheOracle {
    /// Create a new [`MemCacheOracle`]
    ///
    /// This spawns two background tasks:
    /// * one to handle registered [`CacheRequest`]s
    /// * one to prune deleted and un-needed cache entries on an interval
    // TODO(trevor): this should be more configurable, e.g., channel size, prune interval
    fn new(mem_cached_store: Arc<MemCachedObjectStore>) -> Self {
        let (cache_request_tx, cache_request_rx) = channel(CACHE_REQUEST_BUFFER_SIZE);
        background_cache_request_handler(Arc::clone(&mem_cached_store), cache_request_rx);
        background_cache_pruner(mem_cached_store);
        Self { cache_request_tx }
    }
}

impl ParquetCacheOracle for MemCacheOracle {
    fn register(&self, request: CacheRequest) {
        let tx = self.cache_request_tx.clone();
        tokio::spawn(async move {
            if let Err(error) = tx.send(request).await {
                error!(%error, "error registering cache request");
            };
        });
    }
}

/// Helper function for creation of a [`MemCachedObjectStore`] and [`MemCacheOracle`]
/// that returns them as their `Arc<dyn _>` equivalent.
pub fn create_cached_obj_store_and_oracle(
    object_store: Arc<dyn ObjectStore>,
    time_provider: Arc<dyn TimeProvider>,
    cache_capacity: usize,
) -> (Arc<dyn ObjectStore>, Arc<dyn ParquetCacheOracle>) {
    let store = Arc::new(MemCachedObjectStore::new(
        object_store,
        cache_capacity,
        time_provider,
    ));
    let oracle = Arc::new(MemCacheOracle::new(Arc::clone(&store)));
    (store, oracle)
}

/// Create a test cached object store with a cache capacity of 1GB
pub fn test_cached_obj_store_and_oracle(
    object_store: Arc<dyn ObjectStore>,
    time_provider: Arc<dyn TimeProvider>,
) -> (Arc<dyn ObjectStore>, Arc<dyn ParquetCacheOracle>) {
    create_cached_obj_store_and_oracle(object_store, time_provider, 1024 * 1024 * 1024)
}

/// An entry in the cache, containing the actual bytes as well as object store metadata
#[derive(Debug)]
struct CacheValue {
    data: Bytes,
    meta: ObjectMeta,
}

impl CacheValue {
    /// Get the size of the cache value's memory footprint in bytes
    fn size(&self) -> usize {
        let Self { data, meta } = self;
        let ObjectMeta {
            location,
            last_modified: _,
            size: _,
            e_tag,
            version,
        } = meta;

        data.len()
            + location.as_ref().len()
            + e_tag.as_ref().map(|s| s.capacity()).unwrap_or_default()
            + version.as_ref().map(|s| s.capacity()).unwrap_or_default()
    }
}

#[derive(Debug)]
struct CacheEntry {
    state: CacheEntryState,
    hit_time: AtomicI64,
}

impl CacheEntry {
    /// Get the approximate memory footprint of this entry in bytes
    fn size(&self) -> usize {
        self.state.size() + std::mem::size_of::<AtomicI32>()
    }

    fn is_fetching(&self) -> bool {
        matches!(self.state, CacheEntryState::Fetching(_))
    }
}

/// The state of a cache entry
#[derive(Debug)]
enum CacheEntryState {
    /// The cache entry is being fetched from object store
    ///
    /// This holds a [`watch::Sender`] that is used to notify requests made to get this entry
    /// while it is being fetched by the cache oracle.
    Fetching(watch::Sender<Option<Arc<CacheValue>>>),
    /// The cache entry was successfully fetched and is stored in the cache as a [`CacheValue`]
    Success(Arc<CacheValue>),
}

impl CacheEntryState {
    /// Get the approximate size of the cache entry in bytes
    fn size(&self) -> usize {
        match self {
            CacheEntryState::Fetching(tx) => std::mem::size_of_val(tx),
            CacheEntryState::Success(v) => v.size(),
        }
    }
}

/// A cache for storing objects from object storage by their [`Path`]
///
/// This acts as a Least-Frequently-Used (LFU) cache that allows for concurrent reads. See the
/// [`Cache::prune`] method for implementation of how the cache entries are pruned. Pruning must
/// be invoked externally, e.g., on an interval.
#[derive(Debug)]
struct Cache {
    /// The maximum amount of memory this cache should occupy
    capacity: usize,
    /// The current amount of memory being used by the cache
    used: AtomicUsize,
    /// The maximum amount of memory to free during a prune operation
    max_free_amount: usize,
    /// The map storing cache entries
    ///
    /// This uses [`IndexMap`] to preserve insertion order, such that, when iterating over the map
    /// to prune entries, iteration occurs in the order that entries were inserted. This will have
    /// older entries removed before newer entries
    map: RwLock<HashMap<Path, CacheEntry>>,
    time_provider: Arc<dyn TimeProvider>,
}

impl Cache {
    /// Create a new cache with a given capacity and max free percentage
    ///
    /// The cache's `max_free_amount` will be calculated using the provided values
    fn new(capacity: usize, max_free_percent: f64, time_provider: Arc<dyn TimeProvider>) -> Self {
        Self {
            capacity,
            used: AtomicUsize::new(0),
            max_free_amount: (capacity as f64 * max_free_percent).floor() as usize,
            map: RwLock::new(HashMap::new()),
            time_provider,
        }
    }

    /// Get an entry in the cache or `None` if there is not an entry
    ///
    /// If the entry is in `Fetching` state, then this will await the etry having been fetched.
    async fn get(&self, path: &Path) -> Option<Arc<CacheValue>> {
        let map = self.map.read().await;
        let entry = map.get(path)?;
        match &entry.state {
            CacheEntryState::Fetching(tx) => {
                let mut rx = tx.subscribe();
                // TODO(trevor): is it possible that the sender has sent the result at this point?
                // if so we need to check the rx first and maybe use more sophisticated state than
                // an Option in the watch channel type.
                rx.changed().await.ok()?;
                let v = rx.borrow_and_update().clone();
                v
            }
            CacheEntryState::Success(v) => {
                entry
                    .hit_time
                    .store(self.time_provider.now().timestamp_nanos(), Ordering::SeqCst);
                Some(Arc::clone(v))
            }
        }
    }

    /// Check if an entry in the cache is in process of being fetched or if it was already fetched
    /// successfully
    async fn path_already_fetched(&self, path: &Path) -> bool {
        self.map.read().await.get(path).is_some()
    }

    /// Insert a `Fetching` entry to the cache with a watcher
    async fn set_fetching(&self, path: &Path) {
        let (tx, _) = watch::channel(None);
        let entry = CacheEntry {
            state: CacheEntryState::Fetching(tx),
            hit_time: AtomicI64::new(self.time_provider.now().timestamp_nanos()),
        };
        let additional = entry.size();
        self.map.write().await.insert(path.clone(), entry);
        self.used.fetch_add(additional, Ordering::SeqCst);
    }

    /// Update a `Fetching` entry to a `Success` entry in the cache
    async fn set_success(&self, path: &Path, value: CacheValue) -> Result<(), anyhow::Error> {
        match self.map.write().await.entry(path.clone()) {
            Entry::Occupied(mut o) => {
                let entry = o.get_mut();
                let tx = match &entry.state {
                    CacheEntryState::Fetching(tx) => tx.clone(),
                    _ => bail!("attempted to set success state on a non-fetching cache entry"),
                };
                let cache_value = Arc::new(value);
                entry.state = CacheEntryState::Success(Arc::clone(&cache_value));
                entry
                    .hit_time
                    .store(self.time_provider.now().timestamp_nanos(), Ordering::SeqCst);
                // TODO(trevor): what if size is greater than cache capacity?
                let additional = entry.size();
                self.used.fetch_add(additional, Ordering::SeqCst);
                // notify any watching requests that tried to get this entry while it was
                // fetching:
                let _ = tx.send(Some(cache_value));
                Ok(())
            }
            Entry::Vacant(_) => bail!("attempted to set success state on an empty cache entry"),
        }
    }

    /// Remove an entry from the cache, as well as its associated size from the used capacity
    async fn remove(&self, path: &Path) {
        let Some(entry) = self.map.write().await.remove(path) else {
            return;
        };
        self.used.fetch_sub(entry.state.size(), Ordering::SeqCst);
    }

    /// A cache should be pruned if it is using all of its memory capacity
    fn should_prune(&self) -> bool {
        let used = self.used.load(Ordering::SeqCst);
        used >= self.capacity
    }

    /// Prune least recently hit entries from the cache stopping once the `max_freed_amount` has
    /// been pruned.
    async fn prune(&self) {
        let mut map = self.map.write().await;
        let mut time_heap = BinaryHeap::new();
        for (_, entry) in map.iter() {
            let hit_time = entry.hit_time.load(Ordering::Relaxed);
            let size = entry.size();
            time_heap.push(Reverse(PruneHeapItem { hit_time, size }));
        }
        let mut freed = 0;
        let mut cut_off_time = i64::MAX;
        while let Some(Reverse(PruneHeapItem { hit_time, size })) = time_heap.pop() {
            freed += size;
            if freed >= self.max_free_amount {
                cut_off_time = hit_time;
                break;
            }
        }
        // reset freed so we calculate actual amount freed during retain:
        freed = 0;
        map.retain(|_, entry| {
            let hit_time = entry.hit_time.load(Ordering::Relaxed);
            if entry.is_fetching() || hit_time > cut_off_time {
                // keep entries that are still fetching or that were hit after the cut-off:
                true
            } else {
                // drop the rest:
                freed += entry.size();
                false
            }
        });
        self.used.fetch_sub(freed, Ordering::Relaxed);
    }
}

#[derive(Debug, Eq)]
struct PruneHeapItem {
    hit_time: i64,
    size: usize,
}

impl PartialEq for PruneHeapItem {
    fn eq(&self, other: &Self) -> bool {
        self.hit_time.eq(&other.hit_time)
    }
}

impl PartialOrd for PruneHeapItem {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.hit_time.cmp(&other.hit_time))
    }
}

impl Ord for PruneHeapItem {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.hit_time.cmp(&other.hit_time)
    }
}

/// Placeholder name for formatting datafusion errors
const STORE_NAME: &str = "mem_cached_object_store";

/// An object store with an associated cache that can serve GET-style requests using the cache
///
/// The least-recently used (LRU) entries will be evicted when new entries are inserted, if the
/// new entry would exceed the cache's memory capacity
#[derive(Debug)]
pub struct MemCachedObjectStore {
    /// An inner object store for which items will be cached
    inner: Arc<dyn ObjectStore>,
    cache: Arc<Cache>,
}

impl MemCachedObjectStore {
    /// Create a new [`MemCachedObjectStore`] with the given memory capacity
    // TODO(trevor): configurable free percentage, which is hard-coded at 10% right now
    fn new(
        inner: Arc<dyn ObjectStore>,
        memory_capacity: usize,
        time_provider: Arc<dyn TimeProvider>,
    ) -> Self {
        Self {
            inner,
            cache: Arc::new(Cache::new(memory_capacity, 0.1, time_provider)),
        }
    }
}

impl std::fmt::Display for MemCachedObjectStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "MemCachedObjectStore({})", self.inner)
    }
}

/// [`MemCachedObjectStore`] implements most [`ObjectStore`] methods as a pass-through, since
/// caching is decided externally. The exception is `delete`, which will have the entry removed
/// from the cache if the delete to the object store was successful.
///
/// GET-style methods will first check the cache for the object at the given path, before forwarding
/// to the inner [`ObjectStore`]. They do not, however, populate the cache after data has been fetched
/// from the inner store.
#[async_trait]
impl ObjectStore for MemCachedObjectStore {
    async fn put(&self, location: &Path, bytes: PutPayload) -> ObjectStoreResult<PutResult> {
        self.inner.put(location, bytes).await
    }

    async fn put_opts(
        &self,
        location: &Path,
        bytes: PutPayload,
        opts: PutOptions,
    ) -> ObjectStoreResult<PutResult> {
        self.inner.put_opts(location, bytes, opts).await
    }

    async fn put_multipart(&self, location: &Path) -> ObjectStoreResult<Box<dyn MultipartUpload>> {
        self.inner.put_multipart(location).await
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: PutMultipartOpts,
    ) -> ObjectStoreResult<Box<dyn MultipartUpload>> {
        self.inner.put_multipart_opts(location, opts).await
    }

    /// Get an object from the object store. If this object is cached, then it will not make a request
    /// to the inner object store.
    async fn get(&self, location: &Path) -> ObjectStoreResult<GetResult> {
        if let Some(v) = self.cache.get(location).await {
            Ok(GetResult {
                payload: GetResultPayload::Stream(
                    futures::stream::iter([Ok(v.data.clone())]).boxed(),
                ),
                meta: v.meta.clone(),
                range: 0..v.data.len(),
                attributes: Default::default(),
            })
        } else {
            self.inner.get(location).await
        }
    }

    async fn get_opts(&self, location: &Path, options: GetOptions) -> ObjectStoreResult<GetResult> {
        // NOTE(trevor): this could probably be supported through the cache if we need it via the
        // ObjectMeta stored in the cache. For now this is conservative:
        self.inner.get_opts(location, options).await
    }

    async fn get_range(&self, location: &Path, range: Range<usize>) -> ObjectStoreResult<Bytes> {
        Ok(self
            .get_ranges(location, &[range])
            .await?
            .into_iter()
            .next()
            .expect("requested one range"))
    }

    /// This request is used by DataFusion when requesting metadata for Parquet files, so we need
    /// to use the cache to prevent excess network calls during query planning.
    async fn get_ranges(
        &self,
        location: &Path,
        ranges: &[Range<usize>],
    ) -> ObjectStoreResult<Vec<Bytes>> {
        if let Some(v) = self.cache.get(location).await {
            ranges
                .iter()
                .map(|range| {
                    if range.end > v.data.len() {
                        return Err(Error::Generic {
                            store: STORE_NAME,
                            source: format!(
                                "Range end ({}) out of bounds, object size is {}",
                                range.end,
                                v.data.len()
                            )
                            .into(),
                        });
                    }
                    if range.start > range.end {
                        return Err(Error::Generic {
                            store: STORE_NAME,
                            source: format!(
                                "Range end ({}) is before range start ({})",
                                range.end, range.start
                            )
                            .into(),
                        });
                    }
                    Ok(v.data.slice(range.clone()))
                })
                .collect()
        } else {
            self.inner.get_ranges(location, ranges).await
        }
    }

    async fn head(&self, location: &Path) -> ObjectStoreResult<ObjectMeta> {
        if let Some(v) = self.cache.get(location).await {
            Ok(v.meta.clone())
        } else {
            self.inner.head(location).await
        }
    }

    /// Delete an object on object store, but also remove it from the cache.
    async fn delete(&self, location: &Path) -> ObjectStoreResult<()> {
        let result = self.inner.delete(location).await?;
        self.cache.remove(location).await;
        Ok(result)
    }

    fn delete_stream<'a>(
        &'a self,
        locations: BoxStream<'a, ObjectStoreResult<Path>>,
    ) -> BoxStream<'a, ObjectStoreResult<Path>> {
        locations
            .and_then(|_| futures::future::err(Error::NotImplemented))
            .boxed()
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'_, ObjectStoreResult<ObjectMeta>> {
        self.inner.list(prefix)
    }

    fn list_with_offset(
        &self,
        prefix: Option<&Path>,
        offset: &Path,
    ) -> BoxStream<'_, ObjectStoreResult<ObjectMeta>> {
        self.inner.list_with_offset(prefix, offset)
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> ObjectStoreResult<ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy(&self, from: &Path, to: &Path) -> ObjectStoreResult<()> {
        self.inner.copy(from, to).await
    }

    async fn rename(&self, from: &Path, to: &Path) -> ObjectStoreResult<()> {
        self.inner.rename(from, to).await
    }

    async fn copy_if_not_exists(&self, from: &Path, to: &Path) -> ObjectStoreResult<()> {
        self.inner.copy_if_not_exists(from, to).await
    }

    async fn rename_if_not_exists(&self, from: &Path, to: &Path) -> ObjectStoreResult<()> {
        self.inner.rename_if_not_exists(from, to).await
    }
}

/// Handle [`CacheRequest`]s in a background task
///
/// This waits on the given `Receiver` for new cache requests to be registered, i.e., via the oracle.
/// If a cache request is received for an entry that has not already been fetched successfully, or
/// one that is in the process of being fetched, then this will spin a separate background task to
/// fetch the object from object store and update the cache. This is so that cache requests can be
/// handled in parallel.
fn background_cache_request_handler(
    mem_store: Arc<MemCachedObjectStore>,
    mut rx: Receiver<CacheRequest>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        while let Some(CacheRequest { path, notifier }) = rx.recv().await {
            // We assume that objects on object store are immutable, so we can skip objects that
            // we have already fetched:
            if mem_store.cache.path_already_fetched(&path).await {
                continue;
            }
            // Put a `Fetching` state in the entry to prevent concurrent requests to the same path:
            mem_store.cache.set_fetching(&path).await;
            let mem_store_captured = Arc::clone(&mem_store);
            tokio::spawn(async move {
                match mem_store_captured.inner.get(&path).await {
                    Ok(result) => {
                        let meta = result.meta.clone();
                        match result.bytes().await {
                            Ok(data) => {
                                if let Err(error) = mem_store_captured
                                    .cache
                                    .set_success(&path, CacheValue { data, meta })
                                    .await
                                {
                                    // NOTE(trevor): this would be an error if A) it tried to insert on an already
                                    // successful entry, or B) it tried to insert on an empty entry, in either case
                                    // we do not need to remove the entry to clear the fetching state, as in the
                                    // other failure modes below...
                                    warn!(%error, "failed to set the success state on the cache");
                                };
                            }
                            Err(error) => {
                                error!(%error, "failed to retrieve payload from object store get result");
                                mem_store_captured.cache.remove(&path).await;
                            }
                        }
                    }
                    Err(error) => {
                        error!(%error, "failed to fulfill cache request with object store");
                        mem_store_captured.cache.remove(&path).await;
                    }
                };
                // notify that the cache request has been fulfilled:
                let _ = notifier.send(());
            });
        }
        info!("cache request handler closed");
    })
}

/// A background task for pruning un-needed entries in the cache
// TODO(trevor): the interval could be configurable
fn background_cache_pruner(mem_store: Arc<MemCachedObjectStore>) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_millis(10));
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            interval.tick().await;

            if mem_store.cache.should_prune() {
                mem_store.cache.prune().await;
            }
        }
    })
}

#[cfg(test)]
mod tests {
    use std::{ops::Range, sync::Arc, time::Duration};

    use arrow::datatypes::ToByteSlice;
    use async_trait::async_trait;
    use bytes::Bytes;
    use futures::stream::BoxStream;
    use hashbrown::HashMap;
    use iox_time::{MockProvider, Time, TimeProvider};
    use object_store::{
        memory::InMemory, path::Path, GetOptions, GetResult, ListResult, MultipartUpload,
        ObjectMeta, ObjectStore, PutMultipartOpts, PutOptions, PutPayload, PutResult,
    };
    use parking_lot::RwLock;
    use pretty_assertions::assert_eq;

    use crate::parquet_cache::{
        create_cached_obj_store_and_oracle, test_cached_obj_store_and_oracle, CacheRequest,
    };

    macro_rules! assert_payload_at_equals {
        ($store:ident, $expected:ident, $path:ident) => {
            assert_eq!(
                $expected,
                $store
                    .get(&$path)
                    .await
                    .unwrap()
                    .bytes()
                    .await
                    .unwrap()
                    .to_byte_slice()
            )
        };
    }

    #[tokio::test]
    async fn hit_cache_instead_of_object_store() {
        // set up the inner test object store and then wrap it with the mem cached store:
        let inner_store = Arc::new(TestObjectStore::new(Arc::new(InMemory::new())));
        let time_provider: Arc<dyn TimeProvider> =
            Arc::new(MockProvider::new(Time::from_timestamp_nanos(0)));
        let (cached_store, oracle) = test_cached_obj_store_and_oracle(
            Arc::clone(&inner_store) as _,
            Arc::clone(&time_provider),
        );
        // PUT a paylaod into the object store through the outer mem cached store:
        let path = Path::from("0.parquet");
        let payload = b"hello world";
        cached_store
            .put(&path, PutPayload::from_static(payload))
            .await
            .unwrap();

        // GET the payload from the object store before caching:
        assert_payload_at_equals!(cached_store, payload, path);
        assert_eq!(1, inner_store.total_get_request_count());
        assert_eq!(1, inner_store.get_request_count(&path));

        // cache the entry:
        let (cache_request, notifier_rx) = CacheRequest::create(path.clone());
        oracle.register(cache_request);

        // wait for cache notify:
        let _ = notifier_rx.await;

        // another request to inner store should have been made:
        assert_eq!(2, inner_store.total_get_request_count());
        assert_eq!(2, inner_store.get_request_count(&path));

        // get the payload from the outer store again:
        assert_payload_at_equals!(cached_store, payload, path);

        // should hit the cache this time, so the inner store should not have been hit, and counts
        // should therefore be same as previous:
        assert_eq!(2, inner_store.total_get_request_count());
        assert_eq!(2, inner_store.get_request_count(&path));
    }

    #[tokio::test]
    async fn cache_evicts_lru_when_full() {
        let inner_store = Arc::new(TestObjectStore::new(Arc::new(InMemory::new())));
        let time_provider = Arc::new(MockProvider::new(Time::from_timestamp_nanos(0)));
        // this is a magic number that will make it so the third entry exceeds the cache capacity:
        let cache_capacity_bytes = 72;
        let (cached_store, oracle) = create_cached_obj_store_and_oracle(
            Arc::clone(&inner_store) as _,
            Arc::clone(&time_provider) as _,
            cache_capacity_bytes,
        );
        // PUT an entry into the store:
        let path_1 = Path::from("0.parquet");
        let payload_1 = b"Janeway";
        cached_store
            .put(&path_1, PutPayload::from_static(payload_1))
            .await
            .unwrap();

        // cache the entry and wait for it to complete:
        let (cache_request, notifier_rx) = CacheRequest::create(path_1.clone());
        oracle.register(cache_request);
        let _ = notifier_rx.await;
        // there will have been one get request made by the cache oracle:
        assert_eq!(1, inner_store.total_get_request_count());
        assert_eq!(1, inner_store.get_request_count(&path_1));

        // update time:
        time_provider.set(Time::from_timestamp_nanos(1));

        // GET the entry to check its there and was retrieved from cache, i.e., that the request
        // counts do not change:
        assert_payload_at_equals!(cached_store, payload_1, path_1);
        assert_eq!(1, inner_store.total_get_request_count());
        assert_eq!(1, inner_store.get_request_count(&path_1));

        // PUT a second entry into the store:
        let path_2 = Path::from("1.parquet");
        let payload_2 = b"Paris";
        cached_store
            .put(&path_2, PutPayload::from_static(payload_2))
            .await
            .unwrap();

        // update time:
        time_provider.set(Time::from_timestamp_nanos(2));

        // cache the second entry and wait for it to complete, this will not evict the first entry
        // as both can fit in the cache:
        let (cache_request, notifier_rx) = CacheRequest::create(path_2.clone());
        oracle.register(cache_request);
        let _ = notifier_rx.await;
        // will have another request for the second path to the inner store, by the oracle:
        assert_eq!(2, inner_store.total_get_request_count());
        assert_eq!(1, inner_store.get_request_count(&path_1));
        assert_eq!(1, inner_store.get_request_count(&path_2));

        // update time:
        time_provider.set(Time::from_timestamp_nanos(3));

        // GET the second entry and assert that it was retrieved from the cache, i.e., that the
        // request counts do not change:
        assert_payload_at_equals!(cached_store, payload_2, path_2);
        assert_eq!(2, inner_store.total_get_request_count());
        assert_eq!(1, inner_store.get_request_count(&path_1));
        assert_eq!(1, inner_store.get_request_count(&path_2));

        // update time:
        time_provider.set(Time::from_timestamp_nanos(4));

        // GET the first entry again and assert that it was retrieved from the cache as before. This
        // will also update the hit count so that the first entry (janeway) was used more recently
        // than the second entry (paris):
        assert_payload_at_equals!(cached_store, payload_1, path_1);
        assert_eq!(2, inner_store.total_get_request_count());
        assert_eq!(1, inner_store.get_request_count(&path_1));

        // PUT a third entry into the store:
        let path_3 = Path::from("2.parquet");
        let payload_3 = b"Neelix";
        cached_store
            .put(&path_3, PutPayload::from_static(payload_3))
            .await
            .unwrap();

        // update time:
        time_provider.set(Time::from_timestamp_nanos(5));

        // cache the third entry and wait for it to complete, this will push the cache past its
        // capacity:
        let (cache_request, notifier_rx) = CacheRequest::create(path_3.clone());
        oracle.register(cache_request);
        let _ = notifier_rx.await;
        // will now have another request for the third path to the inner store, by the oracle:
        assert_eq!(3, inner_store.total_get_request_count());
        assert_eq!(1, inner_store.get_request_count(&path_1));
        assert_eq!(1, inner_store.get_request_count(&path_2));
        assert_eq!(1, inner_store.get_request_count(&path_3));

        // update time:
        time_provider.set(Time::from_timestamp_nanos(6));

        // GET the new entry from the strore, and check that it was served by the cache:
        assert_payload_at_equals!(cached_store, payload_3, path_3);
        assert_eq!(3, inner_store.total_get_request_count());
        assert_eq!(1, inner_store.get_request_count(&path_1));
        assert_eq!(1, inner_store.get_request_count(&path_2));
        assert_eq!(1, inner_store.get_request_count(&path_3));

        // allow some time for pruning:
        tokio::time::sleep(Duration::from_millis(100)).await;

        // GET paris from the cached store, this will not be served by the cache, because paris was
        // evicted by neelix:
        assert_payload_at_equals!(cached_store, payload_2, path_2);
        assert_eq!(4, inner_store.total_get_request_count());
        assert_eq!(1, inner_store.get_request_count(&path_1));
        assert_eq!(2, inner_store.get_request_count(&path_2));
        assert_eq!(1, inner_store.get_request_count(&path_3));
    }

    type RequestCounter = RwLock<HashMap<Path, usize>>;

    #[derive(Debug)]
    struct TestObjectStore {
        inner: Arc<dyn ObjectStore>,
        get: RequestCounter,
    }

    impl TestObjectStore {
        fn new(inner: Arc<dyn ObjectStore>) -> Self {
            Self {
                inner,
                get: Default::default(),
            }
        }

        fn total_get_request_count(&self) -> usize {
            self.get.read().iter().map(|(_, size)| size).sum()
        }

        fn get_request_count(&self, path: &Path) -> usize {
            self.get.read().get(path).copied().unwrap_or(0)
        }
    }

    impl std::fmt::Display for TestObjectStore {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "TestObjectStore({})", self.inner)
        }
    }

    /// [`MemCachedObjectStore`] implements most [`ObjectStore`] methods as a pass-through, since
    /// caching is decided externally. The exception is `delete`, which will have the entry removed
    /// from the cache if the delete to the object store was successful.
    ///
    /// GET-style methods will first check the cache for the object at the given path, before forwarding
    /// to the inner [`ObjectStore`]. They do not, however, populate the cache after data has been fetched
    /// from the inner store.
    #[async_trait]
    impl ObjectStore for TestObjectStore {
        async fn put(&self, location: &Path, bytes: PutPayload) -> object_store::Result<PutResult> {
            self.inner.put(location, bytes).await
        }

        async fn put_opts(
            &self,
            location: &Path,
            bytes: PutPayload,
            opts: PutOptions,
        ) -> object_store::Result<PutResult> {
            self.inner.put_opts(location, bytes, opts).await
        }

        async fn put_multipart(
            &self,
            location: &Path,
        ) -> object_store::Result<Box<dyn MultipartUpload>> {
            self.inner.put_multipart(location).await
        }

        async fn put_multipart_opts(
            &self,
            location: &Path,
            opts: PutMultipartOpts,
        ) -> object_store::Result<Box<dyn MultipartUpload>> {
            self.inner.put_multipart_opts(location, opts).await
        }

        async fn get(&self, location: &Path) -> object_store::Result<GetResult> {
            *self.get.write().entry(location.clone()).or_insert(0) += 1;
            self.inner.get(location).await
        }

        async fn get_opts(
            &self,
            location: &Path,
            options: GetOptions,
        ) -> object_store::Result<GetResult> {
            self.inner.get_opts(location, options).await
        }

        async fn get_range(
            &self,
            location: &Path,
            range: Range<usize>,
        ) -> object_store::Result<Bytes> {
            self.inner.get_range(location, range).await
        }

        async fn get_ranges(
            &self,
            location: &Path,
            ranges: &[Range<usize>],
        ) -> object_store::Result<Vec<Bytes>> {
            self.inner.get_ranges(location, ranges).await
        }

        async fn head(&self, location: &Path) -> object_store::Result<ObjectMeta> {
            self.inner.head(location).await
        }

        /// Delete an object on object store, but also remove it from the cache.
        async fn delete(&self, location: &Path) -> object_store::Result<()> {
            self.inner.delete(location).await
        }

        fn delete_stream<'a>(
            &'a self,
            locations: BoxStream<'a, object_store::Result<Path>>,
        ) -> BoxStream<'a, object_store::Result<Path>> {
            self.inner.delete_stream(locations)
        }

        fn list(&self, prefix: Option<&Path>) -> BoxStream<'_, object_store::Result<ObjectMeta>> {
            self.inner.list(prefix)
        }

        fn list_with_offset(
            &self,
            prefix: Option<&Path>,
            offset: &Path,
        ) -> BoxStream<'_, object_store::Result<ObjectMeta>> {
            self.inner.list_with_offset(prefix, offset)
        }

        async fn list_with_delimiter(
            &self,
            prefix: Option<&Path>,
        ) -> object_store::Result<ListResult> {
            self.inner.list_with_delimiter(prefix).await
        }

        async fn copy(&self, from: &Path, to: &Path) -> object_store::Result<()> {
            self.inner.copy(from, to).await
        }

        async fn rename(&self, from: &Path, to: &Path) -> object_store::Result<()> {
            self.inner.rename(from, to).await
        }

        async fn copy_if_not_exists(&self, from: &Path, to: &Path) -> object_store::Result<()> {
            self.inner.copy_if_not_exists(from, to).await
        }

        async fn rename_if_not_exists(&self, from: &Path, to: &Path) -> object_store::Result<()> {
            self.inner.rename_if_not_exists(from, to).await
        }
    }
}
