//! An in-memory cache of Parquet files that are persisted to object storage
use std::{
    collections::BinaryHeap,
    fmt::Debug,
    ops::Range,
    sync::{
        atomic::{AtomicI64, AtomicUsize, Ordering},
        Arc,
    },
    time::Duration,
};

use anyhow::bail;
use async_trait::async_trait;
use bytes::Bytes;
use dashmap::{DashMap, Entry};
use futures::{
    future::{BoxFuture, Shared},
    stream::BoxStream,
    FutureExt, StreamExt, TryStreamExt,
};
use iox_time::TimeProvider;
use object_store::{
    path::Path, Error, GetOptions, GetResult, GetResultPayload, ListResult, MultipartUpload,
    ObjectMeta, ObjectStore, PutMultipartOpts, PutOptions, PutPayload, PutResult,
};
use observability_deps::tracing::{error, info, warn};
use tokio::sync::{
    mpsc::{channel, Receiver, Sender},
    oneshot,
};

/// Shared future type for cache values that are being fetched
type SharedCacheValueFuture = Shared<BoxFuture<'static, Result<Arc<CacheValue>, DynError>>>;

/// Dynamic error type that can be cloned
type DynError = Arc<dyn std::error::Error + Send + Sync>;

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
    fn new(mem_cached_store: Arc<MemCachedObjectStore>, prune_interval: Duration) -> Self {
        let (cache_request_tx, cache_request_rx) = channel(CACHE_REQUEST_BUFFER_SIZE);
        background_cache_request_handler(Arc::clone(&mem_cached_store), cache_request_rx);
        background_cache_pruner(mem_cached_store, prune_interval);
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
    prune_percent: f64,
    prune_interval: Duration,
) -> (Arc<dyn ObjectStore>, Arc<dyn ParquetCacheOracle>) {
    let store = Arc::new(MemCachedObjectStore::new(
        object_store,
        cache_capacity,
        time_provider,
        prune_percent,
    ));
    let oracle = Arc::new(MemCacheOracle::new(Arc::clone(&store), prune_interval));
    (store, oracle)
}

/// Create a test cached object store with a cache capacity of 1GB
pub fn test_cached_obj_store_and_oracle(
    object_store: Arc<dyn ObjectStore>,
    time_provider: Arc<dyn TimeProvider>,
) -> (Arc<dyn ObjectStore>, Arc<dyn ParquetCacheOracle>) {
    create_cached_obj_store_and_oracle(
        object_store,
        time_provider,
        1024 * 1024 * 1024,
        0.1,
        Duration::from_millis(10),
    )
}

/// A value in the cache, containing the actual bytes as well as object store metadata
#[derive(Debug)]
struct CacheValue {
    data: Bytes,
    meta: ObjectMeta,
}

impl CacheValue {
    /// Get the size of the cache value's memory footprint in bytes
    fn size(&self) -> usize {
        let Self {
            data,
            meta:
                ObjectMeta {
                    location,
                    last_modified: _,
                    size: _,
                    e_tag,
                    version,
                },
        } = self;

        data.len()
            + location.as_ref().len()
            + e_tag.as_ref().map(|s| s.capacity()).unwrap_or_default()
            + version.as_ref().map(|s| s.capacity()).unwrap_or_default()
    }

    /// Fetch the value from an object store
    async fn fetch(store: Arc<dyn ObjectStore>, path: Path) -> object_store::Result<Self> {
        let res = store.get(&path).await?;
        let meta = res.meta.clone();
        let data = res.bytes().await?;
        Ok(Self { data, meta })
    }
}

/// Holds the state and hit time for an entry in the cache
#[derive(Debug)]
struct CacheEntry {
    state: CacheEntryState,
    /// The nano-second timestamp of when this value was last hit
    hit_time: AtomicI64,
}

impl CacheEntry {
    /// Get the approximate memory footprint of this entry in bytes
    fn size(&self) -> usize {
        self.state.size() + std::mem::size_of::<AtomicI64>()
    }

    fn is_fetching(&self) -> bool {
        matches!(self.state, CacheEntryState::Fetching(_))
    }

    fn is_success(&self) -> bool {
        matches!(self.state, CacheEntryState::Success(_))
    }
}

/// The state of a cache entry
///
/// This implements `Clone` so that a reference to the entry in the `Cache` does not need to be
/// held for long.
#[derive(Debug, Clone)]
enum CacheEntryState {
    /// The cache entry is being fetched from object store
    Fetching(SharedCacheValueFuture),
    /// The cache entry was successfully fetched and is stored in the cache as a [`CacheValue`]
    Success(Arc<CacheValue>),
}

impl CacheEntryState {
    /// Get the approximate size of the cache entry in bytes
    fn size(&self) -> usize {
        match self {
            CacheEntryState::Fetching(_) => 0,
            CacheEntryState::Success(v) => v.size(),
        }
    }

    /// Get the value in this state, or wait for it if it is still fetching
    ///
    /// This takes `self` as it is meant to be used on an entry's state that has been cloned.
    async fn value(self) -> object_store::Result<Arc<CacheValue>> {
        match self {
            CacheEntryState::Fetching(fut) => fut.await.map_err(|e| Error::Generic {
                store: STORE_NAME,
                source: Box::new(e),
            }),
            CacheEntryState::Success(v) => Ok(v),
        }
    }
}

/// A cache for storing objects from object storage by their [`Path`]
///
/// This acts as a Least-Recently-Used (LRU) cache that allows for concurrent reads and writes. See
/// the [`Cache::prune`] method for implementation of how the cache entries are pruned. Pruning must
/// be invoked externally, e.g., on an interval.
#[derive(Debug)]
struct Cache {
    /// The maximum amount of memory this cache should occupy in bytes
    capacity: usize,
    /// The current amount of memory being used by the cache in bytes
    used: AtomicUsize,
    /// What percentage of the total number of cache entries will be pruned during a pruning operation
    prune_percent: f64,
    /// The map storing cache entries
    map: DashMap<Path, CacheEntry>,
    /// Provides timestamps for updating the hit time of each cache entry
    time_provider: Arc<dyn TimeProvider>,
}

impl Cache {
    /// Create a new cache with a given capacity and prune percent
    fn new(capacity: usize, prune_percent: f64, time_provider: Arc<dyn TimeProvider>) -> Self {
        Self {
            capacity,
            used: AtomicUsize::new(0),
            prune_percent,
            map: DashMap::new(),
            time_provider,
        }
    }

    /// Get an entry in the cache or `None` if there is not an entry
    ///
    /// This updates the hit time of the entry and returns a cloned copy of the entry state so that
    /// the reference into the map is dropped
    fn get(&self, path: &Path) -> Option<CacheEntryState> {
        let entry = self.map.get(path)?;
        if entry.is_success() {
            entry
                .hit_time
                .store(self.time_provider.now().timestamp_nanos(), Ordering::SeqCst);
        }
        Some(entry.state.clone())
    }

    /// Check if an entry in the cache is in process of being fetched or if it was already fetched
    /// successfully
    ///
    /// This does not update the hit time of the entry
    fn path_already_fetched(&self, path: &Path) -> bool {
        self.map.get(path).is_some()
    }

    /// Insert a `Fetching` entry to the cache along with the shared future for polling the value
    /// being fetched
    fn set_fetching(&self, path: &Path, fut: SharedCacheValueFuture) {
        let entry = CacheEntry {
            state: CacheEntryState::Fetching(fut),
            hit_time: AtomicI64::new(self.time_provider.now().timestamp_nanos()),
        };
        let additional = entry.size();
        self.map.insert(path.clone(), entry);
        self.used.fetch_add(additional, Ordering::SeqCst);
    }

    /// Update a `Fetching` entry to a `Success` entry in the cache
    fn set_success(&self, path: &Path, value: Arc<CacheValue>) -> Result<(), anyhow::Error> {
        match self.map.entry(path.clone()) {
            Entry::Occupied(mut o) => {
                let entry = o.get_mut();
                if !entry.is_fetching() {
                    // NOTE(trevor): the only other state is Success, so bailing here just
                    // means that we leave the entry alone, and since objects in the store are
                    // treated as immutable, this should be okay.
                    bail!("attempted to store value in non-fetching cache entry");
                }
                entry.state = CacheEntryState::Success(value);
                entry
                    .hit_time
                    .store(self.time_provider.now().timestamp_nanos(), Ordering::SeqCst);
                // TODO(trevor): what if size is greater than cache capacity?
                let additional = entry.size();
                self.used.fetch_add(additional, Ordering::SeqCst);
                Ok(())
            }
            Entry::Vacant(_) => bail!("attempted to set success state on an empty cache entry"),
        }
    }

    /// Remove an entry from the cache, as well as its associated size from the used capacity
    fn remove(&self, path: &Path) {
        let Some((_, entry)) = self.map.remove(path) else {
            return;
        };
        self.used.fetch_sub(entry.state.size(), Ordering::SeqCst);
    }

    /// Prune least recently hit entries from the cache
    ///
    /// This is a no-op if the `used` amount on the cache is not >= its `capacity`
    fn prune(&self) {
        let used = self.used.load(Ordering::SeqCst);
        if used < self.capacity {
            return;
        }
        let n_to_prune = (self.map.len() as f64 * self.prune_percent).floor() as usize;
        // use a BinaryHeap to determine the cut-off time, at which, entries that were
        // last hit before that time will be pruned:
        let mut prune_heap = BinaryHeap::with_capacity(n_to_prune);

        for map_ref in self.map.iter() {
            let hit_time = map_ref.value().hit_time.load(Ordering::SeqCst);
            let size = map_ref.value().size();
            let path = map_ref.key().as_ref();
            if prune_heap.len() < n_to_prune {
                // if the heap isn't full yet, throw this item on:
                prune_heap.push(PruneHeapItem {
                    hit_time,
                    path_ref: path.into(),
                    size,
                });
            } else if hit_time < prune_heap.peek().map(|item| item.hit_time).unwrap() {
                // otherwise, the heap is at its capacity, so only push if the hit_time
                // in question is older than the top of the heap (after pop'ing the top
                // of the heap to make room)
                prune_heap.pop();
                prune_heap.push(PruneHeapItem {
                    path_ref: path.into(),
                    hit_time,
                    size,
                });
            }
        }

        // track the total size of entries that get freed:
        let mut freed = 0;
        // drop entries with hit times before the cut-off:
        for item in prune_heap {
            self.map.remove(&Path::from(item.path_ref.as_ref()));
            freed += item.size;
        }
        // update used mem size with freed amount:
        self.used.fetch_sub(freed, Ordering::SeqCst);
    }
}

/// An item that stores what is needed for pruning [`CacheEntry`]s
#[derive(Debug, Eq)]
struct PruneHeapItem {
    /// Reference to the entry's `Path` key
    path_ref: Arc<str>,
    /// Entry's hit time for comparison and heap insertion
    hit_time: i64,
    /// Entry size used to calculate the amount of memory freed after a prune
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
#[derive(Debug)]
pub struct MemCachedObjectStore {
    /// An inner object store for which items will be cached
    inner: Arc<dyn ObjectStore>,
    cache: Arc<Cache>,
}

impl MemCachedObjectStore {
    /// Create a new [`MemCachedObjectStore`]
    fn new(
        inner: Arc<dyn ObjectStore>,
        memory_capacity: usize,
        time_provider: Arc<dyn TimeProvider>,
        prune_percent: f64,
    ) -> Self {
        Self {
            inner,
            cache: Arc::new(Cache::new(memory_capacity, prune_percent, time_provider)),
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

    /// Get an object from the object store. If this object is cached, then it will not make a request
    /// to the inner object store.
    async fn get(&self, location: &Path) -> object_store::Result<GetResult> {
        if let Some(state) = self.cache.get(location) {
            let v = state.value().await?;
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

    async fn get_opts(
        &self,
        location: &Path,
        options: GetOptions,
    ) -> object_store::Result<GetResult> {
        // NOTE(trevor): this could probably be supported through the cache if we need it via the
        // ObjectMeta stored in the cache. For now this is conservative:
        self.inner.get_opts(location, options).await
    }

    async fn get_range(&self, location: &Path, range: Range<usize>) -> object_store::Result<Bytes> {
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
    ) -> object_store::Result<Vec<Bytes>> {
        if let Some(state) = self.cache.get(location) {
            let v = state.value().await?;
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

    async fn head(&self, location: &Path) -> object_store::Result<ObjectMeta> {
        if let Some(state) = self.cache.get(location) {
            let v = state.value().await?;
            Ok(v.meta.clone())
        } else {
            self.inner.head(location).await
        }
    }

    /// Delete an object on object store, but also remove it from the cache.
    async fn delete(&self, location: &Path) -> object_store::Result<()> {
        let result = self.inner.delete(location).await?;
        self.cache.remove(location);
        Ok(result)
    }

    fn delete_stream<'a>(
        &'a self,
        locations: BoxStream<'a, object_store::Result<Path>>,
    ) -> BoxStream<'a, object_store::Result<Path>> {
        locations
            .and_then(|_| futures::future::err(Error::NotImplemented))
            .boxed()
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

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> object_store::Result<ListResult> {
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
            if mem_store.cache.path_already_fetched(&path) {
                continue;
            }
            // Create a future that will go and fetch the cache value from the store:
            let path_cloned = path.clone();
            let store_cloned = Arc::clone(&mem_store.inner);
            let fut = async move {
                CacheValue::fetch(store_cloned, path_cloned)
                    .await
                    .map(Arc::new)
                    .map_err(|e| Arc::new(e) as _)
            }
            .boxed()
            .shared();
            // Put a `Fetching` state in the entry to prevent concurrent requests to the same path:
            mem_store.cache.set_fetching(&path, fut.clone());
            let mem_store_captured = Arc::clone(&mem_store);
            tokio::spawn(async move {
                match fut.await {
                    Ok(value) => {
                        if let Err(error) = mem_store_captured.cache.set_success(&path, value) {
                            // NOTE(trevor): this would be an error if A) it tried to insert on an already
                            // successful entry, or B) it tried to insert on an empty entry, in either case
                            // we do not need to remove the entry to clear a fetching state, as in the
                            // other failure modes below...
                            warn!(%error, "failed to set the success state on the cache");
                        };
                    }
                    Err(error) => {
                        error!(%error, "failed to fulfill cache request with object store");
                        mem_store_captured.cache.remove(&path);
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
fn background_cache_pruner(
    mem_store: Arc<MemCachedObjectStore>,
    interval_duration: Duration,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(interval_duration);
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            interval.tick().await;
            mem_store.cache.prune();
        }
    })
}

#[cfg(test)]
pub(crate) mod tests {
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
    use tokio::sync::Notify;

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
        // these are magic numbers that will make it so the third entry exceeds the cache capacity:
        let cache_capacity_bytes = 60;
        let cache_prune_percent = 0.4;
        let cache_prune_interval = Duration::from_millis(10);
        let (cached_store, oracle) = create_cached_obj_store_and_oracle(
            Arc::clone(&inner_store) as _,
            Arc::clone(&time_provider) as _,
            cache_capacity_bytes,
            cache_prune_percent,
            cache_prune_interval,
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

    #[tokio::test]
    async fn cache_hit_while_fetching() {
        // Create a test store with a barrier:
        let to_store_notify = Arc::new(Notify::new());
        let from_store_notify = Arc::new(Notify::new());
        let inner_store = Arc::new(
            TestObjectStore::new(Arc::new(InMemory::new()))
                .with_notifies(Arc::clone(&to_store_notify), Arc::clone(&from_store_notify)),
        );
        let time_provider = Arc::new(MockProvider::new(Time::from_timestamp_nanos(0)));
        let (cached_store, oracle) = test_cached_obj_store_and_oracle(
            Arc::clone(&inner_store) as _,
            Arc::clone(&time_provider) as _,
        );

        // PUT an entry into the store:
        let path = Path::from("0.parquet");
        let payload = b"Picard";
        cached_store
            .put(&path, PutPayload::from_static(payload))
            .await
            .unwrap();

        // cache the entry, but don't wait on it until below in spawned task:
        let (cache_request, notifier_rx) = CacheRequest::create(path.clone());
        oracle.register(cache_request);

        // we are in the middle of a get request, i.e., the cache entry is "fetching"
        // once this call to notified wakes:
        let _ = from_store_notify.notified().await;

        // spawn a thread to wake the in-flight get request initiated by the cache oracle
        // after we have started a get request below, such that the get request below hits
        // the cache while the entry is still "fetching" state:
        let h = tokio::spawn(async move {
            to_store_notify.notify_one();
            let _ = notifier_rx.await;
        });

        // make the request to the store, which hits the cache in the "fetching" state
        // since we haven't made the call to notify the store to continue yet:
        assert_payload_at_equals!(cached_store, payload, path);

        // drive the task to completion to ensure that the cache request has been fulfilled:
        h.await.unwrap();

        // there should only have been one request made, i.e., from the cache oracle:
        assert_eq!(1, inner_store.total_get_request_count());
        assert_eq!(1, inner_store.get_request_count(&path));

        // make another request to the store, to be sure that it is in the cache:
        assert_payload_at_equals!(cached_store, payload, path);
        assert_eq!(1, inner_store.total_get_request_count());
        assert_eq!(1, inner_store.get_request_count(&path));
    }

    type RequestCounter = RwLock<HashMap<Path, usize>>;

    #[derive(Debug)]
    pub(crate) struct TestObjectStore {
        inner: Arc<dyn ObjectStore>,
        get: RequestCounter,
        get_opts: RequestCounter,
        get_range: RequestCounter,
        get_ranges: RequestCounter,
        head: RequestCounter,
        notifies: Option<(Arc<Notify>, Arc<Notify>)>,
    }

    impl TestObjectStore {
        pub(crate) fn new(inner: Arc<dyn ObjectStore>) -> Self {
            Self {
                inner,
                get: Default::default(),
                get_opts: Default::default(),
                get_range: Default::default(),
                get_ranges: Default::default(),
                head: Default::default(),
                notifies: None,
            }
        }

        fn with_notifies(mut self, inbound: Arc<Notify>, outbound: Arc<Notify>) -> Self {
            self.notifies = Some((inbound, outbound));
            self
        }

        pub(crate) fn total_get_request_count(&self) -> usize {
            self.get.read().iter().map(|(_, size)| size).sum()
        }

        pub(crate) fn get_request_count(&self, path: &Path) -> usize {
            self.get.read().get(path).copied().unwrap_or(0)
        }

        pub(crate) fn get_opts_request_count(&self, path: &Path) -> usize {
            self.get_opts.read().get(path).copied().unwrap_or(0)
        }

        pub(crate) fn get_range_request_count(&self, path: &Path) -> usize {
            self.get_range.read().get(path).copied().unwrap_or(0)
        }

        pub(crate) fn get_ranges_request_count(&self, path: &Path) -> usize {
            self.get_ranges.read().get(path).copied().unwrap_or(0)
        }

        pub(crate) fn head_request_count(&self, path: &Path) -> usize {
            self.head.read().get(path).copied().unwrap_or(0)
        }
    }

    impl std::fmt::Display for TestObjectStore {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "TestObjectStore({})", self.inner)
        }
    }

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
            if let Some((inbound, outbound)) = &self.notifies {
                outbound.notify_one();
                inbound.notified().await;
            }
            self.inner.get(location).await
        }

        async fn get_opts(
            &self,
            location: &Path,
            options: GetOptions,
        ) -> object_store::Result<GetResult> {
            *self.get_opts.write().entry(location.clone()).or_insert(0) += 1;
            self.inner.get_opts(location, options).await
        }

        async fn get_range(
            &self,
            location: &Path,
            range: Range<usize>,
        ) -> object_store::Result<Bytes> {
            *self.get_range.write().entry(location.clone()).or_insert(0) += 1;
            self.inner.get_range(location, range).await
        }

        async fn get_ranges(
            &self,
            location: &Path,
            ranges: &[Range<usize>],
        ) -> object_store::Result<Vec<Bytes>> {
            *self.get_ranges.write().entry(location.clone()).or_insert(0) += 1;
            self.inner.get_ranges(location, ranges).await
        }

        async fn head(&self, location: &Path) -> object_store::Result<ObjectMeta> {
            *self.head.write().entry(location.clone()).or_insert(0) += 1;
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
