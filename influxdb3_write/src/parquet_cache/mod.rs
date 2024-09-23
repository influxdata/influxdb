//! An in-memory cache of Parquet files that are persisted to object storage
use std::{
    fmt::Debug, hash::RandomState, num::NonZeroUsize, ops::Range, sync::Arc, time::Duration,
};

use async_trait::async_trait;
use bytes::Bytes;
use clru::{CLruCache, CLruCacheConfig, WeightScale};
use futures::{StreamExt, TryStreamExt};
use futures_util::stream::BoxStream;
use object_store::{
    path::Path, Error, GetOptions, GetResult, GetResultPayload, ListResult, MultipartUpload,
    ObjectMeta, ObjectStore, PutMultipartOpts, PutOptions, PutPayload, PutResult,
    Result as ObjectStoreResult,
};
use observability_deps::tracing::{error, info};
use tokio::sync::{
    mpsc::{channel, Receiver, Sender},
    oneshot, Mutex,
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
    cache_capacity: usize,
) -> (Arc<dyn ObjectStore>, Arc<dyn ParquetCacheOracle>) {
    let store = Arc::new(MemCachedObjectStore::new(object_store, cache_capacity));
    let oracle = Arc::new(MemCacheOracle::new(Arc::clone(&store)));
    (store, oracle)
}

/// Create a test cached object store with a cache capacity of 1GB
pub fn test_cached_obj_store_and_oracle(
    object_store: Arc<dyn ObjectStore>,
) -> (Arc<dyn ObjectStore>, Arc<dyn ParquetCacheOracle>) {
    create_cached_obj_store_and_oracle(object_store, 1024 * 1024 * 1024)
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
        // TODO(trevor): could also calculate the size of the metadata...
        self.data.len()
    }
}

/// The state of a cache entry
#[derive(Debug)]
enum CacheEntry {
    /// The cache entry is being fetched from object store
    Fetching,
    /// The cache entry was successfully fetched and is stored in the cache as a [`CacheValue`]
    Success(Arc<CacheValue>),
    /// The request to the object store failed
    Failed,
    /// The cache entry was deleted
    Deleted,
    /// The object is too large for the cache
    TooLarge,
}

impl CacheEntry {
    /// Get the size of thje cache entry in bytes
    fn size(&self) -> usize {
        match self {
            CacheEntry::Fetching => 0,
            CacheEntry::Success(v) => v.size(),
            CacheEntry::Failed => 0,
            CacheEntry::Deleted => 0,
            CacheEntry::TooLarge => 0,
        }
    }

    fn is_fetching(&self) -> bool {
        matches!(self, CacheEntry::Fetching)
    }

    fn is_success(&self) -> bool {
        matches!(self, CacheEntry::Success(_))
    }

    fn keep(&self) -> bool {
        self.is_fetching() || self.is_success()
    }
}

/// Implements the [`WeightScale`] trait to determine a [`CacheEntry`]'s size on insertion to
/// the cache
#[derive(Debug)]
struct CacheEntryScale;

impl WeightScale<Path, CacheEntry> for CacheEntryScale {
    fn weight(&self, key: &Path, value: &CacheEntry) -> usize {
        key.as_ref().len() + value.size()
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
    /// A weighted LRU cache for storing the objects associated with a given path in memory
    // NOTE(trevor): this uses a mutex as the CLruCache type needs &mut self for its get method, so
    // we always need an exclusive lock on the cache. If this creates a performance bottleneck then
    // we will need to look for alternatives.
    //
    // A Tokio mutex is used to prevent blocking the thread while waiting for a lock, and so that
    // the lock can be held accross await points.
    cache: Arc<Mutex<CLruCache<Path, CacheEntry, RandomState, CacheEntryScale>>>,
}

impl MemCachedObjectStore {
    /// Create a new [`MemCachedObjectStore`] with the given memory capacity
    fn new(inner: Arc<dyn ObjectStore>, memory_capacity: usize) -> Self {
        let cache = CLruCache::with_config(
            CLruCacheConfig::new(NonZeroUsize::new(memory_capacity).unwrap())
                .with_scale(CacheEntryScale),
        );
        Self {
            inner,
            cache: Arc::new(Mutex::new(cache)),
        }
    }

    /// Get an entry in the cache if it contains a successful fetch result, or `None` otherwise
    ///
    /// This requires `&mut self` as the underlying method on the cache requires a mutable reference
    /// in order to update the recency of the entry in the cache
    async fn get_cache_value(&self, path: &Path) -> Option<Arc<CacheValue>> {
        self.cache
            .lock()
            .await
            .get(path)
            .and_then(|entry| match entry {
                CacheEntry::Fetching
                | CacheEntry::Failed
                | CacheEntry::Deleted
                | CacheEntry::TooLarge => None,
                CacheEntry::Success(v) => Some(Arc::clone(v)),
            })
    }

    /// Set the state of a cache entry to `Deleted`, since we cannot remove elements from the
    /// cache directly.
    async fn delete_cache_value(&self, path: &Path) {
        let _ = self
            .cache
            .lock()
            .await
            .put_with_weight(path.clone(), CacheEntry::Deleted);
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
        if let Some(v) = self.get_cache_value(location).await {
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
        if let Some(v) = self.get_cache_value(location).await {
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
        if let Some(v) = self.get_cache_value(location).await {
            Ok(v.meta.clone())
        } else {
            self.inner.head(location).await
        }
    }

    /// Delete an object on object store, but also remove it from the cache.
    async fn delete(&self, location: &Path) -> ObjectStoreResult<()> {
        let result = self.inner.delete(location).await?;
        self.delete_cache_value(location).await;
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
            // Check that the cache does not already contain an entry for the provide path, or that
            // it is not already in the process of fetching the given path:
            let mut cache_lock = mem_store.cache.lock().await;
            if cache_lock
                .get(&path)
                .is_some_and(|entry| entry.is_fetching() || entry.is_success())
            {
                continue;
            }
            // Put a `Fetching` state in the entry to prevent concurrent requests to the same path:
            let _ = cache_lock.put_with_weight(path.clone(), CacheEntry::Fetching);
            // Drop the lock before spawning the task below
            drop(cache_lock);
            let mem_store_captured = Arc::clone(&mem_store);
            tokio::spawn(async move {
                let cache_insertion_result = match mem_store_captured.inner.get(&path).await {
                    Ok(result) => {
                        let meta = result.meta.clone();
                        match result.bytes().await {
                            Ok(data) => mem_store_captured.cache.lock().await.put_with_weight(
                                path,
                                CacheEntry::Success(Arc::new(CacheValue { data, meta })),
                            ),
                            Err(error) => {
                                error!(%error, "failed to retrieve payload from object store get result");
                                mem_store_captured
                                    .cache
                                    .lock()
                                    .await
                                    .put_with_weight(path, CacheEntry::Failed)
                            }
                        }
                    }
                    Err(error) => {
                        error!(%error, "failed to fulfill cache request with object store");
                        mem_store_captured
                            .cache
                            .lock()
                            .await
                            .put_with_weight(path, CacheEntry::Failed)
                    }
                };
                // If an entry would not fit in the cache at all, the put_with_weight method returns
                // it as an Err from above, and we would not have cleared the `Fetching` entry, so
                // we need to do that here:
                if let Err((k, _)) = cache_insertion_result {
                    mem_store_captured
                        .cache
                        .lock()
                        .await
                        .put_with_weight(k, CacheEntry::TooLarge)
                        .expect("cache capacity is too small");
                }
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
        let mut interval = tokio::time::interval(Duration::from_secs(60));
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            interval.tick().await;

            mem_store.cache.lock().await.retain(|_, entry| entry.keep());
        }
    })
}

#[cfg(test)]
mod tests {
    use std::{ops::Range, sync::Arc};

    use arrow::datatypes::ToByteSlice;
    use async_trait::async_trait;
    use bytes::Bytes;
    use futures::stream::BoxStream;
    use hashbrown::HashMap;
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
        let (cached_store, oracle) =
            test_cached_obj_store_and_oracle(Arc::clone(&inner_store) as _);
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
        let cache_capacity_bytes = 32;
        let (cached_store, oracle) =
            create_cached_obj_store_and_oracle(Arc::clone(&inner_store) as _, cache_capacity_bytes);
        // PUT an entry into the store:
        let path_1 = Path::from("0.parquet"); // 9 bytes for path
        let payload_1 = b"Janeway"; // 7 bytes for payload
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

        // GET the entry to check its there and was retrieved from cache, i.e., that the request
        // counts do not change:
        assert_payload_at_equals!(cached_store, payload_1, path_1);
        assert_eq!(1, inner_store.total_get_request_count());
        assert_eq!(1, inner_store.get_request_count(&path_1));

        // PUT a second entry into the store:
        let path_2 = Path::from("1.parquet"); // 9 bytes for path
        let payload_2 = b"Paris"; // 5 bytes for payload
        cached_store
            .put(&path_2, PutPayload::from_static(payload_2))
            .await
            .unwrap();

        // cache the second entry and wait for it to complete, this will not evict the first entry
        // as both can fit in the cache whose capacity is 32 bytes:
        let (cache_request, notifier_rx) = CacheRequest::create(path_2.clone());
        oracle.register(cache_request);
        let _ = notifier_rx.await;
        // will have another request for the second path to the inner store, by the oracle:
        assert_eq!(2, inner_store.total_get_request_count());
        assert_eq!(1, inner_store.get_request_count(&path_1));
        assert_eq!(1, inner_store.get_request_count(&path_2));

        // GET the second entry and assert that it was retrieved from the cache, i.e., that the
        // request counts do not change:
        assert_payload_at_equals!(cached_store, payload_2, path_2);
        assert_eq!(2, inner_store.total_get_request_count());
        assert_eq!(1, inner_store.get_request_count(&path_1));
        assert_eq!(1, inner_store.get_request_count(&path_2));

        // GET the first entry again and assert that it was retrieved from the cache as before. This
        // will also update the LRU so that the first entry (janeway) was used more recently than the
        // second entry (paris):
        assert_payload_at_equals!(cached_store, payload_1, path_1);
        assert_eq!(2, inner_store.total_get_request_count());
        assert_eq!(1, inner_store.get_request_count(&path_1));

        // PUT a third entry into the store:
        let path_3 = Path::from("2.parquet"); // 9 bytes for the path
        let payload_3 = b"Neelix"; // 6 bytes for the payload
        cached_store
            .put(&path_3, PutPayload::from_static(payload_3))
            .await
            .unwrap();
        // cache the third entry and wait for it to complete, this will evict paris from the cache
        // as the LRU entry:
        let (cache_request, notifier_rx) = CacheRequest::create(path_3.clone());
        oracle.register(cache_request);
        let _ = notifier_rx.await;
        // will now have another request for the third path to the inner store, by the oracle:
        assert_eq!(3, inner_store.total_get_request_count());
        assert_eq!(1, inner_store.get_request_count(&path_1));
        assert_eq!(1, inner_store.get_request_count(&path_2));
        assert_eq!(1, inner_store.get_request_count(&path_3));

        // GET the new entry from the strore, and check that it was served by the cache:
        assert_payload_at_equals!(cached_store, payload_3, path_3);
        assert_eq!(3, inner_store.total_get_request_count());
        assert_eq!(1, inner_store.get_request_count(&path_1));
        assert_eq!(1, inner_store.get_request_count(&path_2));
        assert_eq!(1, inner_store.get_request_count(&path_3));

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
