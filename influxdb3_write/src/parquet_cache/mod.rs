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

pub struct CacheRequest {
    path: Path,
    notifier: oneshot::Sender<()>,
}

impl CacheRequest {
    pub fn create(path: Path) -> (Self, oneshot::Receiver<()>) {
        let (notifier, receiver) = oneshot::channel();
        (Self { path, notifier }, receiver)
    }
}

/// An interface for interacting with a Parquet Cache by registering [`CacheRequest`]s to it.
pub trait ParquetCacheOracle: Send + Sync + Debug {
    fn register(&self, cache_request: CacheRequest);
}

#[derive(Debug, Clone)]
pub struct MemCacheOracle {
    cache_request_tx: Sender<CacheRequest>,
}

const CACHE_REQUEST_BUFFER_SIZE: usize = 1_000_000;

impl MemCacheOracle {
    /// Create a new [`MemCacheOracle`]
    // TODO(trevor): this should be more configurable, e.g., channel size, prune interval
    fn new(mem_cached_store: Arc<MemCachedObjectStore>) -> Self {
        let (cache_request_tx, cache_request_rx) = channel(CACHE_REQUEST_BUFFER_SIZE);
        background_cache_request_handler(Arc::clone(&mem_cached_store), cache_request_rx);
        background_cache_pruner(mem_cached_store);
        Self { cache_request_tx }
    }
}

pub fn create_cached_obj_store_and_oracle(
    object_store: Arc<dyn ObjectStore>,
    cache_capacity: usize,
) -> (Arc<dyn ObjectStore>, Arc<dyn ParquetCacheOracle>) {
    let store = Arc::new(MemCachedObjectStore::new(object_store, cache_capacity));
    let oracle = Arc::new(MemCacheOracle::new(Arc::clone(&store)));
    (store, oracle)
}

/// Create a test cached objet store with a cache capacity of 1GB
pub fn test_cached_obj_store_and_oracle(
    object_store: Arc<dyn ObjectStore>,
) -> (Arc<dyn ObjectStore>, Arc<dyn ParquetCacheOracle>) {
    create_cached_obj_store_and_oracle(object_store, 1024 * 1024 * 1024)
}

#[derive(Debug)]
struct CacheValue {
    data: Bytes,
    meta: ObjectMeta,
}

impl CacheValue {
    fn size(&self) -> usize {
        // TODO(trevor): could also calculate the size of the metadata...
        self.data.len()
    }
}

#[derive(Debug)]
enum CacheEntry {
    Fetching,
    Success(Arc<CacheValue>),
    Failed,
    Deleted,
    TooLarge,
}

impl CacheEntry {
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

#[derive(Debug)]
struct CacheEntryScale;

impl WeightScale<Path, CacheEntry> for CacheEntryScale {
    fn weight(&self, key: &Path, value: &CacheEntry) -> usize {
        key.as_ref().len() + value.size()
    }
}

const STORE_NAME: &str = "mem_cached_object_store";

#[derive(Debug)]
pub struct MemCachedObjectStore {
    inner: Arc<dyn ObjectStore>,
    cache: Arc<Mutex<CLruCache<Path, CacheEntry, RandomState, CacheEntryScale>>>,
}

impl MemCachedObjectStore {
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

fn background_cache_request_handler(
    mem_store: Arc<MemCachedObjectStore>,
    mut rx: Receiver<CacheRequest>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        while let Some(CacheRequest { path, notifier }) = rx.recv().await {
            let mut cache_lock = mem_store.cache.lock().await;
            if cache_lock
                .get(&path)
                .is_some_and(|entry| entry.is_fetching() || entry.is_success())
            {
                continue;
            }
            let _ = cache_lock.put_with_weight(path.clone(), CacheEntry::Fetching);
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
                // it as an Err from above, so we need to clear the `Fetching` entry:
                if let Err((k, _)) = cache_insertion_result {
                    mem_store_captured
                        .cache
                        .lock()
                        .await
                        .put_with_weight(k, CacheEntry::TooLarge)
                        .expect("cache capacity is too small");
                }
                let _ = notifier.send(());
            });
        }
        info!("cache request handler closed");
    })
}

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

    use crate::parquet_cache::{
        create_cached_obj_store_and_oracle, test_cached_obj_store_and_oracle, CacheRequest,
    };

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
        assert_eq!(
            payload,
            cached_store
                .get(&path)
                .await
                .unwrap()
                .bytes()
                .await
                .unwrap()
                .to_byte_slice()
        );
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
        assert_eq!(
            payload,
            cached_store
                .get(&path)
                .await
                .unwrap()
                .bytes()
                .await
                .unwrap()
                .to_byte_slice()
        );

        // should hit the cache this time, so the inner store should not have been hit, and counts
        // should therefore be same as previous:
        assert_eq!(2, inner_store.total_get_request_count());
        assert_eq!(2, inner_store.get_request_count(&path));
    }

    #[tokio::test]
    async fn cache_evicts_when_full() {
        let inner_store = Arc::new(TestObjectStore::new(Arc::new(InMemory::new())));
        let cache_capacity_bytes = 32;
        let (cached_store, oracle) =
            create_cached_obj_store_and_oracle(Arc::clone(&inner_store) as _, cache_capacity_bytes);
        // PUT an entry into the store:
        let path = Path::from("0.parquet"); // 9 bytes for path
        let payload = b"Commander Spock"; // 15 bytes for payload
        cached_store
            .put(&path, PutPayload::from_static(payload))
            .await
            .unwrap();

        // cache the entry and wait for it to complete:
        let (cache_request, notifier_rx) = CacheRequest::create(path.clone());
        oracle.register(cache_request);
        let _ = notifier_rx.await;
        // there will have been one get request made by the cache oracle:
        assert_eq!(1, inner_store.total_get_request_count());
        assert_eq!(1, inner_store.get_request_count(&path));

        // GET the entry to check its there and was retrieved from cache:
        assert_eq!(
            payload,
            cached_store
                .get(&path)
                .await
                .unwrap()
                .bytes()
                .await
                .unwrap()
                .to_byte_slice()
        );
        // request counts to inner store remain unchanged:
        assert_eq!(1, inner_store.total_get_request_count());
        assert_eq!(1, inner_store.get_request_count(&path));

        // PUT another entry into the store, this combined with the previous entry's size will be
        // greater than the cache's capacity (32 bytes), so will cause spock to be evicted:
        let path_2 = Path::from("1.parquet"); // 9 bytes for path
        let payload_2 = b"Lieutenant Tuvok"; // 16 bytes for payload
        cached_store
            .put(&path_2, PutPayload::from_static(payload_2))
            .await
            .unwrap();

        // cache the second entry and wait for it to complete:
        let (cache_request, notifier_rx) = CacheRequest::create(path_2.clone());
        oracle.register(cache_request);
        let _ = notifier_rx.await;
        // will have another request for the second path to the inner store, by the oracle:
        assert_eq!(2, inner_store.total_get_request_count());
        assert_eq!(1, inner_store.get_request_count(&path_2));

        // GET the first entry and assert that it was retrieved from the store, i.e., becayse its
        // entry was evicted from the cache:
        assert_eq!(
            payload,
            cached_store
                .get(&path)
                .await
                .unwrap()
                .bytes()
                .await
                .unwrap()
                .to_byte_slice()
        );
        assert_eq!(3, inner_store.total_get_request_count());
        assert_eq!(2, inner_store.get_request_count(&path));

        // GET the second entry and assert that it was retrieved from the cache, i.e., the number
        // of total requests to the cache does not change from the previous check:
        assert_eq!(
            payload_2,
            cached_store
                .get(&path_2)
                .await
                .unwrap()
                .bytes()
                .await
                .unwrap()
                .to_byte_slice()
        );
        assert_eq!(3, inner_store.total_get_request_count());
        assert_eq!(1, inner_store.get_request_count(&path_2));
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
