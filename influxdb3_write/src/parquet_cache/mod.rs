//! An in-memory cache of Parquet files that are persisted to object storage
use std::{fmt::Debug, ops::Range, sync::Arc};

use async_trait::async_trait;
use bytes::Bytes;
use dashmap::DashMap;
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
    oneshot,
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
    fn new(mem_cached_store: Arc<MemCachedObjectStore>) -> Self {
        let (cache_request_tx, cache_request_rx) = channel(CACHE_REQUEST_BUFFER_SIZE);
        background_cache_request_handler(Arc::clone(&mem_cached_store), cache_request_rx);
        Self { cache_request_tx }
    }
}

pub fn create_cached_obj_store_and_oracle(
    object_store: Arc<dyn ObjectStore>,
) -> (Arc<dyn ObjectStore>, Arc<dyn ParquetCacheOracle>) {
    let store = Arc::new(MemCachedObjectStore::new(object_store));
    let oracle = Arc::new(MemCacheOracle::new(Arc::clone(&store)));
    (store, oracle)
}

#[derive(Debug)]
struct CacheValue {
    data: Bytes,
    meta: ObjectMeta,
}

#[derive(Debug)]
enum CacheEntry {
    Fetching,
    Success(Arc<CacheValue>),
    Failed,
}

impl CacheEntry {
    fn is_fetching(&self) -> bool {
        matches!(self, CacheEntry::Fetching)
    }

    fn is_success(&self) -> bool {
        matches!(self, CacheEntry::Success(_))
    }
}

const STORE_NAME: &str = "mem_cached_object_store";

#[derive(Debug)]
pub struct MemCachedObjectStore {
    inner: Arc<dyn ObjectStore>,
    cache: Arc<DashMap<Path, CacheEntry>>,
}

impl MemCachedObjectStore {
    fn new(inner: Arc<dyn ObjectStore>) -> Self {
        Self {
            inner,
            cache: Arc::new(DashMap::new()),
        }
    }

    fn get_cache_value(&self, path: &Path) -> Option<Arc<CacheValue>> {
        self.cache.get(path).and_then(|entry| match entry.value() {
            CacheEntry::Success(v) => Some(Arc::clone(v)),
            CacheEntry::Fetching | CacheEntry::Failed => None,
        })
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
        if let Some(v) = self.get_cache_value(location) {
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
        if let Some(v) = self.get_cache_value(location) {
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
        if let Some(v) = self.get_cache_value(location) {
            Ok(v.meta.clone())
        } else {
            self.inner.head(location).await
        }
    }

    /// Delete an object on object store, but also remove it from the cache.
    async fn delete(&self, location: &Path) -> ObjectStoreResult<()> {
        self.inner.delete(location).await.inspect(|_| {
            self.cache.remove(location);
        })
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
    cache: Arc<MemCachedObjectStore>,
    mut rx: Receiver<CacheRequest>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        while let Some(CacheRequest { path, notifier }) = rx.recv().await {
            if cache
                .cache
                .get(&path)
                .is_some_and(|entry| entry.is_fetching() || entry.is_success())
            {
                continue;
            }
            cache.cache.insert(path.clone(), CacheEntry::Fetching);
            let cache_captured = Arc::clone(&cache);
            tokio::spawn(async move {
                match cache_captured.inner.get(&path).await {
                    Ok(result) => {
                        let meta = result.meta.clone();
                        match result.bytes().await {
                            Ok(data) => {
                                cache_captured.cache.insert(
                                    path,
                                    CacheEntry::Success(Arc::new(CacheValue { data, meta })),
                                );
                            }
                            Err(error) => {
                                error!(%error, "failed to retrieve payload from object store get result");
                                cache_captured.cache.insert(path, CacheEntry::Failed);
                            }
                        }
                    }
                    Err(error) => {
                        error!(%error, "failed to fulfill cache request with object store");
                        cache_captured.cache.insert(path, CacheEntry::Failed);
                    }
                }
                let _ = notifier.send(());
            });
        }
        info!("cache request handler closed");
    })
}

#[cfg(test)]
mod tests {
    use std::{ops::Range, sync::Arc};

    use arrow::datatypes::ToByteSlice;
    use async_trait::async_trait;
    use bytes::Bytes;
    use dashmap::DashMap;
    use futures::stream::BoxStream;
    use object_store::{
        memory::InMemory, path::Path, GetOptions, GetResult, ListResult, MultipartUpload,
        ObjectMeta, ObjectStore, PutMultipartOpts, PutOptions, PutPayload, PutResult,
    };

    use crate::parquet_cache::{create_cached_obj_store_and_oracle, CacheRequest};

    #[tokio::test]
    async fn successful_cache_hit() {
        // set up the inner test object store and then wrap it with the mem cached store:
        let inner_store = Arc::new(TestObjectStore::new(Arc::new(InMemory::new())));
        let (cached_store, oracle) =
            create_cached_obj_store_and_oracle(Arc::clone(&inner_store) as _);
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

    type RequestCounter = DashMap<Path, usize>;

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
            self.get.iter().map(|r| *r.value()).sum()
        }

        fn get_request_count(&self, path: &Path) -> usize {
            self.get.get(path).map(|r| *r.value()).unwrap_or(0)
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
            *self.get.entry(location.clone()).or_insert(0) += 1;
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
