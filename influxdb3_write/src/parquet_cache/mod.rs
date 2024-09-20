//! An in-memory cache of Parquet files that are persisted to object storage
use std::{fmt::Debug, ops::Range, sync::Arc};

use anyhow::Context;
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
use object_store_mem_cache::object_store_helpers::any_options_set;
use observability_deps::tracing::{error, info};
use tokio::sync::mpsc::{channel, Receiver, Sender};

pub type Result<T, E = anyhow::Error> = std::result::Result<T, E>;

pub enum CacheRequest {
    Get(Path),
}

/// An interface for interacting with a Parquet Cache by registering [`CacheRequest`]s to it.
#[async_trait]
pub trait ParquetCache {
    async fn register(&self, cache_request: CacheRequest) -> Result<()>;
}

#[derive(Debug)]
struct CacheValue {
    data: Bytes,
    meta: ObjectMeta,
}

const STORE_NAME: &str = "mem_cached_object_store";

#[derive(Debug)]
pub struct MemCachedObjectStore {
    inner: Arc<dyn ObjectStore>,
    cache: Arc<DashMap<Path, CacheValue>>,
    cache_request_tx: Sender<CacheRequest>,
}

const CACHE_REQUEST_BUFFER_SIZE: usize = usize::MAX;

impl MemCachedObjectStore {
    pub fn new(inner: Arc<dyn ObjectStore>) -> Arc<Self> {
        let (cache_request_tx, cache_request_rx) = channel(CACHE_REQUEST_BUFFER_SIZE);
        let mem_cached_obj_store = Arc::new(Self {
            inner,
            cache: Arc::new(DashMap::new()),
            cache_request_tx,
        });
        background_cache_request_handler(Arc::clone(&mem_cached_obj_store), cache_request_rx);
        mem_cached_obj_store
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
        if let Some(v) = self.cache.get(location) {
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
        // Note(trevor): this could probably be supported if we need it via the ObjectMeta
        // stored in the cache. For now this is conservative:
        if any_options_set(&options) {
            self.inner.get_opts(location, options).await
        } else {
            self.get(location).await
        }
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
        if let Some(v) = self.cache.get(location) {
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
        if let Some(v) = self.cache.get(location) {
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

#[async_trait]
impl ParquetCache for MemCachedObjectStore {
    async fn register(&self, request: CacheRequest) -> Result<()> {
        self.cache_request_tx
            .send(request)
            .await
            .context("failed to send cache request")
            .map_err(Into::into)
    }
}

fn background_cache_request_handler(
    cache: Arc<MemCachedObjectStore>,
    mut rx: Receiver<CacheRequest>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        while let Some(cache_request) = rx.recv().await {
            match cache_request {
                CacheRequest::Get(path) => {
                    if cache.cache.contains_key(&path) {
                        // Since we treat parquet files on object storage as immutible, we never
                        // need to update:
                        continue;
                    }
                    match cache.inner.get(&path).await {
                        Ok(result) => {
                            let meta = result.meta.clone();
                            match result.bytes().await {
                                Ok(data) => {
                                    cache.cache.insert(path, CacheValue { data, meta });
                                }
                                Err(error) => {
                                    error!(%error, "failed to retrieve payload from object store get result");
                                }
                            }
                        }
                        Err(error) => {
                            error!(%error, "failed to fulfill cache request with object store")
                        }
                    }
                }
            }
        }
        info!("cache request handler closed");
    })
}
