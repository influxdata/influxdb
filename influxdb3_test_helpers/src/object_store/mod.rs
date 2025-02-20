use std::{ops::Range, sync::Arc};

use async_trait::async_trait;
use bytes::Bytes;
use futures::stream::BoxStream;
use hashbrown::HashMap;
use object_store::{
    GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore, PutMultipartOpts,
    PutOptions, PutPayload, PutResult, path::Path,
};
use parking_lot::RwLock;
use tokio::sync::Notify;

type RequestCounter = RwLock<HashMap<Path, usize>>;

/// A wrapper around an inner object store that tracks requests made to the inner store
#[derive(Debug)]
pub struct RequestCountedObjectStore {
    inner: Arc<dyn ObjectStore>,
    get: RequestCounter,
    get_opts: RequestCounter,
    get_range: RequestCounter,
    get_ranges: RequestCounter,
    head: RequestCounter,
}

impl RequestCountedObjectStore {
    pub fn new(inner: Arc<dyn ObjectStore>) -> Self {
        Self {
            inner,
            get: Default::default(),
            get_opts: Default::default(),
            get_range: Default::default(),
            get_ranges: Default::default(),
            head: Default::default(),
        }
    }

    /// Get the total request count accross READ-style requests for a specific `Path` in the inner
    /// object store.
    pub fn total_read_request_count(&self, path: &Path) -> usize {
        self.get_request_count(path)
            + self.get_opts_request_count(path)
            + self.get_range_request_count(path)
            + self.get_ranges_request_count(path)
            + self.head_request_count(path)
    }

    pub fn get_request_count(&self, path: &Path) -> usize {
        self.get.read().get(path).copied().unwrap_or(0)
    }

    pub fn get_opts_request_count(&self, path: &Path) -> usize {
        self.get_opts.read().get(path).copied().unwrap_or(0)
    }

    pub fn get_range_request_count(&self, path: &Path) -> usize {
        self.get_range.read().get(path).copied().unwrap_or(0)
    }

    pub fn get_ranges_request_count(&self, path: &Path) -> usize {
        self.get_ranges.read().get(path).copied().unwrap_or(0)
    }

    pub fn head_request_count(&self, path: &Path) -> usize {
        self.head.read().get(path).copied().unwrap_or(0)
    }
}

impl std::fmt::Display for RequestCountedObjectStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "TestObjectStore({})", self.inner)
    }
}

#[async_trait]
impl ObjectStore for RequestCountedObjectStore {
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
        *self.get_opts.write().entry(location.clone()).or_insert(0) += 1;
        self.inner.get_opts(location, options).await
    }

    async fn get_range(&self, location: &Path, range: Range<usize>) -> object_store::Result<Bytes> {
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

/// A wrapper around an inner object store that can hold execution of certain object store methods
/// to synchronize other processes before the request is forwarded to the inner object store
///
/// # Example
/// ```ignore
/// // set up notifiers:
/// let to_store_notify = Arc::new(Notify::new());
/// let from_store_notify = Arc::new(Notify::new());
///
/// // create the synchronized store wrapping an in-memory object store:
/// let inner_store = Arc::new(
///     SynchronizedObjectStore::new(Arc::new(InMemory::new()))
///         .with_notifies(Arc::clone(&to_store_notify), Arc::clone(&from_store_notify)),
/// );
///
/// // we are in the middle of a get request once this call to notified wakes:
/// let _ = from_store_notify.notified().await;
///
/// // spawn a thread to wake the in-flight get request:
/// let h = tokio::spawn(async move {
///     to_store_notify.notify_one();
///     let _ = notifier_rx.await;
/// });
/// ```
#[derive(Debug)]
pub struct SynchronizedObjectStore {
    inner: Arc<dyn ObjectStore>,
    get_notifies: Option<(Arc<Notify>, Arc<Notify>)>,
}

impl SynchronizedObjectStore {
    pub fn new(inner: Arc<dyn ObjectStore>) -> Self {
        Self {
            inner,
            get_notifies: None,
        }
    }

    /// Add notifiers for `get` requests so that async execution can be halted to synchronize other
    /// processes before the request is forwarded to the inner object store.
    pub fn with_get_notifies(mut self, inbound: Arc<Notify>, outbound: Arc<Notify>) -> Self {
        self.get_notifies = Some((inbound, outbound));
        self
    }
}

impl std::fmt::Display for SynchronizedObjectStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "TestObjectStore({})", self.inner)
    }
}

#[async_trait]
impl ObjectStore for SynchronizedObjectStore {
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
        if let Some((inbound, outbound)) = &self.get_notifies {
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
        self.inner.get_opts(location, options).await
    }

    async fn get_range(&self, location: &Path, range: Range<usize>) -> object_store::Result<Bytes> {
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
