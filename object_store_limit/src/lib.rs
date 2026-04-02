//! An object store wrapper that limits the maximum operation concurrency of the wrapped
//! implementation and reports permit utilisation via [`AsyncSemaphoreMetrics`].
//!
//! This is a replacement for [`object_store::limit::LimitStore`] that uses
//! [`InstrumentedAsyncSemaphore`] from the `tracker` crate so that permit
//! utilisation, queuing, and acquire durations are reported to `/metrics`.

use std::fmt;
use std::ops::Range;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use async_trait::async_trait;
use bytes::Bytes;
use futures::stream::BoxStream;
use futures::{FutureExt, Stream, StreamExt};
use object_store::{
    GetOptions, GetResult, GetResultPayload, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    PutMultipartOptions, PutOptions, PutPayload, PutResult, Result, UploadPart, path::Path,
};
use tracker::{
    AsyncSemaphoreMetrics, InstrumentedAsyncOwnedSemaphorePermit, InstrumentedAsyncSemaphore,
};

/// Store wrapper that limits the maximum number of concurrent object store
/// operations via an [`InstrumentedAsyncSemaphore`], and reports utilisation
/// through the [`AsyncSemaphoreMetrics`] registered with the metric registry.
///
/// Each call to an [`ObjectStore`] member function is considered a single
/// operation, even if it may result in more than one network call.
///
/// For streaming responses (`get`, `list`), the permit is held for the entire
/// lifetime of the returned stream.
#[derive(Debug)]
pub struct LimitObjectStore {
    inner: Arc<dyn ObjectStore>,
    pub(crate) semaphore: Arc<InstrumentedAsyncSemaphore>,
}

impl LimitObjectStore {
    /// Create a new `LimitObjectStore` that limits the maximum number of
    /// outstanding concurrent requests to `max_permits`.
    ///
    /// Semaphore metrics (permits acquired, pending, total, acquire duration,
    /// etc.) are reported through the provided [`AsyncSemaphoreMetrics`].
    pub fn new(
        inner: Arc<dyn ObjectStore>,
        max_permits: usize,
        metrics: &Arc<AsyncSemaphoreMetrics>,
    ) -> Self {
        Self {
            inner,
            semaphore: Arc::new(metrics.new_semaphore(max_permits)),
        }
    }

    /// Returns the number of permits currently in use.
    pub fn permits_in_use(&self) -> u64 {
        self.semaphore.permits_acquired()
    }

    /// Returns the configured maximum number of permits.
    pub fn max_permits(&self) -> usize {
        self.semaphore.total_permits()
    }

    /// Returns the number of operations currently waiting to acquire a permit.
    pub fn waiters(&self) -> u64 {
        self.semaphore.holders_pending()
    }

    /// Acquire a permit.
    async fn acquire(&self) -> InstrumentedAsyncOwnedSemaphorePermit {
        self.semaphore.acquire_owned(None).await.unwrap()
    }
}

impl fmt::Display for LimitObjectStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "LimitObjectStore({}, {})",
            self.semaphore.total_permits(),
            self.inner
        )
    }
}

#[async_trait]
impl ObjectStore for LimitObjectStore {
    async fn put(&self, location: &Path, payload: PutPayload) -> Result<PutResult> {
        let _permit = self.acquire().await;
        self.inner.put(location, payload).await
    }

    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> Result<PutResult> {
        let _permit = self.acquire().await;
        self.inner.put_opts(location, payload, opts).await
    }

    // NOTE: multipart initiation is not guarded by the semaphore, matching
    // upstream LimitStore. Individual part uploads, complete, and abort are
    // each guarded.
    async fn put_multipart(&self, location: &Path) -> Result<Box<dyn MultipartUpload>> {
        let upload = self.inner.put_multipart(location).await?;
        Ok(Box::new(LimitUpload {
            semaphore: Arc::clone(&self.semaphore),
            upload,
        }))
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: PutMultipartOptions,
    ) -> Result<Box<dyn MultipartUpload>> {
        let upload = self.inner.put_multipart_opts(location, opts).await?;
        Ok(Box::new(LimitUpload {
            semaphore: Arc::clone(&self.semaphore),
            upload,
        }))
    }

    async fn get(&self, location: &Path) -> Result<GetResult> {
        let permit = self.acquire().await;
        let r = self.inner.get(location).await?;
        Ok(permit_get_result(r, permit))
    }

    async fn get_opts(&self, location: &Path, options: GetOptions) -> Result<GetResult> {
        let permit = self.acquire().await;
        let r = self.inner.get_opts(location, options).await?;
        Ok(permit_get_result(r, permit))
    }

    async fn get_range(&self, location: &Path, range: Range<u64>) -> Result<Bytes> {
        let _permit = self.acquire().await;
        self.inner.get_range(location, range).await
    }

    async fn get_ranges(&self, location: &Path, ranges: &[Range<u64>]) -> Result<Vec<Bytes>> {
        let _permit = self.acquire().await;
        self.inner.get_ranges(location, ranges).await
    }

    async fn head(&self, location: &Path) -> Result<ObjectMeta> {
        let _permit = self.acquire().await;
        self.inner.head(location).await
    }

    async fn delete(&self, location: &Path) -> Result<()> {
        let _permit = self.acquire().await;
        self.inner.delete(location).await
    }

    // NOTE: delete_stream bypasses the semaphore, matching upstream LimitStore.
    fn delete_stream<'a>(
        &'a self,
        locations: BoxStream<'a, Result<Path>>,
    ) -> BoxStream<'a, Result<Path>> {
        self.inner.delete_stream(locations)
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, Result<ObjectMeta>> {
        let prefix = prefix.cloned();
        let inner = Arc::clone(&self.inner);
        let semaphore = Arc::clone(&self.semaphore);
        async move {
            let permit = semaphore.acquire_owned(None).await.unwrap();
            let s = inner.list(prefix.as_ref());
            PermitWrapper::new(s, permit)
        }
        .into_stream()
        .flatten()
        .boxed()
    }

    fn list_with_offset(
        &self,
        prefix: Option<&Path>,
        offset: &Path,
    ) -> BoxStream<'static, Result<ObjectMeta>> {
        let prefix = prefix.cloned();
        let offset = offset.clone();
        let inner = Arc::clone(&self.inner);
        let semaphore = Arc::clone(&self.semaphore);
        async move {
            let permit = semaphore.acquire_owned(None).await.unwrap();
            let s = inner.list_with_offset(prefix.as_ref(), &offset);
            PermitWrapper::new(s, permit)
        }
        .into_stream()
        .flatten()
        .boxed()
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> Result<ListResult> {
        let _permit = self.acquire().await;
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy(&self, from: &Path, to: &Path) -> Result<()> {
        let _permit = self.acquire().await;
        self.inner.copy(from, to).await
    }

    async fn rename(&self, from: &Path, to: &Path) -> Result<()> {
        let _permit = self.acquire().await;
        self.inner.rename(from, to).await
    }

    async fn copy_if_not_exists(&self, from: &Path, to: &Path) -> Result<()> {
        let _permit = self.acquire().await;
        self.inner.copy_if_not_exists(from, to).await
    }

    async fn rename_if_not_exists(&self, from: &Path, to: &Path) -> Result<()> {
        let _permit = self.acquire().await;
        self.inner.rename_if_not_exists(from, to).await
    }
}

fn permit_get_result(r: GetResult, permit: InstrumentedAsyncOwnedSemaphorePermit) -> GetResult {
    let payload = match r.payload {
        GetResultPayload::Stream(s) => {
            GetResultPayload::Stream(PermitWrapper::new(s, permit).boxed())
        }
        other => {
            // For non-stream payloads (e.g. File), just drop the permit immediately.
            drop(permit);
            other
        }
    };
    GetResult { payload, ..r }
}

/// Combines an [`InstrumentedAsyncOwnedSemaphorePermit`] with some other type
/// so that the permit is held for the lifetime of the wrapper.
struct PermitWrapper<T> {
    inner: T,
    #[allow(dead_code)]
    permit: InstrumentedAsyncOwnedSemaphorePermit,
}

impl<T> PermitWrapper<T> {
    fn new(inner: T, permit: InstrumentedAsyncOwnedSemaphorePermit) -> Self {
        Self { inner, permit }
    }
}

impl<T: Stream + Unpin> Stream for PermitWrapper<T> {
    type Item = T::Item;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        Pin::new(&mut self.inner).poll_next(cx)
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.inner.size_hint()
    }
}

/// A [`MultipartUpload`] wrapper that limits concurrent requests.
#[derive(Debug)]
struct LimitUpload {
    upload: Box<dyn MultipartUpload>,
    semaphore: Arc<InstrumentedAsyncSemaphore>,
}

#[async_trait]
impl MultipartUpload for LimitUpload {
    fn put_part(&mut self, data: PutPayload) -> UploadPart {
        let upload = self.upload.put_part(data);
        let semaphore = Arc::clone(&self.semaphore);
        Box::pin(async move {
            let _permit = semaphore.acquire_owned(None).await.unwrap();
            upload.await
        })
    }

    async fn complete(&mut self) -> Result<PutResult> {
        let _permit = self.semaphore.acquire_owned(None).await.unwrap();
        self.upload.complete().await
    }

    async fn abort(&mut self) -> Result<()> {
        let _permit = self.semaphore.acquire_owned(None).await.unwrap();
        self.upload.abort().await
    }
}

#[cfg(test)]
mod tests;
