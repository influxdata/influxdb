//! [`ObservedObjectStore`] wraps an inner [`ObjectStore`] and feeds the result
//! of every operation into a shared [`ObjectStoreHealth`] handle for use by
//! the `/ready` endpoint.

use std::ops::Range;
use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use futures::StreamExt;
use futures::stream::BoxStream;
use iox_time::TimeProvider;
use object_store::{
    GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    PutMultipartOptions, PutOptions, PutPayload, PutResult, Result, path::Path,
};

use crate::object_store_health::{ErrorCategory, ObjectStoreHealth};

#[derive(Debug)]
pub struct ObservedObjectStore {
    inner: Arc<dyn ObjectStore>,
    health: Arc<ObjectStoreHealth>,
    time_provider: Arc<dyn TimeProvider>,
}

/// Update the health handle based on a single operation's outcome. Shared
/// between per-call observation and per-stream-item observation so the
/// NotFound-as-success rule stays consistent across all surfaces.
fn record_outcome<T>(
    health: &ObjectStoreHealth,
    time_provider: &dyn TimeProvider,
    result: &Result<T>,
) {
    let now = time_provider.now();
    match result {
        // NotFound means the store responded successfully but the requested
        // path does not exist. For connectivity/credential readiness this
        // is a success: the network and auth paths both worked.
        Ok(_) | Err(object_store::Error::NotFound { .. }) => health.record_success(now),
        Err(e) => health.record_error(now, ErrorCategory::categorize(e)),
    }
}

impl ObservedObjectStore {
    pub fn new(
        inner: Arc<dyn ObjectStore>,
        health: Arc<ObjectStoreHealth>,
        time_provider: Arc<dyn TimeProvider>,
    ) -> Self {
        Self {
            inner,
            health,
            time_provider,
        }
    }

    fn observe<T>(&self, result: &Result<T>) {
        record_outcome(&self.health, self.time_provider.as_ref(), result);
    }
}

impl std::fmt::Display for ObservedObjectStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "ObservedObjectStore({})", self.inner)
    }
}

#[async_trait]
impl ObjectStore for ObservedObjectStore {
    async fn put(&self, location: &Path, payload: PutPayload) -> Result<PutResult> {
        let r = self.inner.put(location, payload).await;
        self.observe(&r);
        r
    }

    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> Result<PutResult> {
        let r = self.inner.put_opts(location, payload, opts).await;
        self.observe(&r);
        r
    }

    async fn put_multipart(&self, location: &Path) -> Result<Box<dyn MultipartUpload>> {
        let r = self.inner.put_multipart(location).await;
        self.observe(&r);
        r
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: PutMultipartOptions,
    ) -> Result<Box<dyn MultipartUpload>> {
        let r = self.inner.put_multipart_opts(location, opts).await;
        self.observe(&r);
        r
    }

    async fn get(&self, location: &Path) -> Result<GetResult> {
        let r = self.inner.get(location).await;
        self.observe(&r);
        r
    }

    async fn get_opts(&self, location: &Path, options: GetOptions) -> Result<GetResult> {
        let r = self.inner.get_opts(location, options).await;
        self.observe(&r);
        r
    }

    async fn get_range(&self, location: &Path, range: Range<u64>) -> Result<Bytes> {
        let r = self.inner.get_range(location, range).await;
        self.observe(&r);
        r
    }

    async fn get_ranges(&self, location: &Path, ranges: &[Range<u64>]) -> Result<Vec<Bytes>> {
        let r = self.inner.get_ranges(location, ranges).await;
        self.observe(&r);
        r
    }

    async fn head(&self, location: &Path) -> Result<ObjectMeta> {
        let r = self.inner.head(location).await;
        self.observe(&r);
        r
    }

    async fn delete(&self, location: &Path) -> Result<()> {
        let r = self.inner.delete(location).await;
        self.observe(&r);
        r
    }

    fn delete_stream<'a>(
        &'a self,
        locations: BoxStream<'a, Result<Path>>,
    ) -> BoxStream<'a, Result<Path>> {
        let health = Arc::clone(&self.health);
        let time_provider = Arc::clone(&self.time_provider);
        self.inner
            .delete_stream(locations)
            .inspect(move |r| record_outcome(&health, time_provider.as_ref(), r))
            .boxed()
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, Result<ObjectMeta>> {
        let health = Arc::clone(&self.health);
        let time_provider = Arc::clone(&self.time_provider);
        self.inner
            .list(prefix)
            .inspect(move |r| record_outcome(&health, time_provider.as_ref(), r))
            .boxed()
    }

    fn list_with_offset(
        &self,
        prefix: Option<&Path>,
        offset: &Path,
    ) -> BoxStream<'static, Result<ObjectMeta>> {
        let health = Arc::clone(&self.health);
        let time_provider = Arc::clone(&self.time_provider);
        self.inner
            .list_with_offset(prefix, offset)
            .inspect(move |r| record_outcome(&health, time_provider.as_ref(), r))
            .boxed()
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> Result<ListResult> {
        let r = self.inner.list_with_delimiter(prefix).await;
        self.observe(&r);
        r
    }

    async fn copy(&self, from: &Path, to: &Path) -> Result<()> {
        let r = self.inner.copy(from, to).await;
        self.observe(&r);
        r
    }

    async fn rename(&self, from: &Path, to: &Path) -> Result<()> {
        let r = self.inner.rename(from, to).await;
        self.observe(&r);
        r
    }

    async fn copy_if_not_exists(&self, from: &Path, to: &Path) -> Result<()> {
        let r = self.inner.copy_if_not_exists(from, to).await;
        self.observe(&r);
        r
    }

    async fn rename_if_not_exists(&self, from: &Path, to: &Path) -> Result<()> {
        let r = self.inner.rename_if_not_exists(from, to).await;
        self.observe(&r);
        r
    }
}

#[cfg(test)]
mod tests;
