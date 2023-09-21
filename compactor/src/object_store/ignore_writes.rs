//! Wrapper that ignores writes.
use std::{fmt::Display, ops::Range, sync::Arc};

use async_trait::async_trait;
use bytes::Bytes;
use futures::stream::BoxStream;
use object_store::{
    path::Path, DynObjectStore, GetOptions, GetResult, ListResult, MultipartId, ObjectMeta,
    ObjectStore, Result,
};
use tokio::io::{sink, AsyncWrite};
use uuid::Uuid;

/// Store that pipes all writes to `/dev/null` but reads from an actual store.
#[derive(Debug)]
pub struct IgnoreWrites {
    inner: Arc<DynObjectStore>,
}

impl IgnoreWrites {
    /// Creates new store that reads from the given inner store.
    pub fn new(inner: Arc<DynObjectStore>) -> Self {
        Self { inner }
    }
}

impl Display for IgnoreWrites {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "ignore_writes({})", self.inner)
    }
}

#[async_trait]
impl ObjectStore for IgnoreWrites {
    async fn put(&self, _location: &Path, _bytes: Bytes) -> Result<()> {
        Ok(())
    }

    async fn put_multipart(
        &self,
        _location: &Path,
    ) -> Result<(MultipartId, Box<dyn AsyncWrite + Unpin + Send>)> {
        Ok((Uuid::new_v4().to_string(), Box::new(sink())))
    }

    async fn abort_multipart(&self, _location: &Path, _multipart_id: &MultipartId) -> Result<()> {
        Ok(())
    }

    async fn get_opts(&self, location: &Path, options: GetOptions) -> Result<GetResult> {
        self.inner.get_opts(location, options).await
    }

    async fn get_range(&self, location: &Path, range: Range<usize>) -> Result<Bytes> {
        self.inner.get_range(location, range).await
    }

    async fn get_ranges(&self, location: &Path, ranges: &[Range<usize>]) -> Result<Vec<Bytes>> {
        self.inner.get_ranges(location, ranges).await
    }

    async fn head(&self, location: &Path) -> Result<ObjectMeta> {
        self.inner.head(location).await
    }

    async fn delete(&self, _location: &Path) -> Result<()> {
        Ok(())
    }

    async fn list(&self, prefix: Option<&Path>) -> Result<BoxStream<'_, Result<ObjectMeta>>> {
        self.inner.list(prefix).await
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> Result<ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy(&self, _from: &Path, _to: &Path) -> Result<()> {
        Ok(())
    }

    async fn rename(&self, _from: &Path, _to: &Path) -> Result<()> {
        Ok(())
    }

    async fn copy_if_not_exists(&self, _from: &Path, _to: &Path) -> Result<()> {
        Ok(())
    }

    async fn rename_if_not_exists(&self, _from: &Path, _to: &Path) -> Result<()> {
        Ok(())
    }
}
