//! Wrapper for metrics.
use std::{
    collections::HashMap,
    fmt::Display,
    ops::Range,
    sync::{Arc, Mutex},
};

use async_trait::async_trait;
use bytes::Bytes;
use futures::stream::BoxStream;
use metric::{Registry, U64Gauge};
use object_store::{
    path::Path, DynObjectStore, Error, GetResult, ListResult, MultipartId, ObjectMeta, ObjectStore,
    Result,
};
use tokio::io::AsyncWrite;

/// Measures how much data is in the given object store.
///
/// This assumes that the store is empty at the beginning. It is also only race-free when objects are never re-created.
#[derive(Debug)]
pub struct MetricsStore {
    inner: Arc<DynObjectStore>,
    gauge_bytes: U64Gauge,
    known: Mutex<HashMap<Path, usize>>,
}

impl MetricsStore {
    /// Create new metrics wrapper.
    pub fn new(inner: Arc<DynObjectStore>, registry: &Registry, name: &'static str) -> Self {
        Self {
            inner,
            gauge_bytes: registry
                .register_metric::<U64Gauge>(
                    "iox_compactor_store_bytes",
                    "Number of bytes in the given store",
                )
                .recorder(&[("name", name)]),
            known: Default::default(),
        }
    }
}

impl Display for MetricsStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "metrics({})", self.inner)
    }
}

#[async_trait]
impl ObjectStore for MetricsStore {
    async fn put(&self, location: &Path, bytes: Bytes) -> Result<()> {
        let size = bytes.len();

        let res = self.inner.put(location, bytes).await;

        if res.is_ok() {
            let mut guard = self.known.lock().expect("not poisoned");
            self.gauge_bytes.inc(size as u64);
            if let Some(old) = guard.insert(location.clone(), size) {
                self.gauge_bytes.dec(old as u64);
            }
        }

        res
    }

    async fn put_multipart(
        &self,
        _location: &Path,
    ) -> Result<(MultipartId, Box<dyn AsyncWrite + Unpin + Send>)> {
        Err(Error::NotImplemented)
    }

    async fn abort_multipart(&self, _location: &Path, _multipart_id: &MultipartId) -> Result<()> {
        Err(Error::NotImplemented)
    }

    async fn get(&self, location: &Path) -> Result<GetResult> {
        self.inner.get(location).await
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

    async fn delete(&self, location: &Path) -> Result<()> {
        let res = self.inner.delete(location).await;

        if res.is_ok() {
            let mut guard = self.known.lock().expect("not poisoned");
            if let Some(old) = guard.remove(location) {
                self.gauge_bytes.dec(old as u64);
            }
        }

        res
    }

    async fn list(&self, prefix: Option<&Path>) -> Result<BoxStream<'_, Result<ObjectMeta>>> {
        self.inner.list(prefix).await
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> Result<ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy(&self, _from: &Path, _to: &Path) -> Result<()> {
        Err(Error::NotImplemented)
    }

    async fn rename(&self, _from: &Path, _to: &Path) -> Result<()> {
        Err(Error::NotImplemented)
    }

    async fn copy_if_not_exists(&self, _from: &Path, _to: &Path) -> Result<()> {
        Err(Error::NotImplemented)
    }

    async fn rename_if_not_exists(&self, _from: &Path, _to: &Path) -> Result<()> {
        Err(Error::NotImplemented)
    }
}
