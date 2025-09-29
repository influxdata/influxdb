use std::fmt::Display;
use std::ops::Range;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

use async_trait::async_trait;
use bytes::Bytes;
use futures::stream::BoxStream;
use object_store::{
    Error as ObjectStoreError, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta,
    ObjectStore, PutMultipartOptions, PutOptions, PutPayload, PutResult, Result, path::Path,
};

/// Configuration for error injection behavior
#[derive(Debug, Clone, Copy)]
pub enum ErrorConfig {
    /// Inject errors with a given percentage chance (0.0 to 100.0)
    PercentageError(f32),
    /// First call always fails, subsequent calls succeed
    FirstCallFails,
    /// Every Nth call fails
    EveryNthFails(usize),
    /// Only the next call fails, then normal operation resumes
    NextCallFails,
    /// No error injection (default)
    NoErrors,
}

/// A test wrapper for ObjectStore that can inject errors
pub struct TestObjectStore {
    inner: Arc<dyn ObjectStore>,
    error_config: ErrorConfig,
    call_count: AtomicUsize,
    fail_next: AtomicBool,
    /// Tracks total number of operations for test assertions
    operation_count: AtomicUsize,
}

impl TestObjectStore {
    /// Create a new TestObjectStore wrapping the given store
    pub fn new(inner: Arc<dyn ObjectStore>) -> Self {
        Self {
            inner,
            error_config: ErrorConfig::NoErrors,
            call_count: AtomicUsize::new(0),
            fail_next: AtomicBool::new(false),
            operation_count: AtomicUsize::new(0),
        }
    }

    /// Configure the error injection behavior
    pub fn with_error_config(mut self, config: ErrorConfig) -> Self {
        if matches!(config, ErrorConfig::NextCallFails) {
            self.fail_next.store(true, Ordering::SeqCst);
        }
        self.error_config = config;
        self
    }

    /// Make only the next call fail
    pub fn fail_next_call(&self) {
        self.fail_next.store(true, Ordering::SeqCst);
    }

    /// Get the total number of operations performed
    pub fn get_call_count(&self) -> usize {
        self.operation_count.load(Ordering::SeqCst)
    }

    /// Reset the operation counter
    pub fn reset_call_count(&self) {
        self.operation_count.store(0, Ordering::SeqCst);
    }

    /// Check if we should inject an error for this call
    fn should_inject_error(&self) -> bool {
        self.operation_count.fetch_add(1, Ordering::SeqCst);

        if self.fail_next.swap(false, Ordering::SeqCst) {
            return true;
        }

        let call_number = self.call_count.fetch_add(1, Ordering::SeqCst);

        match &self.error_config {
            ErrorConfig::NoErrors => false,
            ErrorConfig::FirstCallFails => call_number == 0,
            ErrorConfig::EveryNthFails(n) if *n > 0 => (call_number + 1).is_multiple_of(*n),
            ErrorConfig::PercentageError(chance) => {
                // Simple deterministic approach based on call count
                // In a real implementation, you might want to use rand
                let threshold = (*chance / 100.0 * 100.0) as usize;
                (call_number % 100) < threshold
            }
            ErrorConfig::NextCallFails => false,
            _ => false,
        }
    }

    /// Create an injected error
    fn create_error(&self) -> ObjectStoreError {
        ObjectStoreError::Generic {
            store: "test",
            source: "Injected test error".into(),
        }
    }
}

impl Display for TestObjectStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "TestObjectStore({})", self.inner)
    }
}

impl std::fmt::Debug for TestObjectStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TestObjectStore")
            .field("inner", &format!("{}", self.inner))
            .field("error_config", &self.error_config)
            .field("call_count", &self.call_count.load(Ordering::SeqCst))
            .finish()
    }
}

#[async_trait]
impl ObjectStore for TestObjectStore {
    async fn put(&self, location: &Path, payload: PutPayload) -> Result<PutResult> {
        if self.should_inject_error() {
            return Err(self.create_error());
        }
        self.inner.put(location, payload).await
    }

    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> Result<PutResult> {
        if self.should_inject_error() {
            return Err(self.create_error());
        }
        self.inner.put_opts(location, payload, opts).await
    }

    async fn put_multipart(&self, location: &Path) -> Result<Box<dyn MultipartUpload>> {
        if self.should_inject_error() {
            return Err(self.create_error());
        }
        self.inner.put_multipart(location).await
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: PutMultipartOptions,
    ) -> Result<Box<dyn MultipartUpload>> {
        if self.should_inject_error() {
            return Err(self.create_error());
        }
        self.inner.put_multipart_opts(location, opts).await
    }

    async fn get(&self, location: &Path) -> Result<GetResult> {
        if self.should_inject_error() {
            return Err(self.create_error());
        }
        self.inner.get(location).await
    }

    async fn get_opts(&self, location: &Path, options: GetOptions) -> Result<GetResult> {
        if self.should_inject_error() {
            return Err(self.create_error());
        }
        self.inner.get_opts(location, options).await
    }

    async fn get_range(&self, location: &Path, range: Range<u64>) -> Result<Bytes> {
        if self.should_inject_error() {
            return Err(self.create_error());
        }
        self.inner.get_range(location, range).await
    }

    async fn get_ranges(&self, location: &Path, ranges: &[Range<u64>]) -> Result<Vec<Bytes>> {
        if self.should_inject_error() {
            return Err(self.create_error());
        }
        self.inner.get_ranges(location, ranges).await
    }

    async fn head(&self, location: &Path) -> Result<ObjectMeta> {
        if self.should_inject_error() {
            return Err(self.create_error());
        }
        self.inner.head(location).await
    }

    async fn delete(&self, location: &Path) -> Result<()> {
        if self.should_inject_error() {
            return Err(self.create_error());
        }
        self.inner.delete(location).await
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, Result<ObjectMeta>> {
        if self.should_inject_error() {
            let error = self.create_error();
            return Box::pin(futures::stream::once(async move { Err(error) }));
        }
        self.inner.list(prefix)
    }

    fn list_with_offset(
        &self,
        prefix: Option<&Path>,
        offset: &Path,
    ) -> BoxStream<'static, Result<ObjectMeta>> {
        if self.should_inject_error() {
            let error = self.create_error();
            return Box::pin(futures::stream::once(async move { Err(error) }));
        }
        self.inner.list_with_offset(prefix, offset)
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> Result<ListResult> {
        if self.should_inject_error() {
            return Err(self.create_error());
        }
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy(&self, from: &Path, to: &Path) -> Result<()> {
        if self.should_inject_error() {
            return Err(self.create_error());
        }
        self.inner.copy(from, to).await
    }

    async fn rename(&self, from: &Path, to: &Path) -> Result<()> {
        if self.should_inject_error() {
            return Err(self.create_error());
        }
        self.inner.rename(from, to).await
    }

    async fn copy_if_not_exists(&self, from: &Path, to: &Path) -> Result<()> {
        if self.should_inject_error() {
            return Err(self.create_error());
        }
        self.inner.copy_if_not_exists(from, to).await
    }

    async fn rename_if_not_exists(&self, from: &Path, to: &Path) -> Result<()> {
        if self.should_inject_error() {
            return Err(self.create_error());
        }
        self.inner.rename_if_not_exists(from, to).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use object_store::memory::InMemory;

    #[tokio::test]
    async fn test_no_errors() {
        let inner: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let test_store = Arc::new(TestObjectStore::new(Arc::clone(&inner)));

        let path = Path::from("test.txt");
        let data = PutPayload::from("test data");

        test_store
            .put(&path, data)
            .await
            .expect("put should succeed with no errors configured");
        test_store
            .get(&path)
            .await
            .expect("get should succeed after successful put");
    }

    #[tokio::test]
    async fn test_first_call_fails() {
        let inner: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let test_store = Arc::new(
            TestObjectStore::new(Arc::clone(&inner)).with_error_config(ErrorConfig::FirstCallFails),
        );

        let path = Path::from("test.txt");
        let data = PutPayload::from("test data");

        assert!(test_store.put(&path, data.clone()).await.is_err());
        test_store
            .put(&path, data)
            .await
            .expect("second put should succeed after first failure");
    }

    #[tokio::test]
    async fn test_every_nth_fails() {
        let inner: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let test_store = Arc::new(
            TestObjectStore::new(Arc::clone(&inner))
                .with_error_config(ErrorConfig::EveryNthFails(3)),
        );

        let path = Path::from("test.txt");
        let data = PutPayload::from("test data");

        test_store
            .put(&path, data.clone())
            .await
            .expect("1st call should succeed (EveryNthFails(3))");
        test_store
            .get(&path)
            .await
            .expect("2nd call should succeed (EveryNthFails(3))");
        assert!(test_store.head(&path).await.is_err());
        test_store
            .get(&path)
            .await
            .expect("4th call should succeed (EveryNthFails(3))");
    }

    #[tokio::test]
    async fn test_fail_next_call() {
        let inner: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let test_store = Arc::new(TestObjectStore::new(Arc::clone(&inner)));

        let path = Path::from("test.txt");
        let data = PutPayload::from("test data");

        test_store
            .put(&path, data.clone())
            .await
            .expect("initial put should succeed");

        test_store.fail_next_call();
        assert!(test_store.get(&path).await.is_err());

        test_store
            .get(&path)
            .await
            .expect("get should succeed after fail_next was consumed");
    }
}
