use std::sync::{Arc, OnceLock};
use std::time::Duration;

use backon::{ExponentialBuilder, Retryable};
use futures::stream::{BoxStream, StreamExt};
use object_store::{
    Error as ObjectStoreError, GetResult, ObjectMeta, ObjectStore, PutPayload, PutResult, Result,
    path::Path,
};
use observability_deps::tracing::warn;

type RetryIfCond = Arc<dyn Fn(&ObjectStoreError) -> bool + Send + Sync>;

#[derive(Clone)]
pub struct RetryParams {
    pub max_retries: usize,
    pub min_delay: Duration,
    pub max_delay: Duration,
    pub factor: f32,
    pub with_jitter: bool,
    /// Optional predicate to determine if an error should be retried.
    /// If None, all errors are retried. If Some, only errors for which the predicate returns true are retried.
    pub when: Option<RetryIfCond>,
}

impl std::fmt::Debug for RetryParams {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RetryParams")
            .field("max_retries", &self.max_retries)
            .field("min_delay", &self.min_delay)
            .field("max_delay", &self.max_delay)
            .field("factor", &self.factor)
            .field("with_jitter", &self.with_jitter)
            .field("when", &self.when.as_ref().map(|_| "<predicate>"))
            .finish()
    }
}

impl Default for RetryParams {
    fn default() -> Self {
        Self {
            max_retries: 5,
            min_delay: Duration::from_millis(100),
            max_delay: Duration::from_secs(10),
            factor: 2.0,
            with_jitter: true,
            when: None,
        }
    }
}

impl RetryParams {
    fn exponential_builder(&self) -> ExponentialBuilder {
        let retry_builder = ExponentialBuilder::default()
            .with_factor(self.factor)
            .with_min_delay(self.min_delay)
            .with_max_delay(self.max_delay)
            .with_max_times(self.max_retries);

        if self.with_jitter {
            retry_builder.with_jitter()
        } else {
            retry_builder
        }
    }
}
static DEFAULT_PARAMS: OnceLock<RetryParams> = OnceLock::new();

/// Set the default retry parameters globally. This must be called before any retryable operations.
/// If not called, default values will be used.
pub fn set_default_retry_params(params: RetryParams) -> Result<()> {
    DEFAULT_PARAMS
        .set(params)
        .map_err(|_| object_store::Error::Generic {
            store: "object_store_utils",
            source: "Default retry params have already been set".into(),
        })
}

/// Get the current default retry parameters
fn get_default_retry_params() -> RetryParams {
    DEFAULT_PARAMS.get().cloned().unwrap_or_default()
}

/// Extension trait for ObjectStore that provides automatic retry capabilities with exponential backoff.
///
/// This trait adds retry variants for common ObjectStore operations, allowing for resilient
/// interactions with object storage systems that may experience transient failures.
///
/// The advantage of an extension trait over an `Arc<dyn ObjectStore>` here is that this lets us
/// use additional parameters on top of those supported by the `ObjectStore` trait to contextualize
/// error messages or adjust retry parameters on a per-call basis if desired.
#[async_trait::async_trait]
pub trait RetryableObjectStore: ObjectStore {
    async fn get_with_default_retries(
        &self,
        path: &Path,
        context_message: String,
    ) -> Result<GetResult> {
        self.get_with_retries(path, context_message, get_default_retry_params())
            .await
    }

    async fn get_with_retries(
        &self,
        path: &Path,
        context_message: String,
        retry_params: RetryParams,
    ) -> Result<GetResult> {
        let path_clone = path.clone();
        let store = self;

        let retry_builder = retry_params.exponential_builder();

        if let Some(when_fn) = retry_params.when {
            (|| async { store.get(&path_clone).await })
                .retry(&retry_builder)
                .notify(|err: &ObjectStoreError, dur: Duration| {
                    warn!(
                        "{}: Retrying object store get operation for path '{}' after error: {}. Retry after {}ms",
                        context_message,
                        path_clone,
                        err,
                        dur.as_millis()
                    );
                })
                .when(move |err| when_fn(err))
                .await
        } else {
            (|| async { store.get(&path_clone).await })
                .retry(&retry_builder)
                .notify(|err: &ObjectStoreError, dur: Duration| {
                    warn!(
                        "{}: Retrying object store get operation for path '{}' after error: {}. Retry after {}ms",
                        context_message,
                        path_clone,
                        err,
                        dur.as_millis()
                    );
                })
                .when(|err| !matches!(err, ObjectStoreError::NotFound { .. }))
                .await
        }
    }

    async fn put_with_default_retries(
        &self,
        path: &Path,
        payload: PutPayload,
        context_message: String,
    ) -> Result<PutResult> {
        self.put_with_retries(path, payload, context_message, get_default_retry_params())
            .await
    }

    async fn put_with_retries(
        &self,
        path: &Path,
        payload: PutPayload,
        context_message: String,
        retry_params: RetryParams,
    ) -> Result<PutResult> {
        let path_clone = path.clone();
        let store = self;

        let retry_builder = retry_params.exponential_builder();

        if let Some(when_fn) = retry_params.when {
            (|| async { store.put(&path_clone, payload.clone()).await })
                .retry(&retry_builder)
                .notify(|err: &ObjectStoreError, dur: Duration| {
                    warn!(
                        "{}: Retrying object store put operation for path '{}' after error: {}. Retry after {}ms",
                        context_message,
                        path_clone,
                        err,
                        dur.as_millis()
                    );
                })
                .when(move |err| when_fn(err))
                .await
        } else {
            (|| async { store.put(&path_clone, payload.clone()).await })
                .retry(&retry_builder)
                .notify(|err: &ObjectStoreError, dur: Duration| {
                    warn!(
                        "{}: Retrying object store put operation for path '{}' after error: {}. Retry after {}ms",
                        context_message,
                        path_clone,
                        err,
                        dur.as_millis()
                    );
                })
                .await
        }
    }

    async fn delete_with_default_retries(
        &self,
        path: &Path,
        context_message: String,
    ) -> Result<()> {
        self.delete_with_retries(path, context_message, get_default_retry_params())
            .await
    }

    async fn delete_with_retries(
        &self,
        path: &Path,
        context_message: String,
        retry_params: RetryParams,
    ) -> Result<()> {
        let path_clone = path.clone();
        let store = self;

        let retry_builder = retry_params.exponential_builder();

        // Use custom when predicate if provided, otherwise default to not retrying NotFound
        let result = if let Some(when_fn) = retry_params.when {
            (|| async { store.delete(&path_clone).await })
                .retry(&retry_builder)
                .notify(|err: &ObjectStoreError, dur: Duration| {
                    warn!(
                        "{}: Retrying object store delete operation for path '{}' after error: {}. Retry after {}ms",
                        context_message,
                        path_clone,
                        err,
                        dur.as_millis()
                    );
                })
                .when(move |err| when_fn(err))
                .await
        } else {
            // Default behavior: don't retry NotFound errors
            (|| async { store.delete(&path_clone).await })
                .retry(&retry_builder)
                .notify(|err: &ObjectStoreError, dur: Duration| {
                    // Only log if it's not a NotFound error
                    if !matches!(err, ObjectStoreError::NotFound { .. }) {
                        warn!(
                            "{}: Retrying object store delete operation for path '{}' after error: {}. Retry after {}ms",
                            context_message,
                            path_clone,
                            err,
                            dur.as_millis()
                        );
                    }
                })
                .when(|err| !matches!(err, ObjectStoreError::NotFound { .. }))
                .await
        };

        // Convert NotFound errors to success (idempotent delete)
        match result {
            Err(ObjectStoreError::NotFound { .. }) => Ok(()),
            other => other,
        }
    }

    fn list_with_default_retries(
        &self,
        prefix: Option<&Path>,
        offset: Option<&Path>,
        context_message: String,
    ) -> BoxStream<'static, Result<ObjectMeta>>
    where
        Self: Clone + Send + Sync + 'static,
    {
        self.list_with_retries(prefix, offset, context_message, get_default_retry_params())
    }

    fn list_with_retries(
        &self,
        prefix: Option<&Path>,
        offset: Option<&Path>,
        context_message: String,
        retry_params: RetryParams,
    ) -> BoxStream<'static, Result<ObjectMeta>>
    where
        Self: Clone + Send + Sync + 'static,
    {
        // Note: List operations return streaming results, so we can only retry the initial
        // stream creation, not failures that occur during iteration
        let prefix_clone = prefix.cloned();
        let offset_clone = offset.cloned();
        let prefix_str = prefix
            .map(|p| p.to_string())
            .unwrap_or_else(|| "<root>".to_string());

        // Clone self to move into async block
        let self_clone = self.clone();

        let fut = async move {
            let retry_builder = retry_params.exponential_builder();

            let result: Result<BoxStream<'static, Result<ObjectMeta>>> = (|| async {
                if let Some(offset) = &offset_clone {
                    Ok(self_clone.list_with_offset(prefix_clone.as_ref(), offset))
                } else {
                    Ok(self_clone.list(prefix_clone.as_ref()))
                }
            })
            .retry(&retry_builder)
            .notify(|err: &ObjectStoreError, dur: Duration| {
                warn!(
                    "{context_message}: Retrying object store list_with_offset operation for prefix '{prefix_str}' offset '{offset_clone:?}' after error: {err}. Retry after {}ms",
                    dur.as_millis()
                );
            })
            .await;

            match result {
                Ok(stream) => stream,
                Err(e) => futures::stream::once(async move { Err(e) }).boxed(),
            }
        };

        futures::stream::once(fut).flatten().boxed()
    }
}

// Special implementation for Arc<dyn ObjectStore> to handle dynamic dispatch
#[async_trait::async_trait]
impl RetryableObjectStore for Arc<dyn ObjectStore> {
    fn list_with_retries(
        &self,
        prefix: Option<&Path>,
        offset: Option<&Path>,
        context_message: String,
        retry_params: RetryParams,
    ) -> BoxStream<'static, Result<ObjectMeta>> {
        // Note: List operations return streaming results, so we can only retry the initial
        // stream creation, not failures that occur during iteration
        let prefix_clone = prefix.cloned();
        let offset_clone = offset.cloned();
        let prefix_str = prefix
            .map(|p| p.to_string())
            .unwrap_or_else(|| "<root>".to_string());

        // Clone Arc for use in async block
        let store = Arc::clone(self);

        let fut = async move {
            let retry_builder = retry_params.exponential_builder();

            let o = offset_clone.clone();
            let result: Result<BoxStream<'static, Result<ObjectMeta>>> = (move || {
                let s = Arc::clone(&store);
                let p = prefix_clone.clone();
                let o = o.clone();
                async move {
                    if let Some(offset) = &o {
                        Ok(s.list_with_offset(p.as_ref(), offset))
                    } else {
                        Ok(s.list(p.as_ref()))
                    }
                }
            })
            .retry(&retry_builder)
            .notify(|err: &ObjectStoreError, dur: Duration| {
                warn!(
                    "{context_message}: Retrying object store list_with_offset operation for prefix '{prefix_str}' offset '{offset_clone:?}' after error: {err}. Retry after {}ms",
                    dur.as_millis()
                );
            })
            .await;

            match result {
                Ok(stream) => stream,
                Err(e) => futures::stream::once(async move { Err(e) }).boxed(),
            }
        };

        futures::stream::once(fut).flatten().boxed()
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use futures::StreamExt;
    use object_store::memory::InMemory;

    use super::*;
    use crate::{ErrorConfig, TestObjectStore};

    // Helper function to create test retry params with very short delays
    fn test_retry_params(max_retries: usize) -> RetryParams {
        RetryParams {
            max_retries,
            min_delay: Duration::from_millis(1),
            max_delay: Duration::from_millis(10),
            factor: 2.0,
            with_jitter: false,
            when: None,
        }
    }

    #[tokio::test]
    async fn get_with_retries() {
        // Setup: Create TestObjectStore that fails first call
        let inner: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let path = Path::from("test.txt");
        let data = PutPayload::from("test data");

        // First, put some data using the inner store directly
        inner
            .put(&path, data.clone())
            .await
            .expect("setup: direct put to InMemory should succeed");

        // Wrap with TestObjectStore that fails first call
        let concrete_store = Arc::new(
            TestObjectStore::new(Arc::clone(&inner)).with_error_config(ErrorConfig::FirstCallFails),
        );
        let test_store: Arc<dyn ObjectStore> = Arc::clone(&concrete_store) as _;

        // Reset call count before test
        concrete_store.reset_call_count();

        // Test: Use RetryableObjectStore with retry params that allow 3 retries
        let result = test_store
            .get_with_retries(&path, "test context".to_string(), test_retry_params(3))
            .await;

        // The first call should fail, but retry should succeed
        assert!(result.is_ok(), "Should succeed after retry");

        // Verify exactly 2 calls were made (1 failure + 1 successful retry)
        assert_eq!(
            concrete_store.get_call_count(),
            2,
            "Should make exactly 2 calls (1 failure + 1 successful retry)"
        );
    }

    #[tokio::test]
    async fn put_with_retries() {
        // Setup: TestObjectStore that fails every 2nd call
        let inner: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let concrete_store = Arc::new(
            TestObjectStore::new(Arc::clone(&inner))
                .with_error_config(ErrorConfig::EveryNthFails(2)),
        );
        let test_store: Arc<dyn ObjectStore> = Arc::clone(&concrete_store) as _;

        let path = Path::from("test.txt");
        let data = PutPayload::from("test data");

        // Reset call count before test
        concrete_store.reset_call_count();

        // First put should succeed (1st call)
        let result1 = test_store
            .put_with_retries(
                &path,
                data.clone(),
                "test context".to_string(),
                test_retry_params(3),
            )
            .await;
        assert!(result1.is_ok(), "First put should succeed");
        assert_eq!(
            concrete_store.get_call_count(),
            1,
            "First put should make exactly 1 call"
        );

        // Second put should fail initially (2nd call) but succeed on retry (3rd call)
        let result2 = test_store
            .put_with_retries(
                &path,
                data.clone(),
                "test context".to_string(),
                test_retry_params(3),
            )
            .await;
        assert!(result2.is_ok(), "Second put should succeed after retry");
        assert_eq!(
            concrete_store.get_call_count(),
            3,
            "Total calls should be 3 (1 from first put + 2 from second put with retry)"
        );
    }

    #[tokio::test]
    async fn list_with_retries() {
        let inner: Arc<dyn ObjectStore> = Arc::new(InMemory::new());

        // Add some test data
        let path1 = Path::from("file1.txt");
        let path2 = Path::from("file2.txt");
        inner
            .put(&path1, PutPayload::from("data1"))
            .await
            .expect("setup: direct put to InMemory should succeed");
        inner
            .put(&path2, PutPayload::from("data2"))
            .await
            .expect("setup: direct put to InMemory should succeed");

        // Create TestObjectStore that will succeed (no error injection)
        // Note: list_with_retries only retries stream creation, not errors within the stream
        // So we test that list works when no errors are injected
        let test_store: Arc<dyn ObjectStore> = Arc::new(TestObjectStore::new(Arc::clone(&inner)));

        // Test: List should work normally
        let mut stream = test_store.list_with_retries(
            None,
            None,
            "test context".to_string(),
            test_retry_params(3),
        );

        // Collect results
        let mut results = Vec::new();
        while let Some(item) = stream.next().await {
            results.push(item);
        }

        // Should have successfully listed files
        assert_eq!(results.len(), 2, "Should list 2 files");
        assert!(
            results.iter().all(|r: &Result<ObjectMeta>| r.is_ok()),
            "All results should be Ok"
        );
    }

    #[tokio::test]
    async fn retry_exhaustion() {
        // Setup: Create TestObjectStore that always fails
        let inner: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let concrete_store = Arc::new(
            TestObjectStore::new(Arc::clone(&inner))
                .with_error_config(ErrorConfig::PercentageError(100.0)),
        );
        let test_store: Arc<dyn ObjectStore> = Arc::clone(&concrete_store) as _;

        let path = Path::from("test.txt");

        // Reset call count before test
        concrete_store.reset_call_count();

        // Test: Try to get with limited retries (max_retries = 2)
        let result = test_store
            .get_with_retries(&path, "test context".to_string(), test_retry_params(2))
            .await;

        // Should fail after exhausting retries
        assert!(result.is_err(), "Should fail after exhausting retries");

        // Verify exactly 3 calls were made (1 initial + 2 retries)
        assert_eq!(
            concrete_store.get_call_count(),
            3,
            "Should make exactly 3 calls (1 initial + 2 retries)"
        );
    }

    #[tokio::test]
    async fn no_retry_on_not_found() {
        // Setup: Create a TestObjectStore wrapped around empty InMemory store
        let inner: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let concrete_store = Arc::new(TestObjectStore::new(Arc::clone(&inner)));
        let test_store: Arc<dyn ObjectStore> = Arc::clone(&concrete_store) as _;

        let path = Path::from("nonexistent.txt");

        // Reset call count before test
        concrete_store.reset_call_count();

        // Test: Try to get a non-existent file
        let result = test_store
            .get_with_retries(&path, "test context".to_string(), test_retry_params(5))
            .await;

        // Should fail immediately without retries (NotFound is not retried by default)
        assert!(result.is_err(), "Should fail for non-existent file");

        // Verify only 1 call was made (NotFound errors are not retried by default)
        assert_eq!(
            concrete_store.get_call_count(),
            1,
            "Should only make 1 call for NotFound errors (no retries)"
        );

        // Verify it's a NotFound error
        if let Err(e) = result {
            assert!(
                matches!(e, ObjectStoreError::NotFound { .. }),
                "Should be NotFound error"
            );
        }
    }

    #[tokio::test]
    async fn delete_idempotent_on_not_found() {
        // Setup: Empty store
        let inner: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let test_store: Arc<dyn ObjectStore> = Arc::new(TestObjectStore::new(Arc::clone(&inner)));

        let path = Path::from("nonexistent.txt");

        // Test: Delete non-existent file should succeed (idempotent)
        let result = test_store
            .delete_with_retries(&path, "test context".to_string(), test_retry_params(3))
            .await;

        // Should succeed even though file doesn't exist
        assert!(result.is_ok(), "Delete should be idempotent");
    }

    #[tokio::test]
    async fn custom_retry_condition() {
        // Setup: TestObjectStore that always returns generic errors
        let inner: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let concrete_store = Arc::new(
            TestObjectStore::new(Arc::clone(&inner))
                .with_error_config(ErrorConfig::PercentageError(100.0)),
        );
        let test_store: Arc<dyn ObjectStore> = Arc::clone(&concrete_store) as _;

        let path = Path::from("test.txt");

        // Create custom retry params that never retry (always returns false)
        let mut retry_params = test_retry_params(5);
        retry_params.when = Some(Arc::new(|_err| false));

        // Reset call count before test
        concrete_store.reset_call_count();

        // Test: Should fail immediately without retries
        let result = test_store
            .get_with_retries(&path, "test context".to_string(), retry_params)
            .await;

        assert!(
            result.is_err(),
            "Should fail when retry condition returns false"
        );

        assert_eq!(
            concrete_store.get_call_count(),
            1,
            "Should only make 1 call when retry condition returns false"
        );
    }

    #[tokio::test]
    async fn default_retry_params() {
        // Setup: Set custom default params
        let _custom_params = RetryParams {
            max_retries: 1,
            min_delay: Duration::from_millis(1),
            max_delay: Duration::from_millis(5),
            factor: 2.0,
            with_jitter: false,
            when: None,
        };

        // Note: set_default_retry_params can only be called once per process,
        // so we'll just test the get_default_retry_params function
        let default_params = get_default_retry_params();

        // Verify defaults are reasonable
        assert!(
            default_params.max_retries > 0,
            "Should have some retries by default"
        );
        assert!(
            default_params.min_delay > Duration::ZERO,
            "Should have non-zero min delay"
        );
        assert!(
            default_params.max_delay > default_params.min_delay,
            "Max delay should be greater than min"
        );
    }

    #[tokio::test]
    async fn fail_next_with_retries() {
        // Setup: Normal TestObjectStore
        let inner: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let path = Path::from("test.txt");
        inner
            .put(&path, PutPayload::from("data"))
            .await
            .expect("setup: direct put to InMemory should succeed");

        let concrete_store = Arc::new(TestObjectStore::new(Arc::clone(&inner)));

        // Reset call count before test
        concrete_store.reset_call_count();

        // Set to fail only the next call
        concrete_store.fail_next_call();

        // Cast to trait object for using with RetryableObjectStore
        let test_store: Arc<dyn ObjectStore> = Arc::clone(&concrete_store) as _;

        // Test: Should fail once then succeed on retry
        let result = test_store
            .get_with_retries(&path, "test context".to_string(), test_retry_params(3))
            .await;

        assert!(
            result.is_ok(),
            "Should succeed after single failure and retry"
        );

        // Verify exactly 2 calls were made (1 failure + 1 successful retry)
        assert_eq!(
            concrete_store.get_call_count(),
            2,
            "Should make exactly 2 calls for fail_next (1 failure + 1 retry)"
        );

        // Subsequent call should work without retry
        let result2 = test_store
            .get_with_retries(&path, "test context".to_string(), test_retry_params(0))
            .await;
        assert!(
            result2.is_ok(),
            "Subsequent calls should succeed without retry"
        );

        // Verify only 1 additional call was made (no failure)
        assert_eq!(
            concrete_store.get_call_count(),
            3,
            "Total should be 3 calls (2 from first get + 1 from second get)"
        );
    }
}
