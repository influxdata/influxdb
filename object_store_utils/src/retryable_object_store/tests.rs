use std::time::Duration;

use futures::StreamExt;
use object_store::{PutMode, UpdateVersion, memory::InMemory};

use super::*;
use crate::{ErrorConfig, OperationKind, TestObjectStore};

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

// Retries get_opts through a transient failure and still returns the object.
#[tokio::test]
async fn get_opts_with_retries_retries_transient_errors() {
    let inner: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let path = Path::from("opts.txt");
    let data = PutPayload::from("test data");

    inner
        .put(&path, data.clone())
        .await
        .expect("setup: direct put to InMemory should succeed");

    let concrete_store = Arc::new(
        TestObjectStore::new(Arc::clone(&inner))
            .with_error_config(ErrorConfig::FirstCallFails)
            .with_failure_predicate(|ctx| ctx.kind == OperationKind::GetOpts),
    );
    let test_store: Arc<dyn ObjectStore> = Arc::clone(&concrete_store) as _;

    concrete_store.reset_call_count();

    let result = test_store
        .get_opts_with_retries(
            &path,
            GetOptions::default(),
            "test context".to_string(),
            test_retry_params(3),
        )
        .await;

    assert!(result.is_ok(), "Should succeed after retry");
    assert_eq!(
        concrete_store.get_call_count(),
        2,
        "Should make exactly 2 calls (1 failure + 1 successful retry)"
    );
}

// Confirms get_opts stops immediately on Precondition errors (no retry loop).
#[tokio::test]
async fn get_opts_precondition_not_retried() {
    let inner: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let path = Path::from("opts_precondition.txt");
    inner
        .put(&path, PutPayload::from("data"))
        .await
        .expect("setup: direct put to InMemory should succeed");

    let concrete_store = Arc::new(TestObjectStore::new(Arc::clone(&inner)));
    let test_store: Arc<dyn ObjectStore> = Arc::clone(&concrete_store) as _;

    concrete_store.reset_call_count();

    let options = GetOptions {
        if_match: Some("mismatch".to_string()),
        ..GetOptions::default()
    };

    let result = test_store
        .get_opts_with_retries(
            &path,
            options,
            "precondition get_opts".to_string(),
            test_retry_params(4),
        )
        .await;

    assert!(matches!(result, Err(ObjectStoreError::Precondition { .. })));
    assert_eq!(
        concrete_store.get_call_count(),
        1,
        "Precondition failures should not be retried"
    );
}

#[tokio::test]
async fn put_with_retries() {
    // Setup: TestObjectStore that fails every 2nd call
    let inner: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let concrete_store = Arc::new(
        TestObjectStore::new(Arc::clone(&inner)).with_error_config(ErrorConfig::EveryNthFails(2)),
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

// Retries put_opts through a transient failure and completes the write.
#[tokio::test]
async fn put_opts_with_retries_retries_transient_errors() {
    let inner: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let concrete_store = Arc::new(
        TestObjectStore::new(Arc::clone(&inner))
            .with_error_config(ErrorConfig::FirstCallFails)
            .with_failure_predicate(|ctx| ctx.kind == OperationKind::PutOpts),
    );
    let test_store: Arc<dyn ObjectStore> = Arc::clone(&concrete_store) as _;

    let path = Path::from("put_opts.txt");

    concrete_store.reset_call_count();

    let result = test_store
        .put_opts_with_retries(
            &path,
            PutPayload::from("payload"),
            PutOptions::default(),
            "test context".to_string(),
            test_retry_params(3),
        )
        .await;

    assert!(result.is_ok(), "Should succeed after retry");
    assert_eq!(
        concrete_store.get_call_count(),
        2,
        "Should make exactly 2 calls (1 failure + 1 successful retry)"
    );
}

// Ensures put_opts surfaces Precondition errors without retrying.
#[tokio::test]
async fn put_opts_precondition_not_retried() {
    let inner: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let path = Path::from("put_opts_precondition.txt");
    inner
        .put(&path, PutPayload::from("initial"))
        .await
        .expect("setup: direct put to InMemory should succeed");

    let concrete_store = Arc::new(TestObjectStore::new(Arc::clone(&inner)));
    let test_store: Arc<dyn ObjectStore> = Arc::clone(&concrete_store) as _;

    concrete_store.reset_call_count();

    let options = PutOptions {
        mode: PutMode::Update(UpdateVersion {
            e_tag: Some("bogus".to_string()),
            version: None,
        }),
        ..Default::default()
    };

    let result = test_store
        .put_opts_with_retries(
            &path,
            PutPayload::from("next"),
            options,
            "precondition put_opts".to_string(),
            test_retry_params(4),
        )
        .await;

    assert!(matches!(result, Err(ObjectStoreError::Precondition { .. })));
    assert_eq!(
        concrete_store.get_call_count(),
        1,
        "Precondition failures should not be retried"
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

    // Create TestObjectStore that fails first call
    let concrete_store = Arc::new(
        TestObjectStore::new(Arc::clone(&inner)).with_error_config(ErrorConfig::FirstCallFails),
    );
    let test_store: Arc<dyn ObjectStore> = Arc::clone(&concrete_store) as _;

    // Reset call count before test
    concrete_store.reset_call_count();

    // Test: List should fail first, then succeed on retry
    let mut stream =
        test_store.list_with_retries(None, None, "test context".to_string(), test_retry_params(3));

    // Collect results
    let mut results = Vec::new();
    while let Some(item) = stream.next().await {
        results.push(item);
    }

    // Should have successfully listed files after retry
    assert_eq!(results.len(), 2, "Should list 2 files after retry");
    assert!(
        results.iter().all(|r: &Result<ObjectMeta>| r.is_ok()),
        "All results should be Ok after retry"
    );

    // Verify that multiple list calls were made (1 failure + 1 successful retry)
    assert_eq!(
        concrete_store.get_call_count(),
        2,
        "Should make exactly 2 list calls (1 failure + 1 successful retry)"
    );
}

// Verifies list_with_delimiter retries a transient failure and still returns prefixes.
#[tokio::test]
async fn list_with_delimiter_with_retries_retries_transient_errors() {
    let inner: Arc<dyn ObjectStore> = Arc::new(InMemory::new());

    let path1 = Path::from("imports/job1/file1.txt");
    let path2 = Path::from("imports/job2/file2.txt");
    inner
        .put(&path1, PutPayload::from("data1"))
        .await
        .expect("setup: direct put to InMemory should succeed");
    inner
        .put(&path2, PutPayload::from("data2"))
        .await
        .expect("setup: direct put to InMemory should succeed");

    let concrete_store = Arc::new(
        TestObjectStore::new(Arc::clone(&inner))
            .with_error_config(ErrorConfig::FirstCallFails)
            .with_failure_predicate(|ctx| ctx.kind == OperationKind::ListWithDelimiter),
    );
    let test_store: Arc<dyn ObjectStore> = Arc::clone(&concrete_store) as _;
    concrete_store.reset_call_count();

    let prefix = Path::from("imports");
    let result = test_store
        .list_with_delimiter_with_retries(
            Some(&prefix),
            "test context".to_string(),
            test_retry_params(3),
        )
        .await
        .expect("List should succeed after retry");

    assert_eq!(
        concrete_store.get_call_count(),
        2,
        "Should make exactly 2 calls (1 failure + 1 successful retry)"
    );
    assert_eq!(result.common_prefixes.len(), 2);
    assert!(result.objects.is_empty(), "No direct children at prefix");
}

#[tokio::test]
async fn list_with_retries_exhaustion() {
    let inner: Arc<dyn ObjectStore> = Arc::new(InMemory::new());

    // Add test data
    let path1 = Path::from("file1.txt");
    inner
        .put(&path1, PutPayload::from("data1"))
        .await
        .expect("setup: direct put to InMemory should succeed");

    // Create TestObjectStore that always fails
    let concrete_store = Arc::new(
        TestObjectStore::new(Arc::clone(&inner))
            .with_error_config(ErrorConfig::PercentageError(100.0)),
    );
    let test_store: Arc<dyn ObjectStore> = Arc::clone(&concrete_store) as _;

    // Reset call count before test
    concrete_store.reset_call_count();

    // Test: List should fail after exhausting retries
    let mut stream =
        test_store.list_with_retries(None, None, "test context".to_string(), test_retry_params(4));

    // Collect results - should get error
    let mut has_error = false;
    while let Some(item) = stream.next().await {
        if item.is_err() {
            has_error = true;
        }
    }

    // Should have encountered an error after exhausting retries
    assert!(has_error, "Should fail after exhausting retries");

    // Verify that exactly 5 calls were made -- the initial first attempt and 4 retries
    assert_eq!(
        concrete_store.get_call_count(),
        5,
        "Should make at least 5 list calls, got {}",
        concrete_store.get_call_count()
    );
}

#[tokio::test]
async fn list_with_retries_every_nth_fails() {
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

    // Create TestObjectStore that fails every 2nd call
    let concrete_store = Arc::new(
        TestObjectStore::new(Arc::clone(&inner)).with_error_config(ErrorConfig::EveryNthFails(2)),
    );
    let test_store: Arc<dyn ObjectStore> = Arc::clone(&concrete_store) as _;

    // Reset call count before test
    concrete_store.reset_call_count();

    // First list should succeed (1st call)
    let mut stream1 =
        test_store.list_with_retries(None, None, "test context".to_string(), test_retry_params(3));

    let mut results1 = Vec::new();
    while let Some(item) = stream1.next().await {
        results1.push(item);
    }

    assert_eq!(results1.len(), 2, "First list should succeed");
    assert_eq!(
        concrete_store.get_call_count(),
        1,
        "First list should make 1 call"
    );

    // Second list should fail initially (2nd call) but succeed on retry (3rd call)
    let mut stream2 =
        test_store.list_with_retries(None, None, "test context".to_string(), test_retry_params(3));

    let mut results2 = Vec::new();
    while let Some(item) = stream2.next().await {
        results2.push(item);
    }

    assert_eq!(results2.len(), 2, "Second list should succeed after retry");
    assert_eq!(
        concrete_store.get_call_count(),
        3,
        "Total should be 3 calls (1 from first list + 2 from second list with retry)"
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
async fn raw_delete_returns_ok_for_existing_file() {
    let inner: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let path = Path::from("existing.txt");
    inner
        .put(&path, PutPayload::from("data"))
        .await
        .expect("setup: put should succeed");

    let test_store: Arc<dyn ObjectStore> = Arc::new(TestObjectStore::new(Arc::clone(&inner)));

    let result = test_store
        .raw_delete_with_retries(&path, "test context".to_string(), test_retry_params(3))
        .await;

    assert!(result.is_ok(), "delete should succeed for existing file");

    let get_result = inner.get(&path).await;
    assert!(
        matches!(get_result, Err(ObjectStoreError::NotFound { .. })),
        "File should be deleted from store"
    );
}

#[tokio::test]
async fn raw_delete_preserves_not_found() {
    let inner: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let concrete_store = Arc::new(
        TestObjectStore::new(Arc::clone(&inner))
            .with_error_config(ErrorConfig::FirstCallFails)
            .with_error_type(crate::ErrorType::NotFound)
            .with_failure_predicate(|ctx| ctx.kind == OperationKind::Delete),
    );
    let test_store: Arc<dyn ObjectStore> = Arc::clone(&concrete_store) as _;

    let path = Path::from("test.txt");

    let result = test_store
        .raw_delete_with_retries(&path, "test context".to_string(), test_retry_params(3))
        .await;

    assert!(
        matches!(result, Err(ObjectStoreError::NotFound { .. })),
        "raw_delete should preserve NotFound error"
    );
}

#[tokio::test]
async fn delete_idempotent_on_not_found() {
    let inner: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let concrete_store = Arc::new(
        TestObjectStore::new(Arc::clone(&inner))
            .with_error_config(ErrorConfig::FirstCallFails)
            .with_error_type(crate::ErrorType::NotFound)
            .with_failure_predicate(|ctx| ctx.kind == OperationKind::Delete),
    );
    let test_store: Arc<dyn ObjectStore> = Arc::clone(&concrete_store) as _;

    let path = Path::from("test.txt");

    let result = test_store
        .delete_with_retries(&path, "test context".to_string(), test_retry_params(3))
        .await;

    assert!(
        result.is_ok(),
        "delete_with_retries should convert NotFound to Ok"
    );
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
