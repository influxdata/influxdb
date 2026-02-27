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
        TestObjectStore::new(Arc::clone(&inner)).with_error_config(ErrorConfig::EveryNthFails(3)),
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
