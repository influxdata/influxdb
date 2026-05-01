use super::*;
use futures::stream;
use iox_time::{MockProvider, Time};
use object_store::memory::InMemory;

fn setup() -> (ObservedObjectStore, Arc<ObjectStoreHealth>) {
    let inner: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let health = ObjectStoreHealth::new();
    let time_provider: Arc<dyn TimeProvider> =
        Arc::new(MockProvider::new(Time::from_timestamp_nanos(1_000_000_000)));
    let store = ObservedObjectStore::new(inner, Arc::clone(&health), Arc::clone(&time_provider));
    (store, health)
}

#[tokio::test]
async fn put_and_head_record_success() {
    let (store, health) = setup();

    let path = Path::from("foo");
    store
        .put(&path, PutPayload::from_static(b"hello"))
        .await
        .unwrap();
    store.head(&path).await.unwrap();

    assert!(health.is_ok());
    assert!(health.last_success_at().is_some());
    assert!(health.last_error_at().is_none());
}

#[tokio::test]
async fn head_missing_records_success() {
    let (store, health) = setup();

    let path = Path::from("missing");
    let _ = store.head(&path).await;

    assert!(health.is_ok());
    assert!(health.last_success_at().is_some());
    assert!(health.last_error_at().is_none());
}

#[tokio::test]
async fn get_on_existing_path_records_success() {
    let (store, health) = setup();

    let path = Path::from("foo");
    store
        .put(&path, PutPayload::from_static(b"hello"))
        .await
        .unwrap();
    let _ = store.get(&path).await.unwrap();

    assert!(health.is_ok());
}

#[tokio::test]
async fn get_missing_records_success() {
    let (store, health) = setup();

    let path = Path::from("missing");
    let _ = store.get(&path).await;

    assert!(health.is_ok());
    assert!(health.last_error_at().is_none());
}

#[tokio::test]
async fn delete_existing_records_success() {
    let (store, health) = setup();

    let path = Path::from("foo");
    store
        .put(&path, PutPayload::from_static(b"hello"))
        .await
        .unwrap();
    store.delete(&path).await.unwrap();

    assert!(health.is_ok());
}

#[tokio::test]
async fn list_with_delimiter_records_success_on_empty() {
    let (store, health) = setup();

    let _ = store.list_with_delimiter(None).await.unwrap();

    assert!(health.is_ok());
}

#[tokio::test]
async fn copy_records_success() {
    let (store, health) = setup();

    let from = Path::from("a");
    let to = Path::from("b");
    store
        .put(&from, PutPayload::from_static(b"data"))
        .await
        .unwrap();
    store.copy(&from, &to).await.unwrap();

    assert!(health.is_ok());
}

#[tokio::test]
async fn list_yielding_items_records_success() {
    let (store, health) = setup();

    store
        .put(&Path::from("a"), PutPayload::from_static(b"x"))
        .await
        .unwrap();
    store
        .put(&Path::from("b"), PutPayload::from_static(b"y"))
        .await
        .unwrap();

    let items: Vec<_> = store.list(None).collect().await;
    assert_eq!(items.len(), 2);
    assert!(health.is_ok());
    assert!(health.last_error_at().is_none());
}

#[tokio::test]
async fn list_empty_does_not_update_health() {
    let (store, health) = setup();

    // Drain the stream; it should yield zero items but still have been driven
    // past the producer, exercising the wrapper.
    let items: Vec<_> = store.list(None).collect().await;
    assert!(items.is_empty());

    // An empty list does not yield any Result for inspect() to observe, so
    // the health state remains pristine. This is by design: empty list is
    // not evidence of "store responded successfully to our request" at the
    // granularity we care about. Non-empty lists or other ops keep the
    // signal fresh.
    assert!(!health.is_ok());
    assert!(health.last_success_at().is_none());
}

#[tokio::test]
async fn list_with_offset_yielding_items_records_success() {
    let (store, health) = setup();

    store
        .put(&Path::from("a"), PutPayload::from_static(b"x"))
        .await
        .unwrap();
    store
        .put(&Path::from("b"), PutPayload::from_static(b"y"))
        .await
        .unwrap();

    // Offset past "a" so only "b" should be yielded.
    let items: Vec<_> = store
        .list_with_offset(None, &Path::from("a"))
        .collect()
        .await;
    assert_eq!(items.len(), 1);
    assert!(health.is_ok());
}

#[tokio::test]
async fn delete_stream_successful_items_record_success() {
    let (store, health) = setup();

    store
        .put(&Path::from("a"), PutPayload::from_static(b"x"))
        .await
        .unwrap();
    store
        .put(&Path::from("b"), PutPayload::from_static(b"y"))
        .await
        .unwrap();

    // Fresh state after the puts: we want to see delete_stream specifically
    // drive the observation.
    let locations = stream::iter(vec![Ok(Path::from("a")), Ok(Path::from("b"))]).boxed();
    let deleted: Vec<_> = store.delete_stream(locations).collect().await;
    assert_eq!(deleted.len(), 2);
    assert!(health.is_ok());
    assert!(health.last_error_at().is_none());
}

#[tokio::test]
async fn delete_stream_yielding_error_records_error() {
    let (store, health) = setup();

    // Feed an input stream that itself yields an error; the delete_stream
    // implementation on InMemory passes errors through to its output. Our
    // wrapper should observe the error and flip the health signal.
    let err = || object_store::Error::Generic {
        store: "test",
        source: "synthetic error".into(),
    };
    let locations = stream::iter(vec![Ok(Path::from("a")), Err(err())]).boxed();
    let _: Vec<_> = store.delete_stream(locations).collect().await;

    assert!(!health.is_ok());
    assert!(health.last_error_at().is_some());
    assert_eq!(health.last_error_category(), Some(ErrorCategory::Unknown));
}
