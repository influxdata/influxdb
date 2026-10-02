use super::*;
use object_store::memory::InMemory;
use std::time::Duration;
use tokio::time::timeout;

fn unregistered_metrics() -> Arc<AsyncSemaphoreMetrics> {
    Arc::new(AsyncSemaphoreMetrics::new_unregistered())
}

#[tokio::test]
async fn basic_operations() {
    let store = LimitObjectStore::new(Arc::new(InMemory::new()), 10, &unregistered_metrics());
    let path = Path::from("test");
    store.put(&path, PutPayload::from("hello")).await.unwrap();
    let result = store.get(&path).await.unwrap().bytes().await.unwrap();
    assert_eq!(result.as_ref(), b"hello");
}

#[tokio::test]
async fn permits_in_use_tracking() {
    let inner = Arc::new(InMemory::new());
    let store = LimitObjectStore::new(Arc::clone(&inner) as _, 10, &unregistered_metrics());
    assert_eq!(store.permits_in_use(), 0);
    assert_eq!(store.max_permits(), 10);

    // Put a file so that list streams have data and stay alive after peek
    inner
        .put(&Path::from("test"), PutPayload::from("data"))
        .await
        .unwrap();

    let mut streams = Vec::with_capacity(10);
    for _ in 0..10 {
        let mut stream = store.list(None).peekable();
        Pin::new(&mut stream).peek().await;
        streams.push(stream);
    }
    assert_eq!(store.permits_in_use(), 10);

    // Should not be able to make another request
    let fut = store.list(None).collect::<Vec<_>>();
    assert!(timeout(Duration::from_millis(20), fut).await.is_err());

    streams.pop();
    assert_eq!(store.permits_in_use(), 9);

    // Can now make another request
    store.list(None).collect::<Vec<_>>().await;
}

#[tokio::test]
async fn waiters_tracking() {
    let inner = Arc::new(InMemory::new());
    let store = LimitObjectStore::new(Arc::clone(&inner) as _, 1, &unregistered_metrics());
    assert_eq!(store.waiters(), 0);

    // Put a file so list streams stay alive
    inner
        .put(&Path::from("test"), PutPayload::from("data"))
        .await
        .unwrap();

    // Exhaust the single permit
    let mut stream = store.list(None).peekable();
    Pin::new(&mut stream).peek().await;
    assert_eq!(store.permits_in_use(), 1);
    assert_eq!(store.waiters(), 0);

    // Spawn a task that will block waiting for a permit
    let semaphore = Arc::clone(&store.semaphore);
    let handle = tokio::spawn(async move {
        let _permit = semaphore.acquire_owned(None).await.unwrap();
    });

    // Poll until the spawned task registers as a waiter
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    loop {
        if store.waiters() == 1 {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "timed out waiting for waiter to register"
        );
        tokio::task::yield_now().await;
    }

    // Drop the stream to release the permit
    drop(stream);

    // The spawned task should complete
    handle.await.unwrap();
    assert_eq!(store.waiters(), 0);
}

#[test]
fn display() {
    let store = LimitObjectStore::new(Arc::new(InMemory::new()), 42, &unregistered_metrics());
    assert_eq!(store.to_string(), "LimitObjectStore(42, InMemory)");
}

#[tokio::test]
async fn bypass_limit_ops_skip_the_semaphore() {
    let inner = Arc::new(InMemory::new());
    let store = LimitObjectStore::new(Arc::clone(&inner) as _, 1, &unregistered_metrics());
    let path = Path::from("test");
    store.put(&path, PutPayload::from("data")).await.unwrap();

    // Pin the only permit by holding a list stream open.
    let mut stream = store.list(None).peekable();
    Pin::new(&mut stream).peek().await;
    assert_eq!(store.permits_in_use(), 1);

    // A normal op queues behind the held permit...
    let fut = store.get_opts(&path, GetOptions::default());
    assert!(timeout(Duration::from_millis(20), fut).await.is_err());

    // ...while ops carrying BypassLimit proceed without a permit.
    let mut get_options = GetOptions::default();
    get_options.extensions.insert(BypassLimit);
    let got = timeout(Duration::from_secs(5), store.get_opts(&path, get_options))
        .await
        .expect("bypass get must not queue")
        .unwrap();
    assert_eq!(got.bytes().await.unwrap().as_ref(), b"data");

    let mut put_options = PutOptions::default();
    put_options.extensions.insert(BypassLimit);
    timeout(
        Duration::from_secs(5),
        store.put_opts(&path, PutPayload::from("data2"), put_options),
    )
    .await
    .expect("bypass put must not queue")
    .unwrap();

    // The permit accounting never saw the bypass ops.
    assert_eq!(store.permits_in_use(), 1);
    drop(stream);
}
