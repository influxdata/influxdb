use std::{sync::Arc, time::Duration};

use arrow::datatypes::ToByteSlice;
use bytes::Bytes;
use data_types::TimestampMinMax;
use influxdb3_test_helpers::object_store::{RequestCountedObjectStore, SynchronizedObjectStore};
use iox_time::{MockProvider, Time, TimeProvider};
use metric::{Attributes, Metric, Registry, U64Counter, U64Gauge};
use object_store::{ObjectStore, PutPayload, PutResult, memory::InMemory, path::Path};

use pretty_assertions::assert_eq;
use tokio::sync::Notify;

use crate::parquet_cache::{
    Cache, CacheRequest, ParquetFileDataToCache, create_cached_obj_store_and_oracle,
    metrics::{CACHE_ACCESS_NAME, CACHE_SIZE_BYTES_NAME, CACHE_SIZE_N_FILES_NAME},
    should_request_be_cached, test_cached_obj_store_and_oracle,
};

macro_rules! assert_payload_at_equals {
    ($store:ident, $expected:ident, $path:ident) => {
        assert_eq!(
            $expected,
            $store
                .get(&$path)
                .await
                .unwrap()
                .bytes()
                .await
                .unwrap()
                .to_byte_slice()
        )
    };
}

#[tokio::test]
async fn hit_cache_instead_of_object_store_eventual() {
    // set up the inner test object store and then wrap it with the mem cached store:
    let inner_store = Arc::new(RequestCountedObjectStore::new(Arc::new(InMemory::new())));
    let time_provider: Arc<dyn TimeProvider> =
        Arc::new(MockProvider::new(Time::from_timestamp_nanos(0)));
    let (cached_store, oracle) = test_cached_obj_store_and_oracle(
        Arc::clone(&inner_store) as _,
        Arc::clone(&time_provider),
        Default::default(),
    );
    // PUT a paylaod into the object store through the outer mem cached store:
    let path = Path::from("0.parquet");
    let payload = b"hello world";
    cached_store
        .put(&path, PutPayload::from_static(payload))
        .await
        .unwrap();

    // GET the payload from the object store before caching:
    assert_payload_at_equals!(cached_store, payload, path);
    assert_eq!(1, inner_store.total_read_request_count(&path));

    // cache the entry:
    let (cache_request, notifier_rx) =
        CacheRequest::create_eventual_mode_cache_request(path.clone(), None);
    oracle.register(cache_request);

    // wait for cache notify:
    let _ = notifier_rx.await;

    // another request to inner store should have been made:
    assert_eq!(2, inner_store.total_read_request_count(&path));

    // get the payload from the outer store again:
    assert_payload_at_equals!(cached_store, payload, path);

    // should hit the cache this time, so the inner store should not have been hit, and counts
    // should therefore be same as previous:
    assert_eq!(2, inner_store.total_read_request_count(&path));
}

#[test_log::test(tokio::test)]
async fn hit_cache_instead_of_object_store_immediate() {
    // set up the inner test object store and then wrap it with the mem cached store:
    let inner_store = Arc::new(RequestCountedObjectStore::new(Arc::new(InMemory::new())));
    let time_provider: Arc<dyn TimeProvider> =
        Arc::new(MockProvider::new(Time::from_timestamp_nanos(0)));
    let (cached_store, oracle) = test_cached_obj_store_and_oracle(
        Arc::clone(&inner_store) as _,
        Arc::clone(&time_provider),
        Default::default(),
    );
    let path = Path::from("0.parquet");
    let payload = b"hello world";

    let put_result = PutResult {
        e_tag: Some("some-etag".to_string()),
        version: Some("version-abc".to_string()),
    };

    let to_cache = ParquetFileDataToCache::new(
        &path,
        time_provider.now().date_time(),
        Bytes::from_static(payload),
        put_result,
    );

    // cache the entry:
    let cache_request = CacheRequest::create_immediate_mode_cache_request(path.clone(), to_cache);
    oracle.register(cache_request);

    let _ = cached_store.get(&path).await;
    // create request to inner store, this data is not in object store
    assert_eq!(0, inner_store.total_read_request_count(&path));

    // get the payload from the outer store and it should exist
    assert_payload_at_equals!(cached_store, payload, path);
}

#[test_log::test(tokio::test)]
async fn hit_cache_instead_of_object_store_immediate_and_eventual() {
    // set up the inner test object store and then wrap it with the mem cached store:
    let inner_store = Arc::new(RequestCountedObjectStore::new(Arc::new(InMemory::new())));
    let time_provider: Arc<dyn TimeProvider> =
        Arc::new(MockProvider::new(Time::from_timestamp_nanos(0)));
    let (cached_store, oracle) = test_cached_obj_store_and_oracle(
        Arc::clone(&inner_store) as _,
        Arc::clone(&time_provider),
        Default::default(),
    );
    let path_1_eventually_cached = Path::from("0.parquet");
    let payload_1_eventually_cached = b"hello world";

    let path_2_immediately_cached = Path::from("1.parquet");
    let payload_2_immediately_cached = b"good-bye world";

    // Add file to object store
    cached_store
        .put(
            &path_1_eventually_cached,
            PutPayload::from_static(payload_1_eventually_cached),
        )
        .await
        .unwrap();

    // GET the payload from the object store before caching:
    assert_payload_at_equals!(
        cached_store,
        payload_1_eventually_cached,
        path_1_eventually_cached
    );
    assert_eq!(
        1,
        inner_store.total_read_request_count(&path_1_eventually_cached)
    );

    // prepare for immediate request
    let put_result = PutResult {
        e_tag: Some("some-etag".to_string()),
        version: Some("version-abc".to_string()),
    };
    let to_cache = ParquetFileDataToCache::new(
        &path_2_immediately_cached,
        time_provider.now().date_time(),
        Bytes::from_static(payload_2_immediately_cached),
        put_result,
    );
    // Do an immediate cache request to other file
    let immediate_cache_request = CacheRequest::create_immediate_mode_cache_request(
        path_2_immediately_cached.clone(),
        to_cache,
    );
    oracle.register(immediate_cache_request);

    // Now try to cache 1st path eventually
    let (cache_request, notifier_rx) =
        CacheRequest::create_eventual_mode_cache_request(path_1_eventually_cached.clone(), None);
    oracle.register(cache_request);

    // Oracle should've fulfilled the immediate cache request
    assert_payload_at_equals!(
        cached_store,
        payload_2_immediately_cached,
        path_2_immediately_cached
    );

    // Now wait for eventual request to finish
    let _ = notifier_rx.await;

    // Because eventual mode request went through, there will be another GET request to inner
    // store
    assert_eq!(
        2,
        inner_store.total_read_request_count(&path_1_eventually_cached)
    );

    // get the payload from the outer store again:
    assert_payload_at_equals!(
        cached_store,
        payload_1_eventually_cached,
        path_1_eventually_cached
    );

    // should hit the cache this time, so the inner store should not have been hit, and counts
    // should therefore be same as previous:
    assert_eq!(
        2,
        inner_store.total_read_request_count(&path_1_eventually_cached)
    );

    // Now try to cache 1st path again eventually
    let (cache_request, notifier_rx) =
        CacheRequest::create_eventual_mode_cache_request(path_1_eventually_cached.clone(), None);
    oracle.register(cache_request);
    // should resolve immediately as path is already in cache
    let _ = notifier_rx.await;

    // no changes to inner store
    assert_eq!(
        2,
        inner_store.total_read_request_count(&path_1_eventually_cached)
    );

    // Try caching 2nd path (previously immediately cached)
    let (cache_request, notifier_rx) =
        CacheRequest::create_eventual_mode_cache_request(path_2_immediately_cached.clone(), None);
    oracle.register(cache_request);
    // should resolve immediately as path is already in cache
    let _ = notifier_rx.await;

    // again - no changes to inner store
    assert_eq!(
        2,
        inner_store.total_read_request_count(&path_1_eventually_cached)
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn cache_evicts_lru_when_full() {
    let inner_store = Arc::new(RequestCountedObjectStore::new(Arc::new(InMemory::new())));
    let time_provider = Arc::new(MockProvider::new(Time::from_timestamp_nanos(0)));
    // these are magic numbers that will make it so the third entry exceeds the cache capacity:
    let cache_capacity_bytes = 60;
    let cache_prune_percent = 0.4;
    let cache_prune_interval = Duration::from_millis(10);
    let (cached_store, oracle) = create_cached_obj_store_and_oracle(
        Arc::clone(&inner_store) as _,
        Arc::clone(&time_provider) as _,
        Default::default(),
        cache_capacity_bytes,
        Duration::from_millis(10),
        cache_prune_percent,
        cache_prune_interval,
    );
    let mut prune_notifier = oracle.prune_notifier();
    // PUT an entry into the store:
    let path_1 = Path::from("0.parquet");
    let payload_1 = b"Janeway";
    cached_store
        .put(&path_1, PutPayload::from_static(payload_1))
        .await
        .unwrap();

    // cache the entry and wait for it to complete:
    let (cache_request, notifier_rx) =
        CacheRequest::create_eventual_mode_cache_request(path_1.clone(), None);
    oracle.register(cache_request);
    let _ = notifier_rx.await;
    // there will have been one get request made by the cache oracle:
    assert_eq!(1, inner_store.total_read_request_count(&path_1));

    // update time:
    time_provider.set(Time::from_timestamp_nanos(1));

    // GET the entry to check its there and was retrieved from cache, i.e., that the request
    // counts do not change:
    assert_payload_at_equals!(cached_store, payload_1, path_1);
    assert_eq!(1, inner_store.total_read_request_count(&path_1));

    // PUT a second entry into the store:
    let path_2 = Path::from("1.parquet");
    let payload_2 = b"Paris";
    cached_store
        .put(&path_2, PutPayload::from_static(payload_2))
        .await
        .unwrap();

    // update time:
    time_provider.set(Time::from_timestamp_nanos(2));

    // cache the second entry and wait for it to complete, this will not evict the first entry
    // as both can fit in the cache:
    let (cache_request, notifier_rx) =
        CacheRequest::create_eventual_mode_cache_request(path_2.clone(), None);
    oracle.register(cache_request);
    let _ = notifier_rx.await;
    // will have another request for the second path to the inner store, by the oracle:
    assert_eq!(1, inner_store.total_read_request_count(&path_1));
    assert_eq!(1, inner_store.total_read_request_count(&path_2));

    // update time:
    time_provider.set(Time::from_timestamp_nanos(3));

    // GET the second entry and assert that it was retrieved from the cache, i.e., that the
    // request counts do not change:
    assert_payload_at_equals!(cached_store, payload_2, path_2);
    assert_eq!(1, inner_store.total_read_request_count(&path_1));
    assert_eq!(1, inner_store.total_read_request_count(&path_2));

    // update time:
    time_provider.set(Time::from_timestamp_nanos(4));

    // GET the first entry again and assert that it was retrieved from the cache as before. This
    // will also update the hit count so that the first entry (janeway) was used more recently
    // than the second entry (paris):
    assert_payload_at_equals!(cached_store, payload_1, path_1);
    assert_eq!(1, inner_store.total_read_request_count(&path_1));
    assert_eq!(1, inner_store.total_read_request_count(&path_2));

    // PUT a third entry into the store:
    let path_3 = Path::from("2.parquet");
    let payload_3 = b"Neelix";
    cached_store
        .put(&path_3, PutPayload::from_static(payload_3))
        .await
        .unwrap();

    // update time:
    time_provider.set(Time::from_timestamp_nanos(5));

    // cache the third entry and wait for it to complete, this will push the cache past its
    // capacity:
    let (cache_request, notifier_rx) =
        CacheRequest::create_eventual_mode_cache_request(path_3.clone(), None);
    oracle.register(cache_request);
    let _ = notifier_rx.await;
    // will now have another request for the third path to the inner store, by the oracle:
    assert_eq!(1, inner_store.total_read_request_count(&path_1));
    assert_eq!(1, inner_store.total_read_request_count(&path_2));
    assert_eq!(1, inner_store.total_read_request_count(&path_3));

    // update time:
    time_provider.set(Time::from_timestamp_nanos(6));

    // GET the new entry from the strore, and check that it was served by the cache:
    assert_payload_at_equals!(cached_store, payload_3, path_3);
    assert_eq!(1, inner_store.total_read_request_count(&path_1));
    assert_eq!(1, inner_store.total_read_request_count(&path_2));
    assert_eq!(1, inner_store.total_read_request_count(&path_3));

    prune_notifier.changed().await.unwrap();
    assert_eq!(23, *prune_notifier.borrow_and_update());

    // GET paris from the cached store, this will not be served by the cache, because paris was
    // evicted by neelix:
    assert_payload_at_equals!(cached_store, payload_2, path_2);
    assert_eq!(1, inner_store.total_read_request_count(&path_1));
    assert_eq!(2, inner_store.total_read_request_count(&path_2));
    assert_eq!(1, inner_store.total_read_request_count(&path_3));
}

#[tokio::test]
async fn cache_hit_while_fetching() {
    // Create the object store with the following layers:
    // Synchronized -> RequestCounted -> Inner
    let to_store_notify = Arc::new(Notify::new());
    let from_store_notify = Arc::new(Notify::new());
    let counter = Arc::new(RequestCountedObjectStore::new(Arc::new(InMemory::new())));
    let inner_store = Arc::new(
        SynchronizedObjectStore::new(Arc::clone(&counter) as _)
            .with_get_notifies(Arc::clone(&to_store_notify), Arc::clone(&from_store_notify)),
    );
    let time_provider = Arc::new(MockProvider::new(Time::from_timestamp_nanos(0)));
    let (cached_store, oracle) = test_cached_obj_store_and_oracle(
        Arc::clone(&inner_store) as _,
        Arc::clone(&time_provider) as _,
        Default::default(),
    );

    // PUT an entry into the store:
    let path = Path::from("0.parquet");
    let payload = b"Picard";
    cached_store
        .put(&path, PutPayload::from_static(payload))
        .await
        .unwrap();

    // cache the entry, but don't wait on it until below in spawned task:
    let (cache_request, notifier_rx) =
        CacheRequest::create_eventual_mode_cache_request(path.clone(), None);
    oracle.register(cache_request);

    // we are in the middle of a get request, i.e., the cache entry is "fetching"
    // once this call to notified wakes:
    let _ = from_store_notify.notified().await;

    // spawn a thread to wake the in-flight get request initiated by the cache oracle
    // after we have started a get request below, such that the get request below hits
    // the cache while the entry is still "fetching" state:
    let h = tokio::spawn(async move {
        to_store_notify.notify_one();
        let _ = notifier_rx.await;
    });

    // make the request to the store, which hits the cache in the "fetching" state
    // since we haven't made the call to notify the store to continue yet:
    assert_payload_at_equals!(cached_store, payload, path);

    // drive the task to completion to ensure that the cache request has been fulfilled:
    h.await.unwrap();

    // there should only have been one request made, i.e., from the cache oracle:
    assert_eq!(1, counter.total_read_request_count(&path));

    // make another request to the store, to be sure that it is in the cache:
    assert_payload_at_equals!(cached_store, payload, path);
    assert_eq!(1, counter.total_read_request_count(&path));
}

struct MetricVerifier {
    access_metrics: Metric<U64Counter>,
    size_mb_metrics: Metric<U64Gauge>,
    size_n_files_metrics: Metric<U64Gauge>,
}

impl MetricVerifier {
    fn new(metric_registry: Arc<Registry>) -> Self {
        let access_metrics = metric_registry
            .get_instrument::<Metric<U64Counter>>(CACHE_ACCESS_NAME)
            .unwrap();
        let size_mb_metrics = metric_registry
            .get_instrument::<Metric<U64Gauge>>(CACHE_SIZE_BYTES_NAME)
            .unwrap();
        let size_n_files_metrics = metric_registry
            .get_instrument::<Metric<U64Gauge>>(CACHE_SIZE_N_FILES_NAME)
            .unwrap();
        Self {
            access_metrics,
            size_mb_metrics,
            size_n_files_metrics,
        }
    }

    fn assert_access(
        &self,
        hits_expected: u64,
        misses_expected: u64,
        misses_while_fetching_expected: u64,
    ) {
        let hits_actual = self
            .access_metrics
            .get_observer(&Attributes::from(&[("status", "cached")]))
            .unwrap()
            .fetch();
        let misses_actual = self
            .access_metrics
            .get_observer(&Attributes::from(&[("status", "miss")]))
            .unwrap()
            .fetch();
        let misses_while_fetching_actual = self
            .access_metrics
            .get_observer(&Attributes::from(&[("status", "miss_while_fetching")]))
            .unwrap()
            .fetch();
        assert_eq!(
            hits_actual, hits_expected,
            "cache hits did not match expectation"
        );
        assert_eq!(
            misses_actual, misses_expected,
            "cache misses did not match expectation"
        );
        assert_eq!(
            misses_while_fetching_actual, misses_while_fetching_expected,
            "cache misses while fetching did not match expectation"
        );
    }

    fn assert_size(&self, size_bytes_expected: u64, size_n_files_expected: u64) {
        let size_bytes_actual = self
            .size_mb_metrics
            .get_observer(&Attributes::from(&[]))
            .unwrap()
            .fetch();
        let size_n_files_actual = self
            .size_n_files_metrics
            .get_observer(&Attributes::from(&[]))
            .unwrap()
            .fetch();
        assert_eq!(
            size_bytes_actual, size_bytes_expected,
            "cache size in bytes did not match actual"
        );
        assert_eq!(
            size_n_files_actual, size_n_files_expected,
            "cache size in number of files did not match actual"
        );
    }
}

#[tokio::test]
async fn cache_metrics() {
    // test setup
    let to_store_notify = Arc::new(Notify::new());
    let from_store_notify = Arc::new(Notify::new());
    let counted_store = Arc::new(RequestCountedObjectStore::new(Arc::new(InMemory::new())));
    let inner_store = Arc::new(
        SynchronizedObjectStore::new(Arc::clone(&counted_store) as _)
            .with_get_notifies(Arc::clone(&to_store_notify), Arc::clone(&from_store_notify)),
    );
    let time_provider = Arc::new(MockProvider::new(Time::from_timestamp_nanos(0)));
    let metric_registry = Arc::new(Registry::new());
    let (cached_store, oracle) = test_cached_obj_store_and_oracle(
        Arc::clone(&inner_store) as _,
        Arc::clone(&time_provider) as _,
        Arc::clone(&metric_registry),
    );
    let metric_verifier = MetricVerifier::new(metric_registry);

    // put something in the object store:
    let path = Path::from("0.parquet");
    let payload = b"Janeway";
    cached_store
        .put(&path, PutPayload::from_static(payload))
        .await
        .unwrap();

    // spin off a task to make a request to the object store on a separate thread. We will drive
    // the notifiers from here, as we just need the request to go through to register a cache
    // miss.
    let cached_store_cloned = Arc::clone(&cached_store);
    let path_cloned = path.clone();
    let h = tokio::spawn(async move {
        assert_payload_at_equals!(cached_store_cloned, payload, path_cloned);
    });

    // drive the synchronized store using the notifiers:
    from_store_notify.notified().await;
    to_store_notify.notify_one();
    h.await.unwrap();

    // check that there is a single cache miss:
    metric_verifier.assert_access(0, 1, 0);
    // nothing in the cache so sizes are 0
    metric_verifier.assert_size(0, 0);

    // there should be a single request made to the inner counted store from above:
    assert_eq!(1, counted_store.total_read_request_count(&path));

    // have the cache oracle cache the object:
    let (cache_request, notifier_rx) =
        CacheRequest::create_eventual_mode_cache_request(path.clone(), None);
    oracle.register(cache_request);

    // we are in the middle of a get request, i.e., the cache entry is "fetching" once this
    // call to notified wakes:
    let _ = from_store_notify.notified().await;
    // just a fetching entry in the cache, so it will have n files of 1 and 8 bytes for the atomic
    // i64
    metric_verifier.assert_size(8, 1);

    // spawn a thread to wake the in-flight get request initiated by the cache oracle after we
    // have started a get request below, such that the get request below hits the cache while
    // the entry is still in the "fetching" state:
    let h = tokio::spawn(async move {
        to_store_notify.notify_one();
        let _ = notifier_rx.await;
    });

    // make the request to the store, which hits the cache in the "fetching" state since we
    // haven't made the call to notify the store to continue yet:
    assert_payload_at_equals!(cached_store, payload, path);

    // check that there is a single miss while fetching, note, the metrics are cumulative, so
    // the original miss is still there:
    metric_verifier.assert_access(0, 1, 1);

    // drive the task to completion to ensure that the cache request has been fulfilled:
    h.await.unwrap();

    // there should only have been two requests made, i.e., one from the request before the
    // object was cached, and one from the cache oracle:
    assert_eq!(2, counted_store.total_read_request_count(&path));

    // make another request, this time, it should use the cache:
    assert_payload_at_equals!(cached_store, payload, path);

    // there have now been one of each access metric:
    metric_verifier.assert_access(1, 1, 1);
    // now the cache has a the full entry, which includes the atomic i64, the metadata, and
    // the payload itself:
    metric_verifier.assert_size(25, 1);

    cached_store.delete(&path).await.unwrap();
    // removing the entry should bring the cache sizes back to zero:
    metric_verifier.assert_size(0, 0);
}

#[test_log::test(test)]
fn test_should_request_be_cached_partial_overlap_of_file_time() {
    let time_provider: Arc<dyn TimeProvider> =
        Arc::new(MockProvider::new(Time::from_timestamp_nanos(100)));
    let max_size_bytes = 100;
    let cache = Cache::new(
        max_size_bytes,
        0.1,
        Arc::clone(&time_provider),
        Arc::new(Registry::new()),
        Duration::from_nanos(100),
    );

    let file_timestamp_min_max = Some(TimestampMinMax::new(0, 100));
    let should_cache = should_request_be_cached(file_timestamp_min_max, &cache);
    assert!(should_cache);
}

#[test_log::test(test)]
fn test_should_request_be_cached_no_overlap_of_file_time() {
    let time_provider: Arc<dyn TimeProvider> =
        Arc::new(MockProvider::new(Time::from_timestamp_nanos(1000)));
    let max_size_bytes = 100;
    let cache = Cache::new(
        max_size_bytes,
        0.1,
        Arc::clone(&time_provider),
        Arc::new(Registry::new()),
        Duration::from_nanos(100),
    );

    let file_timestamp_min_max = Some(TimestampMinMax::new(0, 100));
    let should_cache = should_request_be_cached(file_timestamp_min_max, &cache);
    assert!(!should_cache);
}

#[test_log::test(test)]
fn test_should_request_be_cached_no_timestamp_set() {
    let time_provider: Arc<dyn TimeProvider> =
        Arc::new(MockProvider::new(Time::from_timestamp_nanos(1000)));
    let max_size_bytes = 100;
    let cache = Cache::new(
        max_size_bytes,
        0.1,
        Arc::clone(&time_provider),
        Arc::new(Registry::new()),
        Duration::from_nanos(100),
    );

    let file_timestamp_min_max = Some(TimestampMinMax::new(0, 100));
    let should_cache = should_request_be_cached(file_timestamp_min_max, &cache);
    assert!(!should_cache);
}

#[test_log::test(tokio::test)]
async fn prune_does_not_evict_fetching_entries() {
    // Object store layers: Synchronized -> RequestCounted -> Inner
    let to_store_notify = Arc::new(Notify::new());
    let from_store_notify = Arc::new(Notify::new());
    let counter = Arc::new(RequestCountedObjectStore::new(Arc::new(InMemory::new())));
    let inner_store = Arc::new(
        SynchronizedObjectStore::new(Arc::clone(&counter) as _)
            .with_get_notifies(Arc::clone(&to_store_notify), Arc::clone(&from_store_notify)),
    );
    let time_provider = Arc::new(MockProvider::new(Time::from_timestamp_nanos(0)));
    // capacity and prune percent such that with two entries in the map, one prune evicts
    // exactly one entry, and the oldest entry by hit time is the in-flight fetch:
    let (cached_store, oracle) = create_cached_obj_store_and_oracle(
        Arc::clone(&inner_store) as _,
        Arc::clone(&time_provider) as _,
        Default::default(),
        60,
        Duration::from_millis(10),
        0.6,
        Duration::from_millis(10),
    );
    let mut prune_notifier = oracle.prune_notifier();

    // PUT the payload that will be fetched into a `Fetching` entry:
    let path_fetching = Path::from("0.parquet");
    let payload_fetching = b"in-flight";
    cached_store
        .put(&path_fetching, PutPayload::from_static(payload_fetching))
        .await
        .unwrap();

    // register the fetch and wait until it is blocked inside the object store `get`, so the
    // cache holds a `Fetching` entry with the oldest hit time in the map:
    let (cache_request, notifier_rx) =
        CacheRequest::create_eventual_mode_cache_request(path_fetching.clone(), None);
    oracle.register(cache_request);
    from_store_notify.notified().await;

    // at a later time, directly cache a success entry big enough to exceed the cache capacity
    // on its own, making the pruner run:
    time_provider.set(Time::from_timestamp_nanos(1));
    let path_success = Path::from("1.parquet");
    let payload_success = [b'x'; 64];
    let to_cache = ParquetFileDataToCache::new(
        &path_success,
        time_provider.now().date_time(),
        Bytes::copy_from_slice(&payload_success),
        PutResult {
            e_tag: None,
            version: None,
        },
    );
    oracle.register(CacheRequest::create_immediate_mode_cache_request(
        path_success.clone(),
        to_cache,
    ));

    // wait for the prune: the success entry is the only valid victim; the in-flight fetch
    // holds no bytes and must not be evicted:
    prune_notifier.changed().await.unwrap();

    // release the in-flight fetch and wait for the cache request to complete:
    to_store_notify.notify_one();
    let _ = notifier_rx.await;

    // the fetched entry must have landed in the cache: one read request from the oracle's
    // fetch, and none from this GET:
    assert!(oracle.in_cache(&path_fetching));
    assert_payload_at_equals!(cached_store, payload_fetching, path_fetching);
    assert_eq!(1, counter.total_read_request_count(&path_fetching));
}

#[test_log::test(tokio::test)]
async fn concurrent_registrations_share_one_fetch() {
    // Object store layers: Synchronized -> RequestCounted -> Inner
    let to_store_notify = Arc::new(Notify::new());
    let from_store_notify = Arc::new(Notify::new());
    let counter = Arc::new(RequestCountedObjectStore::new(Arc::new(InMemory::new())));
    let inner_store = Arc::new(
        SynchronizedObjectStore::new(Arc::clone(&counter) as _)
            .with_get_notifies(Arc::clone(&to_store_notify), Arc::clone(&from_store_notify)),
    );
    let time_provider: Arc<dyn TimeProvider> =
        Arc::new(MockProvider::new(Time::from_timestamp_nanos(0)));
    let (cached_store, oracle) = test_cached_obj_store_and_oracle(
        Arc::clone(&inner_store) as _,
        Arc::clone(&time_provider),
        Default::default(),
    );

    let path = Path::from("0.parquet");
    let payload = b"only-once";
    cached_store
        .put(&path, PutPayload::from_static(payload))
        .await
        .unwrap();

    // register the same path twice with no await point in between: both registrations see the
    // path as not yet fetched (the single-threaded runtime has not run the request handler
    // yet), so both are enqueued — the file must still only be fetched once:
    let (cache_request_a, notifier_rx_a) =
        CacheRequest::create_eventual_mode_cache_request(path.clone(), None);
    let (cache_request_b, notifier_rx_b) =
        CacheRequest::create_eventual_mode_cache_request(path.clone(), None);
    oracle.register(cache_request_a);
    oracle.register(cache_request_b);

    // wait for a fetch to be blocked inside the object store `get`, then release it (twice, so
    // an erroneous second fetch cannot hang the test):
    from_store_notify.notified().await;
    to_store_notify.notify_one();
    to_store_notify.notify_one();

    let _ = notifier_rx_a.await;
    let _ = notifier_rx_b.await;

    // one read request from the oracle's single fetch:
    assert_eq!(1, counter.total_read_request_count(&path));

    // and the entry is served from the cache:
    assert_payload_at_equals!(cached_store, payload, path);
    assert_eq!(1, counter.total_read_request_count(&path));
}

#[test_log::test(tokio::test)]
async fn prune_quota_counts_only_prunable_entries() {
    use futures::FutureExt;

    let time_provider = Arc::new(MockProvider::new(Time::from_timestamp_nanos(0)));
    let cache = Cache::new(
        100,
        0.5,
        Arc::clone(&time_provider) as Arc<dyn TimeProvider>,
        Arc::new(Registry::new()),
        Duration::from_millis(10),
    );

    // six in-flight fetches, older than every success entry:
    let fetching_paths = (0..6)
        .map(|i| Path::from(format!("f{i}")))
        .collect::<Vec<_>>();
    for path in &fetching_paths {
        let fut = futures::future::pending::<
            Result<Arc<crate::parquet_cache::CacheValue>, crate::parquet_cache::DynError>,
        >()
        .boxed()
        .shared();
        cache.set_fetching(path, fut);
    }

    // four success entries pushing the cache past capacity:
    let success_paths = (0..4)
        .map(|i| Path::from(format!("s{i}")))
        .collect::<Vec<_>>();
    for (i, path) in success_paths.iter().enumerate() {
        time_provider.set(Time::from_timestamp_nanos(10 + i as i64));
        let value = crate::parquet_cache::CacheValue {
            data: Bytes::from(vec![b'x'; 30]),
            meta: object_store::ObjectMeta {
                location: path.clone(),
                last_modified: time_provider.now().date_time(),
                size: 30,
                e_tag: None,
                version: None,
            },
        };
        cache.set_cache_value_directly(path, Arc::new(value));
    }

    // the quota is half of the PRUNABLE entries (4 success -> 2 evicted), not half of the
    // whole map (10 entries -> 5, which would have taken every success entry):
    cache.prune().unwrap();
    for path in &fetching_paths {
        assert!(cache.path_already_fetched(path), "{path} evicted");
    }
    assert!(!cache.path_already_fetched(&success_paths[0]));
    assert!(!cache.path_already_fetched(&success_paths[1]));
    assert!(cache.path_already_fetched(&success_paths[2]));
    assert!(cache.path_already_fetched(&success_paths[3]));
}

#[test_log::test(tokio::test)]
async fn prune_takes_at_least_one_entry_when_over_capacity() {
    let time_provider: Arc<dyn TimeProvider> =
        Arc::new(MockProvider::new(Time::from_timestamp_nanos(0)));
    let cache = Cache::new(
        10,
        0.1,
        Arc::clone(&time_provider),
        Arc::new(Registry::new()),
        Duration::from_millis(10),
    );

    // one success entry over capacity on its own; floor(1 * 0.1) = 0 would leave the cache
    // stuck over capacity forever:
    let path = Path::from("s0");
    let value = crate::parquet_cache::CacheValue {
        data: Bytes::from(vec![b'x'; 30]),
        meta: object_store::ObjectMeta {
            location: path.clone(),
            last_modified: time_provider.now().date_time(),
            size: 30,
            e_tag: None,
            version: None,
        },
    };
    cache.set_cache_value_directly(&path, Arc::new(value));

    assert!(cache.prune().unwrap() > 0);
    assert!(!cache.path_already_fetched(&path));
}

#[test_log::test(tokio::test)]
async fn stale_prune_victims_do_not_remove_new_fetches() {
    use std::collections::BinaryHeap;
    use std::sync::atomic::Ordering;

    use futures::FutureExt;

    use crate::parquet_cache::PruneHeapItem;

    let time_provider = Arc::new(MockProvider::new(Time::from_timestamp_nanos(0)));
    let cache = Cache::new(
        100,
        0.5,
        Arc::clone(&time_provider) as Arc<dyn TimeProvider>,
        Arc::new(Registry::new()),
        Duration::from_millis(10),
    );

    // a success entry selected as a prune victim:
    let path = Path::from("s0");
    let value = crate::parquet_cache::CacheValue {
        data: Bytes::from(vec![b'x'; 30]),
        meta: object_store::ObjectMeta {
            location: path.clone(),
            last_modified: time_provider.now().date_time(),
            size: 30,
            e_tag: None,
            version: None,
        },
    };
    cache.set_cache_value_directly(&path, Arc::new(value));
    let stale_victim = PruneHeapItem {
        hit_time: 0,
        path_ref: path.as_ref().into(),
    };

    // before the removal loop runs, the path is evicted and re-registered as a new fetch:
    cache.remove(&path);
    let fut = futures::future::pending::<
        Result<Arc<crate::parquet_cache::CacheValue>, crate::parquet_cache::DynError>,
    >()
    .boxed()
    .shared();
    cache.set_fetching(&path, fut);
    let used_before = cache.used.load(Ordering::SeqCst);

    // the stale victim must not remove the new fetch, and its already-subtracted size must
    // not be subtracted again (which would wrap `used`):
    let freed = cache.remove_victims(BinaryHeap::from([stale_victim]));
    assert_eq!(freed, 0);
    assert!(cache.path_already_fetched(&path));
    assert_eq!(cache.used.load(Ordering::SeqCst), used_before);
}
