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
