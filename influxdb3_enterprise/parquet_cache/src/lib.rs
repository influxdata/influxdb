use std::{future::Future, ops::Deref, sync::Arc};

use bytes::Bytes;
use futures::{future::join_all, TryFutureExt};
use influxdb3_cache::parquet_cache::{CacheRequest, ParquetCacheOracle, ParquetFileDataToCache};
use influxdb3_write::ParquetFile;
use iox_time::{Time, TimeProvider};
use object_store::{path::Path as ObjPath, PutResult};
use observability_deps::tracing::warn;
use tokio::sync::oneshot::error::RecvError;

#[derive(Debug)]
pub struct ParquetCachePreFetcher {
    /// Cache oracle to prefetch into cache
    parquet_cache: Arc<dyn ParquetCacheOracle>,
    /// Duration allowed for prefetching
    preemptive_cache_age_secs: std::time::Duration,
    /// Time provider
    time_provider: Arc<dyn TimeProvider>,
}

impl ParquetCachePreFetcher {
    pub fn new(
        parquet_cache: Arc<dyn ParquetCacheOracle>,
        preemptive_cache_age: humantime::Duration,
        time_provider: Arc<dyn TimeProvider>,
    ) -> Self {
        Self {
            parquet_cache,
            preemptive_cache_age_secs: std::time::Duration::from_secs(
                preemptive_cache_age.as_secs(),
            ),
            time_provider,
        }
    }

    pub async fn prefetch_all(
        &self,
        parquet_infos: &Vec<ParquetFile>,
    ) -> Vec<Result<(), RecvError>> {
        let all_futures = self.prepare_prefetch_requests(parquet_infos);
        // `join_all` uses `FuturesOrdered` internally, might be nicer to have `FuturesUnordered` for this
        // case
        join_all(all_futures).await
    }

    pub fn add_to_cache(&self, path: Arc<ObjPath>, bytes: Bytes, put_result: PutResult) {
        let path = path.deref();
        let cache_req = CacheRequest::create_immediate_mode_cache_request(
            path.clone(),
            ParquetFileDataToCache::new(
                path,
                self.time_provider.now().date_time(),
                bytes,
                put_result,
            ),
        );
        self.parquet_cache.register(cache_req);
    }

    fn prepare_prefetch_requests(
        &self,
        parquet_infos: &Vec<ParquetFile>,
    ) -> Vec<impl Future<Output = Result<(), RecvError>>> {
        let mut futures = Vec::with_capacity(parquet_infos.len());
        for parquet_meta in parquet_infos {
            let (cache_request, receiver) = CacheRequest::create_eventual_mode_cache_request(
                ObjPath::from(parquet_meta.path.as_str()),
                None,
            );
            let logging_receiver = receiver.inspect_err(|err| {
                // NOTE: This warning message never comes out when file is missing in object store,
                //       instead see "failed to fulfill cache request with object store". Looks like
                //       `background_cache_request_handler` always calls `notifier.send(())`. This
                //       might be the expected behaviour in this interface.
                warn!(err = ?err, "Errored when trying to prefetch into cache");
            });
            self.register(cache_request, parquet_meta.max_time);
            futures.push(logging_receiver);
        }
        futures
    }

    fn register(&self, cache_request: CacheRequest, parquet_max_time: i64) {
        let now = self.time_provider.now();
        self.check_and_register(now, cache_request, parquet_max_time);
    }

    fn check_and_register(&self, now: Time, cache_request: CacheRequest, parquet_max_time: i64) {
        if self.should_prefetch(now, parquet_max_time) {
            self.parquet_cache.register(cache_request);
        }
    }

    fn should_prefetch(&self, now: Time, parquet_max_time: i64) -> bool {
        // This check is to make sure we don't prefetch compacted files that are old
        // and don't cover most recent period. If we are interested in caching only last
        // 3 days periods worth of data, there is no point in prefetching a file that holds
        // data for a period that ends before last 3 days.
        let min_time_for_prefetching = now - self.preemptive_cache_age_secs;
        parquet_max_time >= min_time_for_prefetching.timestamp_nanos()
    }
}

#[cfg(test)]
mod tests {
    use std::{str::FromStr, sync::Arc};

    use chrono::Utc;
    use humantime::Duration;
    use influxdb3_cache::parquet_cache::{
        create_cached_obj_store_and_oracle, CacheRequest, ParquetCacheOracle,
    };
    use influxdb3_id::ParquetFileId;
    use influxdb3_test_helpers::object_store::RequestCountedObjectStore;
    use influxdb3_write::ParquetFile;
    use iox_time::{MockProvider, Time};
    use object_store::{memory::InMemory, path::Path as ObjPath, PutPayload};
    use observability_deps::tracing::debug;
    use pretty_assertions::assert_eq;

    use crate::ParquetCachePreFetcher;

    const PATH: &str = "sample/test/file/path.parquet";

    #[derive(Debug)]
    struct MockCacheOracle;

    impl ParquetCacheOracle for MockCacheOracle {
        fn register(&self, cache_request: CacheRequest) {
            debug!(path = ?cache_request.get_path(), "calling cache with request path");
            assert_eq!(PATH, cache_request.get_path().as_ref());
        }

        fn prune_notifier(&self) -> tokio::sync::watch::Receiver<usize> {
            unimplemented!()
        }
    }

    fn setup_prefetcher_3days(oracle: Arc<dyn ParquetCacheOracle>) -> ParquetCachePreFetcher {
        let now = Utc::now().timestamp_nanos_opt().unwrap();
        let mock_time_provider = Arc::new(MockProvider::new(Time::from_timestamp_nanos(now)));
        ParquetCachePreFetcher::new(
            oracle,
            Duration::from_str("3d").unwrap(),
            mock_time_provider,
        )
    }

    fn setup_mock_prefetcher_3days() -> ParquetCachePreFetcher {
        let mock_cache_oracle = Arc::new(MockCacheOracle);
        setup_prefetcher_3days(mock_cache_oracle)
    }

    fn parquet_max_time_nanos(num_days: i64) -> (Time, i64) {
        let now = Utc::now();
        let parquet_max_time = (now - chrono::Duration::days(num_days))
            .timestamp_nanos_opt()
            .unwrap();
        (
            Time::from_timestamp_nanos(now.timestamp_nanos_opt().unwrap()),
            parquet_max_time,
        )
    }

    #[test]
    fn test_cache_pre_fetcher_time_after_lower_bound() {
        let (now, parquet_max_time) = parquet_max_time_nanos(2);
        let pre_fetcher = setup_mock_prefetcher_3days();

        let should_prefetch = pre_fetcher.should_prefetch(now, parquet_max_time);

        assert!(should_prefetch);
    }

    #[test]
    fn test_cache_pre_fetcher_time_equals_lower_bound() {
        let (now, parquet_max_time) = parquet_max_time_nanos(3);
        let pre_fetcher = setup_mock_prefetcher_3days();

        let should_prefetch = pre_fetcher.should_prefetch(now, parquet_max_time);

        assert!(should_prefetch);
    }

    #[test]
    fn test_cache_pre_fetcher_time_before_lower_bound() {
        let (now, parquet_max_time) = parquet_max_time_nanos(4);
        let pre_fetcher = setup_mock_prefetcher_3days();

        let should_prefetch = pre_fetcher.should_prefetch(now, parquet_max_time);

        assert!(!should_prefetch);
    }

    #[test]
    fn test_cache_prefetcher_register() {
        let (now, parquet_max_time) = parquet_max_time_nanos(3);
        let pre_fetcher = setup_mock_prefetcher_3days();
        let (cache_request, _) =
            CacheRequest::create_eventual_mode_cache_request(ObjPath::from(PATH), None);

        pre_fetcher.check_and_register(now, cache_request, parquet_max_time);
    }

    #[test_log::test(tokio::test)]
    async fn test_cache_prefetcher_with_errors() {
        let inner_store = Arc::new(RequestCountedObjectStore::new(Arc::new(InMemory::new())));
        let time_provider = Arc::new(MockProvider::new(Time::from_timestamp_nanos(0)));
        // These values are taken from parquet_cache tests. These are not relevant per se
        // for these tests just reuse.
        let cache_capacity_bytes = 60;
        let cache_prune_percent = 0.4;
        let cache_prune_interval = std::time::Duration::from_secs(100);
        let (cached_store, oracle) = create_cached_obj_store_and_oracle(
            Arc::clone(&inner_store) as _,
            Arc::clone(&time_provider) as _,
            Default::default(),
            cache_capacity_bytes,
            std::time::Duration::from_millis(1000),
            cache_prune_percent,
            cache_prune_interval,
        );
        // add a file to object store
        let path_100 = ObjPath::from("100.parquet");
        let payload_100 = b"file100";
        cached_store
            .put(&path_100, PutPayload::from_static(payload_100))
            .await
            .unwrap();

        let parquet_cache_prefetcher = setup_prefetcher_3days(oracle);
        let file_1 = ParquetFile {
            id: ParquetFileId::from(0),
            path: "1.parquet".to_owned(),
            size_bytes: 200,
            row_count: 8,
            chunk_time: 123456789000,
            min_time: 123456789000,
            max_time: Utc::now().timestamp_nanos_opt().unwrap(),
        };

        let file_2 = ParquetFile {
            id: ParquetFileId::from(1),
            path: path_100.as_ref().to_owned(),
            size_bytes: 200,
            row_count: 8,
            chunk_time: 123456789000,
            min_time: 123456789000,
            max_time: Utc::now().timestamp_nanos_opt().unwrap(),
        };

        let all_parquet_metas = vec![file_1, file_2];
        let results = parquet_cache_prefetcher
            .prefetch_all(&all_parquet_metas)
            .await;
        // both requests should've been sent
        assert_eq!(2, results.len());
        debug!(results = ?results, "results");
        // both requests should return ok although `file_1` was not
        // cached as it was missing in object store
        assert!(results.first().unwrap().is_ok());
        assert!(results.get(1).unwrap().is_ok());
    }
}
