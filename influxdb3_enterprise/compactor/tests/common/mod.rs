use std::{str::FromStr, sync::Arc};

use chrono::Utc;
use influxdb3_cache::parquet_cache::test_cached_obj_store_and_oracle;
use influxdb3_enterprise_parquet_cache::ParquetCachePreFetcher;
use influxdb3_test_helpers::object_store::RequestCountedObjectStore;
use iox_time::{MockProvider, SystemProvider, Time, TimeProvider};
use object_store::{memory::InMemory, ObjectStore};

pub(crate) fn build_parquet_cache_prefetcher(
    obj_store: &Arc<InMemory>,
) -> Option<Arc<ParquetCachePreFetcher>> {
    let time_provider: Arc<dyn TimeProvider> = Arc::new(SystemProvider::new());
    let as_obj_store: Arc<dyn ObjectStore> = Arc::<InMemory>::clone(obj_store);
    let test_store = Arc::new(RequestCountedObjectStore::new(Arc::clone(&as_obj_store)));
    let (_, parquet_cache) = test_cached_obj_store_and_oracle(
        Arc::clone(&test_store) as _,
        Arc::clone(&time_provider),
        Default::default(),
    );
    let now = Utc::now().timestamp_nanos_opt().unwrap();
    let mock_time_provider = Arc::new(MockProvider::new(Time::from_timestamp_nanos(now)));

    Some(Arc::new(ParquetCachePreFetcher::new(
        parquet_cache,
        humantime::Duration::from_str("1d").unwrap(),
        mock_time_provider,
    )))
}
