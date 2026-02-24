use std::{hash::RandomState, num::NonZeroUsize, ops::Range, sync::Arc};

use async_trait::async_trait;
use bloom2::{Bloom2, BloomFilterBuilder, CompressedBitmap, FilterSize};
use bytes::Bytes;
use futures::stream::BoxStream;
use metric::{Registry, U64Histogram, U64HistogramOptions};
use object_store::{
    Attributes, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    PutMultipartOptions, PutOptions, PutPayload, PutResult, Result, path::Path,
};
use tracing::debug;
use tracker::{LockMetrics, Mutex};

use crate::{
    StoreType,
    cache_state::{ATTR_CACHE_STATE, CacheStateKind},
};

pub(crate) const CACHE_METRIC_BYTES: &str = "parquet_cache_bytes";

/// Arguments for the bloom filter contained in [`ObjectStoreCacheMetrics`].
#[derive(Debug, Clone)]
#[expect(missing_copy_implementations)] // allow later extensions
pub struct FilterArgs {
    /// Expected maximum number of objects.
    pub max_number_of_unique_objects: NonZeroUsize,

    /// False positive rate, as a nonzero percentage.
    pub false_positive_rate: NonZeroUsize,
}

/// State of [`ObjectStoreCacheMetrics`] that depends on the fact if it has a bloom filter registered or not.
#[derive(Debug, Clone)]
enum HasFilter {
    /// Bloom filter present.
    Yes {
        /// Track seen items.
        once_contained: Arc<Mutex<Bloom2<RandomState, CompressedBitmap, String>>>,

        /// Metric to estimate the degree of cache thrashing.
        ///
        /// This is the thrashed part of the [otherwise collapsed histogram](Self::No::bytes_miss).
        bytes_thrashed: U64Histogram,

        /// Metric to estimate the novel, never before seen bytes.
        ///
        /// This is the novel part of the [otherwise collapsed histogram](Self::No::bytes_miss).
        bytes_novel: U64Histogram,
    },

    /// No bloom filter present.
    No {
        /// Metric to cache MISSes.
        ///
        /// This collapses the two histograms [thrashed](Self::Yes::bytes_thrashed) and [novel](Self::Yes::bytes_novel)
        /// into one.
        bytes_miss: U64Histogram,
    },
}

impl HasFilter {
    /// Register a cache MISS for the given object store path and object size (in bytes).
    fn register_miss(&self, store_type: &StoreType, location: &Path, size: u64) {
        match self {
            Self::Yes {
                once_contained,
                bytes_thrashed,
                bytes_novel,
            } => {
                let location = location.to_string();

                let maybe_seen = {
                    let mut guard = once_contained.lock();
                    let maybe_seen = guard.contains(&location);

                    // insert is expensive, so only do if required
                    if !maybe_seen {
                        guard.insert(&location);
                    }
                    maybe_seen
                };

                if maybe_seen {
                    bytes_thrashed.record(size);
                    debug!(state="MISS", thrashed=true, store_type=store_type.0.as_ref(), %location, "object store cache");
                } else {
                    bytes_novel.record(size);
                    debug!(state="MISS", thrashed=false, store_type=store_type.0.as_ref(), %location, "object store cache");
                }
            }
            Self::No { bytes_miss } => {
                bytes_miss.record(size);
                debug!(state="MISS", store_type=store_type.0.as_ref(), %location, "object store cache");
            }
        }
    }
}

/// Metrics for a parquet cache.
#[derive(Debug)]
pub struct ObjectStoreCacheMetrics {
    inner: Arc<dyn ObjectStore>,

    /// Metric to estimate how many bytes are successfully serviced by the cache.
    bytes_cache_hit: U64Histogram,

    /// Metric to estimate how many bytes had a "cache MISS, but data is already loading"
    bytes_cache_miss_already_loading: U64Histogram,

    has_filter: HasFilter,

    store_type: StoreType,
}

impl ObjectStoreCacheMetrics {
    /// Create a new [`ObjectStoreCacheMetrics`].
    pub fn new(
        inner: Arc<dyn ObjectStore>,
        registry: &Registry,
        store_type: StoreType,
        filter: Option<FilterArgs>,
    ) -> Self {
        let bytes = registry.register_metric_with_options::<U64Histogram, _>(
            CACHE_METRIC_BYTES,
            "Distribution of bytes requested per parquet file.",
            || {
                U64HistogramOptions::new([
                    4_u64.pow(5),  // 1 kibibyte
                    4_u64.pow(6),  // 4 kibibytes
                    4_u64.pow(7),  // 16 kibibytes
                    4_u64.pow(8),  // 64 kibibytes
                    4_u64.pow(9),  // 256 kibibytes
                    4_u64.pow(10), // 1 mebibyte
                    4_u64.pow(11), // 4 mebibytes
                    4_u64.pow(12), // 16 mebibytes
                    4_u64.pow(13), // 64 mebibytes
                    4_u64.pow(14), // 256 mebibytes
                    4_u64.pow(15), // 1 gibibyte
                    4_u64.pow(16), // 4 gibibytes
                    u64::MAX,
                ])
            },
        );

        let bytes_cache_hit =
            bytes.recorder([("store", store_type.0.clone()), ("state", "hit".into())]);

        let bytes_cache_miss_already_loading = bytes.recorder([
            ("store", store_type.0.clone()),
            ("state", "miss_already_loading".into()),
        ]);

        let has_filter = match filter {
            Some(FilterArgs {
                max_number_of_unique_objects,
                false_positive_rate,
            }) => {
                let bytes_thrashed = bytes.recorder([
                    ("store", store_type.0.clone()),
                    ("state", "miss".into()),
                    ("thrashed", "yes".into()),
                ]);
                let bytes_novel = bytes.recorder([
                    ("store", store_type.0.clone()),
                    ("state", "miss".into()),
                    ("thrashed", "no".into()),
                ]);

                let lock_metrics = Arc::new(LockMetrics::new(
                    registry,
                    &[("lock", "object_store_cache_metrics")],
                ));
                let filter_size = set_bloom_filter_sizing(
                    max_number_of_unique_objects.get(),
                    false_positive_rate.get() as f64 / 100.0,
                );
                let once_contained = Arc::new(
                    lock_metrics.new_mutex(BloomFilterBuilder::default().size(filter_size).build()),
                );

                HasFilter::Yes {
                    once_contained,
                    bytes_thrashed,
                    bytes_novel,
                }
            }
            None => {
                let bytes_miss =
                    bytes.recorder([("store", store_type.0.clone()), ("state", "miss".into())]);

                HasFilter::No { bytes_miss }
            }
        };

        Self {
            inner,
            bytes_cache_hit,
            bytes_cache_miss_already_loading,
            has_filter,
            store_type,
        }
    }

    /// Register where the parquet object was found.
    fn register_get(&self, location: &Path, size: u64, result_attributes: &Attributes) {
        if let Some(state) = result_attributes
            .get(&ATTR_CACHE_STATE)
            .and_then(|val| CacheStateKind::try_from(val).ok())
        {
            match state {
                CacheStateKind::NewEntry => {
                    self.has_filter
                        .register_miss(&self.store_type, location, size);
                }
                CacheStateKind::AlreadyLoading => {
                    self.bytes_cache_miss_already_loading.record(size);
                    debug!(state="MISS_ALREADY_LOADING", store_type=self.store_type.0.as_ref(), %location, "object store cache");
                }
                CacheStateKind::WasCached => {
                    self.bytes_cache_hit.record(size);
                    debug!(state="HIT", store_type=self.store_type.0.as_ref(), %location, "object store cache");
                }
            }
        };
    }

    /// For testing, in order to mimic cache thrashing.
    #[cfg(test)]
    pub(crate) fn replace_inner_store(&self, inner: Arc<dyn ObjectStore>) -> Arc<Self> {
        Arc::new(Self {
            inner,
            bytes_cache_hit: self.bytes_cache_hit.clone(),
            bytes_cache_miss_already_loading: self.bytes_cache_miss_already_loading.clone(),
            has_filter: self.has_filter.clone(),
            store_type: self.store_type.clone(),
        })
    }
}

/// Determine the setting for the bloom2 filter.
///
/// Given a max anticipated number of items, and an acceptable false positive rate,
/// determine the [`FilterSize`] required.
///
/// Each [`FilterSize`] corresponds to a certain number of slots and a key size
/// (refer to <https://docs.rs/bloom2/0.5.1/bloom2/enum.FilterSize.html#variants>).
///
/// Note that when using [`BloomFilterBuilder::size`] this sets both `k` and `m`
/// as calculated by <https://hur.st/bloomfilter>.
///     k = filter_size (1-5)
///     m = filter bits, calculated in bloom2 as 2^(8 * k)
///     p = false_positives
///     n = num_items
///
/// There are graphs in the [`BloomFilterBuilder::size`], which match the output
/// from using <https://hur.st/bloomfilter>.
///
/// **NOTE THAT THE LONGER A QUERIER IS RUNNING, THE MORE THE BLOOM FILTER WILL BE FILLED.**
/// Therefore, it is imperative to provide an estimated `num_items` based upon this consideration.
fn set_bloom_filter_sizing(num_items: usize, false_positives: f64) -> FilterSize {
    match (
        expected_false_positive_rate(num_items, 1),
        expected_false_positive_rate(num_items, 2),
        expected_false_positive_rate(num_items, 3),
        expected_false_positive_rate(num_items, 4),
    ) {
        (Some(rate), _, _, _) if rate <= false_positives => FilterSize::KeyBytes1,
        (_, Some(rate), _, _) if rate <= false_positives => FilterSize::KeyBytes2, // max mem ~8KB
        (_, _, Some(rate), _) if rate <= false_positives => FilterSize::KeyBytes3, // max mem ~2MB
        // DO NOT use FilterSize::KeyBytes5 (k=5).
        // It has a memory usage of ~17GB and a maximum memory usage of ~1117GB when fully populated
        // Whereas the previous option (k=4) can handle 887 million items with max mem ~603MB, for
        // a <= 0.1 false positive rate.
        _ => FilterSize::KeyBytes4, // max mem ~603MB
    }
}

/// Uses the formula in <https://hur.st/bloomfilter>.
fn expected_false_positive_rate(num_items: usize, key_size: usize) -> Option<f64> {
    // p = pow(1 - exp(-k / (m / n)), k)
    // p = (1 - exp(-k / (m / n))) ^ k
    // p = (1 - exp(-k / ( (2^(8*k)) / n))) ^ k

    // Just a small preference - the small `* -1_f64` below is clearer than just adding a negative
    // before it, and we want this to be clear and easy to understand
    #[expect(clippy::neg_multiply)]
    // (2^(8*k))
    2_u128
        .checked_pow(8 * (key_size as u32))
        // ( (2^(8*k)) / n)
        .map(|nom| (nom as f64) / (num_items as f64))
        // -k / ( (2^(8*k)) / n)
        .map(|denom| (key_size as f64) * -1_f64 / denom)
        // (1 - exp(-k / ( (2^(8*k)) / n)))
        .map(|slope| 1_f64 - slope.exp())
        // (1 - exp(-k / ( (2^(8*k)) / n))) ^ k
        .map(|inv| inv.powi(key_size as i32))
}

#[deny(clippy::missing_trait_methods)]
#[async_trait]
impl ObjectStore for ObjectStoreCacheMetrics {
    async fn put(&self, location: &Path, payload: PutPayload) -> Result<PutResult> {
        self.inner.put(location, payload).await
    }

    async fn put_opts(
        &self,
        location: &Path,
        bytes: PutPayload,
        opts: PutOptions,
    ) -> Result<PutResult> {
        self.inner.put_opts(location, bytes, opts).await
    }

    async fn put_multipart(&self, location: &Path) -> Result<Box<dyn MultipartUpload>> {
        self.inner.put_multipart(location).await
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: PutMultipartOptions,
    ) -> Result<Box<dyn MultipartUpload>> {
        self.inner.put_multipart_opts(location, opts).await
    }

    async fn get(&self, location: &Path) -> Result<GetResult> {
        // use own implementation with metrics
        self.get_opts(location, Default::default()).await
    }

    async fn get_opts(&self, location: &Path, options: GetOptions) -> Result<GetResult> {
        let res = self.inner.get_opts(location, options).await;
        if let Ok(result) = &res {
            self.register_get(location, result.meta.size, &result.attributes);
        };

        res
    }

    async fn get_range(&self, location: &Path, range: Range<u64>) -> Result<Bytes> {
        // currently we use GET RANGE for reading ranges on already fetched (in whole GET) objects.
        // therefore, no metrics are recorded.
        self.inner.get_range(location, range).await
    }

    async fn get_ranges(&self, location: &Path, ranges: &[Range<u64>]) -> Result<Vec<Bytes>> {
        // currently we use GET RANGE for reading ranges on already fetched (in whole GET) objects.
        // therefore, no metrics are recorded.
        self.inner.get_ranges(location, ranges).await
    }

    async fn head(&self, location: &Path) -> Result<ObjectMeta> {
        // mem cache and disk cache implementations of HEAD use GET,
        // but then remove the GetResult.attributes.
        let res = self.inner.get(location).await;

        if let Ok(result) = &res {
            self.register_get(location, result.meta.size, &result.attributes);
        };

        res.map(|get_result| get_result.meta)
    }

    async fn delete(&self, location: &Path) -> Result<()> {
        self.inner.delete(location).await
    }

    fn delete_stream<'a>(
        &'a self,
        locations: BoxStream<'a, Result<Path>>,
    ) -> BoxStream<'a, Result<Path>> {
        self.inner.delete_stream(locations)
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, Result<ObjectMeta>> {
        self.inner.list(prefix)
    }

    fn list_with_offset(
        &self,
        prefix: Option<&Path>,
        offset: &Path,
    ) -> BoxStream<'static, Result<ObjectMeta>> {
        self.inner.list_with_offset(prefix, offset)
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> Result<ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy(&self, from: &Path, to: &Path) -> Result<()> {
        self.inner.copy(from, to).await
    }

    async fn rename(&self, from: &Path, to: &Path) -> Result<()> {
        self.inner.rename(from, to).await
    }

    async fn copy_if_not_exists(&self, from: &Path, to: &Path) -> Result<()> {
        self.inner.copy_if_not_exists(from, to).await
    }

    async fn rename_if_not_exists(&self, from: &Path, to: &Path) -> Result<()> {
        self.inner.rename_if_not_exists(from, to).await
    }
}

impl std::fmt::Display for ObjectStoreCacheMetrics {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "ObjectStoreCacheMetrics({})", self.inner)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_utils::{assert_counter_value, assert_u64histogram_hits};

    use std::num::NonZeroUsize;

    use futures_test_utils::AssertFutureExt;
    use object_store::memory::InMemory;
    use object_store_mem_cache::MemCacheObjectStoreParams;

    use object_store_mock::{MockCall::GetOpts, MockStore, get_result_stream, path};
    use object_store_size_hinting::hint_size;
    use rust_decimal::{Decimal, prelude::FromPrimitive};
    use test_helpers::tracing::TracingCapture;
    use tokio::sync::Barrier;

    async fn get_3_novel_2_repeated(store: impl ObjectStore, put: bool) -> Result<()> {
        // 3 unique ids
        let id1 = Path::from("foo");
        let id2 = Path::from("bar");
        let id3 = Path::from("baz");

        let data = b"be8bytes";

        if put {
            // create 3 objects
            store.put(&id1, PutPayload::from_static(data)).await?;
            store.copy(&id1, &id2).await?;
            store.copy(&id1, &id3).await?;
        }

        // get all 3
        store.get_opts(&id1, hint_size(data.len() as _)).await?;
        store.get_opts(&id2, hint_size(data.len() as _)).await?;
        store.get_opts(&id3, hint_size(data.len() as _)).await?;

        // repeat get 2
        store.get_opts(&id2, hint_size(data.len() as _)).await?;
        store.get_opts(&id3, hint_size(data.len() as _)).await?;

        Ok(())
    }

    fn wrap_in_cache(
        inner: Arc<dyn ObjectStore>,
        metrics: &metric::Registry,
    ) -> Arc<dyn ObjectStore> {
        Arc::new(
            MemCacheObjectStoreParams {
                inner,
                memory_limit: NonZeroUsize::MAX,
                metrics,
                s3fifo_main_threshold: usize::MAX,
                s3_fifo_ghost_memory_limit: NonZeroUsize::MAX,
                inflight_bytes: 1024 * 1024 * 1024, // 1GB
            }
            .build(),
        )
    }

    /// Selectively records for cache requests based on attributes reflecting [`CacheState`].
    #[tokio::test]
    async fn test_cache_metrics() -> Result<()> {
        let capture = capture();
        let inner: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let max_number_of_unique_objects = NonZeroUsize::new(100).unwrap();
        let false_positive_rate = NonZeroUsize::new(10).unwrap();

        // Test: non cache store should not record any metrics
        let metrics = Arc::new(metric::Registry::default());
        let store = ObjectStoreCacheMetrics::new(
            Arc::clone(&inner),
            &metrics,
            "without_cache".into(),
            Some(FilterArgs {
                max_number_of_unique_objects,
                false_positive_rate,
            }),
        );
        get_3_novel_2_repeated(store, true).await?;
        assert_u64histogram_hits(
            &metrics,
            CACHE_METRIC_BYTES,
            [("store", "without_cache"), ("state", "hit")],
            0,
        );
        assert_u64histogram_hits(
            &metrics,
            CACHE_METRIC_BYTES,
            [
                ("store", "without_cache"),
                ("state", "miss"),
                ("thrashed", "no"),
            ],
            0,
        );
        assert_u64histogram_hits(
            &metrics,
            CACHE_METRIC_BYTES,
            [
                ("store", "without_cache"),
                ("state", "miss"),
                ("thrashed", "yes"),
            ],
            0,
        );

        // Test: cache store should record metrics
        let metrics = Arc::new(metric::Registry::default());
        let cache_store = wrap_in_cache(inner, &metrics);
        let store = ObjectStoreCacheMetrics::new(
            cache_store,
            &metrics,
            "with_cache".into(),
            Some(FilterArgs {
                max_number_of_unique_objects,
                false_positive_rate,
            }),
        );
        get_3_novel_2_repeated(store, false).await?;
        assert_u64histogram_hits(
            &metrics,
            CACHE_METRIC_BYTES,
            [("store", "with_cache"), ("state", "hit")],
            2,
        );
        assert_u64histogram_hits(
            &metrics,
            CACHE_METRIC_BYTES,
            [
                ("store", "with_cache"),
                ("state", "miss"),
                ("thrashed", "no"),
            ],
            3,
        );
        assert_u64histogram_hits(
            &metrics,
            CACHE_METRIC_BYTES,
            [
                ("store", "with_cache"),
                ("state", "miss"),
                ("thrashed", "yes"),
            ],
            0,
        );

        // Test: lock contention activity should also be recorded
        assert_counter_value(
            &metrics,
            "catalog_lock",
            [
                ("lock", "object_store_cache_metrics"),
                ("access", "exclusive"),
            ],
            3,
        );

        insta::assert_yaml_snapshot!(
            capture.lines_as_maps(),
            @r#"
        - level: DEBUG
          location: foo
          message: object store cache
          state: "\"MISS\""
          store_type: "\"with_cache\""
          thrashed: false
        - level: DEBUG
          location: bar
          message: object store cache
          state: "\"MISS\""
          store_type: "\"with_cache\""
          thrashed: false
        - level: DEBUG
          location: baz
          message: object store cache
          state: "\"MISS\""
          store_type: "\"with_cache\""
          thrashed: false
        - level: DEBUG
          location: bar
          message: object store cache
          state: "\"HIT\""
          store_type: "\"with_cache\""
        - level: DEBUG
          location: baz
          message: object store cache
          state: "\"HIT\""
          store_type: "\"with_cache\""
        "#);

        Ok(())
    }

    /// test for thrashing
    #[tokio::test]
    async fn test_cache_metrics_can_record_thrash() -> Result<()> {
        let capture = capture();
        let inner: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let max_number_of_unique_objects = NonZeroUsize::new(100).unwrap();
        let false_positive_rate = NonZeroUsize::new(10).unwrap();

        // populate store with objects
        get_3_novel_2_repeated(Arc::clone(&inner), true).await?;

        // Test: using the `ObjectStoreCacheMetrics` directly
        let metrics = Arc::new(metric::Registry::default());
        let cache_store = wrap_in_cache(Arc::clone(&inner), &metrics);
        let mut cache_metrics_store = Arc::new(ObjectStoreCacheMetrics::new(
            cache_store,
            &metrics,
            "owned_cache_metrics_store".into(),
            Some(FilterArgs {
                max_number_of_unique_objects,
                false_positive_rate,
            }),
        ));
        get_3_novel_2_repeated(
            Arc::clone(&cache_metrics_store) as Arc<dyn ObjectStore>,
            false,
        )
        .await?;
        assert_u64histogram_hits(
            &metrics,
            CACHE_METRIC_BYTES,
            [("store", "owned_cache_metrics_store"), ("state", "hit")],
            2,
        );
        assert_u64histogram_hits(
            &metrics,
            CACHE_METRIC_BYTES,
            [
                ("store", "owned_cache_metrics_store"),
                ("state", "miss"),
                ("thrashed", "no"),
            ],
            3,
        );
        assert_u64histogram_hits(
            &metrics,
            CACHE_METRIC_BYTES,
            [
                ("store", "owned_cache_metrics_store"),
                ("state", "miss"),
                ("thrashed", "yes"),
            ],
            0,
        );

        // Test thrash, buy emptying the inner cache
        let empty_cache = wrap_in_cache(Arc::clone(&inner), &metrics);
        cache_metrics_store = cache_metrics_store.replace_inner_store(empty_cache);
        get_3_novel_2_repeated(cache_metrics_store as Arc<dyn ObjectStore>, false).await?;
        assert_u64histogram_hits(
            &metrics,
            CACHE_METRIC_BYTES,
            [("store", "owned_cache_metrics_store"), ("state", "hit")],
            4,
        );
        assert_u64histogram_hits(
            &metrics,
            CACHE_METRIC_BYTES,
            [
                ("store", "owned_cache_metrics_store"),
                ("state", "miss"),
                ("thrashed", "no"),
            ],
            3,
        );
        assert_u64histogram_hits(
            &metrics,
            CACHE_METRIC_BYTES,
            [
                ("store", "owned_cache_metrics_store"),
                ("state", "miss"),
                ("thrashed", "yes"),
            ],
            3,
        );

        insta::assert_yaml_snapshot!(
            capture.lines_as_maps(),
            @r#"
        - level: DEBUG
          location: foo
          message: object store cache
          state: "\"MISS\""
          store_type: "\"owned_cache_metrics_store\""
          thrashed: false
        - level: DEBUG
          location: bar
          message: object store cache
          state: "\"MISS\""
          store_type: "\"owned_cache_metrics_store\""
          thrashed: false
        - level: DEBUG
          location: baz
          message: object store cache
          state: "\"MISS\""
          store_type: "\"owned_cache_metrics_store\""
          thrashed: false
        - level: DEBUG
          location: bar
          message: object store cache
          state: "\"HIT\""
          store_type: "\"owned_cache_metrics_store\""
        - level: DEBUG
          location: baz
          message: object store cache
          state: "\"HIT\""
          store_type: "\"owned_cache_metrics_store\""
        - level: DEBUG
          location: foo
          message: object store cache
          state: "\"MISS\""
          store_type: "\"owned_cache_metrics_store\""
          thrashed: true
        - level: DEBUG
          location: bar
          message: object store cache
          state: "\"MISS\""
          store_type: "\"owned_cache_metrics_store\""
          thrashed: true
        - level: DEBUG
          location: baz
          message: object store cache
          state: "\"MISS\""
          store_type: "\"owned_cache_metrics_store\""
          thrashed: true
        - level: DEBUG
          location: bar
          message: object store cache
          state: "\"HIT\""
          store_type: "\"owned_cache_metrics_store\""
        - level: DEBUG
          location: baz
          message: object store cache
          state: "\"HIT\""
          store_type: "\"owned_cache_metrics_store\""
        "#);

        Ok(())
    }

    #[tokio::test]
    async fn test_no_filter_args() -> Result<()> {
        let capture = capture();
        let inner: Arc<dyn ObjectStore> = Arc::new(InMemory::new());

        // populate store with objects
        get_3_novel_2_repeated(Arc::clone(&inner), true).await?;

        // Test: using the `ObjectStoreCacheMetrics` directly
        let metrics = Arc::new(metric::Registry::default());
        let cache_store = wrap_in_cache(Arc::clone(&inner), &metrics);
        let mut cache_metrics_store = Arc::new(ObjectStoreCacheMetrics::new(
            cache_store,
            &metrics,
            "owned_cache_metrics_store".into(),
            None,
        ));
        get_3_novel_2_repeated(
            Arc::clone(&cache_metrics_store) as Arc<dyn ObjectStore>,
            false,
        )
        .await?;
        assert_u64histogram_hits(
            &metrics,
            CACHE_METRIC_BYTES,
            [("store", "owned_cache_metrics_store"), ("state", "hit")],
            2,
        );
        assert_u64histogram_hits(
            &metrics,
            CACHE_METRIC_BYTES,
            [("store", "owned_cache_metrics_store"), ("state", "miss")],
            3,
        );

        // Test thrash, buy emptying the inner cache
        let empty_cache = wrap_in_cache(Arc::clone(&inner), &metrics);
        cache_metrics_store = cache_metrics_store.replace_inner_store(empty_cache);
        get_3_novel_2_repeated(cache_metrics_store as Arc<dyn ObjectStore>, false).await?;
        assert_u64histogram_hits(
            &metrics,
            CACHE_METRIC_BYTES,
            [("store", "owned_cache_metrics_store"), ("state", "hit")],
            4,
        );
        assert_u64histogram_hits(
            &metrics,
            CACHE_METRIC_BYTES,
            [("store", "owned_cache_metrics_store"), ("state", "miss")],
            6,
        );

        insta::assert_yaml_snapshot!(
            capture.lines_as_maps(),
            @r#"
        - level: DEBUG
          location: foo
          message: object store cache
          state: "\"MISS\""
          store_type: "\"owned_cache_metrics_store\""
        - level: DEBUG
          location: bar
          message: object store cache
          state: "\"MISS\""
          store_type: "\"owned_cache_metrics_store\""
        - level: DEBUG
          location: baz
          message: object store cache
          state: "\"MISS\""
          store_type: "\"owned_cache_metrics_store\""
        - level: DEBUG
          location: bar
          message: object store cache
          state: "\"HIT\""
          store_type: "\"owned_cache_metrics_store\""
        - level: DEBUG
          location: baz
          message: object store cache
          state: "\"HIT\""
          store_type: "\"owned_cache_metrics_store\""
        - level: DEBUG
          location: foo
          message: object store cache
          state: "\"MISS\""
          store_type: "\"owned_cache_metrics_store\""
        - level: DEBUG
          location: bar
          message: object store cache
          state: "\"MISS\""
          store_type: "\"owned_cache_metrics_store\""
        - level: DEBUG
          location: baz
          message: object store cache
          state: "\"MISS\""
          store_type: "\"owned_cache_metrics_store\""
        - level: DEBUG
          location: bar
          message: object store cache
          state: "\"HIT\""
          store_type: "\"owned_cache_metrics_store\""
        - level: DEBUG
          location: baz
          message: object store cache
          state: "\"HIT\""
          store_type: "\"owned_cache_metrics_store\""
        "#);

        Ok(())
    }

    fn into_decimal_with_precision(maybe_f: Option<f64>, precision: u32) -> Option<Decimal> {
        maybe_f.and_then(|f| Decimal::from_f64(f).map(|dec| dec.round_dp(precision)))
    }

    /// All expected outcomes where also confirmed on <https://hur.st/bloomfilter/>.
    /// Website calculations match the expected formula outcome.
    #[test]
    fn test_false_positive_calculation() {
        // for n=100000
        let num_items = 100000;

        // p = (1 - exp(-k / ( (2^(8*k)) / 100000))) ^ k
        // k=1  =>   (1 - exp(-1 / ( (2^(8*1)) / 100000))) ^ 1  => 1
        let key_size = 1;
        let precision: u32 = 1;
        assert_eq!(
            into_decimal_with_precision(
                expected_false_positive_rate(num_items, key_size),
                precision
            ),
            Some(Decimal::new(10, precision)), // 1.0
        );
        // k=2  =>   (1 - exp(-2 / ( (2^(8*2)) / 100000))) ^ 2  => 0.907683..
        let key_size = 2;
        let precision = 6;
        assert_eq!(
            into_decimal_with_precision(
                expected_false_positive_rate(num_items, key_size),
                precision
            ),
            Some(Decimal::new(907683, precision)),
        );
        // k=3  =>   (1 - exp(-3 / ( (2^(8*3)) / 100000))) ^ 3  => 0.000005566..
        let key_size = 3;
        let precision = 9;
        assert_eq!(
            into_decimal_with_precision(
                expected_false_positive_rate(num_items, key_size),
                precision
            ),
            Some(Decimal::new(5566, precision)),
        );
        // k=4  =>   (1 - exp(-4 / ( (2^(8*4)) / 100000))) ^ 4  => 7.5 e-17
        let key_size = 4;
        let precision = 17 + 1;
        assert_eq!(
            into_decimal_with_precision(
                expected_false_positive_rate(num_items, key_size),
                precision
            ),
            Some(Decimal::new(75, precision)),
        );
        // k=5  =>   (1 - exp(-5 / ( (2^(8*5)) / 100000))) ^ 5  => 1.9 e-32
        // rust_decimal does not allow precision above 28.
        // But we can at least confirm that expected_false_positive_rate is Some
        let key_size = 5;
        assert!(expected_false_positive_rate(num_items, key_size).is_some(),);

        // for n=10000000
        let num_items = 10000000;

        // p = (1 - exp(-k / ( (2^(8*k)) / 10000000))) ^ k
        // k=1  =>   (1 - exp(-1 / ( (2^(8*1)) / 10000000))) ^ 1  => 1
        let key_size = 1;
        let precision = 1;
        assert_eq!(
            into_decimal_with_precision(
                expected_false_positive_rate(num_items, key_size),
                precision
            ),
            Some(Decimal::new(10, precision)), // 1.0
        );
        // k=2  =>   (1 - exp(-2 / ( (2^(8*2)) / 10000000))) ^ 2  => 1
        let key_size = 2;
        let precision = 1;
        assert_eq!(
            into_decimal_with_precision(
                expected_false_positive_rate(num_items, key_size),
                precision
            ),
            Some(Decimal::new(10, precision)), // 1.0
        );
        // k=3  =>   (1 - exp(-3 / ( (2^(8*3)) / 10000000))) ^ 3  => 0.577..
        let key_size = 3;
        let precision = 3;
        assert_eq!(
            into_decimal_with_precision(
                expected_false_positive_rate(num_items, key_size),
                precision
            ),
            Some(Decimal::new(577, precision)),
        );
        // k=4  =>   (1 - exp(-4 / ( (2^(8*4)) / 10000000))) ^ 4  => 0.00000000738..
        let key_size = 4;
        let precision = 11;
        assert_eq!(
            into_decimal_with_precision(
                expected_false_positive_rate(num_items, key_size),
                precision
            ),
            Some(Decimal::new(738, precision)),
        );
        // k=5  =>   (1 - exp(-5 / ( (2^(8*5)) / 10000000))) ^ 5  => 1.9 e-22
        let key_size = 5;
        let precision = 22 + 1;
        assert_eq!(
            into_decimal_with_precision(
                expected_false_positive_rate(num_items, key_size),
                precision
            ),
            Some(Decimal::new(19, precision)),
        );
    }

    #[tokio::test]
    async fn test_already_loading() -> Result<()> {
        let capture = capture();

        let location = path();

        let mut get_opts = hint_size(object_store_mock::DATA.len() as _);
        let (tx, _rx) = object_store_mem_cache::buffer_channel::channel();
        get_opts.extensions.insert(tx);

        let barrier = Arc::new(Barrier::new(2));
        let inner: Arc<dyn ObjectStore> = MockStore::new()
            .mock_next(GetOpts {
                params: (location.clone(), get_opts.into()),
                barriers: vec![Arc::clone(&barrier)],
                res: Ok(get_result_stream()),
            })
            .as_store();

        let max_number_of_unique_objects = NonZeroUsize::new(100).unwrap();
        let false_positive_rate = NonZeroUsize::new(10).unwrap();
        let metrics = Arc::new(metric::Registry::default());
        let cache_store = wrap_in_cache(inner, &metrics);
        let store = ObjectStoreCacheMetrics::new(
            cache_store,
            &metrics,
            "test".into(),
            Some(FilterArgs {
                max_number_of_unique_objects,
                false_positive_rate,
            }),
        );

        let mut f1 = store.get_opts(&location, hint_size(object_store_mock::DATA.len() as _));
        f1.assert_pending().await;

        let mut f2 = store.get_opts(&location, hint_size(object_store_mock::DATA.len() as _));
        f2.assert_pending().await;

        let (res1, _) = tokio::join!(f1, barrier.wait());
        res1.unwrap();
        f2.await.unwrap();

        assert_u64histogram_hits(
            &metrics,
            CACHE_METRIC_BYTES,
            [("store", "test"), ("state", "hit")],
            0,
        );
        assert_u64histogram_hits(
            &metrics,
            CACHE_METRIC_BYTES,
            [("store", "test"), ("state", "miss_already_loading")],
            1,
        );
        assert_u64histogram_hits(
            &metrics,
            CACHE_METRIC_BYTES,
            [("store", "test"), ("state", "miss"), ("thrashed", "no")],
            1,
        );
        assert_u64histogram_hits(
            &metrics,
            CACHE_METRIC_BYTES,
            [("store", "test"), ("state", "miss"), ("thrashed", "yes")],
            0,
        );

        insta::assert_yaml_snapshot!(
                capture.lines_as_maps(),
                @r#"
        - level: DEBUG
          location: path
          message: object store cache
          state: "\"MISS\""
          store_type: "\"test\""
          thrashed: false
        - level: DEBUG
          location: path
          message: object store cache
          state: "\"MISS_ALREADY_LOADING\""
          store_type: "\"test\""
        "#);

        Ok(())
    }

    fn capture() -> TracingCapture {
        TracingCapture::builder()
            .filter_target("object_store_metrics::cache_metrics")
            .build()
    }
}
