use metric::{Registry, U64Counter, U64Gauge};

#[derive(Debug)]
pub(super) struct AccessMetrics {
    cache_hits: U64Counter,
    cache_misses: U64Counter,
    cache_misses_while_fetching: U64Counter,
}

pub(super) const CACHE_ACCESS_NAME: &str = "influxdb3_parquet_cache_access";

impl AccessMetrics {
    pub(super) fn new(metric_registry: &Registry) -> Self {
        let m_access = metric_registry.register_metric::<U64Counter>(
            CACHE_ACCESS_NAME,
            "track accesses to the in-memory parquet cache",
        );
        let cache_hits = m_access.recorder(&[("status", "cached")]);
        let cache_misses = m_access.recorder(&[("status", "miss")]);
        let cache_misses_while_fetching = m_access.recorder(&[("status", "miss_while_fetching")]);
        Self {
            cache_hits,
            cache_misses,
            cache_misses_while_fetching,
        }
    }

    pub(super) fn record_cache_hit(&self) {
        self.cache_hits.inc(1);
    }

    pub(super) fn record_cache_miss(&self) {
        self.cache_misses.inc(1);
    }

    pub(super) fn record_cache_miss_while_fetching(&self) {
        self.cache_misses_while_fetching.inc(1);
    }
}

#[derive(Debug)]
pub(super) struct SizeMetrics {
    cache_size_bytes: U64Gauge,
    cache_size_n_files: U64Gauge,
}

pub(super) const CACHE_SIZE_MB_NAME: &str = "influxdb3_parquet_cache_size_bytes";
pub(super) const CACHE_SIZE_N_FILES_NAME: &str = "influxdb3_parquet_cache_size_number_of_files";

impl SizeMetrics {
    pub(super) fn new(metric_registry: &Registry) -> Self {
        let cache_size_bytes = metric_registry
            .register_metric::<U64Gauge>(
                CACHE_SIZE_MB_NAME,
                "track size of in-memory parquet cache",
            )
            .recorder(&[]);
        let cache_size_n_files = metric_registry
            .register_metric::<U64Gauge>(
                CACHE_SIZE_N_FILES_NAME,
                "track number of files in the in-memory parquet cache",
            )
            .recorder(&[]);
        Self {
            cache_size_bytes,
            cache_size_n_files,
        }
    }

    pub(super) fn record_file_addition(&self, size_bytes: u64) {
        self.cache_size_bytes.inc(size_bytes);
        self.cache_size_n_files.inc(1);
    }

    pub(super) fn record_file_deletions(&self, total_size_bytes: u64, n_files: u64) {
        self.cache_size_bytes.dec(total_size_bytes);
        self.cache_size_n_files.dec(n_files);
    }
}
