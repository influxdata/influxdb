//! ParquetFile cache

use backoff::{Backoff, BackoffConfig};
use cache_system::{
    backend::{
        lru::{LruBackend, ResourcePool},
        resource_consumption::FunctionEstimator,
        shared::SharedBackend,
    },
    cache::{driver::CacheDriver, metrics::CacheWithMetrics, Cache},
    loader::{metrics::MetricsLoader, FunctionLoader},
};
use data_types::{ParquetFile, SequenceNumber, TableId};
use iox_catalog::interface::Catalog;
use iox_time::TimeProvider;
use snafu::{ResultExt, Snafu};
use std::{collections::HashMap, mem, sync::Arc};
use trace::span::Span;

use super::ram::RamSize;

const CACHE_ID: &str = "parquet_file";

#[derive(Debug, Snafu)]
#[allow(missing_copy_implementations, missing_docs)]
pub enum Error {
    #[snafu(display("CatalogError refreshing parquet file cache: {}", source))]
    Catalog {
        source: iox_catalog::interface::Error,
    },
}

/// Holds catalog information about a parquet file
#[derive(Debug)]
pub struct CachedParquetFiles {
    /// Parquet catalog information
    pub files: Arc<Vec<Arc<ParquetFile>>>,
}

impl CachedParquetFiles {
    fn new(parquet_files: Vec<ParquetFile>) -> Self {
        let files: Vec<_> = parquet_files.into_iter().map(Arc::new).collect();

        Self {
            files: Arc::new(files),
        }
    }

    /// return the underying files as a new Vec
    #[cfg(test)]
    fn vec(&self) -> Vec<Arc<ParquetFile>> {
        self.files.as_ref().clone()
    }

    /// Estimate the memory consumption of this object and its contents
    fn size(&self) -> usize {
        // simplify accounting by ensuring len and capacity of vector are the same
        assert_eq!(self.files.len(), self.files.capacity());

        // Note size_of_val is the size of the Azrc
        // https://play.rust-lang.org/?version=stable&mode=debug&edition=2021&gist=ae8fee8b4f7f5f013dc01ea1fda165da

        // size of the Arc itself
        mem::size_of_val(self) +
        // Vec overhead
            mem::size_of_val(self.files.as_ref()) +
        // size of the underlying parquet files
            self.files.iter().map(|f| f.size()).sum::<usize>()
    }

    /// Returns the greatest parquet sequence number stored in this cache entry
    pub(crate) fn max_parquet_sequence_number(&self) -> Option<SequenceNumber> {
        self.files.iter().map(|f| f.max_sequence_number).max()
    }
}

type CacheT = Box<
    dyn Cache<
        K = TableId,
        V = Arc<CachedParquetFiles>,
        GetExtra = ((), Option<Span>),
        PeekExtra = ((), Option<Span>),
    >,
>;

/// Cache for parquet file information.
///
/// DOES NOT CACHE the actual parquet bytes from object store
#[derive(Debug)]
pub struct ParquetFileCache {
    cache: CacheT,

    /// Handle that allows clearing entries for existing cache entries
    backend: SharedBackend<TableId, Arc<CachedParquetFiles>>,
}

impl ParquetFileCache {
    /// Create new empty cache.
    pub fn new(
        catalog: Arc<dyn Catalog>,
        backoff_config: BackoffConfig,
        time_provider: Arc<dyn TimeProvider>,
        metric_registry: &metric::Registry,
        ram_pool: Arc<ResourcePool<RamSize>>,
        testing: bool,
    ) -> Self {
        let loader = Box::new(FunctionLoader::new(move |table_id: TableId, _extra: ()| {
            let catalog = Arc::clone(&catalog);
            let backoff_config = backoff_config.clone();

            async move {
                Backoff::new(&backoff_config)
                    .retry_all_errors("get parquet_files", || async {
                        // TODO refreshing all parquet files for the
                        // entire table is likely to be quite wasteful
                        // for large tables.
                        //
                        // Some this code could be more efficeint:
                        //
                        // 1. incrementally fetch only NEW parquet
                        // files that aren't already in the cache
                        //
                        // 2. track time ranges needed for queries and
                        // limit files fetched to what is actually
                        // needed
                        let parquet_files: Vec<_> = catalog
                            .repositories()
                            .await
                            .parquet_files()
                            .list_by_table_not_to_delete(table_id)
                            .await
                            .context(CatalogSnafu)?;

                        Ok(Arc::new(CachedParquetFiles::new(parquet_files)))
                            as std::result::Result<_, Error>
                    })
                    .await
                    .expect("retry forever")
            }
        }));
        let loader = Arc::new(MetricsLoader::new(
            loader,
            CACHE_ID,
            Arc::clone(&time_provider),
            metric_registry,
            testing,
        ));

        // add to memory pool
        let backend = Box::new(LruBackend::new(
            Box::new(HashMap::new()),
            Arc::clone(&ram_pool),
            CACHE_ID,
            Arc::new(FunctionEstimator::new(
                |k: &TableId, v: &Arc<CachedParquetFiles>| {
                    RamSize(mem::size_of_val(k) + mem::size_of_val(v) + v.size())
                },
            )),
        ));

        // get a direct handle so we can clear out entries as needed
        let backend = SharedBackend::new(backend, CACHE_ID, metric_registry);

        let cache = Box::new(CacheDriver::new(loader, Box::new(backend.clone())));
        let cache = Box::new(CacheWithMetrics::new(
            cache,
            CACHE_ID,
            time_provider,
            metric_registry,
        ));

        Self { cache, backend }
    }

    /// Get list of cached parquet files, by table id
    pub async fn get(&self, table_id: TableId, span: Option<Span>) -> Arc<CachedParquetFiles> {
        self.cache.get(table_id, ((), span)).await
    }

    /// Mark the entry for table_id as expired (and needs a refresh)
    #[cfg(test)]
    pub fn expire(&self, table_id: TableId) {
        self.backend.remove_if(&table_id, |_| true);
    }

    /// Clear the parquet file cache if the cache does not contain any
    /// files that have the specified `max_parquet_sequence_number`.
    ///
    /// If `None` is passed, returns false and does not clear the cache.
    ///
    /// Returns true if the cache was cleared (it will be refreshed on
    /// the next call to get).
    ///
    /// This API is designed to be called with a response from the
    /// ingster so there is a single place were the invalidation logic
    /// is handled. An `Option` is accepted because the ingester may
    /// or may or may not have a `max_parquet_sequence_number`.
    ///
    /// If a `max_parquet_sequence_number` is supplied that is not in
    /// our cache, it means the ingester has written new data to the
    /// catalog and the cache is out of date.
    pub fn expire_on_newly_persisted_files(
        &self,
        table_id: TableId,
        max_parquet_sequence_number: Option<SequenceNumber>,
    ) -> bool {
        if let Some(max_parquet_sequence_number) = max_parquet_sequence_number {
            // check backend cache to see if the maximum sequence
            // number desired is less than what we know about
            self.backend.remove_if(&table_id, |cached_file| {
                let max_cached = cached_file.max_parquet_sequence_number();

                if let Some(max_cached) = max_cached {
                    max_cached < max_parquet_sequence_number
                } else {
                    // a max sequence was provided but there were no
                    // files in the cache. Means we need to refresh
                    true
                }
            })
        } else {
            false
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use super::*;
    use data_types::{ColumnType, ParquetFileId};
    use iox_tests::util::{
        TestCatalog, TestNamespace, TestParquetFile, TestParquetFileBuilder, TestPartition,
        TestTable,
    };

    use crate::cache::{ram::test_util::test_ram_pool, test_util::assert_histogram_metric_count};

    const METRIC_NAME: &str = "parquet_list_by_table_not_to_delete";
    const TABLE1_LINE_PROTOCOL: &str = "table1 foo=1 11";
    const TABLE2_LINE_PROTOCOL: &str = "table2 foo=1 11";

    #[tokio::test]
    async fn test_parquet_chunks() {
        let (catalog, table, partition) = make_catalog().await;
        let builder = TestParquetFileBuilder::default().with_line_protocol(TABLE1_LINE_PROTOCOL);
        let tfile = partition.create_parquet_file(builder).await;

        let cache = make_cache(&catalog);
        let cached_files = cache.get(table.table.id, None).await.vec();

        assert_eq!(cached_files.len(), 1);
        let expected_parquet_file = &tfile.parquet_file;
        assert_eq!(cached_files[0].as_ref(), expected_parquet_file);

        // validate a second request doens't result in a catalog request
        assert_histogram_metric_count(&catalog.metric_registry, METRIC_NAME, 1);
        cache.get(table.table.id, None).await;
        assert_histogram_metric_count(&catalog.metric_registry, METRIC_NAME, 1);
    }

    #[tokio::test]
    async fn test_multiple_tables() {
        let catalog = TestCatalog::new();
        let ns = catalog.create_namespace("ns").await;

        let (table1, partition1) = make_table_and_partition("table1", &ns).await;
        let (table2, partition2) = make_table_and_partition("table2", &ns).await;

        let builder = TestParquetFileBuilder::default().with_line_protocol(TABLE1_LINE_PROTOCOL);
        let tfile1 = partition1.create_parquet_file(builder).await;
        let builder = TestParquetFileBuilder::default().with_line_protocol(TABLE2_LINE_PROTOCOL);
        let tfile2 = partition2.create_parquet_file(builder).await;

        let cache = make_cache(&catalog);

        let cached_files = cache.get(table1.table.id, None).await.vec();
        assert_eq!(cached_files.len(), 1);
        let expected_parquet_file = &tfile1.parquet_file;
        assert_eq!(cached_files[0].as_ref(), expected_parquet_file);

        let cached_files = cache.get(table2.table.id, None).await.vec();
        assert_eq!(cached_files.len(), 1);
        let expected_parquet_file = &tfile2.parquet_file;
        assert_eq!(cached_files[0].as_ref(), expected_parquet_file);
    }

    #[tokio::test]
    async fn test_table_does_not_exist() {
        let (_catalog, table, partition) = make_catalog().await;
        let builder = TestParquetFileBuilder::default().with_line_protocol(TABLE1_LINE_PROTOCOL);
        partition.create_parquet_file(builder).await;

        // check in a different catalog where the table doesn't exist (yet)
        let different_catalog = TestCatalog::new();
        let cache = make_cache(&different_catalog);

        let cached_files = cache.get(table.table.id, None).await.vec();
        assert!(cached_files.is_empty());
    }

    #[tokio::test]
    async fn test_size_estimation() {
        let (catalog, table, partition) = make_catalog().await;
        let builder = TestParquetFileBuilder::default().with_line_protocol(TABLE1_LINE_PROTOCOL);
        partition.create_parquet_file(builder).await;
        let table_id = table.table.id;

        let single_file_size = 216;
        let two_file_size = 400;
        assert!(single_file_size < two_file_size);

        let cache = make_cache(&catalog);
        let cached_files = cache.get(table_id, None).await;
        assert_eq!(cached_files.size(), single_file_size);

        // add a second file, and force the cache to find it
        let builder = TestParquetFileBuilder::default().with_line_protocol(TABLE1_LINE_PROTOCOL);
        partition.create_parquet_file(builder).await;
        cache.expire(table_id);
        let cached_files = cache.get(table_id, None).await;
        assert_eq!(cached_files.size(), two_file_size);
    }

    #[tokio::test]
    async fn test_max_persisted_sequence_number() {
        let (catalog, table, partition) = make_catalog().await;
        let _sequence_number_1 = SequenceNumber::new(1);
        let sequence_number_2 = SequenceNumber::new(2);
        let sequence_number_3 = SequenceNumber::new(3);
        let sequence_number_10 = SequenceNumber::new(10);

        let builder = TestParquetFileBuilder::default()
            .with_line_protocol(TABLE1_LINE_PROTOCOL)
            .with_max_seq(sequence_number_2.get())
            .with_min_time(0)
            .with_max_time(100);
        let tfile1_2 = partition.create_parquet_file(builder).await;

        let builder = TestParquetFileBuilder::default()
            .with_line_protocol(TABLE1_LINE_PROTOCOL)
            .with_max_seq(sequence_number_3.get())
            .with_min_time(0)
            .with_max_time(100);
        let tfile1_3 = partition.create_parquet_file(builder).await;

        let cache = make_cache(&catalog);
        let table_id = table.table.id;
        assert_eq!(
            cache.get(table_id, None).await.ids(),
            ids(&[&tfile1_2, &tfile1_3])
        );

        // simulate request with sequence number 2
        // should not expire anything
        assert_histogram_metric_count(&catalog.metric_registry, METRIC_NAME, 1);
        cache.expire_on_newly_persisted_files(table_id, Some(sequence_number_2));
        assert_eq!(
            cache.get(table_id, None).await.ids(),
            ids(&[&tfile1_2, &tfile1_3])
        );
        assert_histogram_metric_count(&catalog.metric_registry, METRIC_NAME, 1);

        // simulate request with no sequence number
        // should not expire anything
        cache.expire_on_newly_persisted_files(table_id, None);
        assert_eq!(
            cache.get(table_id, None).await.ids(),
            ids(&[&tfile1_2, &tfile1_3])
        );
        assert_histogram_metric_count(&catalog.metric_registry, METRIC_NAME, 1);

        // new file is created, but cache is stale
        let builder = TestParquetFileBuilder::default()
            .with_line_protocol(TABLE1_LINE_PROTOCOL)
            .with_max_seq(sequence_number_10.get())
            .with_min_time(0)
            .with_max_time(100);
        let tfile1_10 = partition.create_parquet_file(builder).await;
        // cache doesn't have tfile1_10
        assert_eq!(
            cache.get(table_id, None).await.ids(),
            ids(&[&tfile1_2, &tfile1_3])
        );

        // new request includes sequence 10 and causes a cache refresh
        cache.expire_on_newly_persisted_files(table_id, Some(sequence_number_10));
        // now cache has tfile!_10 (yay!)
        assert_eq!(
            cache.get(table_id, None).await.ids(),
            ids(&[&tfile1_2, &tfile1_3, &tfile1_10])
        );
        assert_histogram_metric_count(&catalog.metric_registry, METRIC_NAME, 2);
    }

    #[tokio::test]
    async fn test_expire_empty() {
        let (catalog, table, partition) = make_catalog().await;
        let cache = make_cache(&catalog);
        let table_id = table.table.id;

        // no parquet files, sould be none
        assert!(cache.get(table_id, None).await.files.is_empty());
        assert_histogram_metric_count(&catalog.metric_registry, METRIC_NAME, 1);

        // second request should be cached
        assert!(cache.get(table_id, None).await.files.is_empty());
        assert_histogram_metric_count(&catalog.metric_registry, METRIC_NAME, 1);

        // Calls to expire if there is no known persisted file, should still be cached
        cache.expire_on_newly_persisted_files(table_id, None);
        assert!(cache.get(table_id, None).await.files.is_empty());
        assert_histogram_metric_count(&catalog.metric_registry, METRIC_NAME, 1);

        // make a new parquet file
        let sequence_number_1 = SequenceNumber::new(1);
        let builder = TestParquetFileBuilder::default()
            .with_line_protocol(TABLE1_LINE_PROTOCOL)
            .with_max_seq(sequence_number_1.get())
            .with_min_time(0)
            .with_max_time(100);
        let tfile = partition.create_parquet_file(builder).await;

        // cache is stale
        assert!(cache.get(table_id, None).await.files.is_empty());
        assert_histogram_metric_count(&catalog.metric_registry, METRIC_NAME, 1);

        // Now call to expire with knowledge of new file, will cause a cache refresh
        cache.expire_on_newly_persisted_files(table_id, Some(sequence_number_1));
        assert_eq!(cache.get(table_id, None).await.ids(), ids(&[&tfile]));
        assert_histogram_metric_count(&catalog.metric_registry, METRIC_NAME, 2);
    }

    fn ids(files: &[&TestParquetFile]) -> HashSet<ParquetFileId> {
        files.iter().map(|f| f.parquet_file.id).collect()
    }

    /// Extracts parquet ids from various objects
    trait ParquetIds {
        fn ids(&self) -> HashSet<ParquetFileId>;
    }

    impl ParquetIds for &CachedParquetFiles {
        fn ids(&self) -> HashSet<ParquetFileId> {
            self.files.iter().map(|f| f.id).collect()
        }
    }

    impl ParquetIds for Arc<CachedParquetFiles> {
        fn ids(&self) -> HashSet<ParquetFileId> {
            self.as_ref().ids()
        }
    }

    async fn make_catalog() -> (Arc<TestCatalog>, Arc<TestTable>, Arc<TestPartition>) {
        let catalog = TestCatalog::new();
        let ns = catalog.create_namespace("ns").await;

        let (table, partition) = make_table_and_partition("table1", &ns).await;
        table.create_column("foo", ColumnType::F64).await;
        table.create_column("time", ColumnType::Time).await;
        (catalog, table, partition)
    }

    async fn make_table_and_partition(
        table_name: &str,
        ns: &Arc<TestNamespace>,
    ) -> (Arc<TestTable>, Arc<TestPartition>) {
        let table = ns.create_table(table_name).await;
        table.create_column("foo", ColumnType::F64).await;
        table.create_column("time", ColumnType::Time).await;
        let sequencer1 = ns.create_sequencer(1).await;

        let partition = table
            .with_sequencer(&sequencer1)
            .create_partition("k")
            .await;

        (table, partition)
    }

    fn make_cache(catalog: &TestCatalog) -> ParquetFileCache {
        ParquetFileCache::new(
            catalog.catalog(),
            BackoffConfig::default(),
            catalog.time_provider(),
            &catalog.metric_registry(),
            test_ram_pool(),
            true,
        )
    }
}
