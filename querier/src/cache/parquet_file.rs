//! ParquetFile cache

use backoff::{Backoff, BackoffConfig};
use cache_system::{
    backend::policy::{
        lru::{LruPolicy, ResourcePool},
        remove_if::{RemoveIfHandle, RemoveIfPolicy},
        ttl::{ConstantValueTtlProvider, TtlPolicy},
        PolicyBackend,
    },
    cache::{driver::CacheDriver, metrics::CacheWithMetrics, Cache},
    loader::{metrics::MetricsLoader, FunctionLoader},
    resource_consumption::FunctionEstimator,
};
use data_types::{ParquetFile, TableId};
use iox_catalog::interface::Catalog;
use iox_time::TimeProvider;
use snafu::{ResultExt, Snafu};
use std::{collections::HashMap, mem, sync::Arc, time::Duration};
use trace::span::Span;
use uuid::Uuid;

use super::ram::RamSize;

/// Duration to keep cached view.
///
/// This is currently `12h`.
pub const TTL: Duration = Duration::from_secs(12 * 60 * 60);

const CACHE_ID: &str = "parquet_file";

#[derive(Debug, Snafu)]
#[allow(missing_copy_implementations, missing_docs)]
pub enum Error {
    #[snafu(display("CatalogError refreshing parquet file cache: {}", source))]
    Catalog {
        source: iox_catalog::interface::Error,
    },
}

type IngesterCounts = Option<Arc<[(Uuid, u64)]>>;

/// Holds catalog information about a parquet file
#[derive(Debug)]
pub struct CachedParquetFiles {
    /// Parquet catalog information
    pub files: Arc<[Arc<ParquetFile>]>,

    /// Number of persisted Parquet files per table ID per ingester UUID that ingesters have told
    /// us about. When a call to `get` includes a number of persisted Parquet files for this table
    /// and a particular ingester UUID that doesn't match what we've previously seen, the cache
    /// needs to be expired.
    ///
    /// **This list is sorted!**
    persisted_file_counts_from_ingesters: IngesterCounts,
}

impl CachedParquetFiles {
    fn new(
        parquet_files: Vec<ParquetFile>,
        persisted_file_counts_from_ingesters: IngesterCounts,
    ) -> Self {
        let files = parquet_files.into_iter().map(Arc::new).collect();

        Self {
            files,
            persisted_file_counts_from_ingesters,
        }
    }

    /// return the underlying files as a new Vec
    #[cfg(test)]
    fn vec(&self) -> Vec<Arc<ParquetFile>> {
        self.files.as_ref().to_vec()
    }

    /// Estimate the memory consumption of this object and its contents
    fn size(&self) -> usize {
        // simplify accounting by ensuring len and capacity of vector are the same
        assert_eq!(self.files.len(), self.files.len());

        // Note size_of_val is the size of the Arc
        // https://play.rust-lang.org/?version=stable&mode=debug&edition=2021&gist=ae8fee8b4f7f5f013dc01ea1fda165da

        // size of the Arc+(Option+HashMap) itself
        mem::size_of_val(self) +
        // Vec overhead
            mem::size_of_val(self.files.as_ref()) +
        // size of the underlying parquet files
            self.files.iter().map(|f| f.size()).sum::<usize>() +
        // hashmap data
            self.persisted_file_counts_from_ingesters
                .as_ref()
                .map(|map| {
                    std::mem::size_of_val(map.as_ref()) +
                        map.len() * mem::size_of::<(Uuid, u64)>()
                }).unwrap_or_default()
    }
}

type CacheT = Box<
    dyn Cache<
        K = TableId,
        V = Arc<CachedParquetFiles>,
        GetExtra = (IngesterCounts, Option<Span>),
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
    remove_if_handle: RemoveIfHandle<TableId, Arc<CachedParquetFiles>>,
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
        let loader = FunctionLoader::new(move |table_id: TableId, extra: IngesterCounts| {
            let catalog = Arc::clone(&catalog);
            let backoff_config = backoff_config.clone();

            async move {
                Backoff::new(&backoff_config)
                    .retry_all_errors("get parquet_files", || {
                        let extra = extra.clone();
                        async {
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

                            Ok(Arc::new(CachedParquetFiles::new(parquet_files, extra)))
                                as std::result::Result<_, Error>
                        }
                    })
                    .await
                    .expect("retry forever")
            }
        });
        let loader = Arc::new(MetricsLoader::new(
            loader,
            CACHE_ID,
            Arc::clone(&time_provider),
            metric_registry,
            testing,
        ));

        let mut backend = PolicyBackend::hashmap_backed(Arc::clone(&time_provider));
        let (policy_constructor, remove_if_handle) =
            RemoveIfPolicy::create_constructor_and_handle(CACHE_ID, metric_registry);
        backend.add_policy(policy_constructor);
        backend.add_policy(LruPolicy::new(
            Arc::clone(&ram_pool),
            CACHE_ID,
            Arc::new(FunctionEstimator::new(
                |k: &TableId, v: &Arc<CachedParquetFiles>| {
                    RamSize(mem::size_of_val(k) + mem::size_of_val(v) + v.size())
                },
            )),
        ));
        backend.add_policy(TtlPolicy::new(
            Arc::new(ConstantValueTtlProvider::new(Some(TTL))),
            CACHE_ID,
            metric_registry,
        ));

        let cache = CacheDriver::new(loader, backend);
        let cache = Box::new(CacheWithMetrics::new(
            cache,
            CACHE_ID,
            time_provider,
            metric_registry,
        ));

        Self {
            cache,
            remove_if_handle,
        }
    }

    /// Get list of cached parquet files, by table ID. This API is designed to be called with a
    /// response from the ingster(s) so there is a single place where the invalidation logic
    /// is handled.
    ///
    /// # Expiration
    ///
    /// Clear the Parquet file cache if the information from the ingesters contains an ingester
    /// UUID we have never seen before (which indicates an ingester started or restarted) or if an
    /// ingester UUID we *have* seen before reports a different number of persisted Parquet files
    /// than what we've previously been told from the ingesters for this table. Otherwise, we can
    /// use the cache.
    pub async fn get(
        &self,
        table_id: TableId,
        persisted_file_counts_by_ingester_uuid: Option<HashMap<Uuid, u64>>,
        span: Option<Span>,
    ) -> Arc<CachedParquetFiles> {
        let persisted_file_counts_by_ingester_uuid =
            persisted_file_counts_by_ingester_uuid.map(|map| {
                let mut entries = map.into_iter().collect::<Vec<_>>();
                entries.sort();
                entries.into()
            });
        let persisted_file_counts_by_ingester_uuid_captured =
            persisted_file_counts_by_ingester_uuid.clone();

        self.remove_if_handle
            .remove_if_and_get(
                &self.cache,
                table_id,
                |cached_file| {
                    if let Some(ingester_counts) = &persisted_file_counts_by_ingester_uuid_captured
                    {
                        // If there's new or different information about the ingesters or the
                        // number of files they've persisted, we need to refresh.
                        different(
                            cached_file
                                .persisted_file_counts_from_ingesters
                                .as_ref()
                                .map(|x| x.as_ref()),
                            ingester_counts,
                        )
                    } else {
                        false
                    }
                },
                (persisted_file_counts_by_ingester_uuid, span),
            )
            .await
    }

    /// Mark the entry for table_id as expired (and needs a refresh)
    #[cfg(test)]
    pub fn expire(&self, table_id: TableId) {
        self.remove_if_handle.remove_if(&table_id, |_| true);
    }
}

fn different(stored_counts: Option<&[(Uuid, u64)]>, ingester_counts: &[(Uuid, u64)]) -> bool {
    // If we have some information stored for this table,
    if let Some(stored) = stored_counts {
        ingester_counts != stored
    } else {
        // Otherwise, we've never seen ingester file counts for this table.
        // If the hashmap we got is empty, then we still haven't gotten any information, so we
        // don't need to refresh the cache.
        // If we did get information about the ingester state, then we do need to refresh the cache.
        !ingester_counts.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use std::{collections::HashSet, time::Duration};

    use super::*;
    use data_types::{ColumnType, ParquetFileId};
    use iox_tests::{TestCatalog, TestNamespace, TestParquetFileBuilder, TestPartition, TestTable};

    use crate::cache::{
        ram::test_util::test_ram_pool, test_util::assert_catalog_access_metric_count,
    };

    const METRIC_NAME: &str = "parquet_list_by_table_not_to_delete";
    const TABLE1_LINE_PROTOCOL: &str = "table1 foo=1 11";
    const TABLE1_LINE_PROTOCOL2: &str = "table1 foo=2 22";
    const TABLE1_LINE_PROTOCOL3: &str = "table1 foo=3 33";
    const TABLE2_LINE_PROTOCOL: &str = "table2 foo=1 11";

    #[tokio::test]
    async fn test_parquet_chunks() {
        let (catalog, table, partition) = make_catalog().await;
        let builder = TestParquetFileBuilder::default().with_line_protocol(TABLE1_LINE_PROTOCOL);
        let tfile = partition.create_parquet_file(builder).await;

        let cache = make_cache(&catalog);
        let cached_files = cache.get(table.table.id, None, None).await.vec();

        assert_eq!(cached_files.len(), 1);
        let expected_parquet_file = &tfile.parquet_file;
        assert_eq!(cached_files[0].as_ref(), expected_parquet_file);

        // validate a second request doesn't result in a catalog request
        assert_catalog_access_metric_count(&catalog.metric_registry, METRIC_NAME, 1);
        cache.get(table.table.id, None, None).await;
        assert_catalog_access_metric_count(&catalog.metric_registry, METRIC_NAME, 1);
    }

    #[tokio::test]
    async fn test_multiple_tables() {
        let catalog = TestCatalog::new();
        let ns = catalog.create_namespace_1hr_retention("ns").await;

        let (table1, partition1) = make_table_and_partition("table1", &ns).await;
        let (table2, partition2) = make_table_and_partition("table2", &ns).await;

        let builder = TestParquetFileBuilder::default().with_line_protocol(TABLE1_LINE_PROTOCOL);
        let tfile1 = partition1.create_parquet_file(builder).await;
        let builder = TestParquetFileBuilder::default().with_line_protocol(TABLE2_LINE_PROTOCOL);
        let tfile2 = partition2.create_parquet_file(builder).await;

        let cache = make_cache(&catalog);

        let cached_files = cache.get(table1.table.id, None, None).await.vec();
        assert_eq!(cached_files.len(), 1);
        let expected_parquet_file = &tfile1.parquet_file;
        assert_eq!(cached_files[0].as_ref(), expected_parquet_file);

        let cached_files = cache.get(table2.table.id, None, None).await.vec();
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

        let cached_files = cache.get(table.table.id, None, None).await.vec();
        assert!(cached_files.is_empty());
    }

    #[tokio::test]
    async fn test_size_estimation() {
        let (catalog, table, partition) = make_catalog().await;
        let builder = TestParquetFileBuilder::default().with_line_protocol(TABLE1_LINE_PROTOCOL);
        partition.create_parquet_file(builder).await;
        let table_id = table.table.id;

        let single_file_size = 256;
        let two_file_size = 480;
        assert!(single_file_size < two_file_size);

        let cache = make_cache(&catalog);
        let cached_files = cache.get(table_id, None, None).await;
        assert_eq!(cached_files.size(), single_file_size);

        // add a second file, and force the cache to find it
        let builder = TestParquetFileBuilder::default().with_line_protocol(TABLE1_LINE_PROTOCOL);
        partition.create_parquet_file(builder).await;
        cache.expire(table_id);
        let cached_files = cache.get(table_id, None, None).await;
        assert_eq!(cached_files.size(), two_file_size);
    }

    #[tokio::test]
    async fn ingester_uuid_file_counts() {
        let (catalog, table, _partition) = make_catalog().await;
        let uuid = Uuid::new_v4();
        let table_id = table.table.id;
        let cache = make_cache(&catalog);

        // No metadata: make one request that should be cached
        cache.get(table_id, None, None).await;
        assert_catalog_access_metric_count(&catalog.metric_registry, METRIC_NAME, 1);
        cache.get(table_id, None, None).await;
        assert_catalog_access_metric_count(&catalog.metric_registry, METRIC_NAME, 1);

        // Empty metadata: make one request, should still be cached
        cache.get(table_id, Some(HashMap::new()), None).await;
        assert_catalog_access_metric_count(&catalog.metric_registry, METRIC_NAME, 1);

        // See a new UUID: refresh the cache
        cache
            .get(table_id, Some(HashMap::from([(uuid, 3)])), None)
            .await;
        assert_catalog_access_metric_count(&catalog.metric_registry, METRIC_NAME, 2);

        // See the same UUID with the same count: should still be cached
        cache
            .get(table_id, Some(HashMap::from([(uuid, 3)])), None)
            .await;
        assert_catalog_access_metric_count(&catalog.metric_registry, METRIC_NAME, 2);

        // See the same UUID with a different count: refresh the cache
        cache
            .get(table_id, Some(HashMap::from([(uuid, 4)])), None)
            .await;
        assert_catalog_access_metric_count(&catalog.metric_registry, METRIC_NAME, 3);

        // Empty metadata again: still use the cache
        cache.get(table_id, Some(HashMap::new()), None).await;
        assert_catalog_access_metric_count(&catalog.metric_registry, METRIC_NAME, 4);

        // See a new UUID and not the old one: refresh the cache
        let new_uuid = Uuid::new_v4();
        cache
            .get(table_id, Some(HashMap::from([(new_uuid, 1)])), None)
            .await;
        assert_catalog_access_metric_count(&catalog.metric_registry, METRIC_NAME, 5);
    }

    #[tokio::test]
    async fn test_ttl() {
        let (catalog, table, partition) = make_catalog().await;
        let builder = TestParquetFileBuilder::default()
            .with_line_protocol(TABLE1_LINE_PROTOCOL)
            .with_creation_time(catalog.time_provider.now());
        let tfile1 = partition.create_parquet_file(builder).await;
        let builder = TestParquetFileBuilder::default()
            .with_line_protocol(TABLE1_LINE_PROTOCOL2)
            .with_creation_time(catalog.time_provider.now());
        let tfile2 = partition.create_parquet_file(builder).await;

        let cache = make_cache(&catalog);
        let mut cached_files = cache.get(table.table.id, None, None).await.vec();
        cached_files.sort_by_key(|f| f.id);
        assert_eq!(cached_files.len(), 2);
        assert_eq!(cached_files[0].as_ref(), &tfile1.parquet_file);
        assert_eq!(cached_files[1].as_ref(), &tfile2.parquet_file);

        // update state
        // replace first file and add a new one
        catalog.mock_time_provider().inc(Duration::from_secs(60));
        let builder = TestParquetFileBuilder::default()
            .with_line_protocol(TABLE1_LINE_PROTOCOL)
            .with_creation_time(catalog.time_provider.now());
        let tfile3 = partition.create_parquet_file(builder).await;
        let builder = TestParquetFileBuilder::default()
            .with_line_protocol(TABLE1_LINE_PROTOCOL3)
            .with_creation_time(catalog.time_provider.now());
        let tfile4 = partition.create_parquet_file(builder).await;
        tfile1.flag_for_delete().await;

        // validate a second request doesn't result in a catalog request
        assert_catalog_access_metric_count(&catalog.metric_registry, METRIC_NAME, 1);
        let cached_files = cache.get(table.table.id, None, None).await.vec();
        assert_catalog_access_metric_count(&catalog.metric_registry, METRIC_NAME, 1);

        // still the old data
        assert_eq!(cached_files.len(), 2);
        assert_eq!(cached_files[0].as_ref(), &tfile1.parquet_file);
        assert_eq!(cached_files[1].as_ref(), &tfile2.parquet_file);

        // trigger TTL
        catalog.mock_time_provider().inc(TTL);
        let mut cached_files = cache.get(table.table.id, None, None).await.vec();
        cached_files.sort_by_key(|f| f.id);
        assert_catalog_access_metric_count(&catalog.metric_registry, METRIC_NAME, 2);
        assert_eq!(cached_files.len(), 3);
        assert_eq!(cached_files[0].as_ref(), &tfile2.parquet_file);
        assert_eq!(cached_files[1].as_ref(), &tfile3.parquet_file);
        assert_eq!(cached_files[2].as_ref(), &tfile4.parquet_file);
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
        let ns = catalog.create_namespace_1hr_retention("ns").await;

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

        let partition = table.create_partition("k").await;

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
