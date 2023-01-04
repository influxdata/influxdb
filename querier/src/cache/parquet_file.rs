//! ParquetFile cache

use backoff::{Backoff, BackoffConfig};
use cache_system::{
    backend::policy::{
        lru::{LruPolicy, ResourcePool},
        remove_if::{RemoveIfHandle, RemoveIfPolicy},
        PolicyBackend,
    },
    cache::{driver::CacheDriver, metrics::CacheWithMetrics, Cache},
    loader::{metrics::MetricsLoader, FunctionLoader},
    resource_consumption::FunctionEstimator,
};
use data_types::{ParquetFile, SequenceNumber, TableId};
use iox_catalog::interface::Catalog;
use iox_time::TimeProvider;
use observability_deps::tracing::debug;
use parking_lot::RwLock;
use snafu::{ResultExt, Snafu};
use std::{collections::HashMap, mem, sync::Arc};
use trace::span::Span;
use uuid::Uuid;

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

    /// return the underlying files as a new Vec
    #[cfg(test)]
    fn vec(&self) -> Vec<Arc<ParquetFile>> {
        self.files.as_ref().clone()
    }

    /// Estimate the memory consumption of this object and its contents
    fn size(&self) -> usize {
        // simplify accounting by ensuring len and capacity of vector are the same
        assert_eq!(self.files.len(), self.files.capacity());

        // Note size_of_val is the size of the Arc
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
    remove_if_handle: RemoveIfHandle<TableId, Arc<CachedParquetFiles>>,

    /// Number of persisted Parquet files per table ID per ingester UUID that ingesters have told
    /// us about. When a call to `get` includes a number of persisted Parquet files for this table
    /// and a particular ingester UUID that doesn't match what we've previously seen, the cache
    /// needs to be expired.
    persisted_file_counts_from_ingesters: RwLock<HashMap<TableId, HashMap<Uuid, u64>>>,
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
        let loader = FunctionLoader::new(move |table_id: TableId, _extra: ()| {
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
            persisted_file_counts_from_ingesters: Default::default(),
        }
    }

    /// Get list of cached parquet files, by table ID. This API is designed to be called with a
    /// response from the ingster(s) so there is a single place where the invalidation logic
    /// is handled.
    ///
    /// # Expiration
    ///
    /// If a `max_parquet_sequence_number` is specified, assume we are in the write buffer path.
    /// If `max_parquet_sequence_number` is `None`, check to see if
    /// `persisted_file_counts_by_ingester_uuid` is specified, which means we are in the RPC write
    /// path and need to check ingester2 UUIDs and their associated file counts returned from the
    /// ingester requests.
    ///
    /// ## Write Buffer path (based on sequence number)
    ///
    /// Clear the Parquet file cache if the cache does not contain any files that have the
    /// specified `max_parquet_sequence_number`.
    ///
    /// Returns true if the cache was cleared (it will be refreshed on the next call to get).
    ///
    /// If a `max_parquet_sequence_number` is supplied that is not in our cache, it means the
    /// ingester has written new data to the catalog and the cache is out of date.
    ///
    /// ## RPC write path (based on ingester UUIDs and persisted file counts)
    ///
    /// Clear the Parquet file cache if the information from the ingesters contains an ingester
    /// UUID we have never seen before (which indicates an ingester started or restarted) or if an
    /// ingester UUID we *have* seen before reports a different number of persisted Parquet files
    /// than what we've previously been told from the ingesters for this table. Otherwise, we can
    /// use the cache.
    pub async fn get(
        &self,
        table_id: TableId,
        max_parquet_sequence_number: Option<SequenceNumber>,
        persisted_file_counts_by_ingester_uuid: Option<HashMap<Uuid, u64>>,
        span: Option<Span>,
    ) -> Arc<CachedParquetFiles> {
        // Make a copy of the information we've stored about the ingesters and the number of files
        // they've persisted, for the closure to use to decide whether to expire the cache.
        let stored_counts = self
            .persisted_file_counts_from_ingesters
            .read()
            .get(&table_id)
            .cloned();

        // Update the stored information with what we've just seen from the ingesters, if anything.
        if let Some(ingester_counts) = &persisted_file_counts_by_ingester_uuid {
            self.persisted_file_counts_from_ingesters
                .write()
                .entry(table_id)
                .and_modify(|existing| existing.extend(ingester_counts))
                .or_insert_with(|| ingester_counts.clone());
        }

        self.remove_if_handle
            .remove_if_and_get(
                &self.cache,
                table_id,
                |cached_file| {
                    if let Some(max_parquet_sequence_number) = max_parquet_sequence_number {
                        let max_cached = cached_file.max_parquet_sequence_number();

                        let expire = if let Some(max_cached) = max_cached {
                            max_cached < max_parquet_sequence_number
                        } else {
                            // a max sequence was provided but there were no
                            // files in the cache. Means we need to refresh
                            true
                        };

                        debug!(
                            expire,
                            ?max_cached,
                            max_parquet_sequence_number = max_parquet_sequence_number.get(),
                            table_id = table_id.get(),
                            "expire parquet file cache",
                        );

                        expire
                    } else if let Some(ingester_counts) = &persisted_file_counts_by_ingester_uuid {
                        // If there's new or different information about the ingesters or the
                        // number of files they've persisted, we need to refresh.
                        new_or_different(stored_counts.as_ref(), ingester_counts)
                    } else {
                        false
                    }
                },
                ((), span),
            )
            .await
    }

    /// Mark the entry for table_id as expired (and needs a refresh)
    #[cfg(test)]
    pub fn expire(&self, table_id: TableId) {
        self.remove_if_handle.remove_if(&table_id, |_| true);
    }
}

fn new_or_different(
    stored_counts: Option<&HashMap<Uuid, u64>>,
    ingester_counts: &HashMap<Uuid, u64>,
) -> bool {
    // If we have some information stored for this table,
    if let Some(stored) = stored_counts {
        // Go through all the ingester info we just got
        for (ingester_uuid, ingester_file_count) in ingester_counts {
            // Look up the value we've stored for this ingester UUID (if any)
            let stored_file_count = stored.get(ingester_uuid);
            match stored_file_count {
                // If we've never seen this UUID before, we need to refresh the cache.
                None => return true,
                // If we've seen this UUID before but the file count we have stored is different,
                // we need to refresh the cache
                Some(s) if s != ingester_file_count => return true,
                // Otherwise, the file count is the same and we DON'T need to refresh the cache on
                // account of this ingester; keep checking the rest.
                Some(_) => (),
            }
        }
        // If we get to this point, all ingester UUIDs and all counts we just got from the ingester
        // requests match up, and we don't need to refresh the cache.
        false
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
        let cached_files = cache.get(table.table.id, None, None, None).await.vec();

        assert_eq!(cached_files.len(), 1);
        let expected_parquet_file = &tfile.parquet_file;
        assert_eq!(cached_files[0].as_ref(), expected_parquet_file);

        // validate a second request doesn't result in a catalog request
        assert_histogram_metric_count(&catalog.metric_registry, METRIC_NAME, 1);
        cache.get(table.table.id, None, None, None).await;
        assert_histogram_metric_count(&catalog.metric_registry, METRIC_NAME, 1);
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

        let cached_files = cache.get(table1.table.id, None, None, None).await.vec();
        assert_eq!(cached_files.len(), 1);
        let expected_parquet_file = &tfile1.parquet_file;
        assert_eq!(cached_files[0].as_ref(), expected_parquet_file);

        let cached_files = cache.get(table2.table.id, None, None, None).await.vec();
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

        let cached_files = cache.get(table.table.id, None, None, None).await.vec();
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
        let cached_files = cache.get(table_id, None, None, None).await;
        assert_eq!(cached_files.size(), single_file_size);

        // add a second file, and force the cache to find it
        let builder = TestParquetFileBuilder::default().with_line_protocol(TABLE1_LINE_PROTOCOL);
        partition.create_parquet_file(builder).await;
        cache.expire(table_id);
        let cached_files = cache.get(table_id, None, None, None).await;
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
            cache.get(table_id, None, None, None).await.ids(),
            ids(&[&tfile1_2, &tfile1_3])
        );

        // simulate request with sequence number 2
        // should not expire anything
        assert_histogram_metric_count(&catalog.metric_registry, METRIC_NAME, 1);
        assert_eq!(
            cache
                .get(table_id, Some(sequence_number_2), None, None)
                .await
                .ids(),
            ids(&[&tfile1_2, &tfile1_3])
        );
        assert_histogram_metric_count(&catalog.metric_registry, METRIC_NAME, 1);

        // simulate request with no sequence number
        // should not expire anything
        assert_eq!(
            cache.get(table_id, None, None, None).await.ids(),
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
            cache.get(table_id, None, None, None).await.ids(),
            ids(&[&tfile1_2, &tfile1_3])
        );

        // new request includes sequence 10 and causes a cache refresh
        // now cache has tfile!_10 (yay!)
        assert_eq!(
            cache
                .get(table_id, Some(sequence_number_10), None, None)
                .await
                .ids(),
            ids(&[&tfile1_2, &tfile1_3, &tfile1_10])
        );
        assert_histogram_metric_count(&catalog.metric_registry, METRIC_NAME, 2);
    }

    #[tokio::test]
    async fn ingester2_uuid_file_counts() {
        let (catalog, table, _partition) = make_catalog().await;
        let uuid = Uuid::new_v4();
        let table_id = table.table.id;
        let cache = make_cache(&catalog);

        // No metadata: make one request that should be cached
        cache.get(table_id, None, None, None).await;
        assert_histogram_metric_count(&catalog.metric_registry, METRIC_NAME, 1);
        cache.get(table_id, None, None, None).await;
        assert_histogram_metric_count(&catalog.metric_registry, METRIC_NAME, 1);

        // Empty metadata: make one request, should still be cached
        cache.get(table_id, None, Some(HashMap::new()), None).await;
        assert_histogram_metric_count(&catalog.metric_registry, METRIC_NAME, 1);

        // See a new UUID: refresh the cache
        cache
            .get(table_id, None, Some(HashMap::from([(uuid, 3)])), None)
            .await;
        assert_histogram_metric_count(&catalog.metric_registry, METRIC_NAME, 2);

        // See the same UUID with the same count: should still be cached
        cache
            .get(table_id, None, Some(HashMap::from([(uuid, 3)])), None)
            .await;
        assert_histogram_metric_count(&catalog.metric_registry, METRIC_NAME, 2);

        // See the same UUID with a different count: refresh the cache
        cache
            .get(table_id, None, Some(HashMap::from([(uuid, 4)])), None)
            .await;
        assert_histogram_metric_count(&catalog.metric_registry, METRIC_NAME, 3);

        // Empty metadata again: still use the cache
        cache.get(table_id, None, Some(HashMap::new()), None).await;
        assert_histogram_metric_count(&catalog.metric_registry, METRIC_NAME, 3);

        // See a new UUID and not the old one: refresh the cache
        let new_uuid = Uuid::new_v4();
        cache
            .get(table_id, None, Some(HashMap::from([(new_uuid, 1)])), None)
            .await;
        assert_histogram_metric_count(&catalog.metric_registry, METRIC_NAME, 4);

        // See the "new" UUID *and* the old UUID with the same counts as seen before: still use the
        // cache
        cache
            .get(
                table_id,
                None,
                Some(HashMap::from([(new_uuid, 1), (uuid, 4)])),
                None,
            )
            .await;
        assert_histogram_metric_count(&catalog.metric_registry, METRIC_NAME, 4);
    }

    #[tokio::test]
    async fn test_expire_empty() {
        let (catalog, table, partition) = make_catalog().await;
        let cache = make_cache(&catalog);
        let table_id = table.table.id;

        // no parquet files, should be none
        assert!(cache.get(table_id, None, None, None).await.files.is_empty());
        assert_histogram_metric_count(&catalog.metric_registry, METRIC_NAME, 1);

        // second request should be cached
        assert!(cache.get(table_id, None, None, None).await.files.is_empty());
        assert_histogram_metric_count(&catalog.metric_registry, METRIC_NAME, 1);

        // Calls to expire if there is no known persisted file, should still be cached
        assert!(cache.get(table_id, None, None, None).await.files.is_empty());
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
        assert!(cache.get(table_id, None, None, None).await.files.is_empty());
        assert_histogram_metric_count(&catalog.metric_registry, METRIC_NAME, 1);

        // Now call to expire with knowledge of new file, will cause a cache refresh
        assert_eq!(
            cache
                .get(table_id, Some(sequence_number_1), None, None)
                .await
                .ids(),
            ids(&[&tfile])
        );
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
        let shard1 = ns.create_shard(1).await;

        let partition = table.with_shard(&shard1).create_partition("k").await;

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
