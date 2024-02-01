//! Cache layer.

use std::{
    collections::{HashMap, HashSet},
    ops::ControlFlow,
    sync::Arc,
};

use async_trait::async_trait;
use backoff::{Backoff, BackoffConfig};
use catalog_cache::{
    api::quorum::{Error as QuorumError, QuorumCatalogCache},
    CacheKey, CacheValue,
};
use data_types::{
    partition_template::{NamespacePartitionTemplateOverride, TablePartitionTemplateOverride},
    snapshot::partition::PartitionSnapshot,
    snapshot::table::TableSnapshot,
    Column, ColumnType, CompactionLevel, MaxColumnsPerTable, MaxTables, Namespace, NamespaceId,
    NamespaceName, NamespaceServiceProtectionLimitsOverride, ObjectStoreId, ParquetFile,
    ParquetFileId, ParquetFileParams, Partition, PartitionId, PartitionKey, SkippedCompaction,
    SortKeyIds, Table, TableId, Timestamp,
};
use futures::{StreamExt, TryStreamExt};
use generated_types::influxdata::iox::catalog_cache::v1 as proto;
use generated_types::prost::Message;
use iox_time::TimeProvider;
use observability_deps::tracing::{debug, warn};

use crate::{
    interface::{
        CasFailure, Catalog, ColumnRepo, Error, NamespaceRepo, ParquetFileRepo, PartitionRepo,
        RepoCollection, Result, SoftDeletedRows, TableRepo,
    },
    metrics::MetricDecorator,
};

/// Caching catalog.
#[derive(Debug)]
pub struct CachingCatalog {
    backing: Arc<dyn Catalog>,
    cache: Arc<QuorumCatalogCache>,
    metrics: Arc<metric::Registry>,
    time_provider: Arc<dyn TimeProvider>,
    quorum_fanout: usize,
    backoff_config: Arc<BackoffConfig>,
}

impl CachingCatalog {
    /// Create new caching catalog.
    ///
    /// Sets:
    /// - `cache`: quorum-based cache
    /// - `backing`: underlying backing catalog
    /// - `metrics`: metrics registry
    /// - `time_provider`: time provider, used for metrics
    /// - `quorum_fanout`: number of concurrent quorum operations that a single request can trigger
    pub fn new(
        cache: Arc<QuorumCatalogCache>,
        backing: Arc<dyn Catalog>,
        metrics: Arc<metric::Registry>,
        time_provider: Arc<dyn TimeProvider>,
        quorum_fanout: usize,
    ) -> Self {
        let backoff_config = Arc::new(BackoffConfig::default());

        Self {
            backing,
            cache,
            metrics,
            time_provider,
            quorum_fanout,
            backoff_config,
        }
    }
}

impl std::fmt::Display for CachingCatalog {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "caching")
    }
}

#[async_trait]
impl Catalog for CachingCatalog {
    async fn setup(&self) -> Result<(), Error> {
        Ok(())
    }

    fn repositories(&self) -> Box<dyn RepoCollection> {
        Box::new(MetricDecorator::new(
            Repos {
                backing: Arc::clone(&self.backing),
                cache: Arc::clone(&self.cache),
                quorum_fanout: self.quorum_fanout,
                backoff_config: Arc::clone(&self.backoff_config),
            },
            Arc::clone(&self.metrics),
            self.time_provider(),
        ))
    }

    #[cfg(test)]
    fn metrics(&self) -> Arc<metric::Registry> {
        Arc::clone(&self.metrics)
    }

    fn time_provider(&self) -> Arc<dyn TimeProvider> {
        Arc::clone(&self.time_provider)
    }
}

#[derive(Debug)]
struct Repos {
    backing: Arc<dyn Catalog>,
    cache: Arc<QuorumCatalogCache>,
    quorum_fanout: usize,
    backoff_config: Arc<BackoffConfig>,
}

impl Repos {
    /// Get data from quorum cache.
    ///
    /// This method implements retries.
    async fn get_quorum(&self, key: CacheKey) -> Result<Option<CacheValue>, Error> {
        Backoff::new(&self.backoff_config)
            .retry_with_backoff(&format!("quorum GET: {key:?}"), || async move {
                match self.cache.get(key).await {
                    Ok(val) => ControlFlow::Break(Ok(val)),
                    Err(e @ QuorumError::Quorum { .. }) => ControlFlow::Continue(e),
                    Err(e) => ControlFlow::Break(Err(Error::from(e))),
                }
            })
            .await
            .map_err(|e| Error::External {
                source: Box::new(e),
            })?
    }

    /// Refresh cached value of given partition.
    ///
    /// This requests a new snapshot and performs a quorum-write.
    ///
    /// Note that this also performs a snapshot+write if the partition was NOT cached yet.
    async fn refresh_partition(&self, partition_id: PartitionId) -> Result<PartitionSnapshot> {
        let snapshot = self
            .backing
            .repositories()
            .partitions()
            .snapshot(partition_id)
            .await?;
        assert_eq!(snapshot.partition_id(), partition_id);

        let generation = snapshot.generation();

        let proto: proto::Partition = snapshot.clone().into();
        let data = proto.encode_to_vec().into();

        debug!(
            partition_id = partition_id.get(),
            generation, "refresh partition",
        );
        self.cache
            .put(
                CacheKey::Partition(partition_id.get()),
                CacheValue::new(data, generation),
            )
            .await
            .map_err(|e| {
                warn!(
                    partition_id=partition_id.get(),
                    generation,
                    %e,
                    "partition quorum write failed",
                );

                e
            })?;

        Ok(snapshot)
    }

    /// Get snapshot for a partition.
    ///
    /// This first tries to quorum-read the partition. If the partition does not exist yet, this will perform a
    /// [refresh](Self::refresh_partition).
    async fn get_partition(&self, partition_id: PartitionId) -> Result<PartitionSnapshot> {
        if let Some(val) = self
            .get_quorum(CacheKey::Partition(partition_id.get()))
            .await
            .map_err(|e| {
                warn!(
                    partition_id=partition_id.get(),
                    %e,
                    "partition quorum read failed",
                );

                e
            })?
        {
            debug!(
                partition_id = partition_id.get(),
                status = "HIT",
                generation = val.generation(),
                "get partition",
            );

            let proto = proto::Partition::decode(val.data().clone())?;
            return Ok(PartitionSnapshot::decode(proto, val.generation()));
        }

        debug!(
            partition_id = partition_id.get(),
            status = "MISS",
            "get partition",
        );
        self.refresh_partition(partition_id).await
    }
}

impl RepoCollection for Repos {
    fn namespaces(&mut self) -> &mut dyn NamespaceRepo {
        self
    }

    fn tables(&mut self) -> &mut dyn TableRepo {
        self
    }

    fn columns(&mut self) -> &mut dyn ColumnRepo {
        self
    }

    fn partitions(&mut self) -> &mut dyn PartitionRepo {
        self
    }

    fn parquet_files(&mut self) -> &mut dyn ParquetFileRepo {
        self
    }
}

#[async_trait]
impl NamespaceRepo for Repos {
    async fn create(
        &mut self,
        name: &NamespaceName<'_>,
        partition_template: Option<NamespacePartitionTemplateOverride>,
        retention_period_ns: Option<i64>,
        service_protection_limits: Option<NamespaceServiceProtectionLimitsOverride>,
    ) -> Result<Namespace> {
        self.backing
            .repositories()
            .namespaces()
            .create(
                name,
                partition_template,
                retention_period_ns,
                service_protection_limits,
            )
            .await
    }

    async fn update_retention_period(
        &mut self,
        name: &str,
        retention_period_ns: Option<i64>,
    ) -> Result<Namespace> {
        self.backing
            .repositories()
            .namespaces()
            .update_retention_period(name, retention_period_ns)
            .await
    }

    async fn list(&mut self, deleted: SoftDeletedRows) -> Result<Vec<Namespace>> {
        self.backing.repositories().namespaces().list(deleted).await
    }

    async fn get_by_id(
        &mut self,
        id: NamespaceId,
        deleted: SoftDeletedRows,
    ) -> Result<Option<Namespace>> {
        self.backing
            .repositories()
            .namespaces()
            .get_by_id(id, deleted)
            .await
    }

    async fn get_by_name(
        &mut self,
        name: &str,
        deleted: SoftDeletedRows,
    ) -> Result<Option<Namespace>> {
        self.backing
            .repositories()
            .namespaces()
            .get_by_name(name, deleted)
            .await
    }

    async fn soft_delete(&mut self, name: &str) -> Result<()> {
        self.backing
            .repositories()
            .namespaces()
            .soft_delete(name)
            .await
    }

    async fn update_table_limit(&mut self, name: &str, new_max: MaxTables) -> Result<Namespace> {
        self.backing
            .repositories()
            .namespaces()
            .update_table_limit(name, new_max)
            .await
    }

    async fn update_column_limit(
        &mut self,
        name: &str,
        new_max: MaxColumnsPerTable,
    ) -> Result<Namespace> {
        self.backing
            .repositories()
            .namespaces()
            .update_column_limit(name, new_max)
            .await
    }
}

#[async_trait]
impl TableRepo for Repos {
    async fn create(
        &mut self,
        name: &str,
        partition_template: TablePartitionTemplateOverride,
        namespace_id: NamespaceId,
    ) -> Result<Table> {
        self.backing
            .repositories()
            .tables()
            .create(name, partition_template, namespace_id)
            .await
    }

    async fn get_by_id(&mut self, table_id: TableId) -> Result<Option<Table>> {
        self.backing
            .repositories()
            .tables()
            .get_by_id(table_id)
            .await
    }

    async fn get_by_namespace_and_name(
        &mut self,
        namespace_id: NamespaceId,
        name: &str,
    ) -> Result<Option<Table>> {
        self.backing
            .repositories()
            .tables()
            .get_by_namespace_and_name(namespace_id, name)
            .await
    }

    async fn list_by_namespace_id(&mut self, namespace_id: NamespaceId) -> Result<Vec<Table>> {
        self.backing
            .repositories()
            .tables()
            .list_by_namespace_id(namespace_id)
            .await
    }

    async fn list(&mut self) -> Result<Vec<Table>> {
        self.backing.repositories().tables().list().await
    }

    async fn snapshot(&mut self, table_id: TableId) -> Result<TableSnapshot> {
        self.backing
            .repositories()
            .tables()
            .snapshot(table_id)
            .await
    }
}

#[async_trait]
impl ColumnRepo for Repos {
    async fn create_or_get(
        &mut self,
        name: &str,
        table_id: TableId,
        column_type: ColumnType,
    ) -> Result<Column> {
        self.backing
            .repositories()
            .columns()
            .create_or_get(name, table_id, column_type)
            .await
    }

    async fn create_or_get_many_unchecked(
        &mut self,
        table_id: TableId,
        columns: HashMap<&str, ColumnType>,
    ) -> Result<Vec<Column>> {
        self.backing
            .repositories()
            .columns()
            .create_or_get_many_unchecked(table_id, columns)
            .await
    }

    async fn list_by_namespace_id(&mut self, namespace_id: NamespaceId) -> Result<Vec<Column>> {
        self.backing
            .repositories()
            .columns()
            .list_by_namespace_id(namespace_id)
            .await
    }

    async fn list_by_table_id(&mut self, table_id: TableId) -> Result<Vec<Column>> {
        self.backing
            .repositories()
            .columns()
            .list_by_table_id(table_id)
            .await
    }

    async fn list(&mut self) -> Result<Vec<Column>> {
        self.backing.repositories().columns().list().await
    }
}

#[async_trait]
impl PartitionRepo for Repos {
    async fn create_or_get(&mut self, key: PartitionKey, table_id: TableId) -> Result<Partition> {
        // read-through: need to wire up table snapshots to look this up efficiently
        self.backing
            .repositories()
            .partitions()
            .create_or_get(key, table_id)
            .await
    }

    async fn get_by_id_batch(&mut self, partition_ids: &[PartitionId]) -> Result<Vec<Partition>> {
        futures::stream::iter(prepare_set(partition_ids.iter().cloned()))
            .map(|p_id| {
                let this = &self;
                async move {
                    let snapshot = match this.get_partition(p_id).await {
                        Ok(s) => s,
                        Err(Error::NotFound { .. }) => {
                            return Ok(futures::stream::empty().boxed());
                        }
                        Err(e) => {
                            return Err(e);
                        }
                    };

                    match snapshot.partition() {
                        Ok(p) => Ok(futures::stream::once(async move { Ok(p) }).boxed()),
                        Err(e) => Err(Error::from(e)),
                    }
                }
            })
            .buffer_unordered(self.quorum_fanout)
            .try_flatten()
            .try_collect::<Vec<_>>()
            .await
    }

    async fn list_by_table_id(&mut self, table_id: TableId) -> Result<Vec<Partition>> {
        // read-through: need to wire up table snapshots to look this up efficiently
        self.backing
            .repositories()
            .partitions()
            .list_by_table_id(table_id)
            .await
    }

    async fn list_ids(&mut self) -> Result<Vec<PartitionId>> {
        // read-through: only used for testing, we should eventually remove this interface
        self.backing.repositories().partitions().list_ids().await
    }

    async fn cas_sort_key(
        &mut self,
        partition_id: PartitionId,
        old_sort_key_ids: Option<&SortKeyIds>,
        new_sort_key_ids: &SortKeyIds,
    ) -> Result<Partition, CasFailure<SortKeyIds>> {
        let res = self
            .backing
            .repositories()
            .partitions()
            .cas_sort_key(partition_id, old_sort_key_ids, new_sort_key_ids)
            .await?;

        self.refresh_partition(partition_id)
            .await
            .map_err(CasFailure::QueryError)?;

        Ok(res)
    }

    #[allow(clippy::too_many_arguments)]
    async fn record_skipped_compaction(
        &mut self,
        partition_id: PartitionId,
        reason: &str,
        num_files: usize,
        limit_num_files: usize,
        limit_num_files_first_in_partition: usize,
        estimated_bytes: u64,
        limit_bytes: u64,
    ) -> Result<()> {
        self.backing
            .repositories()
            .partitions()
            .record_skipped_compaction(
                partition_id,
                reason,
                num_files,
                limit_num_files,
                limit_num_files_first_in_partition,
                estimated_bytes,
                limit_bytes,
            )
            .await?;

        self.refresh_partition(partition_id).await?;

        Ok(())
    }

    async fn get_in_skipped_compactions(
        &mut self,
        partition_id: &[PartitionId],
    ) -> Result<Vec<SkippedCompaction>> {
        futures::stream::iter(prepare_set(partition_id.iter().cloned()))
            .map(|p_id| {
                let this = &self;
                async move {
                    let snapshot = match this.get_partition(p_id).await {
                        Ok(s) => s,
                        Err(Error::NotFound { .. }) => {
                            return Ok(futures::stream::empty().boxed());
                        }
                        Err(e) => {
                            return Err(e);
                        }
                    };

                    match snapshot.skipped_compaction() {
                        Some(sc) => Ok(futures::stream::once(async move { Ok(sc) }).boxed()),
                        None => Ok(futures::stream::empty().boxed()),
                    }
                }
            })
            .buffer_unordered(self.quorum_fanout)
            .try_flatten()
            .try_collect::<Vec<_>>()
            .await
    }

    async fn list_skipped_compactions(&mut self) -> Result<Vec<SkippedCompaction>> {
        // read-through: used for debugging, this should be replaced w/ proper hierarchy-traversal
        self.backing
            .repositories()
            .partitions()
            .list_skipped_compactions()
            .await
    }

    async fn delete_skipped_compactions(
        &mut self,
        partition_id: PartitionId,
    ) -> Result<Option<SkippedCompaction>> {
        let res = self
            .backing
            .repositories()
            .partitions()
            .delete_skipped_compactions(partition_id)
            .await?;

        self.refresh_partition(partition_id).await?;

        Ok(res)
    }

    async fn most_recent_n(&mut self, n: usize) -> Result<Vec<Partition>> {
        // read-through: used for ingester warm-up at the moment
        self.backing
            .repositories()
            .partitions()
            .most_recent_n(n)
            .await
    }

    async fn partitions_new_file_between(
        &mut self,
        minimum_time: Timestamp,
        maximum_time: Option<Timestamp>,
    ) -> Result<Vec<PartitionId>> {
        // read-through: used by the compactor for scheduling, we should eventually find a better interface
        self.backing
            .repositories()
            .partitions()
            .partitions_new_file_between(minimum_time, maximum_time)
            .await
    }

    async fn list_old_style(&mut self) -> Result<Vec<Partition>> {
        // read-through: used by the ingester due to hash-id stuff
        self.backing
            .repositories()
            .partitions()
            .list_old_style()
            .await
    }

    async fn snapshot(&mut self, partition_id: PartitionId) -> Result<PartitionSnapshot> {
        self.get_partition(partition_id).await
    }
}

#[async_trait]
impl ParquetFileRepo for Repos {
    async fn flag_for_delete_by_retention(&mut self) -> Result<Vec<(PartitionId, ObjectStoreId)>> {
        let res = self
            .backing
            .repositories()
            .parquet_files()
            .flag_for_delete_by_retention()
            .await?;

        let affected_partitions = res
            .iter()
            .map(|(p_id, _os_id)| *p_id)
            .collect::<HashSet<_>>();

        // ensure deterministic order
        let mut affected_partitions = affected_partitions.into_iter().collect::<Vec<_>>();
        affected_partitions.sort_unstable();

        // refresh ALL partitons that are affected, NOT just only the ones that were cached. This should avoid the
        // following "lost update" race condition:
        //
        // This scenario assumes that the partition in question is NOT cached yet.
        //
        // | T | Thread 1                              | Thread 2                                           |
        // | - | ------------------------------------- | -------------------------------------------------- |
        // | 1 | receive `create_update_delete`        |                                                    |
        // | 2 | execute change within backing catalog |                                                    |
        // | 3 | takes snapshot from backing catalog   |                                                    |
        // | 4 |                                       | receive `flag_for_delete_by_retention`             |
        // | 5 |                                       | execute change within backing catalog              |
        // | 6 |                                       | affected partition not cached => no snapshot taken |
        // | 7 |                                       | return                                             |
        // | 8 | quorum-write snapshot                 |                                                    |
        // | 9 | return                                |                                                    |
        //
        // The partition is now cached by does NOT contain the `flag_for_delete_by_retention` change and will not
        // automatically converge.
        futures::stream::iter(affected_partitions)
            .map(|p_id| {
                let this = &self;
                async move {
                    this.refresh_partition(p_id).await?;
                    Ok::<(), Error>(())
                }
            })
            .buffer_unordered(self.quorum_fanout)
            .try_collect::<()>()
            .await?;

        Ok(res)
    }

    async fn delete_old_ids_only(&mut self, older_than: Timestamp) -> Result<Vec<ObjectStoreId>> {
        // deleted files are NOT part of the snapshot, so this bypasses the cache
        self.backing
            .repositories()
            .parquet_files()
            .delete_old_ids_only(older_than)
            .await
    }

    async fn list_by_partition_not_to_delete_batch(
        &mut self,
        partition_ids: Vec<PartitionId>,
    ) -> Result<Vec<ParquetFile>> {
        futures::stream::iter(prepare_set(partition_ids))
            .map(|p_id| {
                let this = &self;
                async move {
                    let snapshot = match this.get_partition(p_id).await {
                        Ok(s) => s,
                        Err(Error::NotFound { .. }) => {
                            return Ok(futures::stream::empty().boxed());
                        }
                        Err(e) => {
                            return Err(e);
                        }
                    };

                    // Decode files so we can drop the snapshot early.
                    //
                    // Need to collect the file results into a vec though because we cannot return borrowed data and
                    // "owned iterators" aren't a thing.
                    let files = snapshot
                        .files()
                        .map(|res| res.map_err(Error::from))
                        .collect::<Vec<_>>();
                    Ok::<_, Error>(futures::stream::iter(files).boxed())
                }
            })
            .buffer_unordered(self.quorum_fanout)
            .try_flatten()
            .try_collect::<Vec<_>>()
            .await
    }

    async fn get_by_object_store_id(
        &mut self,
        object_store_id: ObjectStoreId,
    ) -> Result<Option<ParquetFile>> {
        // read-through: see https://github.com/influxdata/influxdb_iox/issues/9719
        self.backing
            .repositories()
            .parquet_files()
            .get_by_object_store_id(object_store_id)
            .await
    }

    async fn exists_by_object_store_id_batch(
        &mut self,
        object_store_ids: Vec<ObjectStoreId>,
    ) -> Result<Vec<ObjectStoreId>> {
        // read-through: this is used by the GC, so this is not overall latency-critical
        self.backing
            .repositories()
            .parquet_files()
            .exists_by_object_store_id_batch(object_store_ids)
            .await
    }

    async fn create_upgrade_delete(
        &mut self,
        partition_id: PartitionId,
        delete: &[ObjectStoreId],
        upgrade: &[ObjectStoreId],
        create: &[ParquetFileParams],
        target_level: CompactionLevel,
    ) -> Result<Vec<ParquetFileId>> {
        let res = self
            .backing
            .repositories()
            .parquet_files()
            .create_upgrade_delete(partition_id, delete, upgrade, create, target_level)
            .await?;

        self.refresh_partition(partition_id).await?;

        Ok(res)
    }
}

/// Prepare set of elements in deterministic order.
fn prepare_set<S, T>(set: S) -> Vec<T>
where
    S: IntoIterator<Item = T>,
    T: Eq + Ord,
{
    // ensure deterministic order (also required for de-dup)
    let mut set = set.into_iter().collect::<Vec<_>>();
    set.sort_unstable();

    // de-dup
    set.dedup();

    set
}

#[cfg(test)]
mod tests {
    use catalog_cache::api::server::test_util::TestCacheServer;
    use catalog_cache::local::CatalogCache;
    use iox_time::SystemProvider;

    use crate::{interface_tests::TestCatalog, mem::MemCatalog};

    use super::*;
    use std::sync::Arc;

    #[tokio::test]
    async fn test_catalog() {
        crate::interface_tests::test_catalog(|| async {
            let metrics = Arc::new(metric::Registry::default());
            let time_provider = Arc::new(SystemProvider::new()) as _;
            let backing = Arc::new(MemCatalog::new(metrics, Arc::clone(&time_provider)));

            let peer0 = TestCacheServer::bind_ephemeral();
            let peer1 = TestCacheServer::bind_ephemeral();
            let cache = Arc::new(QuorumCatalogCache::new(
                Arc::new(CatalogCache::default()),
                Arc::new([peer0.client(), peer1.client()]),
            ));

            // use new metrics registry so the two layers don't double-count
            let metrics = Arc::new(metric::Registry::default());
            let caching_catalog = Arc::new(CachingCatalog::new(
                cache,
                backing,
                metrics,
                time_provider,
                10,
            ));

            let test_catalog = TestCatalog::new(caching_catalog);
            test_catalog.hold_onto(peer0);
            test_catalog.hold_onto(peer1);

            Arc::new(test_catalog) as _
        })
        .await;
    }
}
