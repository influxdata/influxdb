//! Data for the lifecycle of the Ingester

use std::{
    collections::BTreeMap,
    sync::Arc,
    time::{Duration, Instant},
};

use async_trait::async_trait;
use backoff::{Backoff, BackoffConfig};
use data_types::{
    CompactionLevel, NamespaceId, PartitionId, SequenceNumber, ShardId, ShardIndex, TableId,
};
use dml::DmlOperation;
use iox_catalog::interface::{get_table_schema_by_id, CasFailure, Catalog};
use iox_query::exec::Executor;
use iox_time::{SystemProvider, TimeProvider};
use metric::{Attributes, Metric, U64Histogram, U64HistogramOptions};
use object_store::DynObjectStore;
use observability_deps::tracing::*;
use parquet_file::{
    metadata::IoxMetadata,
    storage::{ParquetStorage, StorageId},
};
use snafu::{OptionExt, Snafu};
use thiserror::Error;
use uuid::Uuid;
use write_summary::ShardProgress;

use self::shard::ShardData;
use crate::{
    buffer_tree::{
        namespace::name_resolver::{NamespaceNameProvider, NamespaceNameResolver},
        partition::resolver::{CatalogPartitionResolver, PartitionCache, PartitionProvider},
        table::name_resolver::{TableNameProvider, TableNameResolver},
    },
    compact::{compact_persisting_batch, CompactedStream},
    lifecycle::LifecycleHandle,
};

mod shard;

#[cfg(test)]
pub mod triggers;

/// The maximum duration of time between creating a [`PartitionData`] and its
/// [`SortKey`] being fetched from the catalog.
///
/// [`PartitionData`]: crate::buffer_tree::partition::PartitionData
/// [`SortKey`]: schema::sort::SortKey
const SORT_KEY_PRE_FETCH: Duration = Duration::from_secs(30);

/// The maximum duration of time between observing an initialising the
/// [`NamespaceData`] in response to observing an operation for a namespace, and
/// fetching the string identifier for it in the background via a
/// [`DeferredLoad`].
///
/// [`NamespaceData`]: crate::buffer_tree::namespace::NamespaceData
/// [`DeferredLoad`]: crate::deferred_load::DeferredLoad
pub(crate) const NAMESPACE_NAME_PRE_FETCH: Duration = Duration::from_secs(60);

/// The maximum duration of time between observing and initialising the
/// [`TableData`] in response to observing an operation for a table, and
/// fetching the string identifier for it in the background via a
/// [`DeferredLoad`].
///
/// [`TableData`]: crate::buffer_tree::table::TableData
/// [`DeferredLoad`]: crate::deferred_load::DeferredLoad
pub const TABLE_NAME_PRE_FETCH: Duration = Duration::from_secs(60);

#[derive(Debug, Snafu)]
#[allow(missing_copy_implementations, missing_docs)]
pub enum Error {
    #[snafu(display("Shard {} not found in data map", shard_id))]
    ShardNotFound { shard_id: ShardId },

    #[snafu(display("Error adding to buffer in mutable batch: {}", source))]
    BufferWrite { source: mutable_batch::Error },
}

/// Errors that occur during initialisation of an [`IngesterData`].
#[derive(Debug, Error)]
pub enum InitError {
    /// A catalog error occured while fetching the most recent partitions for
    /// the internal cache.
    #[error("failed to pre-warm partition cache: {0}")]
    PreWarmPartitions(iox_catalog::interface::Error),
}

/// A specialized `Error` for Ingester Data errors
pub type Result<T, E = Error> = std::result::Result<T, E>;

/// Contains all buffered and cached data for the ingester.
#[derive(Debug)]
pub struct IngesterData {
    /// Object store for persistence of parquet files
    store: ParquetStorage,

    /// The global catalog for schema, parquet files and tombstones
    catalog: Arc<dyn Catalog>,

    /// This map gets set up on initialization of the ingester so it won't ever be modified.
    /// The content of each ShardData will get changed when more namespaces and tables
    /// get ingested.
    shards: BTreeMap<ShardId, ShardData>,

    /// Executor for running queries and compacting and persisting
    exec: Arc<Executor>,

    /// Backoff config
    backoff_config: BackoffConfig,

    /// Metrics for file size of persisted Parquet files
    persisted_file_size_bytes: Metric<U64Histogram>,
}

impl IngesterData {
    /// Create new instance.
    pub async fn new<T>(
        object_store: Arc<DynObjectStore>,
        catalog: Arc<dyn Catalog>,
        shards: T,
        exec: Arc<Executor>,
        backoff_config: BackoffConfig,
        metrics: Arc<metric::Registry>,
    ) -> Result<Self, InitError>
    where
        T: IntoIterator<Item = (ShardId, ShardIndex)> + Send,
    {
        let persisted_file_size_bytes = metrics.register_metric_with_options(
            "ingester_persisted_file_size_bytes",
            "Size of files persisted by the ingester",
            || {
                U64HistogramOptions::new([
                    500 * 1024,       // 500 KB
                    1024 * 1024,      // 1 MB
                    3 * 1024 * 1024,  // 3 MB
                    10 * 1024 * 1024, // 10 MB
                    30 * 1024 * 1024, // 30 MB
                    u64::MAX,         // Inf
                ])
            },
        );

        // Read the most recently created partitions for the shards this
        // ingester instance will be consuming from.
        //
        // By caching these hot partitions overall catalog load after an
        // ingester starts up is reduced, and the associated query latency is
        // removed from the (blocking) ingest hot path.
        let shards = shards.into_iter().collect::<Vec<_>>();
        let shard_ids = shards.iter().map(|(id, _)| *id).collect::<Vec<_>>();
        let recent_partitions = catalog
            .repositories()
            .await
            .partitions()
            .most_recent_n_in_shards(10_000, &shard_ids)
            .await
            .map_err(InitError::PreWarmPartitions)?;

        // Build the partition provider.
        let partition_provider = CatalogPartitionResolver::new(Arc::clone(&catalog));
        let partition_provider = PartitionCache::new(
            partition_provider,
            recent_partitions,
            SORT_KEY_PRE_FETCH,
            Arc::clone(&catalog),
            BackoffConfig::default(),
        );
        let partition_provider: Arc<dyn PartitionProvider> = Arc::new(partition_provider);

        // Initialise the deferred namespace name resolver.
        let namespace_name_provider: Arc<dyn NamespaceNameProvider> =
            Arc::new(NamespaceNameResolver::new(
                NAMESPACE_NAME_PRE_FETCH,
                Arc::clone(&catalog),
                backoff_config.clone(),
            ));

        // Initialise the deferred table name resolver.
        let table_name_provider: Arc<dyn TableNameProvider> = Arc::new(TableNameResolver::new(
            TABLE_NAME_PRE_FETCH,
            Arc::clone(&catalog),
            backoff_config.clone(),
        ));

        let shards = shards
            .into_iter()
            .map(|(id, index)| {
                (
                    id,
                    ShardData::new(
                        index,
                        id,
                        Arc::clone(&namespace_name_provider),
                        Arc::clone(&table_name_provider),
                        Arc::clone(&partition_provider),
                        Arc::clone(&metrics),
                    ),
                )
            })
            .collect();

        Ok(Self {
            store: ParquetStorage::new(object_store, StorageId::from("iox")),
            catalog,
            shards,
            exec,
            backoff_config,
            persisted_file_size_bytes,
        })
    }

    /// Executor for running queries and compacting and persisting
    pub(crate) fn exec(&self) -> &Arc<Executor> {
        &self.exec
    }

    /// Get shard data for specific shard.
    #[cfg(test)]
    pub(crate) fn shard(&self, shard_id: ShardId) -> Option<&ShardData> {
        self.shards.get(&shard_id)
    }

    /// Get iterator over shards (ID and data).
    pub(crate) fn shards(&self) -> impl Iterator<Item = (&ShardId, &ShardData)> {
        self.shards.iter()
    }

    /// Store the write or delete in the in memory buffer. Deletes will
    /// be written into the catalog before getting stored in the buffer.
    /// Any writes that create new IOx partitions will have those records
    /// created in the catalog before putting into the buffer. Writes will
    /// get logged in the lifecycle manager. If it indicates ingest should
    /// be paused, this function will return true.
    pub async fn buffer_operation(
        &self,
        shard_id: ShardId,
        dml_operation: DmlOperation,
        lifecycle_handle: &dyn LifecycleHandle,
    ) -> Result<DmlApplyAction> {
        let shard_data = self
            .shards
            .get(&shard_id)
            .context(ShardNotFoundSnafu { shard_id })?;
        shard_data
            .buffer_operation(dml_operation, lifecycle_handle)
            .await
    }

    /// Return the ingestion progress for the specified shards
    /// Returns an empty `ShardProgress` for any shards that this ingester doesn't know about.
    pub(super) async fn progresses(
        &self,
        shard_indexes: Vec<ShardIndex>,
    ) -> BTreeMap<ShardIndex, ShardProgress> {
        let mut progresses = BTreeMap::new();
        for shard_index in shard_indexes {
            let shard_data = self
                .shards
                .values()
                .find(|shard_data| shard_data.shard_index() == shard_index);

            let progress = match shard_data {
                Some(shard_data) => shard_data.progress().await,
                None => ShardProgress::new(), // don't know about this shard
            };

            progresses.insert(shard_index, progress);
        }
        progresses
    }
}

/// The Persister has a function to persist a given partition ID and to update the
/// associated shard's `min_unpersisted_sequence_number`.
#[async_trait]
pub trait Persister: Send + Sync + 'static {
    /// Persits the partition ID. Will retry forever until it succeeds.
    async fn persist(
        &self,
        shard_id: ShardId,
        namespace_id: NamespaceId,
        table_id: TableId,
        partition_id: PartitionId,
    );

    /// Updates the shard's `min_unpersisted_sequence_number` in the catalog.
    /// This number represents the minimum that might be unpersisted, which is the
    /// farthest back the ingester would need to read in the write buffer to ensure
    /// that all data would be correctly replayed on startup.
    async fn update_min_unpersisted_sequence_number(
        &self,
        shard_id: ShardId,
        sequence_number: SequenceNumber,
    );
}

#[async_trait]
impl Persister for IngesterData {
    async fn persist(
        &self,
        shard_id: ShardId,
        namespace_id: NamespaceId,
        table_id: TableId,
        partition_id: PartitionId,
    ) {
        // Record time it takes for this persist operation to help
        // identify partitions that are taking substantial time.
        let start_time = Instant::now();

        // lookup the state from the ingester data. If something isn't found,
        // it's unexpected. Crash so someone can take a look.
        let namespace = self
            .shards
            .get(&shard_id)
            .and_then(|s| s.namespace(namespace_id))
            .unwrap_or_else(|| panic!("namespace {namespace_id} not in shard {shard_id} state"));

        // Begin resolving the load-deferred name concurrently if it is not
        // already available.
        let namespace_name = namespace.namespace_name();
        namespace_name.prefetch_now();

        // Assert the namespace ID matches the index key.
        assert_eq!(namespace.namespace_id(), namespace_id);

        let table_data = namespace.table(table_id).unwrap_or_else(|| {
            panic!("table {table_id} in namespace {namespace_id} not in shard {shard_id} state")
        });
        // Assert various properties of the table to ensure the index is
        // correct, out of an abundance of caution.
        assert_eq!(table_data.shard_id(), shard_id);
        assert_eq!(table_data.namespace_id(), namespace_id);
        assert_eq!(table_data.table_id(), table_id);

        // Begin resolving the load-deferred name concurrently if it is not
        // already available.
        let table_name = Arc::clone(table_data.table_name());
        table_name.prefetch_now();

        let partition = table_data.get_partition(partition_id).unwrap_or_else(|| {
                panic!(
                    "partition {partition_id} in table {table_id} in namespace {namespace_id} not in shard {shard_id} state"
                )
            });

        let partition_key;
        let sort_key;
        let last_persisted_sequence_number;
        let batch;
        let batch_sequence_number_range;
        {
            // Acquire a write lock over the partition and extract all the
            // necessary data.
            let mut guard = partition.lock();

            // Assert various properties of the partition to ensure the index is
            // correct, out of an abundance of caution.
            assert_eq!(guard.partition_id(), partition_id);
            assert_eq!(guard.shard_id(), shard_id);
            assert_eq!(guard.namespace_id(), namespace_id);
            assert_eq!(guard.table_id(), table_id);
            assert!(Arc::ptr_eq(guard.table_name(), &table_name));

            partition_key = guard.partition_key().clone();
            sort_key = guard.sort_key().clone();
            last_persisted_sequence_number = guard.max_persisted_sequence_number();

            // The sequence number MUST be read without releasing the write lock
            // to ensure a consistent snapshot of batch contents and batch
            // sequence number range.
            batch = guard.mark_persisting();
            batch_sequence_number_range = guard.sequence_number_range();
        };

        // From this point on, the code MUST be infallible.
        //
        // The partition data was moved to the persisting slot, and any
        // subsequent calls would be an error.
        //
        // This is NOT an invariant, and this could be changed in the future to
        // allow partitions to be marked as persisting repeatedly. Today
        // however, the code is infallible (or rather, terminal - it does cause
        // a retry).

        let sort_key = sort_key.get().await;
        trace!(
            %shard_id,
            %namespace_id,
            %namespace_name,
            %table_id,
            %table_name,
            %partition_id,
            %partition_key,
            ?sort_key,
            "fetched sort key"
        );

        debug!(
            %shard_id,
            %namespace_id,
            %namespace_name,
            %table_id,
            %table_name,
            %partition_id,
            %partition_key,
            ?sort_key,
            "persisting partition"
        );

        // Check if there is any data to persist.
        let batch = match batch {
            Some(v) => {
                // The partition state machine will NOT return an empty batch.
                assert!(!v.record_batches().is_empty());
                v
            }
            None => {
                // But it MAY return no batch at all.
                warn!(
                    %shard_id,
                    %namespace_id,
                    %namespace_name,
                    %table_id,
                    %table_name,
                    %partition_id,
                    %partition_key,
                    "partition marked for persistence contains no data"
                );
                return;
            }
        };

        // At this point, the table name is necessary, so demand it be resolved
        // if it is not yet available.
        let table_name = table_name.get().await;

        // Prepare the plan for CPU intensive work of compaction, de-duplication and sorting
        let CompactedStream {
            stream: record_stream,
            catalog_sort_key_update,
            data_sort_key,
        } = compact_persisting_batch(&self.exec, sort_key.clone(), table_name.clone(), batch)
            .await
            .expect("unable to compact persisting batch");

        // Generate a UUID to uniquely identify this parquet file in object
        // storage.
        let object_store_id = Uuid::new_v4();

        // Construct the metadata for this parquet file.
        let time_now = SystemProvider::new().now();
        let iox_metadata = IoxMetadata {
            object_store_id,
            creation_timestamp: time_now,
            shard_id,
            namespace_id,
            namespace_name: Arc::clone(&*namespace.namespace_name().get().await),
            table_id,
            table_name: Arc::clone(&*table_name),
            partition_id,
            partition_key: partition_key.clone(),
            max_sequence_number: batch_sequence_number_range.inclusive_max().unwrap(),
            compaction_level: CompactionLevel::Initial,
            sort_key: Some(data_sort_key),
            max_l0_created_at: time_now,
        };

        // Save the compacted data to a parquet file in object storage.
        //
        // This call retries until it completes.
        let (md, file_size) = self
            .store
            .upload(record_stream, &iox_metadata)
            .await
            .expect("unexpected fatal persist error");

        // Update the sort key in the catalog if there are
        // additional columns BEFORE adding parquet file to the
        // catalog. If the order is reversed, the querier or
        // compactor may see a parquet file with an inconsistent
        // sort key. https://github.com/influxdata/influxdb_iox/issues/5090
        if let Some(new_sort_key) = catalog_sort_key_update {
            let new_sort_key_str = new_sort_key.to_columns().collect::<Vec<_>>();
            let old_sort_key: Option<Vec<String>> =
                sort_key.map(|v| v.to_columns().map(ToString::to_string).collect());
            Backoff::new(&self.backoff_config)
                .retry_all_errors("cas_sort_key", || {
                    let old_sort_key = old_sort_key.clone();
                    async {
                        let mut repos = self.catalog.repositories().await;
                        match repos
                            .partitions()
                            .cas_sort_key(partition_id, old_sort_key, &new_sort_key_str)
                            .await
                        {
                            Ok(_) => {}
                            Err(CasFailure::ValueMismatch(_)) => {
                                // An ingester concurrently updated the sort key.
                                //
                                // This breaks a sort-key update invariant - sort
                                // key updates MUST be serialised. This should
                                // not happen because writes for a given table
                                // are always mapped to a single ingester
                                // instance.
                                panic!("detected concurrent sort key update");
                            }
                            Err(CasFailure::QueryError(e)) => return Err(e),
                        };
                        // compiler insisted on getting told the type of the error :shrug:
                        Ok(()) as Result<(), iox_catalog::interface::Error>
                    }
                })
                .await
                .expect("retry forever");

            // Update the sort key in the partition cache.
            partition.lock().update_sort_key(Some(new_sort_key.clone()));

            debug!(
                %object_store_id,
                %shard_id,
                %namespace_id,
                %namespace_name,
                %table_id,
                %table_name,
                %partition_id,
                %partition_key,
                ?old_sort_key,
                %new_sort_key,
                "adjusted sort key during batch compact & persist"
            );
        }

        // Read the table schema from the catalog to act as a map of column name
        // -> column IDs.
        //
        // TODO: this can be removed once the ingester uses column IDs
        let table_schema = Backoff::new(&self.backoff_config)
            .retry_all_errors("get table schema", || async {
                let mut repos = self.catalog.repositories().await;
                get_table_schema_by_id(table_id, repos.as_mut()).await
            })
            .await
            .expect("retry forever");

        // Add the parquet file to the catalog until succeed
        let parquet_file = iox_metadata.to_parquet_file(partition_id, file_size, &md, |name| {
            table_schema.columns.get(name).expect("Unknown column").id
        });

        // Assert partitions are persisted in-order.
        //
        // It is an invariant that partitions are persisted in order so that
        // both the per-shard, and per-partition watermarks are correctly
        // advanced and accurate.
        if let Some(last_persist) = last_persisted_sequence_number {
            assert!(
                parquet_file.max_sequence_number > last_persist,
                "out of order partition persistence, persisting {}, previously persisted {}",
                parquet_file.max_sequence_number.get(),
                last_persist.get(),
            );
        }

        // Add the parquet file to the catalog.
        //
        // This has the effect of allowing the queriers to "discover" the
        // parquet file by polling / querying the catalog.
        Backoff::new(&self.backoff_config)
            .retry_all_errors("add parquet file to catalog", || async {
                let mut repos = self.catalog.repositories().await;
                let parquet_file = repos.parquet_files().create(parquet_file.clone()).await?;
                debug!(
                    ?partition_id,
                    table_id=?parquet_file.table_id,
                    parquet_file_id=?parquet_file.id,
                    table_name=%iox_metadata.table_name,
                    "parquet file written to catalog"
                );
                // compiler insisted on getting told the type of the error :shrug:
                Ok(()) as Result<(), iox_catalog::interface::Error>
            })
            .await
            .expect("retry forever");

        // Update the per-partition persistence watermark, so that new
        // ingester instances skip the just-persisted ops during replay.
        //
        // This could be transactional with the above parquet insert to
        // maintain catalog consistency, though in practice it is an
        // unnecessary overhead - the system can tolerate replaying the ops
        // that lead to this parquet file being generated, and tolerate
        // creating a parquet file containing duplicate data (remedied by
        // compaction).
        //
        // This means it is possible to observe a parquet file with a
        // max_persisted_sequence_number >
        // partition.persisted_sequence_number, either in-between these
        // catalog updates, or for however long it takes a crashed ingester
        // to restart and replay the ops, and re-persist a file containing
        // the same (or subset of) data.
        //
        // The above is also true of the per-shard persist marker that
        // governs the ingester's replay start point, which is
        // non-transactionally updated after all partitions have persisted.
        Backoff::new(&self.backoff_config)
            .retry_all_errors("set partition persist marker", || async {
                self.catalog
                    .repositories()
                    .await
                    .partitions()
                    .update_persisted_sequence_number(
                        parquet_file.partition_id,
                        parquet_file.max_sequence_number,
                    )
                    .await
            })
            .await
            .expect("retry forever");

        // Record metrics
        let attributes = Attributes::from([("shard_id", format!("{shard_id}").into())]);
        self.persisted_file_size_bytes
            .recorder(attributes)
            .record(file_size as u64);

        // Mark the partition as having completed persistence, causing it to
        // release the reference to the in-flight persistence data it is
        // holding.
        //
        // This SHOULD cause the data to be dropped, but there MAY be ongoing
        // queries that currently hold a reference to the data. In either case,
        // the persisted data will be dropped "shortly".
        partition
            .lock()
            .mark_persisted(iox_metadata.max_sequence_number);

        // BUG: ongoing queries retain references to the persisting data,
        // preventing it from being dropped, but memory is released back to
        // lifecycle memory tracker when this fn returns.
        //
        //  https://github.com/influxdata/influxdb_iox/issues/5805
        //
        info!(
            %object_store_id,
            %shard_id,
            %namespace_id,
            %namespace_name,
            %table_id,
            %table_name,
            %partition_id,
            %partition_key,
            duration_sec=(Instant::now() - start_time).as_secs(),
            max_sequence_number=%iox_metadata.max_sequence_number.get(),
            "persisted partition"
        );
    }

    async fn update_min_unpersisted_sequence_number(
        &self,
        shard_id: ShardId,
        sequence_number: SequenceNumber,
    ) {
        Backoff::new(&self.backoff_config)
            .retry_all_errors("updating min_unpersisted_sequence_number", || async {
                self.catalog
                    .repositories()
                    .await
                    .shards()
                    .update_min_unpersisted_sequence_number(shard_id, sequence_number)
                    .await
            })
            .await
            .expect("retry forever")
    }
}

/// A successful DML apply operation can perform one of these actions
#[derive(Clone, Copy, Debug)]
pub enum DmlApplyAction {
    /// The DML operation was successful; bool indicates if ingestion should be paused
    Applied(bool),

    /// The DML operation was skipped because it has already been applied
    Skipped,
}

#[cfg(test)]
mod tests {
    use std::{ops::DerefMut, sync::Arc, time::Duration};

    use assert_matches::assert_matches;
    use data_types::{
        DeletePredicate, Namespace, NamespaceSchema, NonEmptyString, PartitionKey, Sequence, Shard,
        Table, TimestampRange,
    };
    use dml::{DmlDelete, DmlMeta, DmlWrite};
    use futures::TryStreamExt;
    use iox_catalog::{mem::MemCatalog, validate_or_insert_schema};
    use iox_time::Time;
    use object_store::memory::InMemory;
    use schema::sort::SortKey;

    use super::*;
    use crate::{
        lifecycle::{LifecycleConfig, LifecycleManager},
        test_util::make_write_op,
    };

    struct TestContext {
        metrics: Arc<metric::Registry>,
        catalog: Arc<dyn Catalog>,
        object_store: Arc<DynObjectStore>,
        namespace: Namespace,
        table1: Table,
        table2: Table,
        partition_key: PartitionKey,
        shard1: Shard,
        shard2: Shard,
        data: Arc<IngesterData>,
    }

    impl TestContext {
        async fn new() -> Self {
            let metrics = Arc::new(metric::Registry::new());
            let catalog: Arc<dyn Catalog> = Arc::new(MemCatalog::new(Arc::clone(&metrics)));
            let partition_key = PartitionKey::from("1970-01-01");

            let (namespace, table1, table2, shard1, shard2) = {
                let mut repos = catalog.repositories().await;

                let topic = repos.topics().create_or_get("whatevs").await.unwrap();
                let query_pool = repos.query_pools().create_or_get("whatevs").await.unwrap();

                let namespace = repos
                    .namespaces()
                    .create("foo", None, topic.id, query_pool.id)
                    .await
                    .unwrap();

                let table1 = repos
                    .tables()
                    .create_or_get("mem", namespace.id)
                    .await
                    .unwrap();

                let table2 = repos
                    .tables()
                    .create_or_get("cpu", namespace.id)
                    .await
                    .unwrap();

                let schema =
                    NamespaceSchema::new(namespace.id, topic.id, query_pool.id, 100, 42, None);

                let shard_index = ShardIndex::new(0);
                let shard1 = repos
                    .shards()
                    .create_or_get(&topic, shard_index)
                    .await
                    .unwrap();

                let shard2 = repos
                    .shards()
                    .create_or_get(&topic, shard_index)
                    .await
                    .unwrap();

                // Put the columns in the catalog (these writes don't actually get inserted)
                // This will be different once column IDs are used instead of names
                let table1_write = Self::arbitrary_write_with_seq_num_at_time(
                    1,
                    0,
                    &partition_key,
                    shard_index,
                    namespace.id,
                    &table1,
                );
                validate_or_insert_schema(
                    table1_write
                        .tables()
                        .map(|(_id, batch)| (table1.name.as_str(), batch)),
                    &schema,
                    repos.deref_mut(),
                )
                .await
                .unwrap()
                .unwrap();

                let table2_write = Self::arbitrary_write_with_seq_num_at_time(
                    1,
                    0,
                    &partition_key,
                    shard_index,
                    namespace.id,
                    &table2,
                );
                validate_or_insert_schema(
                    table2_write
                        .tables()
                        .map(|(_id, batch)| (table2.name.as_str(), batch)),
                    &schema,
                    repos.deref_mut(),
                )
                .await
                .unwrap()
                .unwrap();

                (namespace, table1, table2, shard1, shard2)
            };

            let object_store: Arc<DynObjectStore> = Arc::new(InMemory::new());

            let data = Arc::new(
                IngesterData::new(
                    Arc::clone(&object_store),
                    Arc::clone(&catalog),
                    [
                        (shard1.id, shard1.shard_index),
                        (shard2.id, shard2.shard_index),
                    ],
                    Arc::new(Executor::new_testing()),
                    BackoffConfig::default(),
                    Arc::clone(&metrics),
                )
                .await
                .expect("failed to initialise ingester"),
            );

            Self {
                metrics,
                catalog,
                object_store,
                namespace,
                table1,
                table2,
                partition_key,
                shard1,
                shard2,
                data,
            }
        }

        fn arbitrary_write_with_seq_num_at_time(
            sequence_number: i64,
            timestamp: i64,
            partition_key: &PartitionKey,
            shard_index: ShardIndex,
            namespace_id: NamespaceId,
            table: &Table,
        ) -> DmlWrite {
            make_write_op(
                partition_key,
                shard_index,
                namespace_id,
                &table.name,
                table.id,
                sequence_number,
                &format!(
                    "{} foo=1 {}\n{} foo=2 {}",
                    table.name,
                    timestamp,
                    table.name,
                    timestamp + 10
                ),
            )
        }

        fn arbitrary_write_with_seq_num(&self, table: &Table, sequence_number: i64) -> DmlWrite {
            Self::arbitrary_write_with_seq_num_at_time(
                sequence_number,
                10,
                &self.partition_key,
                self.shard1.shard_index,
                self.namespace.id,
                table,
            )
        }

        async fn persist_data(&self, table: &Table) {
            let partition_id = {
                let sd = self.data.shards.get(&self.shard1.id).unwrap();
                let n = sd.namespace(self.namespace.id).unwrap();
                let t = n.table(table.id).unwrap();
                let p = t
                    .get_partition_by_key(&self.partition_key)
                    .unwrap()
                    .lock()
                    .partition_id();
                p
            };

            self.data
                .persist(self.shard1.id, self.namespace.id, table.id, partition_id)
                .await;
        }
    }

    #[tokio::test]
    async fn buffer_write_updates_lifecycle_manager_indicates_pause() {
        test_helpers::maybe_start_logging();

        let ctx = TestContext::new().await;
        let shard = &ctx.shard1;

        let w1 = ctx.arbitrary_write_with_seq_num(&ctx.table1, 1);

        let pause_size = w1.size() + 1;
        let manager = LifecycleManager::new(
            LifecycleConfig::new(
                pause_size,
                0,
                0,
                Duration::from_secs(1),
                Duration::from_secs(1),
                1000000,
            ),
            Arc::clone(&ctx.metrics),
            Arc::new(SystemProvider::new()),
        );

        let action = ctx
            .data
            .buffer_operation(shard.id, DmlOperation::Write(w1), &manager.handle())
            .await
            .unwrap();
        assert_matches!(action, DmlApplyAction::Applied(false));

        let w2 = ctx.arbitrary_write_with_seq_num(&ctx.table1, 2);

        let action = ctx
            .data
            .buffer_operation(shard.id, DmlOperation::Write(w2), &manager.handle())
            .await
            .unwrap();
        assert_matches!(action, DmlApplyAction::Applied(true));
    }

    #[tokio::test]
    async fn persist_row_count_trigger() {
        test_helpers::maybe_start_logging();

        let ctx = TestContext::new().await;
        let shard = &ctx.shard1;

        let w1 = ctx.arbitrary_write_with_seq_num(&ctx.table1, 1);

        let manager = LifecycleManager::new(
            LifecycleConfig::new(
                1000000000,
                0,
                0,
                Duration::from_secs(1),
                Duration::from_secs(1),
                1, // This row count will be hit
            ),
            Arc::clone(&ctx.metrics),
            Arc::new(SystemProvider::new()),
        );

        let action = ctx
            .data
            .buffer_operation(shard.id, DmlOperation::Write(w1), &manager.handle())
            .await
            .unwrap();
        // Exceeding the row count doesn't pause ingest (like other partition
        // limits)
        assert_matches!(action, DmlApplyAction::Applied(false));

        ctx.persist_data(&ctx.table1).await;

        // verify that a file got put into object store
        let file_paths: Vec<_> = ctx
            .object_store
            .list(None)
            .await
            .unwrap()
            .try_collect()
            .await
            .unwrap();
        assert_eq!(file_paths.len(), 1);
    }

    #[tokio::test]
    async fn persist() {
        test_helpers::maybe_start_logging();
        let ctx = TestContext::new().await;
        let namespace = &ctx.namespace;
        let shard1 = &ctx.shard1;
        let shard2 = &ctx.shard2;
        let data = &ctx.data;

        let w1 = ctx.arbitrary_write_with_seq_num(&ctx.table1, 1);
        // different table as w1, same sequence number
        let w2 = ctx.arbitrary_write_with_seq_num(&ctx.table2, 1);
        // same table as w1, next sequence number
        let w3 = ctx.arbitrary_write_with_seq_num(&ctx.table1, 2);

        let manager = LifecycleManager::new(
            LifecycleConfig::new(
                1,
                0,
                0,
                Duration::from_secs(1),
                Duration::from_secs(1),
                1000000,
            ),
            Arc::clone(&ctx.metrics),
            Arc::new(SystemProvider::new()),
        );

        data.buffer_operation(shard1.id, DmlOperation::Write(w1), &manager.handle())
            .await
            .unwrap();
        data.buffer_operation(shard2.id, DmlOperation::Write(w2), &manager.handle())
            .await
            .unwrap();
        data.buffer_operation(shard1.id, DmlOperation::Write(w3), &manager.handle())
            .await
            .unwrap();

        let expected_progress = ShardProgress::new()
            .with_buffered(SequenceNumber::new(1))
            .with_buffered(SequenceNumber::new(2));
        assert_progress(data, shard1.shard_index, expected_progress).await;

        let sd = data.shards.get(&shard1.id).unwrap();
        let n = sd.namespace(namespace.id).unwrap();
        let partition_id;
        {
            let mem_table = n.table(ctx.table1.id).unwrap();

            let p = mem_table
                .get_partition_by_key(&"1970-01-01".into())
                .unwrap();
            partition_id = p.lock().partition_id();
        }
        {
            // verify the partition doesn't have a sort key before any data has been persisted
            let mut repos = ctx.catalog.repositories().await;
            let partition_info = repos
                .partitions()
                .get_by_id(partition_id)
                .await
                .unwrap()
                .unwrap();
            assert!(partition_info.sort_key.is_empty());
        }

        data.persist(shard1.id, namespace.id, ctx.table1.id, partition_id)
            .await;

        // verify that a file got put into object store
        let file_paths: Vec<_> = ctx
            .object_store
            .list(None)
            .await
            .unwrap()
            .try_collect()
            .await
            .unwrap();
        assert_eq!(file_paths.len(), 1);

        let mut repos = ctx.catalog.repositories().await;
        // verify it put the record in the catalog
        let parquet_files = repos
            .parquet_files()
            .list_by_shard_greater_than(shard1.id, SequenceNumber::new(0))
            .await
            .unwrap();
        assert_eq!(parquet_files.len(), 1);
        let pf = parquet_files.first().unwrap();
        assert_eq!(pf.partition_id, partition_id);
        assert_eq!(pf.table_id, ctx.table1.id);
        assert_eq!(pf.max_sequence_number, SequenceNumber::new(2));
        assert_eq!(pf.shard_id, shard1.id);
        assert!(pf.to_delete.is_none());

        // Verify the per-partition persist mark was updated to a value
        // inclusive of the persisted data, forming the exclusive lower-bound
        // from which a partition should start applying ops to ingest new data.
        let partition = repos
            .partitions()
            .get_by_id(partition_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            partition.persisted_sequence_number,
            Some(SequenceNumber::new(2))
        );

        // verify it set a sort key on the partition in the catalog
        assert_eq!(partition.sort_key, vec!["time"]);

        // Verify the partition sort key cache was updated to reflect the new
        // catalog value.
        let cached_sort_key = data
            .shard(shard1.id)
            .unwrap()
            .namespace(namespace.id)
            .unwrap()
            .table(ctx.table1.id)
            .unwrap()
            .get_partition(partition_id)
            .unwrap()
            .lock()
            .sort_key()
            .clone();
        let cached_sort_key = cached_sort_key.get().await;
        assert_eq!(
            cached_sort_key,
            Some(SortKey::from_columns(partition.sort_key))
        );

        // This value should be recorded in the metrics asserted next;
        // it is less than 500 KB
        //
        // note that since the file has metadata with timestamps
        // embedded in it, and those timestamps may compress slightly
        // different, the file may change slightly from time to time
        //
        // https://github.com/influxdata/influxdb_iox/issues/5434
        let expected_size = 1345;
        let allowable_delta = 10;
        let size_delta = (pf.file_size_bytes - expected_size).abs();
        assert!(
            size_delta < allowable_delta,
            "Unexpected parquet file size. Expected {} +/- {} bytes, got {}",
            expected_size,
            allowable_delta,
            pf.file_size_bytes
        );

        // verify metrics
        let persisted_file_size_bytes: Metric<U64Histogram> = ctx
            .metrics
            .get_instrument("ingester_persisted_file_size_bytes")
            .unwrap();

        let observation = persisted_file_size_bytes
            .get_observer(&Attributes::from([(
                "shard_id",
                format!("{}", shard1.id).into(),
            )]))
            .unwrap()
            .fetch();
        assert_eq!(observation.sample_count(), 1);
        let buckets_with_counts: Vec<_> = observation
            .buckets
            .iter()
            .filter_map(|o| if o.count == 0 { None } else { Some(o.le) })
            .collect();
        // Only the < 500 KB bucket has a count
        assert_eq!(buckets_with_counts, &[500 * 1024]);

        let mem_table = n.table(ctx.table1.id).unwrap();

        // verify that the parquet_max_sequence_number got updated
        assert_eq!(
            mem_table
                .get_partition(partition_id)
                .unwrap()
                .lock()
                .max_persisted_sequence_number(),
            Some(SequenceNumber::new(2))
        );

        // check progresses after persist
        let expected_progress = ShardProgress::new()
            .with_buffered(SequenceNumber::new(1))
            .with_persisted(SequenceNumber::new(2));
        assert_progress(data, shard1.shard_index, expected_progress).await;
    }

    #[tokio::test]
    async fn partial_write_progress() {
        test_helpers::maybe_start_logging();
        let ctx = TestContext::new().await;
        let namespace = &ctx.namespace;
        let shard1 = &ctx.shard1;
        let data = &ctx.data;

        let w1 = ctx.arbitrary_write_with_seq_num(&ctx.table1, 1);
        // write with sequence number 2
        let w2 = ctx.arbitrary_write_with_seq_num(&ctx.table1, 2);

        let manager = LifecycleManager::new(
            LifecycleConfig::new(
                1,
                0,
                0,
                Duration::from_secs(1),
                Duration::from_secs(1),
                1000000,
            ),
            Arc::clone(&ctx.metrics),
            Arc::new(SystemProvider::new()),
        );

        // buffer operation 1, expect progress buffered sequence number should be 1
        data.buffer_operation(shard1.id, DmlOperation::Write(w1), &manager.handle())
            .await
            .unwrap();

        // Get the namespace
        let sd = data.shards.get(&shard1.id).unwrap();
        let n = sd.namespace(namespace.id).unwrap();

        let expected_progress = ShardProgress::new().with_buffered(SequenceNumber::new(1));
        assert_progress(data, shard1.shard_index, expected_progress).await;

        // configure the the namespace to wait after each insert.
        n.test_triggers.enable_pause_after_write().await;

        // now, buffer operation 2 which has two tables,
        let captured_data = Arc::clone(data);
        let shard1_id = shard1.id;
        let task = tokio::task::spawn(async move {
            captured_data
                .buffer_operation(shard1_id, DmlOperation::Write(w2), &manager.handle())
                .await
                .unwrap();
        });

        n.test_triggers.wait_for_pause_after_write().await;

        // Check that while the write is only partially complete, the
        // buffered sequence number hasn't increased
        let expected_progress = ShardProgress::new()
            // sequence 2 hasn't been buffered yet
            .with_buffered(SequenceNumber::new(1));
        assert_progress(data, shard1.shard_index, expected_progress).await;

        // allow the write to complete
        n.test_triggers.release_pause_after_write().await;
        task.await.expect("task completed unsuccessfully");

        // check progresses after the write completes
        let expected_progress = ShardProgress::new()
            .with_buffered(SequenceNumber::new(1))
            .with_buffered(SequenceNumber::new(2));
        assert_progress(data, shard1.shard_index, expected_progress).await;
    }

    #[tokio::test]
    async fn buffer_deletes_updates_tombstone_watermark() {
        test_helpers::maybe_start_logging();
        let ctx = TestContext::new().await;
        let shard1 = &ctx.shard1;
        let data = &ctx.data;

        let w1 = ctx.arbitrary_write_with_seq_num(&ctx.table1, 1);

        let pause_size = w1.size() + 1;
        let manager = LifecycleManager::new(
            LifecycleConfig::new(
                pause_size,
                0,
                0,
                Duration::from_secs(1),
                Duration::from_secs(1),
                1000000,
            ),
            Arc::clone(&ctx.metrics),
            Arc::new(SystemProvider::new()),
        );

        data.buffer_operation(shard1.id, DmlOperation::Write(w1), &manager.handle())
            .await
            .unwrap();

        let predicate = DeletePredicate {
            range: TimestampRange::new(1, 2),
            exprs: vec![],
        };
        let ignored_ts = Time::from_timestamp_millis(42).unwrap();
        let d1 = DmlDelete::new(
            NamespaceId::new(42),
            predicate,
            Some(NonEmptyString::new(&ctx.table1.name).unwrap()),
            DmlMeta::sequenced(
                Sequence::new(ShardIndex::new(1), SequenceNumber::new(2)),
                ignored_ts,
                None,
                1337,
            ),
        );
        data.buffer_operation(shard1.id, DmlOperation::Delete(d1), &manager.handle())
            .await
            .unwrap();
    }

    /// Verifies that the progress in data is the same as expected_progress
    async fn assert_progress(
        data: &IngesterData,
        shard_index: ShardIndex,
        expected_progress: ShardProgress,
    ) {
        let progresses = data.progresses(vec![shard_index]).await;
        let expected_progresses = [(shard_index, expected_progress)]
            .into_iter()
            .collect::<BTreeMap<_, _>>();

        assert_eq!(progresses, expected_progresses);
    }
}
