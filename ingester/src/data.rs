//! Data for the lifecycle of the Ingester

use crate::{
    compact::{compact_persisting_batch, CompactedStream},
    lifecycle::LifecycleHandle,
    querier_handler::query,
};
use arrow::{error::ArrowError, record_batch::RecordBatch};
use arrow_util::optimize::{optimize_record_batch, optimize_schema};
use async_trait::async_trait;
use backoff::{Backoff, BackoffConfig};
use data_types::{
    DeletePredicate, NamespaceId, PartitionId, PartitionInfo, PartitionKey, SequenceNumber,
    ShardId, ShardIndex, TableId, Timestamp, Tombstone,
};
use datafusion::physical_plan::SendableRecordBatchStream;
use dml::DmlOperation;
use futures::{Stream, StreamExt};
use iox_catalog::interface::{get_table_schema_by_id, Catalog};
use iox_query::exec::Executor;
use iox_time::SystemProvider;
use metric::{Attributes, Metric, U64Counter, U64Histogram, U64HistogramOptions};
use mutable_batch::MutableBatch;
use object_store::DynObjectStore;
use observability_deps::tracing::{debug, warn};
use parking_lot::RwLock;
use parquet_file::storage::ParquetStorage;
use predicate::Predicate;
use schema::selection::Selection;
use snafu::{OptionExt, ResultExt, Snafu};
use std::{
    collections::{btree_map::Entry, BTreeMap},
    pin::Pin,
    sync::Arc,
};
use uuid::Uuid;
use write_summary::ShardProgress;

mod triggers;
use self::triggers::TestTriggers;

#[derive(Debug, Snafu)]
#[allow(missing_copy_implementations, missing_docs)]
pub enum Error {
    #[snafu(display("Shard {} not found in data map", shard_id))]
    ShardNotFound { shard_id: ShardId },

    #[snafu(display("Namespace {} not found in catalog", namespace))]
    NamespaceNotFound { namespace: String },

    #[snafu(display("Table {} not found in buffer", table_name))]
    TableNotFound { table_name: String },

    #[snafu(display("Table must be specified in delete"))]
    TableNotPresent,

    #[snafu(display("Error accessing catalog: {}", source))]
    Catalog {
        source: iox_catalog::interface::Error,
    },

    #[snafu(display("Snapshot error: {}", source))]
    Snapshot { source: mutable_batch::Error },

    #[snafu(display("Error while filtering columns from snapshot: {}", source))]
    FilterColumn { source: arrow::error::ArrowError },

    #[snafu(display("Error while copying buffer to snapshot: {}", source))]
    BufferToSnapshot { source: mutable_batch::Error },

    #[snafu(display("Error adding to buffer in mutable batch: {}", source))]
    BufferWrite { source: mutable_batch::Error },
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
    pub fn new(
        object_store: Arc<DynObjectStore>,
        catalog: Arc<dyn Catalog>,
        shards: BTreeMap<ShardId, ShardData>,
        exec: Arc<Executor>,
        backoff_config: BackoffConfig,
        metrics: Arc<metric::Registry>,
    ) -> Self {
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

        Self {
            store: ParquetStorage::new(object_store),
            catalog,
            shards,
            exec,
            backoff_config,
            persisted_file_size_bytes,
        }
    }

    /// Executor for running queries and compacting and persisting
    pub(crate) fn exec(&self) -> &Arc<Executor> {
        &self.exec
    }

    /// Get shard data for specific shard.
    #[allow(dead_code)] // Used in tests
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
    ) -> Result<bool> {
        let shard_data = self
            .shards
            .get(&shard_id)
            .context(ShardNotFoundSnafu { shard_id })?;
        shard_data
            .buffer_operation(
                dml_operation,
                shard_id,
                self.catalog.as_ref(),
                lifecycle_handle,
                &self.exec,
            )
            .await
    }

    /// Return the ingestion progress for the specified shards
    /// Returns an empty `ShardProgress` for any shards that this ingester doesn't know about.
    pub(crate) async fn progresses(
        &self,
        shard_indexes: Vec<ShardIndex>,
    ) -> BTreeMap<ShardIndex, ShardProgress> {
        let mut progresses = BTreeMap::new();
        for shard_index in shard_indexes {
            let shard_data = self
                .shards
                .iter()
                .map(|(_, shard_data)| shard_data)
                .find(|shard_data| shard_data.shard_index == shard_index);

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
/// assocated shard's `min_unpersisted_sequence_number`.
#[async_trait]
pub trait Persister: Send + Sync + 'static {
    /// Persits the partition ID. Will retry forever until it succeeds.
    async fn persist(&self, partition_id: PartitionId);

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
    async fn persist(&self, partition_id: PartitionId) {
        // lookup the partition_info from the catalog
        let partition_info = Backoff::new(&self.backoff_config)
            .retry_all_errors("get partition_info_by_id", || async {
                let mut repos = self.catalog.repositories().await;
                repos.partitions().partition_info_by_id(partition_id).await
            })
            .await
            .expect("retry forever");

        // lookup the state from the ingester data. If something isn't found, it's unexpected. Crash
        // so someone can take a look.
        let partition_info = partition_info
            .unwrap_or_else(|| panic!("partition {} not found in catalog", partition_id));
        let shard_data = self
            .shards
            .get(&partition_info.partition.shard_id)
            .unwrap_or_else(|| {
                panic!(
                    "shard state for {} not in ingester data",
                    partition_info.partition.shard_id
                )
            }); //{
        let namespace = shard_data
            .namespace(&partition_info.namespace_name)
            .unwrap_or_else(|| {
                panic!(
                    "namespace {} not in shard {} state",
                    partition_info.namespace_name, partition_info.partition.shard_id
                )
            });
        debug!(?partition_id, ?partition_info, "Persisting");

        // lookup column IDs from catalog
        // TODO: this can be removed once the ingester uses column IDs internally as well
        let table_schema = Backoff::new(&self.backoff_config)
            .retry_all_errors("get table schema", || async {
                let mut repos = self.catalog.repositories().await;
                let table = repos
                    .tables()
                    .get_by_namespace_and_name(namespace.namespace_id, &partition_info.table_name)
                    .await?
                    .expect("table not found in catalog");
                get_table_schema_by_id(table.id, repos.as_mut()).await
            })
            .await
            .expect("retry forever");

        let persisting_batch = namespace.snapshot_to_persisting(&partition_info).await;

        if let Some(persisting_batch) = persisting_batch {
            // do the CPU intensive work of compaction, de-duplication and sorting
            let compacted_stream = match compact_persisting_batch(
                Arc::new(SystemProvider::new()),
                &self.exec,
                namespace.namespace_id.get(),
                &partition_info,
                Arc::clone(&persisting_batch),
            )
            .await
            {
                Err(e) => {
                    // this should never error out. if it does, we need to crash hard so
                    // someone can take a look.
                    panic!("unable to compact persisting batch with error: {:?}", e);
                }
                Ok(Some(r)) => r,
                Ok(None) => {
                    warn!("persist called with no data");
                    return;
                }
            };
            let CompactedStream {
                stream: record_stream,
                iox_metadata,
                sort_key_update,
            } = compacted_stream;
            debug!(
                ?partition_id,
                ?sort_key_update,
                "Adjusted sort key during compacting the persting batch"
            );

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
            if let Some(new_sort_key) = sort_key_update {
                let sort_key = new_sort_key.to_columns().collect::<Vec<_>>();
                Backoff::new(&self.backoff_config)
                    .retry_all_errors("update_sort_key", || async {
                        let mut repos = self.catalog.repositories().await;
                        let partition = repos
                            .partitions()
                            .update_sort_key(partition_id, &sort_key)
                            .await?;

                        debug!(
                            partition_id=?partition.id,
                            table_id=?partition.table_id,
                            sort_key=?partition.sort_key,
                            "Updated sort key in catalog"
                        );
                        // compiler insisted on getting told the type of the error :shrug:
                        Ok(()) as Result<(), iox_catalog::interface::Error>
                    })
                    .await
                    .expect("retry forever");
            }

            // Add the parquet file to the catalog until succeed
            let parquet_file = iox_metadata.to_parquet_file(partition_id, file_size, &md, |name| {
                table_schema.columns.get(name).expect("Unknown column").id
            });
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

            // Record metrics
            let attributes = Attributes::from([(
                "shard_id",
                format!("{}", partition_info.partition.shard_id).into(),
            )]);
            self.persisted_file_size_bytes
                .recorder(attributes)
                .record(file_size as u64);

            // and remove the persisted data from memory
            debug!(
                ?partition_id,
                table_name=%partition_info.table_name,
                partition_key=%partition_info.partition.partition_key,
                max_sequence_number=%iox_metadata.max_sequence_number.get(),
                "mark_persisted"
            );
            namespace
                .mark_persisted(
                    &partition_info.table_name,
                    &partition_info.partition.partition_key,
                    iox_metadata.max_sequence_number,
                )
                .await;
        }
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

/// Data of a Shard
#[derive(Debug)]
pub struct ShardData {
    /// The shard index for this shard
    shard_index: ShardIndex,

    // New namespaces can come in at any time so we need to be able to add new ones
    namespaces: RwLock<BTreeMap<String, Arc<NamespaceData>>>,

    metrics: Arc<metric::Registry>,
    namespace_count: U64Counter,
}

impl ShardData {
    /// Initialise a new [`ShardData`] that emits metrics to `metrics`.
    pub fn new(shard_index: ShardIndex, metrics: Arc<metric::Registry>) -> Self {
        let namespace_count = metrics
            .register_metric::<U64Counter>(
                "ingester_namespaces_total",
                "Number of namespaces known to the ingester",
            )
            .recorder(&[]);

        Self {
            shard_index,
            namespaces: Default::default(),
            metrics,
            namespace_count,
        }
    }

    /// Initialize new ShardData with namespace for testing purpose only
    #[cfg(test)]
    pub fn new_for_test(
        shard_index: ShardIndex,
        namespaces: BTreeMap<String, Arc<NamespaceData>>,
    ) -> Self {
        Self {
            shard_index,
            namespaces: RwLock::new(namespaces),
            metrics: Default::default(),
            namespace_count: Default::default(),
        }
    }

    /// Store the write or delete in the shard. Deletes will
    /// be written into the catalog before getting stored in the buffer.
    /// Any writes that create new IOx partitions will have those records
    /// created in the catalog before putting into the buffer.
    pub async fn buffer_operation(
        &self,
        dml_operation: DmlOperation,
        shard_id: ShardId,
        catalog: &dyn Catalog,
        lifecycle_handle: &dyn LifecycleHandle,
        executor: &Executor,
    ) -> Result<bool> {
        let namespace_data = match self.namespace(dml_operation.namespace()) {
            Some(d) => d,
            None => {
                self.insert_namespace(dml_operation.namespace(), catalog)
                    .await?
            }
        };

        namespace_data
            .buffer_operation(dml_operation, shard_id, catalog, lifecycle_handle, executor)
            .await
    }

    /// Gets the namespace data out of the map
    pub fn namespace(&self, namespace: &str) -> Option<Arc<NamespaceData>> {
        let n = self.namespaces.read();
        n.get(namespace).cloned()
    }

    /// Retrieves the namespace from the catalog and initializes an empty buffer, or
    /// retrieves the buffer if some other caller gets it first
    async fn insert_namespace(
        &self,
        namespace: &str,
        catalog: &dyn Catalog,
    ) -> Result<Arc<NamespaceData>> {
        let mut repos = catalog.repositories().await;
        let namespace = repos
            .namespaces()
            .get_by_name(namespace)
            .await
            .context(CatalogSnafu)?
            .context(NamespaceNotFoundSnafu { namespace })?;

        let mut n = self.namespaces.write();

        let data = match n.entry(namespace.name) {
            Entry::Vacant(v) => {
                let v = v.insert(Arc::new(NamespaceData::new(namespace.id, &*self.metrics)));
                self.namespace_count.inc(1);
                Arc::clone(v)
            }
            Entry::Occupied(v) => Arc::clone(v.get()),
        };

        Ok(data)
    }

    /// Return the progress of this shard
    async fn progress(&self) -> ShardProgress {
        let namespaces: Vec<_> = self.namespaces.read().values().map(Arc::clone).collect();

        let mut progress = ShardProgress::new();

        for namespace_data in namespaces {
            progress = progress.combine(namespace_data.progress().await);
        }
        progress
    }
}

/// Data of a Namespace that belongs to a given Shard
#[derive(Debug)]
pub struct NamespaceData {
    namespace_id: NamespaceId,
    tables: RwLock<BTreeMap<String, Arc<tokio::sync::RwLock<TableData>>>>,

    table_count: U64Counter,

    /// The sequence number being actively written, if any.
    ///
    /// This is used to know when a sequence number is only partially
    /// buffered for readability reporting. For example, in the
    /// following diagram a write for SequenceNumber 10 is only
    /// partially readable because it has been written into partitions
    /// A and B but not yet C. The max buffered number on each
    /// PartitionData is not sufficient to determine if the write is
    /// complete.
    ///
    /// ```text
    /// ╔═══════════════════════════════════════════════╗
    /// ║                                               ║   DML Operation (write)
    /// ║  ┏━━━━━━━━━━━━━┳━━━━━━━━━━━━━┳━━━━━━━━━━━━━┓  ║   SequenceNumber = 10
    /// ║  ┃ Data for C  ┃ Data for B  ┃ Data for A  ┃  ║
    /// ║  ┗━━━━━━━━━━━━━┻━━━━━━━━━━━━━┻━━━━━━━━━━━━━┛  ║
    /// ║         │             │             │         ║
    /// ╚═══════════════════════╬═════════════╬═════════╝
    ///           │             │             │           ┌──────────────────────────────────┐
    ///                         │             │           │           Partition A            │
    ///           │             │             └──────────▶│        max buffered = 10         │
    ///                         │                         └──────────────────────────────────┘
    ///           │             │
    ///                         │                         ┌──────────────────────────────────┐
    ///           │             │                         │           Partition B            │
    ///                         └────────────────────────▶│        max buffered = 10         │
    ///           │                                       └──────────────────────────────────┘
    ///
    ///           │
    ///                                                   ┌──────────────────────────────────┐
    ///           │                                       │           Partition C            │
    ///            ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ▶│         max buffered = 7         │
    ///                                                   └──────────────────────────────────┘
    ///           Write is partially buffered. It has been
    ///            written to Partitions A and B, but not
    ///                  yet written to Partition C
    ///                                                               PartitionData
    ///                                                       (Ingester state per partition)
    ///```
    buffering_sequence_number: RwLock<Option<SequenceNumber>>,

    /// Control the flow of ingest, for testing purposes
    test_triggers: TestTriggers,
}

impl NamespaceData {
    /// Initialize new tables with default partition template of daily
    pub fn new(namespace_id: NamespaceId, metrics: &metric::Registry) -> Self {
        let table_count = metrics
            .register_metric::<U64Counter>(
                "ingester_tables_total",
                "Number of tables known to the ingester",
            )
            .recorder(&[]);

        Self {
            namespace_id,
            tables: Default::default(),
            table_count,
            buffering_sequence_number: RwLock::new(None),
            test_triggers: TestTriggers::new(),
        }
    }

    /// Initialize new tables with data for testing purpose only
    #[cfg(test)]
    pub(crate) fn new_for_test(
        namespace_id: NamespaceId,
        tables: BTreeMap<String, Arc<tokio::sync::RwLock<TableData>>>,
    ) -> Self {
        Self {
            namespace_id,
            tables: RwLock::new(tables),
            table_count: Default::default(),
            buffering_sequence_number: RwLock::new(None),
            test_triggers: TestTriggers::new(),
        }
    }

    /// Buffer the operation in the cache, adding any new partitions or delete tombstones to the
    /// catalog. Returns true if ingest should be paused due to memory limits set in the passed
    /// lifecycle manager.
    pub async fn buffer_operation(
        &self,
        dml_operation: DmlOperation,
        shard_id: ShardId,
        catalog: &dyn Catalog,
        lifecycle_handle: &dyn LifecycleHandle,
        executor: &Executor,
    ) -> Result<bool> {
        let sequence_number = dml_operation
            .meta()
            .sequence()
            .expect("must have sequence number")
            .sequence_number;

        // Note that this namespace is actively writing this sequence
        // number. Since there is no namespace wide lock held during a
        // write, this number is used to detect and update reported
        // progress during a write
        let _sequence_number_guard =
            ScopedSequenceNumber::new(sequence_number, &self.buffering_sequence_number);

        match dml_operation {
            DmlOperation::Write(write) => {
                let mut pause_writes = false;

                // Extract the partition key derived by the router.
                let partition_key = write
                    .partition_key()
                    .expect("no partition key in dml write")
                    .clone();

                for (t, b) in write.into_tables() {
                    let table_data = match self.table_data(&t) {
                        Some(t) => t,
                        None => self.insert_table(shard_id, &t, catalog).await?,
                    };

                    {
                        // lock scope
                        let mut table_data = table_data.write().await;
                        let should_pause = table_data
                            .buffer_table_write(
                                sequence_number,
                                b,
                                partition_key.clone(),
                                shard_id,
                                catalog,
                                lifecycle_handle,
                            )
                            .await?;
                        pause_writes = pause_writes || should_pause;
                    }
                    self.test_triggers.on_write().await;
                }

                Ok(pause_writes)
            }
            DmlOperation::Delete(delete) => {
                let table_name = delete.table_name().context(TableNotPresentSnafu)?;
                let table_data = match self.table_data(table_name) {
                    Some(t) => t,
                    None => self.insert_table(shard_id, table_name, catalog).await?,
                };

                let mut table_data = table_data.write().await;

                table_data
                    .buffer_delete(
                        table_name,
                        delete.predicate(),
                        shard_id,
                        sequence_number,
                        catalog,
                        executor,
                    )
                    .await?;

                // don't pause writes since deletes don't count towards memory limits
                Ok(false)
            }
        }
    }

    /// Snapshots the mutable buffer for the partition, which clears it out and moves it over to
    /// snapshots. Then return a vec of the snapshots and the optional persisting batch.
    pub async fn snapshot(
        &self,
        table_name: &str,
        partition_key: &PartitionKey,
    ) -> Option<(Vec<Arc<SnapshotBatch>>, Option<Arc<PersistingBatch>>)> {
        if let Some(t) = self.table_data(table_name) {
            let mut t = t.write().await;

            return t.partition_data.get_mut(partition_key).map(|p| {
                p.data
                    .snapshot()
                    .expect("snapshot on mutable batch should never fail");
                (p.data.snapshots.to_vec(), p.data.persisting.clone())
            });
        }

        None
    }

    /// Snapshots the mutable buffer for the partition, which clears it out and then moves all
    /// snapshots over to a persisting batch, which is returned. If there is no data to snapshot
    /// or persist, None will be returned.
    pub async fn snapshot_to_persisting(
        &self,
        partition_info: &PartitionInfo,
    ) -> Option<Arc<PersistingBatch>> {
        if let Some(table_data) = self.table_data(&partition_info.table_name) {
            let mut table_data = table_data.write().await;

            return table_data
                .partition_data
                .get_mut(&partition_info.partition.partition_key)
                .and_then(|partition_data| {
                    partition_data.snapshot_to_persisting_batch(
                        partition_info.partition.shard_id,
                        partition_info.partition.table_id,
                        partition_info.partition.id,
                        &partition_info.table_name,
                    )
                });
        }

        None
    }

    /// Gets the buffered table data
    pub(crate) fn table_data(
        &self,
        table_name: &str,
    ) -> Option<Arc<tokio::sync::RwLock<TableData>>> {
        let t = self.tables.read();
        t.get(table_name).cloned()
    }

    /// Inserts the table or returns it if it happens to be inserted by some other thread
    async fn insert_table(
        &self,
        shard_id: ShardId,
        table_name: &str,
        catalog: &dyn Catalog,
    ) -> Result<Arc<tokio::sync::RwLock<TableData>>> {
        let mut repos = catalog.repositories().await;
        let info = repos
            .tables()
            .get_table_persist_info(shard_id, self.namespace_id, table_name)
            .await
            .context(CatalogSnafu)?
            .context(TableNotFoundSnafu { table_name })?;

        let mut t = self.tables.write();

        let data = match t.entry(table_name.to_string()) {
            Entry::Vacant(v) => {
                let v = v.insert(Arc::new(tokio::sync::RwLock::new(TableData::new(
                    info.table_id,
                    info.tombstone_max_sequence_number,
                ))));
                self.table_count.inc(1);
                Arc::clone(v)
            }
            Entry::Occupied(v) => Arc::clone(v.get()),
        };

        Ok(data)
    }

    /// Walks down the table and partition and clears the persisting batch. The sequence number is
    /// the max_sequence_number for the persisted parquet file, which should be kept in the table
    /// data buffer.
    async fn mark_persisted(
        &self,
        table_name: &str,
        partition_key: &PartitionKey,
        sequence_number: SequenceNumber,
    ) {
        if let Some(t) = self.table_data(table_name) {
            let mut t = t.write().await;
            let partition = t.partition_data.get_mut(partition_key);

            if let Some(p) = partition {
                p.data.max_persisted_sequence_number = Some(sequence_number);
                p.data.persisting = None;
                // clear the deletes kept for this persisting batch
                p.data.deletes_during_persisting.clear();
            }
        }
    }

    /// Return progress from this Namespace
    async fn progress(&self) -> ShardProgress {
        let tables: Vec<_> = self.tables.read().values().map(Arc::clone).collect();

        // Consolidate progtress across partitions.
        let mut progress = ShardProgress::new()
            // Properly account for any sequence number that is
            // actively buffering and thus not yet completely
            // readable.
            .actively_buffering(*self.buffering_sequence_number.read());

        for table_data in tables {
            progress = progress.combine(table_data.read().await.progress())
        }
        progress
    }
}

/// RAAI struct that sets buffering sequence number on creation and clears it on free
struct ScopedSequenceNumber<'a> {
    sequence_number: SequenceNumber,
    buffering_sequence_number: &'a RwLock<Option<SequenceNumber>>,
}

impl<'a> ScopedSequenceNumber<'a> {
    fn new(
        sequence_number: SequenceNumber,
        buffering_sequence_number: &'a RwLock<Option<SequenceNumber>>,
    ) -> Self {
        *buffering_sequence_number.write() = Some(sequence_number);

        Self {
            sequence_number,
            buffering_sequence_number,
        }
    }
}

impl<'a> Drop for ScopedSequenceNumber<'a> {
    fn drop(&mut self) {
        // clear write on drop
        let mut buffering_sequence_number = self.buffering_sequence_number.write();
        assert_eq!(
            *buffering_sequence_number,
            Some(self.sequence_number),
            "multiple operations are being buffered concurrently"
        );
        *buffering_sequence_number = None;
    }
}

/// Data of a Table in a given Namesapce that belongs to a given Shard
#[derive(Debug)]
pub(crate) struct TableData {
    table_id: TableId,
    // the max sequence number for a tombstone associated with this table
    tombstone_max_sequence_number: Option<SequenceNumber>,
    // Map pf partition key to its data
    partition_data: BTreeMap<PartitionKey, PartitionData>,
}

impl TableData {
    /// Initialize new table buffer
    pub fn new(table_id: TableId, tombstone_max_sequence_number: Option<SequenceNumber>) -> Self {
        Self {
            table_id,
            tombstone_max_sequence_number,
            partition_data: Default::default(),
        }
    }

    /// Initialize new table buffer for testing purpose only
    #[cfg(test)]
    pub fn new_for_test(
        table_id: TableId,
        tombstone_max_sequence_number: Option<SequenceNumber>,
        partitions: BTreeMap<PartitionKey, PartitionData>,
    ) -> Self {
        Self {
            table_id,
            tombstone_max_sequence_number,
            partition_data: partitions,
        }
    }

    /// Return parquet_max_sequence_number
    pub fn parquet_max_sequence_number(&self) -> Option<SequenceNumber> {
        self.partition_data
            .values()
            .map(|p| p.data.max_persisted_sequence_number)
            .max()
            .flatten()
    }

    /// Return tombstone_max_sequence_number
    #[allow(dead_code)] // Used in tests
    pub fn tombstone_max_sequence_number(&self) -> Option<SequenceNumber> {
        self.tombstone_max_sequence_number
    }

    // buffers the table write and returns true if the lifecycle manager indicates that
    // ingest should be paused.
    async fn buffer_table_write(
        &mut self,
        sequence_number: SequenceNumber,
        batch: MutableBatch,
        partition_key: PartitionKey,
        shard_id: ShardId,
        catalog: &dyn Catalog,
        lifecycle_handle: &dyn LifecycleHandle,
    ) -> Result<bool> {
        let partition_data = match self.partition_data.get_mut(&partition_key) {
            Some(p) => p,
            None => {
                self.insert_partition(partition_key.clone(), shard_id, catalog)
                    .await?;
                self.partition_data.get_mut(&partition_key).unwrap()
            }
        };

        // skip the write if it has already been persisted
        if let Some(max) = partition_data.data.max_persisted_sequence_number {
            if max >= sequence_number {
                return Ok(false);
            }
        }

        let should_pause = lifecycle_handle.log_write(
            partition_data.id,
            shard_id,
            sequence_number,
            batch.size(),
            batch.rows(),
        );
        partition_data.buffer_write(sequence_number, batch)?;

        Ok(should_pause)
    }

    async fn buffer_delete(
        &mut self,
        table_name: &str,
        predicate: &DeletePredicate,
        shard_id: ShardId,
        sequence_number: SequenceNumber,
        catalog: &dyn Catalog,
        executor: &Executor,
    ) -> Result<()> {
        let min_time = Timestamp::new(predicate.range.start());
        let max_time = Timestamp::new(predicate.range.end());

        let mut repos = catalog.repositories().await;
        let tombstone = repos
            .tombstones()
            .create_or_get(
                self.table_id,
                shard_id,
                sequence_number,
                min_time,
                max_time,
                &predicate.expr_sql_string(),
            )
            .await
            .context(CatalogSnafu)?;

        // remember "persisted" state
        self.tombstone_max_sequence_number = Some(sequence_number);

        // modify one partition at a time
        for data in self.partition_data.values_mut() {
            data.buffer_tombstone(executor, table_name, tombstone.clone())
                .await;
        }

        Ok(())
    }

    pub fn unpersisted_partition_data(&self) -> Vec<UnpersistedPartitionData> {
        self.partition_data
            .values()
            .map(|p| UnpersistedPartitionData {
                partition_id: p.id,
                non_persisted: p
                    .get_non_persisting_data()
                    .expect("get_non_persisting should always work"),
                persisting: p.get_persisting_data(),
                partition_status: PartitionStatus {
                    parquet_max_sequence_number: p.data.max_persisted_sequence_number,
                    tombstone_max_sequence_number: self.tombstone_max_sequence_number,
                },
            })
            .collect()
    }

    async fn insert_partition(
        &mut self,
        partition_key: PartitionKey,
        shard_id: ShardId,
        catalog: &dyn Catalog,
    ) -> Result<()> {
        let mut repos = catalog.repositories().await;
        let partition = repos
            .partitions()
            .create_or_get(partition_key, shard_id, self.table_id)
            .await
            .context(CatalogSnafu)?;

        // get info on the persisted parquet files to use later for replay or for snapshot
        // information on query.
        let files = repos
            .parquet_files()
            .list_by_partition_not_to_delete(partition.id)
            .await
            .context(CatalogSnafu)?;
        // for now we just need the max persisted
        let max_persisted_sequence_number = files.iter().map(|p| p.max_sequence_number).max();

        let mut data = PartitionData::new(partition.id);
        data.data.max_persisted_sequence_number = max_persisted_sequence_number;

        self.partition_data.insert(partition.partition_key, data);

        Ok(())
    }

    /// Return progress from this Table
    fn progress(&self) -> ShardProgress {
        let progress = ShardProgress::new();
        let progress = match self.parquet_max_sequence_number() {
            Some(n) => progress.with_persisted(n),
            None => progress,
        };

        self.partition_data
            .values()
            .fold(progress, |progress, partition_data| {
                progress.combine(partition_data.progress())
            })
    }
}

/// Read only copy of the unpersisted data for a partition in the ingester for a specific partition.
#[derive(Debug)]
pub(crate) struct UnpersistedPartitionData {
    pub partition_id: PartitionId,
    pub non_persisted: Vec<Arc<SnapshotBatch>>,
    pub persisting: Option<QueryableBatch>,
    pub partition_status: PartitionStatus,
}

/// Data of an IOx Partition of a given Table of a Namesapce that belongs to a given Shard
#[derive(Debug)]
pub(crate) struct PartitionData {
    id: PartitionId,
    data: DataBuffer,
}

impl PartitionData {
    /// Initialize a new partition data buffer
    pub fn new(id: PartitionId) -> Self {
        Self {
            id,
            data: Default::default(),
        }
    }

    /// Snapshot anything in the buffer and move all snapshot data into a persisting batch
    pub fn snapshot_to_persisting_batch(
        &mut self,
        shard_id: ShardId,
        table_id: TableId,
        partition_id: PartitionId,
        table_name: &str,
    ) -> Option<Arc<PersistingBatch>> {
        self.data
            .snapshot_to_persisting(shard_id, table_id, partition_id, table_name)
    }

    /// Snapshot whatever is in the buffer and return a new vec of the
    /// arc cloned snapshots
    #[allow(dead_code)] // Used in tests
    pub fn snapshot(&mut self) -> Result<Vec<Arc<SnapshotBatch>>> {
        self.data.snapshot().context(SnapshotSnafu)?;
        Ok(self.data.snapshots.to_vec())
    }

    /// Return non persisting data
    pub fn get_non_persisting_data(&self) -> Result<Vec<Arc<SnapshotBatch>>> {
        self.data.buffer_and_snapshots()
    }

    /// Return persisting data
    pub fn get_persisting_data(&self) -> Option<QueryableBatch> {
        self.data.get_persisting_data()
    }

    /// Write the given mb in the buffer
    pub(crate) fn buffer_write(
        &mut self,
        sequence_number: SequenceNumber,
        mb: MutableBatch,
    ) -> Result<()> {
        match &mut self.data.buffer {
            Some(buf) => {
                buf.max_sequence_number = sequence_number.max(buf.max_sequence_number);
                buf.data.extend_from(&mb).context(BufferWriteSnafu)?;
            }
            None => {
                self.data.buffer = Some(BufferBatch {
                    min_sequence_number: sequence_number,
                    max_sequence_number: sequence_number,
                    data: mb,
                })
            }
        }

        Ok(())
    }

    /// Buffers a new tombstone:
    ///   . All the data in the `buffer` and `snapshots` will be replaced with one
    ///     tombstone-applied snapshot
    ///   . The tombstone is only added in the `deletes_during_persisting` if the `persisting`
    ///     exists
    pub(crate) async fn buffer_tombstone(
        &mut self,
        executor: &Executor,
        table_name: &str,
        tombstone: Tombstone,
    ) {
        self.data.add_tombstone(tombstone.clone());

        // ----------------------------------------------------------
        // First apply the tombstone on all in-memeory & non-persisting data
        // Make a QueryableBatch for all buffer + snapshots + the given tombstone
        let max_sequence_number = tombstone.sequence_number;
        let query_batch = match self
            .data
            .snapshot_to_queryable_batch(table_name, Some(tombstone.clone()))
        {
            Some(query_batch) if !query_batch.is_empty() => query_batch,
            _ => {
                // No need to proceed further
                return;
            }
        };

        let (min_sequence_number, _) = query_batch.min_max_sequence_numbers();
        assert!(min_sequence_number <= max_sequence_number);

        // Run query on the QueryableBatch to apply the tombstone.
        let stream = match query(
            executor,
            Arc::new(query_batch),
            Predicate::default(),
            Selection::All,
        )
        .await
        {
            Err(e) => {
                // this should never error out. if it does, we need to crash hard so
                // someone can take a look.
                panic!("unable to apply tombstones on snapshots: {:?}", e);
            }
            Ok(stream) => stream,
        };
        let record_batches = match datafusion::physical_plan::common::collect(stream).await {
            Err(e) => {
                // this should never error out. if it does, we need to crash hard so
                // someone can take a look.
                panic!("unable to collect record batches: {:?}", e);
            }
            Ok(batches) => batches,
        };

        // Merge all result record batches into one record batch
        // and make a snapshot for it
        let snapshot = if !record_batches.is_empty() {
            let record_batch = RecordBatch::concat(&record_batches[0].schema(), &record_batches)
                .unwrap_or_else(|e| {
                    panic!("unable to concat record batches: {:?}", e);
                });
            let snapshot = SnapshotBatch {
                min_sequence_number,
                max_sequence_number,
                data: Arc::new(record_batch),
            };

            Some(Arc::new(snapshot))
        } else {
            None
        };

        // ----------------------------------------------------------
        // Add the tombstone-applied data back in as one snapshot
        if let Some(snapshot) = snapshot {
            self.data.snapshots.push(snapshot);
        }
    }

    /// Return the progress from this Partition
    fn progress(&self) -> ShardProgress {
        self.data.progress()
    }
}

/// Data of an IOx partition split into batches
/// ┌────────────────────────┐        ┌────────────────────────┐      ┌─────────────────────────┐
/// │         Buffer         │        │       Snapshots        │      │       Persisting        │
/// │  ┌───────────────────┐ │        │                        │      │                         │
/// │  │  ┌───────────────┐│ │        │ ┌───────────────────┐  │      │  ┌───────────────────┐  │
/// │  │ ┌┴──────────────┐│├─┼────────┼─┼─▶┌───────────────┐│  │      │  │  ┌───────────────┐│  │
/// │  │┌┴──────────────┐├┘│ │        │ │ ┌┴──────────────┐││  │      │  │ ┌┴──────────────┐││  │
/// │  ││  BufferBatch  ├┘ │ │        │ │┌┴──────────────┐├┘│──┼──────┼─▶│┌┴──────────────┐├┘│  │
/// │  │└───────────────┘  │ │    ┌───┼─▶│ SnapshotBatch ├┘ │  │      │  ││ SnapshotBatch ├┘ │  │
/// │  └───────────────────┘ │    │   │ │└───────────────┘  │  │      │  │└───────────────┘  │  │
/// │          ...           │    │   │ └───────────────────┘  │      │  └───────────────────┘  │
/// │  ┌───────────────────┐ │    │   │                        │      │                         │
/// │  │  ┌───────────────┐│ │    │   │          ...           │      │           ...           │
/// │  │ ┌┴──────────────┐││ │    │   │                        │      │                         │
/// │  │┌┴──────────────┐├┘│─┼────┘   │ ┌───────────────────┐  │      │  ┌───────────────────┐  │
/// │  ││  BufferBatch  ├┘ │ │        │ │  ┌───────────────┐│  │      │  │  ┌───────────────┐│  │
/// │  │└───────────────┘  │ │        │ │ ┌┴──────────────┐││  │      │  │ ┌┴──────────────┐││  │
/// │  └───────────────────┘ │        │ │┌┴──────────────┐├┘│──┼──────┼─▶│┌┴──────────────┐├┘│  │
/// │                        │        │ ││ SnapshotBatch ├┘ │  │      │  ││ SnapshotBatch ├┘ │  │
/// │          ...           │        │ │└───────────────┘  │  │      │  │└───────────────┘  │  │
/// │                        │        │ └───────────────────┘  │      │  └───────────────────┘  │
/// └────────────────────────┘        └────────────────────────┘      └─────────────────────────┘
#[derive(Debug, Default)]
struct DataBuffer {
    /// Buffer of incoming writes
    pub(crate) buffer: Option<BufferBatch>,

    /// The max_persisted_sequence number for any parquet_file in this partition
    pub(crate) max_persisted_sequence_number: Option<SequenceNumber>,

    /// Buffer of tombstones whose time range may overlap with this partition.
    /// All tombstones were already applied to corresponding snapshots. This list
    /// only keep the ones that come during persisting. The reason
    /// we keep them becasue if a query comes, we need to apply these tombstones
    /// on the persiting data before sending it to the Querier
    /// When the `persiting` is done and removed, this list will get empty, too
    pub(crate) deletes_during_persisting: Vec<Tombstone>,

    /// Data in `buffer` will be moved to a `snapshot` when one of these happens:
    ///  . A background persist is called
    ///  . A read request from Querier
    /// The `buffer` will be empty when this happens.
    pub(crate) snapshots: Vec<Arc<SnapshotBatch>>,
    /// When a persist is called, data in `buffer` will be moved to a `snapshot`
    /// and then all `snapshots` will be moved to a `persisting`.
    /// Both `buffer` and 'snaphots` will be empty when this happens.
    pub(crate) persisting: Option<Arc<PersistingBatch>>,
    // Extra Notes:
    //  . In MVP, we will only persist a set of snapshots at a time.
    //    In later version, multiple perssiting operations may be happenning concurrently but
    //    their persisted info must be added into the Catalog in their data
    //    ingesting order.
    //  . When a read request comes from a Querier, all data from `snaphots`
    //    and `persisting` must be sent to the Querier.
    //  . After the `persiting` data is persisted and successfully added
    //    into the Catalog, it will be removed from this Data Buffer.
    //    This data might be added into an extra cache to serve up to
    //    Queriers that may not have loaded the parquet files from object
    //    storage yet. But this will be decided after MVP.
}

impl DataBuffer {
    /// Add a new tombstones into the DataBuffer
    pub fn add_tombstone(&mut self, tombstone: Tombstone) {
        // Only keep this tombstone if some data is being persisted
        if self.persisting.is_some() {
            self.deletes_during_persisting.push(tombstone);
        }
    }

    /// Move `BufferBatch`es to a `SnapshotBatch`.
    pub fn snapshot(&mut self) -> Result<(), mutable_batch::Error> {
        let snapshot = self.copy_buffer_to_snapshot()?;
        if let Some(snapshot) = snapshot {
            self.snapshots.push(snapshot);
            self.buffer = None;
        }

        Ok(())
    }

    /// Returns snapshot of the buffer but keep data in the buffer
    pub fn copy_buffer_to_snapshot(
        &self,
    ) -> Result<Option<Arc<SnapshotBatch>>, mutable_batch::Error> {
        if let Some(buf) = &self.buffer {
            return Ok(Some(Arc::new(SnapshotBatch {
                min_sequence_number: buf.min_sequence_number,
                max_sequence_number: buf.max_sequence_number,
                data: Arc::new(buf.data.to_arrow(Selection::All)?),
            })));
        }

        Ok(None)
    }

    /// Snapshots the buffer and make a QueryableBatch for all the snapshots
    /// Both buffer and snapshots will be empty after this
    pub fn snapshot_to_queryable_batch(
        &mut self,
        table_name: &str,
        tombstone: Option<Tombstone>,
    ) -> Option<QueryableBatch> {
        self.snapshot()
            .expect("This mutable batch snapshot error should be impossible.");

        let mut data = vec![];
        std::mem::swap(&mut data, &mut self.snapshots);

        let mut tombstones = vec![];
        if let Some(tombstone) = tombstone {
            tombstones.push(tombstone);
        }

        // only produce batch if there is any data
        if data.is_empty() {
            None
        } else {
            Some(QueryableBatch::new(table_name, data, tombstones))
        }
    }

    /// Returns all existing snapshots plus data in the buffer
    /// This only read data. Data in the buffer will be kept in the buffer
    pub fn buffer_and_snapshots(&self) -> Result<Vec<Arc<SnapshotBatch>>> {
        // Existing snapshots
        let mut snapshots = self.snapshots.clone();

        // copy the buffer to a snapshot
        let buffer_snapshot = self
            .copy_buffer_to_snapshot()
            .context(BufferToSnapshotSnafu)?;
        snapshots.extend(buffer_snapshot);

        Ok(snapshots)
    }

    /// Snapshots the buffer and moves snapshots over to the `PersistingBatch`.
    ///
    /// # Panic
    ///
    /// Panics if there is already a persisting batch.
    pub fn snapshot_to_persisting(
        &mut self,
        shard_id: ShardId,
        table_id: TableId,
        partition_id: PartitionId,
        table_name: &str,
    ) -> Option<Arc<PersistingBatch>> {
        if self.persisting.is_some() {
            panic!("Unable to snapshot while persisting. This is an unexpected state.")
        }

        if let Some(queryable_batch) = self.snapshot_to_queryable_batch(table_name, None) {
            let persisting_batch = Arc::new(PersistingBatch {
                shard_id,
                table_id,
                partition_id,
                object_store_id: Uuid::new_v4(),
                data: Arc::new(queryable_batch),
            });

            self.persisting = Some(Arc::clone(&persisting_batch));

            Some(persisting_batch)
        } else {
            None
        }
    }

    /// Return a QueryableBatch of the persisting batch after applying new tombstones
    pub fn get_persisting_data(&self) -> Option<QueryableBatch> {
        let persisting = match &self.persisting {
            Some(p) => p,
            None => return None,
        };

        // persisting data
        let mut queryable_batch = (*persisting.data).clone();

        // Add new tombstones if any
        queryable_batch.add_tombstones(&self.deletes_during_persisting);

        Some(queryable_batch)
    }

    /// Return the progress in this DataBuffer
    fn progress(&self) -> ShardProgress {
        let progress = ShardProgress::new();

        let progress = if let Some(buffer) = &self.buffer {
            progress.combine(buffer.progress())
        } else {
            progress
        };

        let progress = self.snapshots.iter().fold(progress, |progress, snapshot| {
            progress.combine(snapshot.progress())
        });

        if let Some(persisting) = &self.persisting {
            persisting
                .data
                .data
                .iter()
                .fold(progress, |progress, snapshot| {
                    progress.combine(snapshot.progress())
                })
        } else {
            progress
        }
    }
}

/// BufferBatch is a MutableBatch with its ingesting order, sequence_number, that helps the
/// ingester keep the batches of data in their ingesting order
#[derive(Debug)]
pub struct BufferBatch {
    /// Sequence number of the first write in this batch
    pub(crate) min_sequence_number: SequenceNumber,
    /// Sequence number of the last write in this batch
    pub(crate) max_sequence_number: SequenceNumber,
    /// Ingesting data
    pub(crate) data: MutableBatch,
}

impl BufferBatch {
    /// Return the progress in this DataBuffer
    fn progress(&self) -> ShardProgress {
        ShardProgress::new()
            .with_buffered(self.min_sequence_number)
            .with_buffered(self.max_sequence_number)
    }
}

/// SnapshotBatch contains data of many contiguous BufferBatches
#[derive(Debug, PartialEq)]
pub struct SnapshotBatch {
    /// Min sequence number of its combined BufferBatches
    pub(crate) min_sequence_number: SequenceNumber,
    /// Max sequence number of its combined BufferBatches
    pub(crate) max_sequence_number: SequenceNumber,
    /// Data of its combined BufferBatches kept in one RecordBatch
    pub(crate) data: Arc<RecordBatch>,
}

impl SnapshotBatch {
    /// Return only data of the given columns
    pub fn scan(&self, selection: Selection<'_>) -> Result<Option<Arc<RecordBatch>>> {
        Ok(match selection {
            Selection::All => Some(Arc::clone(&self.data)),
            Selection::Some(columns) => {
                let schema = self.data.schema();

                let indices = columns
                    .iter()
                    .filter_map(|&column_name| {
                        match schema.index_of(column_name) {
                            Ok(idx) => Some(idx),
                            _ => None, // this batch does not include data of this column_name
                        }
                    })
                    .collect::<Vec<_>>();
                if indices.is_empty() {
                    None
                } else {
                    Some(Arc::new(
                        self.data.project(&indices).context(FilterColumnSnafu {})?,
                    ))
                }
            }
        })
    }

    /// Return progress in this data
    fn progress(&self) -> ShardProgress {
        ShardProgress::new()
            .with_buffered(self.min_sequence_number)
            .with_buffered(self.max_sequence_number)
    }
}

/// PersistingBatch contains all needed info and data for creating
/// a parquet file for given set of SnapshotBatches
#[derive(Debug, PartialEq, Clone)]
pub struct PersistingBatch {
    /// Shard id of the data
    pub(crate) shard_id: ShardId,

    /// Table id of the data
    pub(crate) table_id: TableId,

    /// Partition Id of the data
    pub(crate) partition_id: PartitionId,

    /// Id of to-be-created parquet file of this data
    pub(crate) object_store_id: Uuid,

    /// data
    pub(crate) data: Arc<QueryableBatch>,
}

/// Queryable data used for both query and persistence
#[derive(Debug, PartialEq, Clone)]
pub struct QueryableBatch {
    /// data
    pub(crate) data: Vec<Arc<SnapshotBatch>>,

    /// Delete predicates of the tombstones
    pub(crate) delete_predicates: Vec<Arc<DeletePredicate>>,

    /// This is needed to return a reference for a trait function
    pub(crate) table_name: String,
}

/// Status of a partition that has unpersisted data.
///
/// Note that this structure is specific to a partition (which itself is bound to a table and
/// shard)!
#[derive(Debug, Clone, PartialEq, Eq)]
#[allow(missing_copy_implementations)]
pub struct PartitionStatus {
    /// Max sequence number persisted
    pub parquet_max_sequence_number: Option<SequenceNumber>,

    /// Max sequence number for a tombstone
    pub tombstone_max_sequence_number: Option<SequenceNumber>,
}

/// Stream of snapshots.
///
/// Every snapshot is a dedicated [`SendableRecordBatchStream`].
pub(crate) type SnapshotStream =
    Pin<Box<dyn Stream<Item = Result<SendableRecordBatchStream, ArrowError>> + Send>>;

/// Response data for a single partition.
pub(crate) struct IngesterQueryPartition {
    /// Stream of snapshots.
    snapshots: SnapshotStream,

    /// Partition ID.
    id: PartitionId,

    /// Partition persistence status.
    status: PartitionStatus,
}

impl std::fmt::Debug for IngesterQueryPartition {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("IngesterQueryPartition")
            .field("snapshots", &"<SNAPSHOT STREAM>")
            .field("id", &self.id)
            .field("status", &self.status)
            .finish()
    }
}

impl IngesterQueryPartition {
    pub(crate) fn new(snapshots: SnapshotStream, id: PartitionId, status: PartitionStatus) -> Self {
        Self {
            snapshots,
            id,
            status,
        }
    }
}

/// Stream of partitions in this response.
pub(crate) type IngesterQueryPartitionStream =
    Pin<Box<dyn Stream<Item = Result<IngesterQueryPartition, ArrowError>> + Send>>;

/// Response streams for querier<>ingester requests.
///
/// The data structure is constructed to allow lazy/streaming data generation. For easier
/// consumption according to the wire protocol, use the [`flatten`](Self::flatten) method.
pub struct IngesterQueryResponse {
    /// Stream of partitions.
    partitions: IngesterQueryPartitionStream,
}

impl std::fmt::Debug for IngesterQueryResponse {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("IngesterQueryResponse")
            .field("partitions", &"<PARTITION STREAM>")
            .finish()
    }
}

impl IngesterQueryResponse {
    /// Make a response
    pub(crate) fn new(partitions: IngesterQueryPartitionStream) -> Self {
        Self { partitions }
    }

    /// Flattens the data according to the wire protocol.
    pub fn flatten(self) -> FlatIngesterQueryResponseStream {
        self.partitions
            .flat_map(|partition_res| match partition_res {
                Ok(partition) => {
                    let head = futures::stream::once(async move {
                        Ok(FlatIngesterQueryResponse::StartPartition {
                            partition_id: partition.id,
                            status: partition.status,
                        })
                    });
                    let tail = partition
                        .snapshots
                        .flat_map(|snapshot_res| match snapshot_res {
                            Ok(snapshot) => {
                                let schema = Arc::new(optimize_schema(&snapshot.schema()));

                                let schema_captured = Arc::clone(&schema);
                                let head = futures::stream::once(async {
                                    Ok(FlatIngesterQueryResponse::StartSnapshot {
                                        schema: schema_captured,
                                    })
                                });

                                let tail = snapshot.map(move |batch_res| match batch_res {
                                    Ok(batch) => Ok(FlatIngesterQueryResponse::RecordBatch {
                                        batch: optimize_record_batch(&batch, Arc::clone(&schema))?,
                                    }),
                                    Err(e) => Err(e),
                                });

                                head.chain(tail).boxed()
                            }
                            Err(e) => futures::stream::once(async { Err(e) }).boxed(),
                        });

                    head.chain(tail).boxed()
                }
                Err(e) => futures::stream::once(async { Err(e) }).boxed(),
            })
            .boxed()
    }
}

/// Flattened version of [`IngesterQueryResponse`].
pub type FlatIngesterQueryResponseStream =
    Pin<Box<dyn Stream<Item = Result<FlatIngesterQueryResponse, ArrowError>> + Send>>;

/// Element within the flat wire protocol.
#[derive(Debug, PartialEq)]
pub enum FlatIngesterQueryResponse {
    /// Start a new partition.
    StartPartition {
        /// Partition ID.
        partition_id: PartitionId,

        /// Partition persistence status.
        status: PartitionStatus,
    },

    /// Start a new snapshot.
    ///
    /// The snapshot belongs to the partition of the last [`StartPartition`](Self::StartPartition)
    /// message.
    StartSnapshot {
        /// Snapshot schema.
        schema: Arc<arrow::datatypes::Schema>,
    },

    /// Add a record batch to the snapshot that was announced by the last
    /// [`StartSnapshot`](Self::StartSnapshot) message.
    RecordBatch {
        /// Record batch.
        batch: RecordBatch,
    },
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        lifecycle::{LifecycleConfig, LifecycleManager},
        test_util::create_tombstone,
    };
    use arrow::datatypes::SchemaRef;
    use arrow_util::assert_batches_sorted_eq;
    use assert_matches::assert_matches;
    use data_types::{
        ColumnId, ColumnSet, CompactionLevel, NamespaceSchema, NonEmptyString, ParquetFileParams,
        Sequence, TimestampRange,
    };
    use datafusion::physical_plan::RecordBatchStream;
    use dml::{DmlDelete, DmlMeta, DmlWrite};
    use futures::TryStreamExt;
    use iox_catalog::{mem::MemCatalog, validate_or_insert_schema};
    use iox_time::Time;
    use metric::{MetricObserver, Observation};
    use mutable_batch_lp::{lines_to_batches, test_helpers::lp_to_mutable_batch};
    use object_store::memory::InMemory;
    use std::{
        ops::DerefMut,
        task::{Context, Poll},
        time::Duration,
    };

    #[test]
    fn snapshot_empty_buffer_adds_no_snapshots() {
        let mut data_buffer = DataBuffer::default();

        data_buffer.snapshot().unwrap();

        assert!(data_buffer.snapshots.is_empty());
    }

    #[test]
    fn snapshot_buffer_batch_moves_to_snapshots() {
        let mut data_buffer = DataBuffer::default();

        let seq_num1 = SequenceNumber::new(1);
        let (_, mutable_batch1) =
            lp_to_mutable_batch(r#"foo,t1=asdf iv=1i,uv=774u,fv=1.0,bv=true,sv="hi" 1"#);
        let buffer_batch1 = BufferBatch {
            min_sequence_number: seq_num1,
            max_sequence_number: seq_num1,
            data: mutable_batch1,
        };
        let record_batch1 = buffer_batch1.data.to_arrow(Selection::All).unwrap();
        data_buffer.buffer = Some(buffer_batch1);

        data_buffer.snapshot().unwrap();

        assert!(data_buffer.buffer.is_none());
        assert_eq!(data_buffer.snapshots.len(), 1);

        let snapshot = &data_buffer.snapshots[0];
        assert_eq!(snapshot.min_sequence_number, seq_num1);
        assert_eq!(snapshot.max_sequence_number, seq_num1);
        assert_eq!(&*snapshot.data, &record_batch1);
    }

    #[test]
    fn snapshot_buffer_different_but_compatible_schemas() {
        let mut partition_data = PartitionData {
            id: PartitionId::new(1),
            data: Default::default(),
        };

        let seq_num1 = SequenceNumber::new(1);
        // Missing tag `t1`
        let (_, mut mutable_batch1) =
            lp_to_mutable_batch(r#"foo iv=1i,uv=774u,fv=1.0,bv=true,sv="hi" 1"#);
        partition_data
            .buffer_write(seq_num1, mutable_batch1.clone())
            .unwrap();

        let seq_num2 = SequenceNumber::new(2);
        // Missing field `iv`
        let (_, mutable_batch2) =
            lp_to_mutable_batch(r#"foo,t1=aoeu uv=1u,fv=12.0,bv=false,sv="bye" 10000"#);

        partition_data
            .buffer_write(seq_num2, mutable_batch2.clone())
            .unwrap();
        partition_data.data.snapshot().unwrap();

        assert!(partition_data.data.buffer.is_none());
        assert_eq!(partition_data.data.snapshots.len(), 1);

        let snapshot = &partition_data.data.snapshots[0];
        assert_eq!(snapshot.min_sequence_number, seq_num1);
        assert_eq!(snapshot.max_sequence_number, seq_num2);

        mutable_batch1.extend_from(&mutable_batch2).unwrap();
        let combined_record_batch = mutable_batch1.to_arrow(Selection::All).unwrap();
        assert_eq!(&*snapshot.data, &combined_record_batch);
    }

    #[tokio::test]
    async fn buffer_write_updates_lifecycle_manager_indicates_pause() {
        let metrics = Arc::new(metric::Registry::new());
        let catalog: Arc<dyn Catalog> = Arc::new(MemCatalog::new(Arc::clone(&metrics)));
        let mut repos = catalog.repositories().await;
        let topic = repos.topics().create_or_get("whatevs").await.unwrap();
        let query_pool = repos.query_pools().create_or_get("whatevs").await.unwrap();
        let shard_index = ShardIndex::new(0);
        let namespace = repos
            .namespaces()
            .create("foo", "inf", topic.id, query_pool.id)
            .await
            .unwrap();
        let shard1 = repos
            .shards()
            .create_or_get(&topic, shard_index)
            .await
            .unwrap();

        let mut shards = BTreeMap::new();
        let shard_index = ShardIndex::new(0);
        shards.insert(shard1.id, ShardData::new(shard_index, Arc::clone(&metrics)));

        let object_store: Arc<DynObjectStore> = Arc::new(InMemory::new());

        let data = Arc::new(IngesterData::new(
            Arc::clone(&object_store),
            Arc::clone(&catalog),
            shards,
            Arc::new(Executor::new(1)),
            BackoffConfig::default(),
            Arc::clone(&metrics),
        ));

        let schema = NamespaceSchema::new(namespace.id, topic.id, query_pool.id);

        let ignored_ts = Time::from_timestamp_millis(42);

        let w1 = DmlWrite::new(
            "foo",
            lines_to_batches("mem foo=1 10", 0).unwrap(),
            Some("1970-01-01".into()),
            DmlMeta::sequenced(
                Sequence::new(ShardIndex::new(1), SequenceNumber::new(1)),
                ignored_ts,
                None,
                50,
            ),
        );

        let _ = validate_or_insert_schema(w1.tables(), &schema, repos.deref_mut())
            .await
            .unwrap()
            .unwrap();

        std::mem::drop(repos);
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
            metrics,
            Arc::new(SystemProvider::new()),
        );
        let should_pause = data
            .buffer_operation(
                shard1.id,
                DmlOperation::Write(w1.clone()),
                &manager.handle(),
            )
            .await
            .unwrap();
        assert!(!should_pause);
        let should_pause = data
            .buffer_operation(shard1.id, DmlOperation::Write(w1), &manager.handle())
            .await
            .unwrap();
        assert!(should_pause);
    }

    #[tokio::test]
    async fn persist_row_count_trigger() {
        let metrics = Arc::new(metric::Registry::new());
        let catalog: Arc<dyn Catalog> = Arc::new(MemCatalog::new(Arc::clone(&metrics)));
        let mut repos = catalog.repositories().await;
        let topic = repos.topics().create_or_get("whatevs").await.unwrap();
        let query_pool = repos.query_pools().create_or_get("whatevs").await.unwrap();
        let shard_index = ShardIndex::new(0);
        let namespace = repos
            .namespaces()
            .create("foo", "inf", topic.id, query_pool.id)
            .await
            .unwrap();
        let shard1 = repos
            .shards()
            .create_or_get(&topic, shard_index)
            .await
            .unwrap();
        let mut shards = BTreeMap::new();
        shards.insert(
            shard1.id,
            ShardData::new(shard1.shard_index, Arc::clone(&metrics)),
        );

        let object_store: Arc<DynObjectStore> = Arc::new(InMemory::new());

        let data = Arc::new(IngesterData::new(
            Arc::clone(&object_store),
            Arc::clone(&catalog),
            shards,
            Arc::new(Executor::new(1)),
            BackoffConfig::default(),
            Arc::clone(&metrics),
        ));

        let schema = NamespaceSchema::new(namespace.id, topic.id, query_pool.id);

        let w1 = DmlWrite::new(
            "foo",
            lines_to_batches("mem foo=1 10\nmem foo=1 11", 0).unwrap(),
            Some("1970-01-01".into()),
            DmlMeta::sequenced(
                Sequence::new(ShardIndex::new(1), SequenceNumber::new(1)),
                Time::from_timestamp_millis(42),
                None,
                50,
            ),
        );
        let _schema = validate_or_insert_schema(w1.tables(), &schema, repos.deref_mut())
            .await
            .unwrap()
            .unwrap();

        // drop repos so the mem catalog won't deadlock.
        std::mem::drop(repos);

        let manager = LifecycleManager::new(
            LifecycleConfig::new(
                1000000000,
                0,
                0,
                Duration::from_secs(1),
                Duration::from_secs(1),
                1, // This row count will be hit
            ),
            Arc::clone(&metrics),
            Arc::new(SystemProvider::new()),
        );

        let should_pause = data
            .buffer_operation(shard1.id, DmlOperation::Write(w1), &manager.handle())
            .await
            .unwrap();
        // Exceeding the row count doesn't pause ingest (like other partition
        // limits)
        assert!(!should_pause);

        let partition_id = {
            let sd = data.shards.get(&shard1.id).unwrap();
            let n = sd.namespace("foo").unwrap();
            let mem_table = n.table_data("mem").unwrap();
            assert!(n.table_data("mem").is_some());
            let mem_table = mem_table.write().await;
            let p = mem_table.partition_data.get(&"1970-01-01".into()).unwrap();
            p.id
        };

        data.persist(partition_id).await;

        // verify that a file got put into object store
        let file_paths: Vec<_> = object_store
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
        let metrics = Arc::new(metric::Registry::new());
        let catalog: Arc<dyn Catalog> = Arc::new(MemCatalog::new(Arc::clone(&metrics)));
        let mut repos = catalog.repositories().await;
        let topic = repos.topics().create_or_get("whatevs").await.unwrap();
        let query_pool = repos.query_pools().create_or_get("whatevs").await.unwrap();
        let shard_index = ShardIndex::new(0);
        let namespace = repos
            .namespaces()
            .create("foo", "inf", topic.id, query_pool.id)
            .await
            .unwrap();
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
        let mut shards = BTreeMap::new();
        shards.insert(
            shard1.id,
            ShardData::new(shard1.shard_index, Arc::clone(&metrics)),
        );
        shards.insert(
            shard2.id,
            ShardData::new(shard2.shard_index, Arc::clone(&metrics)),
        );

        let object_store: Arc<DynObjectStore> = Arc::new(InMemory::new());

        let data = Arc::new(IngesterData::new(
            Arc::clone(&object_store),
            Arc::clone(&catalog),
            shards,
            Arc::new(Executor::new(1)),
            BackoffConfig::default(),
            Arc::clone(&metrics),
        ));

        let schema = NamespaceSchema::new(namespace.id, topic.id, query_pool.id);

        let ignored_ts = Time::from_timestamp_millis(42);

        let w1 = DmlWrite::new(
            "foo",
            lines_to_batches("mem foo=1 10", 0).unwrap(),
            Some("1970-01-01".into()),
            DmlMeta::sequenced(
                Sequence::new(ShardIndex::new(1), SequenceNumber::new(1)),
                ignored_ts,
                None,
                50,
            ),
        );
        let schema = validate_or_insert_schema(w1.tables(), &schema, repos.deref_mut())
            .await
            .unwrap()
            .unwrap();

        let w2 = DmlWrite::new(
            "foo",
            lines_to_batches("cpu foo=1 10", 1).unwrap(),
            Some("1970-01-01".into()),
            DmlMeta::sequenced(
                Sequence::new(ShardIndex::new(2), SequenceNumber::new(1)),
                ignored_ts,
                None,
                50,
            ),
        );
        let _ = validate_or_insert_schema(w2.tables(), &schema, repos.deref_mut())
            .await
            .unwrap()
            .unwrap();

        // drop repos so the mem catalog won't deadlock.
        std::mem::drop(repos);
        let w3 = DmlWrite::new(
            "foo",
            lines_to_batches("mem foo=1 30", 2).unwrap(),
            Some("1970-01-01".into()),
            DmlMeta::sequenced(
                Sequence::new(ShardIndex::new(1), SequenceNumber::new(2)),
                ignored_ts,
                None,
                50,
            ),
        );

        let manager = LifecycleManager::new(
            LifecycleConfig::new(
                1,
                0,
                0,
                Duration::from_secs(1),
                Duration::from_secs(1),
                1000000,
            ),
            Arc::clone(&metrics),
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
        assert_progress(&data, shard_index, expected_progress).await;

        let sd = data.shards.get(&shard1.id).unwrap();
        let n = sd.namespace("foo").unwrap();
        let partition_id;
        let table_id;
        {
            let mem_table = n.table_data("mem").unwrap();
            assert!(n.table_data("cpu").is_some());
            let mem_table = mem_table.write().await;
            let p = mem_table.partition_data.get(&"1970-01-01".into()).unwrap();

            table_id = mem_table.table_id;
            partition_id = p.id;
        }
        {
            // verify the partition doesn't have a sort key before any data has been persisted
            let mut repos = catalog.repositories().await;
            let partition_info = repos
                .partitions()
                .partition_info_by_id(partition_id)
                .await
                .unwrap()
                .unwrap();
            assert!(partition_info.partition.sort_key.is_empty());
        }

        data.persist(partition_id).await;

        // verify that a file got put into object store
        let file_paths: Vec<_> = object_store
            .list(None)
            .await
            .unwrap()
            .try_collect()
            .await
            .unwrap();
        assert_eq!(file_paths.len(), 1);

        let mut repos = catalog.repositories().await;
        // verify it put the record in the catalog
        let parquet_files = repos
            .parquet_files()
            .list_by_shard_greater_than(shard1.id, SequenceNumber::new(0))
            .await
            .unwrap();
        assert_eq!(parquet_files.len(), 1);
        let pf = parquet_files.first().unwrap();
        assert_eq!(pf.partition_id, partition_id);
        assert_eq!(pf.table_id, table_id);
        assert_eq!(pf.min_time, Timestamp::new(10));
        assert_eq!(pf.max_time, Timestamp::new(30));
        assert_eq!(pf.max_sequence_number, SequenceNumber::new(2));
        assert_eq!(pf.shard_id, shard1.id);
        assert!(pf.to_delete.is_none());

        // This value should be recorded in the metrics asserted next;
        // it is less than 500 KB
        //
        // note that since the file has metadata with timestamps
        // embedded in it, and those timestamps may compress slightly
        // different, the file may change slightly from time to time
        //
        // https://github.com/influxdata/influxdb_iox/issues/5434
        let expected_size = 1252;
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
        let persisted_file_size_bytes: Metric<U64Histogram> = metrics
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

        // verify it set a sort key on the partition in the catalog
        let partition_info = repos
            .partitions()
            .partition_info_by_id(partition_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(partition_info.partition.sort_key, vec!["time"]);

        let mem_table = n.table_data("mem").unwrap();
        let mem_table = mem_table.read().await;

        // verify that the parquet_max_sequence_number got updated
        assert_eq!(
            mem_table.parquet_max_sequence_number(),
            Some(SequenceNumber::new(2))
        );

        // check progresses after persist
        let expected_progress = ShardProgress::new()
            .with_buffered(SequenceNumber::new(1))
            .with_persisted(SequenceNumber::new(2));
        assert_progress(&data, shard_index, expected_progress).await;
    }

    #[tokio::test]
    async fn partial_write_progress() {
        test_helpers::maybe_start_logging();
        let metrics = Arc::new(metric::Registry::new());
        let catalog: Arc<dyn Catalog> = Arc::new(MemCatalog::new(Arc::clone(&metrics)));
        let mut repos = catalog.repositories().await;
        let topic = repos.topics().create_or_get("whatevs").await.unwrap();
        let query_pool = repos.query_pools().create_or_get("whatevs").await.unwrap();
        let shard_index = ShardIndex::new(0);
        let namespace = repos
            .namespaces()
            .create("foo", "inf", topic.id, query_pool.id)
            .await
            .unwrap();
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
        let mut shards = BTreeMap::new();
        shards.insert(
            shard1.id,
            ShardData::new(shard1.shard_index, Arc::clone(&metrics)),
        );
        shards.insert(
            shard2.id,
            ShardData::new(shard2.shard_index, Arc::clone(&metrics)),
        );

        let object_store: Arc<DynObjectStore> = Arc::new(InMemory::new());

        let data = Arc::new(IngesterData::new(
            Arc::clone(&object_store),
            Arc::clone(&catalog),
            shards,
            Arc::new(Executor::new(1)),
            BackoffConfig::default(),
            Arc::clone(&metrics),
        ));

        let schema = NamespaceSchema::new(namespace.id, topic.id, query_pool.id);

        let ignored_ts = Time::from_timestamp_millis(42);

        // write with sequence number 1
        let w1 = DmlWrite::new(
            "foo",
            lines_to_batches("mem foo=1 10", 0).unwrap(),
            Some("1970-01-01".into()),
            DmlMeta::sequenced(
                Sequence::new(ShardIndex::new(1), SequenceNumber::new(1)),
                ignored_ts,
                None,
                50,
            ),
        );
        let _ = validate_or_insert_schema(w1.tables(), &schema, repos.deref_mut())
            .await
            .unwrap()
            .unwrap();

        // write with sequence number 2
        let w2 = DmlWrite::new(
            "foo",
            lines_to_batches("mem foo=1 30\ncpu bar=1 20", 0).unwrap(),
            Some("1970-01-01".into()),
            DmlMeta::sequenced(
                Sequence::new(ShardIndex::new(1), SequenceNumber::new(2)),
                ignored_ts,
                None,
                50,
            ),
        );
        let _ = validate_or_insert_schema(w2.tables(), &schema, repos.deref_mut())
            .await
            .unwrap()
            .unwrap();

        drop(repos); // release catalog transaction

        let manager = LifecycleManager::new(
            LifecycleConfig::new(
                1,
                0,
                0,
                Duration::from_secs(1),
                Duration::from_secs(1),
                1000000,
            ),
            metrics,
            Arc::new(SystemProvider::new()),
        );

        // buffer operation 1, expect progress buffered sequence number should be 1
        data.buffer_operation(shard1.id, DmlOperation::Write(w1), &manager.handle())
            .await
            .unwrap();

        // Get the namespace
        let sd = data.shards.get(&shard1.id).unwrap();
        let n = sd.namespace("foo").unwrap();

        let expected_progress = ShardProgress::new().with_buffered(SequenceNumber::new(1));
        assert_progress(&data, shard_index, expected_progress).await;

        // configure the the namespace to wait after each insert.
        n.test_triggers.enable_pause_after_write().await;

        // now, buffer operation 2 which has two tables,
        let captured_data = Arc::clone(&data);
        let task = tokio::task::spawn(async move {
            captured_data
                .buffer_operation(shard1.id, DmlOperation::Write(w2), &manager.handle())
                .await
                .unwrap();
        });

        n.test_triggers.wait_for_pause_after_write().await;

        // Check that while the write is only partially complete, the
        // buffered sequence number hasn't increased
        let expected_progress = ShardProgress::new()
            // sequence 2 hasn't been buffered yet
            .with_buffered(SequenceNumber::new(1));
        assert_progress(&data, shard_index, expected_progress).await;

        // allow the write to complete
        n.test_triggers.release_pause_after_write().await;
        task.await.expect("task completed unsuccessfully");

        // check progresses after the write completes
        let expected_progress = ShardProgress::new()
            .with_buffered(SequenceNumber::new(1))
            .with_buffered(SequenceNumber::new(2));
        assert_progress(&data, shard_index, expected_progress).await;
    }

    // Test deletes mixed with writes on a single parittion
    #[tokio::test]
    async fn writes_and_deletes() {
        // Make a partition with empty DataBuffer
        let s_id = 1;
        let t_id = 1;
        let p_id = 1;
        let table_name = "restaurant";
        let mut p = PartitionData::new(PartitionId::new(p_id));
        let exec = Executor::new(1);

        // ------------------------------------------
        // Fill `buffer`
        // --- seq_num: 1
        let (_, mb) = lp_to_mutable_batch(r#"restaurant,city=Boston day="fri",temp=50 10"#);
        p.buffer_write(SequenceNumber::new(1), mb).unwrap();

        // --- seq_num: 2
        let (_, mb) = lp_to_mutable_batch(r#"restaurant,city=Andover day="thu",temp=44 15"#);

        p.buffer_write(SequenceNumber::new(2), mb).unwrap();

        // verify data
        assert_eq!(
            p.data.buffer.as_ref().unwrap().min_sequence_number,
            SequenceNumber::new(1)
        );
        assert_eq!(
            p.data.buffer.as_ref().unwrap().max_sequence_number,
            SequenceNumber::new(2)
        );
        assert_eq!(p.data.snapshots.len(), 0);
        assert_eq!(p.data.deletes_during_persisting.len(), 0);
        assert_eq!(p.data.persisting, None);

        // ------------------------------------------
        // Delete
        // --- seq_num: 3
        let ts = create_tombstone(
            1,         // tombstone id
            t_id,      // table id
            s_id,      // shard id
            3,         // delete's seq_number
            0,         // min time of data to get deleted
            20,        // max time of data to get deleted
            "day=thu", // delete predicate
        );
        // one row will get deleted, the other is moved to snapshot
        p.buffer_tombstone(&exec, "restaurant", ts).await;

        // verify data
        assert!(p.data.buffer.is_none()); // always empty after delete
        assert_eq!(p.data.snapshots.len(), 1); // one snpashot if there is data
        assert_eq!(p.data.deletes_during_persisting.len(), 0);
        assert_eq!(p.data.persisting, None);
        // snapshot only has one row since the other one got deleted
        let data = (*p.data.snapshots[0].data).clone();
        let expected = vec![
            "+--------+-----+------+--------------------------------+",
            "| city   | day | temp | time                           |",
            "+--------+-----+------+--------------------------------+",
            "| Boston | fri | 50   | 1970-01-01T00:00:00.000000010Z |",
            "+--------+-----+------+--------------------------------+",
        ];
        assert_batches_sorted_eq!(&expected, &[data]);
        assert_eq!(p.data.snapshots[0].min_sequence_number.get(), 1);
        assert_eq!(p.data.snapshots[0].max_sequence_number.get(), 3);

        // ------------------------------------------
        // Fill `buffer`
        // --- seq_num: 4
        let (_, mb) = lp_to_mutable_batch(
            r#"
                restaurant,city=Medford day="sun",temp=55 22
                restaurant,city=Boston day="sun",temp=57 24
            "#,
        );
        p.buffer_write(SequenceNumber::new(4), mb).unwrap();

        // --- seq_num: 5
        let (_, mb) = lp_to_mutable_batch(r#"restaurant,city=Andover day="tue",temp=56 30"#);

        p.buffer_write(SequenceNumber::new(5), mb).unwrap();

        // verify data
        assert_eq!(
            p.data.buffer.as_ref().unwrap().min_sequence_number,
            SequenceNumber::new(4)
        );
        assert_eq!(
            p.data.buffer.as_ref().unwrap().max_sequence_number,
            SequenceNumber::new(5)
        );
        assert_eq!(p.data.snapshots.len(), 1); // existing sanpshot
        assert_eq!(p.data.deletes_during_persisting.len(), 0);
        assert_eq!(p.data.persisting, None);

        // ------------------------------------------
        // Delete
        // --- seq_num: 6
        let ts = create_tombstone(
            2,             // tombstone id
            t_id,          // table id
            s_id,          // shard id
            6,             // delete's seq_number
            10,            // min time of data to get deleted
            50,            // max time of data to get deleted
            "city=Boston", // delete predicate
        );
        // two rows will get deleted, one from existing snapshot, one from the buffer being moved
        // to snpashot
        p.buffer_tombstone(&exec, "restaurant", ts).await;

        // verify data
        assert!(p.data.buffer.is_none()); // always empty after delete
        assert_eq!(p.data.snapshots.len(), 1); // one snpashot
        assert_eq!(p.data.deletes_during_persisting.len(), 0);
        assert_eq!(p.data.persisting, None);
        // snapshot only has two rows since the other 2 rows with city=Boston have got deleted
        let data = (*p.data.snapshots[0].data).clone();
        let expected = vec![
            "+---------+-----+------+--------------------------------+",
            "| city    | day | temp | time                           |",
            "+---------+-----+------+--------------------------------+",
            "| Andover | tue | 56   | 1970-01-01T00:00:00.000000030Z |",
            "| Medford | sun | 55   | 1970-01-01T00:00:00.000000022Z |",
            "+---------+-----+------+--------------------------------+",
        ];
        assert_batches_sorted_eq!(&expected, &[data]);
        assert_eq!(p.data.snapshots[0].min_sequence_number.get(), 1);
        assert_eq!(p.data.snapshots[0].max_sequence_number.get(), 6);

        // ------------------------------------------
        // Persisting
        let p_batch = p
            .snapshot_to_persisting_batch(
                ShardId::new(s_id),
                TableId::new(t_id),
                PartitionId::new(p_id),
                table_name,
            )
            .unwrap();

        // verify data
        assert!(p.data.buffer.is_none()); // always empty after issuing persit
        assert_eq!(p.data.snapshots.len(), 0); // always empty after issuing persit
        assert_eq!(p.data.deletes_during_persisting.len(), 0); // deletes not happen yet
        assert_eq!(p.data.persisting, Some(Arc::clone(&p_batch)));

        // ------------------------------------------
        // Delete
        // --- seq_num: 7
        let ts = create_tombstone(
            3,         // tombstone id
            t_id,      // table id
            s_id,      // shard id
            7,         // delete's seq_number
            10,        // min time of data to get deleted
            50,        // max time of data to get deleted
            "temp=55", // delete predicate
        );
        // if a query come while persisting, the row with temp=55 will be deleted before
        // data is sent back to Querier
        p.buffer_tombstone(&exec, "restaurant", ts).await;

        // verify data
        assert!(p.data.buffer.is_none()); // always empty after delete
                                          // no snpashots becasue buffer has not data yet and the
                                          // snapshot was empty too
        assert_eq!(p.data.snapshots.len(), 0);
        assert_eq!(p.data.deletes_during_persisting.len(), 1); // tombstone added since data is
                                                               // persisting
        assert_eq!(p.data.persisting, Some(Arc::clone(&p_batch)));

        // ------------------------------------------
        // Fill `buffer`
        // --- seq_num: 8
        let (_, mb) = lp_to_mutable_batch(
            r#"
                restaurant,city=Wilmington day="sun",temp=55 35
                restaurant,city=Boston day="sun",temp=60 36
                restaurant,city=Boston day="sun",temp=62 38
            "#,
        );
        p.buffer_write(SequenceNumber::new(8), mb).unwrap();

        // verify data
        assert_eq!(
            p.data.buffer.as_ref().unwrap().min_sequence_number,
            SequenceNumber::new(8)
        ); // 1 newly added mutable batch of 3 rows of data
        assert_eq!(p.data.snapshots.len(), 0); // still empty
        assert_eq!(p.data.deletes_during_persisting.len(), 1);
        assert_eq!(p.data.persisting, Some(Arc::clone(&p_batch)));

        // ------------------------------------------
        // Take snaphot of the `buffer`
        p.snapshot().unwrap();
        // verify data
        assert!(p.data.buffer.is_none()); // empty after snaphot
        assert_eq!(p.data.snapshots.len(), 1); // data moved from buffer
        assert_eq!(p.data.deletes_during_persisting.len(), 1);
        assert_eq!(p.data.persisting, Some(Arc::clone(&p_batch)));
        // snapshot has three rows moved from buffer
        let data = (*p.data.snapshots[0].data).clone();
        let expected = vec![
            "+------------+-----+------+--------------------------------+",
            "| city       | day | temp | time                           |",
            "+------------+-----+------+--------------------------------+",
            "| Wilmington | sun | 55   | 1970-01-01T00:00:00.000000035Z |",
            "| Boston     | sun | 60   | 1970-01-01T00:00:00.000000036Z |",
            "| Boston     | sun | 62   | 1970-01-01T00:00:00.000000038Z |",
            "+------------+-----+------+--------------------------------+",
        ];
        assert_batches_sorted_eq!(&expected, &[data]);
        assert_eq!(p.data.snapshots[0].min_sequence_number.get(), 8);
        assert_eq!(p.data.snapshots[0].max_sequence_number.get(), 8);

        // ------------------------------------------
        // Delete
        // --- seq_num: 9
        let ts = create_tombstone(
            4,         // tombstone id
            t_id,      // table id
            s_id,      // shard id
            9,         // delete's seq_number
            10,        // min time of data to get deleted
            50,        // max time of data to get deleted
            "temp=60", // delete predicate
        );
        // the row with temp=60 will be removed from the sanphot
        p.buffer_tombstone(&exec, "restaurant", ts).await;

        // verify data
        assert!(p.data.buffer.is_none()); // always empty after delete
        assert_eq!(p.data.snapshots.len(), 1); // new snapshot of the existing with delete applied
        assert_eq!(p.data.deletes_during_persisting.len(), 2); // one more tombstone added make it 2
        assert_eq!(p.data.persisting, Some(Arc::clone(&p_batch)));
        // snapshot has only 2 rows becasue the row with tem=60 was removed
        let data = (*p.data.snapshots[0].data).clone();
        let expected = vec![
            "+------------+-----+------+--------------------------------+",
            "| city       | day | temp | time                           |",
            "+------------+-----+------+--------------------------------+",
            "| Wilmington | sun | 55   | 1970-01-01T00:00:00.000000035Z |",
            "| Boston     | sun | 62   | 1970-01-01T00:00:00.000000038Z |",
            "+------------+-----+------+--------------------------------+",
        ];
        assert_batches_sorted_eq!(&expected, &[data]);
        assert_eq!(p.data.snapshots[0].min_sequence_number.get(), 8);
        assert_eq!(p.data.snapshots[0].max_sequence_number.get(), 9);

        exec.join().await;
    }

    #[tokio::test]
    async fn buffer_operation_ignores_already_persisted_data() {
        let metrics = Arc::new(metric::Registry::new());
        let catalog: Arc<dyn Catalog> = Arc::new(MemCatalog::new(Arc::clone(&metrics)));
        let mut repos = catalog.repositories().await;
        let topic = repos.topics().create_or_get("whatevs").await.unwrap();
        let query_pool = repos.query_pools().create_or_get("whatevs").await.unwrap();
        let shard_index = ShardIndex::new(0);
        let namespace = repos
            .namespaces()
            .create("foo", "inf", topic.id, query_pool.id)
            .await
            .unwrap();
        let shard = repos
            .shards()
            .create_or_get(&topic, shard_index)
            .await
            .unwrap();

        let schema = NamespaceSchema::new(namespace.id, topic.id, query_pool.id);

        let ignored_ts = Time::from_timestamp_millis(42);

        let w1 = DmlWrite::new(
            "foo",
            lines_to_batches("mem foo=1 10", 0).unwrap(),
            Some("1970-01-01".into()),
            DmlMeta::sequenced(
                Sequence::new(ShardIndex::new(1), SequenceNumber::new(1)),
                ignored_ts,
                None,
                50,
            ),
        );
        let w2 = DmlWrite::new(
            "foo",
            lines_to_batches("mem foo=1 10", 0).unwrap(),
            Some("1970-01-01".into()),
            DmlMeta::sequenced(
                Sequence::new(ShardIndex::new(1), SequenceNumber::new(2)),
                ignored_ts,
                None,
                50,
            ),
        );

        let _ = validate_or_insert_schema(w1.tables(), &schema, repos.deref_mut())
            .await
            .unwrap()
            .unwrap();

        // create some persisted state
        let table = repos
            .tables()
            .create_or_get("mem", namespace.id)
            .await
            .unwrap();
        let partition = repos
            .partitions()
            .create_or_get("1970-01-01".into(), shard.id, table.id)
            .await
            .unwrap();
        let partition2 = repos
            .partitions()
            .create_or_get("1970-01-02".into(), shard.id, table.id)
            .await
            .unwrap();

        let parquet_file_params = ParquetFileParams {
            shard_id: shard.id,
            namespace_id: namespace.id,
            table_id: table.id,
            partition_id: partition.id,
            object_store_id: Uuid::new_v4(),
            max_sequence_number: SequenceNumber::new(1),
            min_time: Timestamp::new(1),
            max_time: Timestamp::new(1),
            file_size_bytes: 0,
            row_count: 0,
            compaction_level: CompactionLevel::Initial,
            created_at: Timestamp::new(1),
            column_set: ColumnSet::new([ColumnId::new(1), ColumnId::new(2)]),
        };
        repos
            .parquet_files()
            .create(parquet_file_params.clone())
            .await
            .unwrap();

        // now create a parquet file in another partition with a much higher sequence persisted
        // sequence number. We want to make sure that this doesn't cause our write in the other
        // partition to get ignored.
        let other_file_params = ParquetFileParams {
            max_sequence_number: SequenceNumber::new(15),
            object_store_id: Uuid::new_v4(),
            partition_id: partition2.id,
            ..parquet_file_params
        };
        repos
            .parquet_files()
            .create(other_file_params)
            .await
            .unwrap();
        std::mem::drop(repos);

        let manager = LifecycleManager::new(
            LifecycleConfig::new(
                1,
                0,
                0,
                Duration::from_secs(1),
                Duration::from_secs(1),
                1000000,
            ),
            Arc::clone(&metrics),
            Arc::new(SystemProvider::new()),
        );
        let exec = Executor::new(1);

        let data = NamespaceData::new(namespace.id, &*metrics);

        // w1 should be ignored so it shouldn't be present in the buffer
        let should_pause = data
            .buffer_operation(
                DmlOperation::Write(w1),
                shard.id,
                catalog.as_ref(),
                &manager.handle(),
                &exec,
            )
            .await
            .unwrap();
        {
            let table_data = data.table_data("mem").unwrap();
            let table = table_data.read().await;
            let p = table.partition_data.get(&"1970-01-01".into()).unwrap();
            assert_eq!(
                p.data.max_persisted_sequence_number,
                Some(SequenceNumber::new(1))
            );
            assert!(p.data.buffer.is_none());
        }
        assert!(!should_pause);

        // w2 should be in the buffer
        data.buffer_operation(
            DmlOperation::Write(w2),
            shard.id,
            catalog.as_ref(),
            &manager.handle(),
            &exec,
        )
        .await
        .unwrap();

        let table_data = data.table_data("mem").unwrap();
        let table = table_data.read().await;
        let partition = table.partition_data.get(&"1970-01-01".into()).unwrap();
        assert_eq!(
            partition.data.buffer.as_ref().unwrap().min_sequence_number,
            SequenceNumber::new(2)
        );

        assert_matches!(data.table_count.observe(), Observation::U64Counter(v) => {
            assert_eq!(v, 1, "unexpected table count metric value");
        });
    }

    #[tokio::test]
    async fn buffer_deletes_updates_tombstone_watermark() {
        let metrics = Arc::new(metric::Registry::new());
        let catalog: Arc<dyn Catalog> = Arc::new(MemCatalog::new(Arc::clone(&metrics)));
        let mut repos = catalog.repositories().await;
        let topic = repos.topics().create_or_get("whatevs").await.unwrap();
        let query_pool = repos.query_pools().create_or_get("whatevs").await.unwrap();
        let shard_index = ShardIndex::new(0);
        let namespace = repos
            .namespaces()
            .create("foo", "inf", topic.id, query_pool.id)
            .await
            .unwrap();
        let shard1 = repos
            .shards()
            .create_or_get(&topic, shard_index)
            .await
            .unwrap();

        let mut shards = BTreeMap::new();
        let shard_index = ShardIndex::new(0);
        shards.insert(shard1.id, ShardData::new(shard_index, Arc::clone(&metrics)));

        let object_store: Arc<DynObjectStore> = Arc::new(InMemory::new());

        let data = Arc::new(IngesterData::new(
            Arc::clone(&object_store),
            Arc::clone(&catalog),
            shards,
            Arc::new(Executor::new(1)),
            BackoffConfig::default(),
            Arc::clone(&metrics),
        ));

        let schema = NamespaceSchema::new(namespace.id, topic.id, query_pool.id);

        let ignored_ts = Time::from_timestamp_millis(42);

        let w1 = DmlWrite::new(
            "foo",
            lines_to_batches("mem foo=1 10", 0).unwrap(),
            Some("1970-01-01".into()),
            DmlMeta::sequenced(
                Sequence::new(ShardIndex::new(1), SequenceNumber::new(1)),
                ignored_ts,
                None,
                50,
            ),
        );

        let _ = validate_or_insert_schema(w1.tables(), &schema, repos.deref_mut())
            .await
            .unwrap()
            .unwrap();

        std::mem::drop(repos);
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
            metrics,
            Arc::new(SystemProvider::new()),
        );
        data.buffer_operation(
            shard1.id,
            DmlOperation::Write(w1.clone()),
            &manager.handle(),
        )
        .await
        .unwrap();

        assert_eq!(
            data.shard(shard1.id)
                .unwrap()
                .namespace(&namespace.name)
                .unwrap()
                .table_data("mem")
                .unwrap()
                .read()
                .await
                .tombstone_max_sequence_number(),
            None,
        );

        let predicate = DeletePredicate {
            range: TimestampRange::new(1, 2),
            exprs: vec![],
        };
        let d1 = DmlDelete::new(
            "foo",
            predicate,
            Some(NonEmptyString::new("mem").unwrap()),
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

        assert_eq!(
            data.shard(shard1.id)
                .unwrap()
                .namespace(&namespace.name)
                .unwrap()
                .table_data("mem")
                .unwrap()
                .read()
                .await
                .tombstone_max_sequence_number(),
            Some(SequenceNumber::new(2)),
        );
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

    #[tokio::test]
    async fn test_ingester_query_response_flatten() {
        let batch_1_1 = lp_to_batch("table x=1 0");
        let batch_1_2 = lp_to_batch("table x=2 1");
        let batch_2 = lp_to_batch("table y=1 10");
        let batch_3 = lp_to_batch("table z=1 10");

        let schema_1 = batch_1_1.schema();
        let schema_2 = batch_2.schema();
        let schema_3 = batch_3.schema();

        let response = IngesterQueryResponse::new(Box::pin(futures::stream::iter([
            Ok(IngesterQueryPartition::new(
                Box::pin(futures::stream::iter([
                    Ok(Box::pin(TestRecordBatchStream::new(
                        vec![
                            Ok(batch_1_1.clone()),
                            Err(ArrowError::NotYetImplemented("not yet implemeneted".into())),
                            Ok(batch_1_2.clone()),
                        ],
                        Arc::clone(&schema_1),
                    )) as _),
                    Err(ArrowError::InvalidArgumentError("invalid arg".into())),
                    Ok(Box::pin(TestRecordBatchStream::new(
                        vec![Ok(batch_2.clone())],
                        Arc::clone(&schema_2),
                    )) as _),
                    Ok(Box::pin(TestRecordBatchStream::new(vec![], Arc::clone(&schema_3))) as _),
                ])),
                PartitionId::new(2),
                PartitionStatus {
                    parquet_max_sequence_number: None,
                    tombstone_max_sequence_number: Some(SequenceNumber::new(1)),
                },
            )),
            Err(ArrowError::IoError("some io error".into())),
            Ok(IngesterQueryPartition::new(
                Box::pin(futures::stream::iter([])),
                PartitionId::new(1),
                PartitionStatus {
                    parquet_max_sequence_number: None,
                    tombstone_max_sequence_number: None,
                },
            )),
        ])));

        let actual: Vec<_> = response.flatten().collect().await;
        let expected = vec![
            Ok(FlatIngesterQueryResponse::StartPartition {
                partition_id: PartitionId::new(2),
                status: PartitionStatus {
                    parquet_max_sequence_number: None,
                    tombstone_max_sequence_number: Some(SequenceNumber::new(1)),
                },
            }),
            Ok(FlatIngesterQueryResponse::StartSnapshot { schema: schema_1 }),
            Ok(FlatIngesterQueryResponse::RecordBatch { batch: batch_1_1 }),
            Err(ArrowError::NotYetImplemented("not yet implemeneted".into())),
            Ok(FlatIngesterQueryResponse::RecordBatch { batch: batch_1_2 }),
            Err(ArrowError::InvalidArgumentError("invalid arg".into())),
            Ok(FlatIngesterQueryResponse::StartSnapshot { schema: schema_2 }),
            Ok(FlatIngesterQueryResponse::RecordBatch { batch: batch_2 }),
            Ok(FlatIngesterQueryResponse::StartSnapshot { schema: schema_3 }),
            Err(ArrowError::IoError("some io error".into())),
            Ok(FlatIngesterQueryResponse::StartPartition {
                partition_id: PartitionId::new(1),
                status: PartitionStatus {
                    parquet_max_sequence_number: None,
                    tombstone_max_sequence_number: None,
                },
            }),
        ];

        assert_eq!(actual.len(), expected.len());
        for (actual, expected) in actual.into_iter().zip(expected) {
            match (actual, expected) {
                (Ok(actual), Ok(expected)) => {
                    assert_eq!(actual, expected);
                }
                (Err(_), Err(_)) => {
                    // cannot compare `ArrowError`, but it's unlikely that someone changed the error
                }
                (Ok(_), Err(_)) => panic!("Actual is Ok but expected is Err"),
                (Err(_), Ok(_)) => panic!("Actual is Err but expected is Ok"),
            }
        }
    }

    fn lp_to_batch(lp: &str) -> RecordBatch {
        lp_to_mutable_batch(lp).1.to_arrow(Selection::All).unwrap()
    }

    pub struct TestRecordBatchStream {
        schema: SchemaRef,
        batches: Vec<Result<RecordBatch, ArrowError>>,
    }

    impl TestRecordBatchStream {
        pub fn new(batches: Vec<Result<RecordBatch, ArrowError>>, schema: SchemaRef) -> Self {
            Self { schema, batches }
        }
    }

    impl RecordBatchStream for TestRecordBatchStream {
        fn schema(&self) -> SchemaRef {
            Arc::clone(&self.schema)
        }
    }

    impl futures::Stream for TestRecordBatchStream {
        type Item = Result<RecordBatch, ArrowError>;

        fn poll_next(
            mut self: std::pin::Pin<&mut Self>,
            _: &mut Context<'_>,
        ) -> Poll<Option<Self::Item>> {
            if self.batches.is_empty() {
                Poll::Ready(None)
            } else {
                Poll::Ready(Some(self.batches.remove(0)))
            }
        }

        fn size_hint(&self) -> (usize, Option<usize>) {
            (self.batches.len(), Some(self.batches.len()))
        }
    }
}
