//! Data for the lifecycle of the Ingester

use crate::{
    compact::compact_persisting_batch,
    lifecycle::LifecycleHandle,
    partioning::{Partitioner, PartitionerError},
    querier_handler::query,
};
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use backoff::{Backoff, BackoffConfig};
use data_types::{
    DeletePredicate, KafkaPartition, NamespaceId, PartitionId, PartitionInfo, SequenceNumber,
    SequencerId, TableId, Timestamp, Tombstone,
};
use datafusion::physical_plan::SendableRecordBatchStream;
use dml::DmlOperation;
use iox_catalog::interface::Catalog;
use iox_query::exec::Executor;
use iox_time::SystemProvider;
use metric::U64Counter;
use mutable_batch::MutableBatch;
use object_store::DynObjectStore;
use observability_deps::tracing::{debug, warn};
use parking_lot::RwLock;
use parquet_file::storage::ParquetStorage;
use predicate::Predicate;
use schema::{selection::Selection, Schema};
use snafu::{OptionExt, ResultExt, Snafu};
use std::{
    collections::{btree_map::Entry, BTreeMap},
    convert::TryFrom,
    sync::Arc,
};
use uuid::Uuid;
use write_summary::SequencerProgress;

#[derive(Debug, Snafu)]
#[allow(missing_copy_implementations, missing_docs)]
pub enum Error {
    #[snafu(display("Error while reading Topic {}", name))]
    ReadTopic {
        source: iox_catalog::interface::Error,
        name: String,
    },

    #[snafu(display("Error while reading Kafka Partition id {}", id.get()))]
    ReadSequencer {
        source: iox_catalog::interface::Error,
        id: KafkaPartition,
    },

    #[snafu(display("Sequencer {} not found in data map", sequencer_id))]
    SequencerNotFound { sequencer_id: SequencerId },

    #[snafu(display(
        "Sequencer not found for kafka partition {} in data map",
        kafka_partition
    ))]
    SequencerForPartitionNotFound { kafka_partition: KafkaPartition },

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

    #[snafu(display("The persisting is in progress. Cannot accept more persisting batch"))]
    PersistingNotEmpty,

    #[snafu(display("Nothing in the Persisting list to get removed"))]
    PersistingEmpty,

    #[snafu(display(
        "The given batch does not match any in the Persisting list. \
        Nothing is removed from the Persisting list"
    ))]
    PersistingNotMatch,

    #[snafu(display("Cannot partition data: {}", source))]
    Partitioning { source: PartitionerError },

    #[snafu(display("Snapshot error: {}", source))]
    Snapshot { source: mutable_batch::Error },

    #[snafu(display("Error while filtering columns from snapshot: {}", source))]
    FilterColumn { source: arrow::error::ArrowError },

    #[snafu(display("Partition not found: {}", partition_id))]
    PartitionNotFound { partition_id: PartitionId },

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
    /// The content of each SequenceData will get changed when more namespaces and tables
    /// get ingested.
    sequencers: BTreeMap<SequencerId, SequencerData>,

    /// Partitioner.
    partitioner: Arc<dyn Partitioner>,

    /// Executor for running queries and compacting and persisting
    exec: Arc<Executor>,

    /// Backoff config
    backoff_config: BackoffConfig,
}

impl IngesterData {
    /// Create new instance.
    pub fn new(
        object_store: Arc<DynObjectStore>,
        catalog: Arc<dyn Catalog>,
        sequencers: BTreeMap<SequencerId, SequencerData>,
        partitioner: Arc<dyn Partitioner>,
        exec: Arc<Executor>,
        backoff_config: BackoffConfig,
    ) -> Self {
        Self {
            store: ParquetStorage::new(object_store),
            catalog,
            sequencers,
            partitioner,
            exec,
            backoff_config,
        }
    }

    /// Executor for running queries and compacting and persisting
    pub(crate) fn exec(&self) -> &Arc<Executor> {
        &self.exec
    }

    /// Get sequencer data for specific sequencer.
    #[allow(dead_code)] // Used in tests
    pub(crate) fn sequencer(&self, sequencer_id: SequencerId) -> Option<&SequencerData> {
        self.sequencers.get(&sequencer_id)
    }

    /// Get iterator over sequencers (ID and data).
    pub(crate) fn sequencers(&self) -> impl Iterator<Item = (&SequencerId, &SequencerData)> {
        self.sequencers.iter()
    }

    /// Store the write or delete in the in memory buffer. Deletes will
    /// be written into the catalog before getting stored in the buffer.
    /// Any writes that create new IOx partitions will have those records
    /// created in the catalog before putting into the buffer. Writes will
    /// get logged in the lifecycle manager. If it indicates ingest should
    /// be paused, this function will return true.
    pub async fn buffer_operation(
        &self,
        sequencer_id: SequencerId,
        dml_operation: DmlOperation,
        lifecycle_handle: &dyn LifecycleHandle,
    ) -> Result<bool> {
        let sequencer_data = self
            .sequencers
            .get(&sequencer_id)
            .context(SequencerNotFoundSnafu { sequencer_id })?;
        sequencer_data
            .buffer_operation(
                dml_operation,
                sequencer_id,
                self.catalog.as_ref(),
                lifecycle_handle,
                self.partitioner.as_ref(),
                &self.exec,
            )
            .await
    }

    /// Return the ingestion progress for the specified kafka
    /// partitions. Returns an empty `SequencerProgress` for any kafka
    /// partitions that this ingester doesn't know about.
    pub(crate) async fn progresses(
        &self,
        partitions: Vec<KafkaPartition>,
    ) -> BTreeMap<KafkaPartition, SequencerProgress> {
        let mut progresses = BTreeMap::new();
        for kafka_partition in partitions {
            let sequencer_data = self
                .sequencers
                .iter()
                .map(|(_, sequencer_data)| sequencer_data)
                .find(|sequencer_data| sequencer_data.kafka_partition == kafka_partition);

            let progress = match sequencer_data {
                Some(sequencer_data) => sequencer_data.progress().await,
                None => SequencerProgress::new(), // don't know about this sequencer
            };

            progresses.insert(kafka_partition, progress);
        }
        progresses
    }
}

/// The Persister has a function to persist a given partition ID and to update the
/// assocated sequencer's `min_unpersisted_sequence_number`.
#[async_trait]
pub trait Persister: Send + Sync + 'static {
    /// Persits the partition ID. Will retry forever until it succeeds.
    async fn persist(&self, partition_id: PartitionId);

    /// Updates the sequencer's `min_unpersisted_sequence_number` in the catalog.
    /// This number represents the minimum that might be unpersisted, which is the
    /// farthest back the ingester would need to read in the write buffer to ensure
    /// that all data would be correctly replayed on startup.
    async fn update_min_unpersisted_sequence_number(
        &self,
        sequencer_id: SequencerId,
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
        let sequencer_data = self
            .sequencers
            .get(&partition_info.partition.sequencer_id)
            .unwrap_or_else(|| {
                panic!(
                    "sequencer state for {} not in ingester data",
                    partition_info.partition.sequencer_id
                )
            }); //{
        let namespace = sequencer_data
            .namespace(&partition_info.namespace_name)
            .unwrap_or_else(|| {
                panic!(
                    "namespace {} not in sequencer {} state",
                    partition_info.namespace_name, partition_info.partition.sequencer_id
                )
            });
        debug!(?partition_info, "Persisting");

        let persisting_batch = namespace.snapshot_to_persisting(&partition_info).await;

        if let Some(persisting_batch) = persisting_batch {
            // do the CPU intensive work of compaction, de-duplication and sorting
            let (record_stream, iox_meta, sort_key_update) = match compact_persisting_batch(
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

            // Save the compacted data to a parquet file in object storage.
            //
            // This call retries until it completes.
            let (md, file_size) = self
                .store
                .upload(record_stream, &iox_meta)
                .await
                .expect("unexpected fatal persist error");

            // Add the parquet file to the catalog until succeed
            let parquet_file = iox_meta.to_parquet_file(partition_id, file_size, &md);
            Backoff::new(&self.backoff_config)
                .retry_all_errors("add parquet file to catalog", || async {
                    let mut repos = self.catalog.repositories().await;
                    debug!(
                        table_name=%iox_meta.table_name,
                        "adding parquet file to catalog"
                    );

                    repos.parquet_files().create(parquet_file.clone()).await
                })
                .await
                .expect("retry forever");

            // Update the sort key in the catalog if there are additional columns
            if let Some(new_sort_key) = sort_key_update {
                let sort_key_string = new_sort_key.to_columns();
                Backoff::new(&self.backoff_config)
                    .retry_all_errors("update_sort_key", || async {
                        let mut repos = self.catalog.repositories().await;
                        repos
                            .partitions()
                            .update_sort_key(partition_id, &sort_key_string)
                            .await
                    })
                    .await
                    .expect("retry forever");
            }

            // and remove the persisted data from memory
            debug!(
                table_name=%partition_info.table_name,
                partition_key=%partition_info.partition.partition_key,
                max_sequence_number=%iox_meta.max_sequence_number.get(),
                "mark_persisted"
            );
            namespace
                .mark_persisted(
                    &partition_info.table_name,
                    &partition_info.partition.partition_key,
                    iox_meta.max_sequence_number,
                )
                .await;
        }
    }

    async fn update_min_unpersisted_sequence_number(
        &self,
        sequencer_id: SequencerId,
        sequence_number: SequenceNumber,
    ) {
        Backoff::new(&self.backoff_config)
            .retry_all_errors("updating min_unpersisted_sequence_number", || async {
                self.catalog
                    .repositories()
                    .await
                    .sequencers()
                    .update_min_unpersisted_sequence_number(sequencer_id, sequence_number)
                    .await
            })
            .await
            .expect("retry forever")
    }
}

/// Data of a Shard
#[derive(Debug)]
pub struct SequencerData {
    /// The kafka partition for this sequencer
    kafka_partition: KafkaPartition,

    // New namespaces can come in at any time so we need to be able to add new ones
    namespaces: RwLock<BTreeMap<String, Arc<NamespaceData>>>,

    metrics: Arc<metric::Registry>,
    namespace_count: U64Counter,
}

impl SequencerData {
    /// Initialise a new [`SequencerData`] that emits metrics to `metrics`.
    pub fn new(kafka_partition: KafkaPartition, metrics: Arc<metric::Registry>) -> Self {
        let namespace_count = metrics
            .register_metric::<U64Counter>(
                "ingester_namespaces_total",
                "Number of namespaces known to the ingester",
            )
            .recorder(&[]);

        Self {
            kafka_partition,
            namespaces: Default::default(),
            metrics,
            namespace_count,
        }
    }

    /// Initialize new SequncerData with namespace for testing purpose only
    #[cfg(test)]
    pub fn new_for_test(
        kafka_partition: KafkaPartition,
        namespaces: BTreeMap<String, Arc<NamespaceData>>,
    ) -> Self {
        Self {
            kafka_partition,
            namespaces: RwLock::new(namespaces),
            metrics: Default::default(),
            namespace_count: Default::default(),
        }
    }

    /// Store the write or delete in the sequencer. Deletes will
    /// be written into the catalog before getting stored in the buffer.
    /// Any writes that create new IOx partitions will have those records
    /// created in the catalog before putting into the buffer.
    pub async fn buffer_operation(
        &self,
        dml_operation: DmlOperation,
        sequencer_id: SequencerId,
        catalog: &dyn Catalog,
        lifecycle_handle: &dyn LifecycleHandle,
        partitioner: &dyn Partitioner,
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
            .buffer_operation(
                dml_operation,
                sequencer_id,
                catalog,
                lifecycle_handle,
                partitioner,
                executor,
            )
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

    /// Return the progress of this sequencer
    async fn progress(&self) -> SequencerProgress {
        let namespaces: Vec<_> = self.namespaces.read().values().map(Arc::clone).collect();

        let mut progress = SequencerProgress::new();

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
        }
    }

    /// Buffer the operation in the cache, adding any new partitions or delete tombstones to the
    /// catalog. Returns true if ingest should be paused due to memory limits set in the passed
    /// lifecycle manager.
    pub async fn buffer_operation(
        &self,
        dml_operation: DmlOperation,
        sequencer_id: SequencerId,
        catalog: &dyn Catalog,
        lifecycle_handle: &dyn LifecycleHandle,
        partitioner: &dyn Partitioner,
        executor: &Executor,
    ) -> Result<bool> {
        let sequence_number = dml_operation
            .meta()
            .sequence()
            .expect("must have sequence number")
            .sequence_number;
        let sequence_number = i64::try_from(sequence_number).expect("sequence out of bounds");
        let sequence_number = SequenceNumber::new(sequence_number);

        // Note that this namespace is actively writing this sequence
        // number. Since there is no namespace wide lock held during a
        // write, this number is used to detect and update reported
        // progress during a write
        let _sequence_number_guard =
            ScopedSequenceNumber::new(sequence_number, &self.buffering_sequence_number);

        match dml_operation {
            DmlOperation::Write(write) => {
                let mut pause_writes = false;

                for (t, b) in write.into_tables() {
                    let table_data = match self.table_data(&t) {
                        Some(t) => t,
                        None => self.insert_table(sequencer_id, &t, catalog).await?,
                    };

                    let mut table_data = table_data.write().await;
                    let should_pause = table_data
                        .buffer_table_write(
                            sequence_number,
                            b,
                            sequencer_id,
                            catalog,
                            lifecycle_handle,
                            partitioner,
                        )
                        .await?;

                    pause_writes = pause_writes || should_pause;
                }

                Ok(pause_writes)
            }
            DmlOperation::Delete(delete) => {
                let table_name = delete.table_name().context(TableNotPresentSnafu)?;
                let table_data = match self.table_data(table_name) {
                    Some(t) => t,
                    None => self.insert_table(sequencer_id, table_name, catalog).await?,
                };

                let mut table_data = table_data.write().await;

                table_data
                    .buffer_delete(
                        table_name,
                        delete.predicate(),
                        sequencer_id,
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
        partition_key: &str,
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
                        partition_info.partition.sequencer_id,
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
        sequencer_id: SequencerId,
        table_name: &str,
        catalog: &dyn Catalog,
    ) -> Result<Arc<tokio::sync::RwLock<TableData>>> {
        let mut repos = catalog.repositories().await;
        let info = repos
            .tables()
            .get_table_persist_info(sequencer_id, self.namespace_id, table_name)
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
        partition_key: &str,
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
    async fn progress(&self) -> SequencerProgress {
        let tables: Vec<_> = self.tables.read().values().map(Arc::clone).collect();

        // Consolidate progtress across partitions.
        let mut progress = SequencerProgress::new()
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
    partition_data: BTreeMap<String, PartitionData>,
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
        partitions: BTreeMap<String, PartitionData>,
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
        sequencer_id: SequencerId,
        catalog: &dyn Catalog,
        lifecycle_handle: &dyn LifecycleHandle,
        partitioner: &dyn Partitioner,
    ) -> Result<bool> {
        let partition_key = partitioner
            .partition_key(&batch)
            .context(PartitioningSnafu)?;

        let partition_data = match self.partition_data.get_mut(&partition_key) {
            Some(p) => p,
            None => {
                self.insert_partition(&partition_key, sequencer_id, catalog)
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
            sequencer_id,
            sequence_number,
            batch.size(),
        );
        partition_data.buffer_write(sequence_number, batch)?;

        Ok(should_pause)
    }

    async fn buffer_delete(
        &mut self,
        table_name: &str,
        predicate: &DeletePredicate,
        sequencer_id: SequencerId,
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
                sequencer_id,
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
        partition_key: &str,
        sequencer_id: SequencerId,
        catalog: &dyn Catalog,
    ) -> Result<()> {
        let mut repos = catalog.repositories().await;
        let partition = repos
            .partitions()
            .create_or_get(partition_key, sequencer_id, self.table_id)
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
    fn progress(&self) -> SequencerProgress {
        let progress = SequencerProgress::new();
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
        sequencer_id: SequencerId,
        table_id: TableId,
        partition_id: PartitionId,
        table_name: &str,
    ) -> Option<Arc<PersistingBatch>> {
        self.data
            .snapshot_to_persisting(sequencer_id, table_id, partition_id, table_name)
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
        sequencer_number: SequenceNumber,
        mb: MutableBatch,
    ) -> Result<()> {
        match &mut self.data.buffer {
            Some(buf) => {
                buf.max_sequence_number = sequencer_number.max(buf.max_sequence_number);
                buf.data.extend_from(&mb).context(BufferWriteSnafu)?;
            }
            None => {
                self.data.buffer = Some(BufferBatch {
                    min_sequence_number: sequencer_number,
                    max_sequence_number: sequencer_number,
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
        let max_sequencer_number = tombstone.sequence_number;
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

        let (min_sequencer_number, _) = query_batch.min_max_sequence_numbers();
        assert!(min_sequencer_number <= max_sequencer_number);

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
                min_sequencer_number,
                max_sequencer_number,
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
    fn progress(&self) -> SequencerProgress {
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
    //    their persisted info must be added into the Catalog in thier data
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
                min_sequencer_number: buf.min_sequence_number,
                max_sequencer_number: buf.max_sequence_number,
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
        sequencer_id: SequencerId,
        table_id: TableId,
        partition_id: PartitionId,
        table_name: &str,
    ) -> Option<Arc<PersistingBatch>> {
        if self.persisting.is_some() {
            panic!("Unable to snapshot while persisting. This is an unexpected state.")
        }

        if let Some(queryable_batch) = self.snapshot_to_queryable_batch(table_name, None) {
            let persisting_batch = Arc::new(PersistingBatch {
                sequencer_id,
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
    fn progress(&self) -> SequencerProgress {
        let progress = SequencerProgress::new();

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

/// BufferBatch is a MutableBatch with its ingesting order, sequencer_number, that helps the
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
    fn progress(&self) -> SequencerProgress {
        SequencerProgress::new()
            .with_buffered(self.min_sequence_number)
            .with_buffered(self.max_sequence_number)
    }
}

/// SnapshotBatch contains data of many contiguous BufferBatches
#[derive(Debug, PartialEq)]
pub struct SnapshotBatch {
    /// Min sequencer number of its combined BufferBatches
    pub(crate) min_sequencer_number: SequenceNumber,
    /// Max sequencer number of its combined BufferBatches
    pub(crate) max_sequencer_number: SequenceNumber,
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
    fn progress(&self) -> SequencerProgress {
        SequencerProgress::new()
            .with_buffered(self.min_sequencer_number)
            .with_buffered(self.max_sequencer_number)
    }
}

/// PersistingBatch contains all needed info and data for creating
/// a parquet file for given set of SnapshotBatches
#[derive(Debug, PartialEq, Clone)]
pub struct PersistingBatch {
    /// Sequencer id of the data
    pub(crate) sequencer_id: SequencerId,

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
/// sequencer)!
#[derive(Debug, Clone)]
#[allow(missing_copy_implementations)]
pub struct PartitionStatus {
    /// Max sequence number persisted
    pub parquet_max_sequence_number: Option<SequenceNumber>,

    /// Max sequence number for a tombstone
    pub tombstone_max_sequence_number: Option<SequenceNumber>,
}

/// Response sending to the query service per its request defined in IngesterQueryRequest
pub struct IngesterQueryResponse {
    /// Stream of RecordBatch results that match the requested query
    pub data: SendableRecordBatchStream,

    /// The schema of the record batches
    pub schema: Arc<Schema>,

    /// Contains status for every partition that has unpersisted data.
    ///
    /// If a partition does NOT appear within this map, then either all data was persisted or the
    /// ingester has never seen data for this partition. In either case the querier may just read
    /// all parquet files for the missing partition.
    pub unpersisted_partitions: BTreeMap<PartitionId, PartitionStatus>,

    /// Map each record batch to a partition ID.
    pub batch_partition_ids: Vec<PartitionId>,
}

impl std::fmt::Debug for IngesterQueryResponse {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("IngesterQueryResponse")
            .field("data", &"<RECORDBATCH STREAM>")
            .field("schema", &self.schema)
            .field("unpersisted_partitions", &self.unpersisted_partitions)
            .field("batch_partition_ids", &self.batch_partition_ids)
            .finish()
    }
}

impl IngesterQueryResponse {
    /// Make a response
    pub fn new(
        data: SendableRecordBatchStream,
        schema: Arc<Schema>,
        unpersisted_partitions: BTreeMap<PartitionId, PartitionStatus>,
        batch_partition_ids: Vec<PartitionId>,
    ) -> Self {
        Self {
            data,
            schema,
            unpersisted_partitions,
            batch_partition_ids,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        lifecycle::{LifecycleConfig, LifecycleManager},
        partioning::DefaultPartitioner,
        test_util::create_tombstone,
    };
    use arrow_util::assert_batches_sorted_eq;
    use assert_matches::assert_matches;
    use data_types::{
        NamespaceSchema, NonEmptyString, ParquetFileParams, Sequence, TimestampRange,
    };
    use dml::{DmlDelete, DmlMeta, DmlWrite};
    use futures::TryStreamExt;
    use iox_catalog::{
        interface::INITIAL_COMPACTION_LEVEL, mem::MemCatalog, validate_or_insert_schema,
    };
    use iox_time::Time;
    use metric::{MetricObserver, Observation};
    use mutable_batch_lp::{lines_to_batches, test_helpers::lp_to_mutable_batch};
    use object_store::memory::InMemory;
    use std::{ops::DerefMut, time::Duration};

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
        assert_eq!(snapshot.min_sequencer_number, seq_num1);
        assert_eq!(snapshot.max_sequencer_number, seq_num1);
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
        assert_eq!(snapshot.min_sequencer_number, seq_num1);
        assert_eq!(snapshot.max_sequencer_number, seq_num2);

        mutable_batch1.extend_from(&mutable_batch2).unwrap();
        let combined_record_batch = mutable_batch1.to_arrow(Selection::All).unwrap();
        assert_eq!(&*snapshot.data, &combined_record_batch);
    }

    #[tokio::test]
    async fn buffer_write_updates_lifecycle_manager_indicates_pause() {
        let metrics = Arc::new(metric::Registry::new());
        let catalog: Arc<dyn Catalog> = Arc::new(MemCatalog::new(Arc::clone(&metrics)));
        let mut repos = catalog.repositories().await;
        let kafka_topic = repos.kafka_topics().create_or_get("whatevs").await.unwrap();
        let query_pool = repos.query_pools().create_or_get("whatevs").await.unwrap();
        let kafka_partition = KafkaPartition::new(0);
        let namespace = repos
            .namespaces()
            .create("foo", "inf", kafka_topic.id, query_pool.id)
            .await
            .unwrap();
        let sequencer1 = repos
            .sequencers()
            .create_or_get(&kafka_topic, kafka_partition)
            .await
            .unwrap();

        let mut sequencers = BTreeMap::new();
        let kafka_partition = KafkaPartition::new(0);
        sequencers.insert(
            sequencer1.id,
            SequencerData::new(kafka_partition, Arc::clone(&metrics)),
        );

        let object_store: Arc<DynObjectStore> = Arc::new(InMemory::new());

        let data = Arc::new(IngesterData::new(
            Arc::clone(&object_store),
            Arc::clone(&catalog),
            sequencers,
            Arc::new(DefaultPartitioner::default()),
            Arc::new(Executor::new(1)),
            BackoffConfig::default(),
        ));

        let schema = NamespaceSchema::new(namespace.id, kafka_topic.id, query_pool.id);

        let ignored_ts = Time::from_timestamp_millis(42);

        let w1 = DmlWrite::new(
            "foo",
            lines_to_batches("mem foo=1 10", 0).unwrap(),
            DmlMeta::sequenced(Sequence::new(1, 1), ignored_ts, None, 50),
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
            ),
            metrics,
            Arc::new(SystemProvider::new()),
        );
        let should_pause = data
            .buffer_operation(
                sequencer1.id,
                DmlOperation::Write(w1.clone()),
                &manager.handle(),
            )
            .await
            .unwrap();
        assert!(!should_pause);
        let should_pause = data
            .buffer_operation(sequencer1.id, DmlOperation::Write(w1), &manager.handle())
            .await
            .unwrap();
        assert!(should_pause);
    }

    #[tokio::test]
    async fn persist() {
        let metrics = Arc::new(metric::Registry::new());
        let catalog: Arc<dyn Catalog> = Arc::new(MemCatalog::new(Arc::clone(&metrics)));
        let mut repos = catalog.repositories().await;
        let kafka_topic = repos.kafka_topics().create_or_get("whatevs").await.unwrap();
        let query_pool = repos.query_pools().create_or_get("whatevs").await.unwrap();
        let kafka_partition = KafkaPartition::new(0);
        let namespace = repos
            .namespaces()
            .create("foo", "inf", kafka_topic.id, query_pool.id)
            .await
            .unwrap();
        let sequencer1 = repos
            .sequencers()
            .create_or_get(&kafka_topic, kafka_partition)
            .await
            .unwrap();
        let sequencer2 = repos
            .sequencers()
            .create_or_get(&kafka_topic, kafka_partition)
            .await
            .unwrap();
        let mut sequencers = BTreeMap::new();
        sequencers.insert(
            sequencer1.id,
            SequencerData::new(sequencer1.kafka_partition, Arc::clone(&metrics)),
        );
        sequencers.insert(
            sequencer2.id,
            SequencerData::new(sequencer2.kafka_partition, Arc::clone(&metrics)),
        );

        let object_store: Arc<DynObjectStore> = Arc::new(InMemory::new());

        let data = Arc::new(IngesterData::new(
            Arc::clone(&object_store),
            Arc::clone(&catalog),
            sequencers,
            Arc::new(DefaultPartitioner::default()),
            Arc::new(Executor::new(1)),
            BackoffConfig::default(),
        ));

        let schema = NamespaceSchema::new(namespace.id, kafka_topic.id, query_pool.id);

        let ignored_ts = Time::from_timestamp_millis(42);

        let w1 = DmlWrite::new(
            "foo",
            lines_to_batches("mem foo=1 10", 0).unwrap(),
            DmlMeta::sequenced(Sequence::new(1, 1), ignored_ts, None, 50),
        );
        let schema = validate_or_insert_schema(w1.tables(), &schema, repos.deref_mut())
            .await
            .unwrap()
            .unwrap();

        let w2 = DmlWrite::new(
            "foo",
            lines_to_batches("cpu foo=1 10", 1).unwrap(),
            DmlMeta::sequenced(Sequence::new(2, 1), ignored_ts, None, 50),
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
            DmlMeta::sequenced(Sequence::new(1, 2), ignored_ts, None, 50),
        );

        let manager = LifecycleManager::new(
            LifecycleConfig::new(1, 0, 0, Duration::from_secs(1), Duration::from_secs(1)),
            metrics,
            Arc::new(SystemProvider::new()),
        );

        data.buffer_operation(sequencer1.id, DmlOperation::Write(w1), &manager.handle())
            .await
            .unwrap();
        data.buffer_operation(sequencer2.id, DmlOperation::Write(w2), &manager.handle())
            .await
            .unwrap();
        data.buffer_operation(sequencer1.id, DmlOperation::Write(w3), &manager.handle())
            .await
            .unwrap();

        let expected_progress = SequencerProgress::new()
            .with_buffered(SequenceNumber::new(1))
            .with_buffered(SequenceNumber::new(2));
        assert_progress(&data, kafka_partition, expected_progress).await;

        let sd = data.sequencers.get(&sequencer1.id).unwrap();
        let n = sd.namespace("foo").unwrap();
        let partition_id;
        let table_id;
        {
            let mem_table = n.table_data("mem").unwrap();
            assert!(n.table_data("cpu").is_some());
            let mem_table = mem_table.write().await;
            let p = mem_table.partition_data.get("1970-01-01").unwrap();

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
            assert!(partition_info.partition.sort_key.is_none());
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
            .list_by_sequencer_greater_than(sequencer1.id, SequenceNumber::new(0))
            .await
            .unwrap();
        assert_eq!(parquet_files.len(), 1);
        let pf = parquet_files.first().unwrap();
        assert_eq!(pf.partition_id, partition_id);
        assert_eq!(pf.table_id, table_id);
        assert_eq!(pf.min_time, Timestamp::new(10));
        assert_eq!(pf.max_time, Timestamp::new(30));
        assert_eq!(pf.min_sequence_number, SequenceNumber::new(1));
        assert_eq!(pf.max_sequence_number, SequenceNumber::new(2));
        assert_eq!(pf.sequencer_id, sequencer1.id);
        assert!(pf.to_delete.is_none());

        // verify it set a sort key on the partition in the catalog
        let partition_info = repos
            .partitions()
            .partition_info_by_id(partition_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(partition_info.partition.sort_key.unwrap(), "time");

        let mem_table = n.table_data("mem").unwrap();
        let mem_table = mem_table.read().await;

        // verify that the parquet_max_sequence_number got updated
        assert_eq!(
            mem_table.parquet_max_sequence_number(),
            Some(SequenceNumber::new(2))
        );

        // check progresses after persist
        let expected_progress = SequencerProgress::new()
            .with_buffered(SequenceNumber::new(1))
            .with_persisted(SequenceNumber::new(2));
        assert_progress(&data, kafka_partition, expected_progress).await;
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
            s_id,      // sequencer id
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
        assert_eq!(p.data.snapshots[0].min_sequencer_number.get(), 1);
        assert_eq!(p.data.snapshots[0].max_sequencer_number.get(), 3);

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
            s_id,          // sequencer id
            6,             // delete's seq_number
            10,            // min time of data to get deleted
            50,            // max time of data to get deleted
            "city=Boston", // delete predicate
        );
        // two rows will get deleted, one from existing snapshot, one from the buffer being moved to snpashot
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
        assert_eq!(p.data.snapshots[0].min_sequencer_number.get(), 1);
        assert_eq!(p.data.snapshots[0].max_sequencer_number.get(), 6);

        // ------------------------------------------
        // Persisting
        let p_batch = p
            .snapshot_to_persisting_batch(
                SequencerId::new(s_id),
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
            s_id,      // sequencer id
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
                                          // no snpashots becasue buffer has not data yet and the sanpshot was empty too
        assert_eq!(p.data.snapshots.len(), 0);
        assert_eq!(p.data.deletes_during_persisting.len(), 1); // tombstone added since data is persisting
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
        ); // 1 newlly added mutable batch of 3 rows of data
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
        assert_eq!(p.data.snapshots[0].min_sequencer_number.get(), 8);
        assert_eq!(p.data.snapshots[0].max_sequencer_number.get(), 8);

        // ------------------------------------------
        // Delete
        // --- seq_num: 9
        let ts = create_tombstone(
            4,         // tombstone id
            t_id,      // table id
            s_id,      // sequencer id
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
        assert_eq!(p.data.snapshots[0].min_sequencer_number.get(), 8);
        assert_eq!(p.data.snapshots[0].max_sequencer_number.get(), 9);

        exec.join().await;
    }

    #[tokio::test]
    async fn buffer_operation_ignores_already_persisted_data() {
        let metrics = Arc::new(metric::Registry::new());
        let catalog: Arc<dyn Catalog> = Arc::new(MemCatalog::new(Arc::clone(&metrics)));
        let mut repos = catalog.repositories().await;
        let kafka_topic = repos.kafka_topics().create_or_get("whatevs").await.unwrap();
        let query_pool = repos.query_pools().create_or_get("whatevs").await.unwrap();
        let kafka_partition = KafkaPartition::new(0);
        let namespace = repos
            .namespaces()
            .create("foo", "inf", kafka_topic.id, query_pool.id)
            .await
            .unwrap();
        let sequencer = repos
            .sequencers()
            .create_or_get(&kafka_topic, kafka_partition)
            .await
            .unwrap();

        let schema = NamespaceSchema::new(namespace.id, kafka_topic.id, query_pool.id);

        let ignored_ts = Time::from_timestamp_millis(42);

        let w1 = DmlWrite::new(
            "foo",
            lines_to_batches("mem foo=1 10", 0).unwrap(),
            DmlMeta::sequenced(Sequence::new(1, 1), ignored_ts, None, 50),
        );
        let w2 = DmlWrite::new(
            "foo",
            lines_to_batches("mem foo=1 10", 0).unwrap(),
            DmlMeta::sequenced(Sequence::new(1, 2), ignored_ts, None, 50),
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
            .create_or_get("1970-01-01", sequencer.id, table.id)
            .await
            .unwrap();
        let partition2 = repos
            .partitions()
            .create_or_get("1970-01-02", sequencer.id, table.id)
            .await
            .unwrap();

        let parquet_file_params = ParquetFileParams {
            sequencer_id: sequencer.id,
            namespace_id: namespace.id,
            table_id: table.id,
            partition_id: partition.id,
            object_store_id: Uuid::new_v4(),
            min_sequence_number: SequenceNumber::new(0),
            max_sequence_number: SequenceNumber::new(1),
            min_time: Timestamp::new(1),
            max_time: Timestamp::new(1),
            file_size_bytes: 0,
            parquet_metadata: vec![],
            row_count: 0,
            compaction_level: INITIAL_COMPACTION_LEVEL,
            created_at: Timestamp::new(1),
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
            min_sequence_number: SequenceNumber::new(12),
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
            LifecycleConfig::new(1, 0, 0, Duration::from_secs(1), Duration::from_secs(1)),
            Arc::clone(&metrics),
            Arc::new(SystemProvider::new()),
        );
        let partitioner = DefaultPartitioner::default();
        let exec = Executor::new(1);

        let data = NamespaceData::new(namespace.id, &*metrics);

        // w1 should be ignored so it shouldn't be present in the buffer
        let should_pause = data
            .buffer_operation(
                DmlOperation::Write(w1),
                sequencer.id,
                catalog.as_ref(),
                &manager.handle(),
                &partitioner,
                &exec,
            )
            .await
            .unwrap();
        {
            let table_data = data.table_data("mem").unwrap();
            let table = table_data.read().await;
            let p = table.partition_data.get("1970-01-01").unwrap();
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
            sequencer.id,
            catalog.as_ref(),
            &manager.handle(),
            &partitioner,
            &exec,
        )
        .await
        .unwrap();

        let table_data = data.table_data("mem").unwrap();
        let table = table_data.read().await;
        let partition = table.partition_data.get("1970-01-01").unwrap();
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
        let kafka_topic = repos.kafka_topics().create_or_get("whatevs").await.unwrap();
        let query_pool = repos.query_pools().create_or_get("whatevs").await.unwrap();
        let kafka_partition = KafkaPartition::new(0);
        let namespace = repos
            .namespaces()
            .create("foo", "inf", kafka_topic.id, query_pool.id)
            .await
            .unwrap();
        let sequencer1 = repos
            .sequencers()
            .create_or_get(&kafka_topic, kafka_partition)
            .await
            .unwrap();

        let mut sequencers = BTreeMap::new();
        let kafka_partition = KafkaPartition::new(0);
        sequencers.insert(
            sequencer1.id,
            SequencerData::new(kafka_partition, Arc::clone(&metrics)),
        );

        let object_store: Arc<DynObjectStore> = Arc::new(InMemory::new());

        let data = Arc::new(IngesterData::new(
            Arc::clone(&object_store),
            Arc::clone(&catalog),
            sequencers,
            Arc::new(DefaultPartitioner::default()),
            Arc::new(Executor::new(1)),
            BackoffConfig::default(),
        ));

        let schema = NamespaceSchema::new(namespace.id, kafka_topic.id, query_pool.id);

        let ignored_ts = Time::from_timestamp_millis(42);

        let w1 = DmlWrite::new(
            "foo",
            lines_to_batches("mem foo=1 10", 0).unwrap(),
            DmlMeta::sequenced(Sequence::new(1, 1), ignored_ts, None, 50),
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
            ),
            metrics,
            Arc::new(SystemProvider::new()),
        );
        data.buffer_operation(
            sequencer1.id,
            DmlOperation::Write(w1.clone()),
            &manager.handle(),
        )
        .await
        .unwrap();

        assert_eq!(
            data.sequencer(sequencer1.id)
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
            DmlMeta::sequenced(Sequence::new(1, 2), ignored_ts, None, 1337),
        );
        data.buffer_operation(sequencer1.id, DmlOperation::Delete(d1), &manager.handle())
            .await
            .unwrap();

        assert_eq!(
            data.sequencer(sequencer1.id)
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
        kafka_partition: KafkaPartition,
        expected_progress: SequencerProgress,
    ) {
        let progresses = data.progresses(vec![kafka_partition]).await;
        let expected_progresses = [(kafka_partition, expected_progress)]
            .into_iter()
            .collect::<BTreeMap<_, _>>();

        assert_eq!(progresses, expected_progresses);
    }
}
