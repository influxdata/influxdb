//! Data Points for the lifecycle of the Compactor

use crate::{
    handler::CompactorConfig,
    query::QueryableParquetChunk,
    utils::{CatalogUpdate, CompactedData, GroupWithTombstones, ParquetFileWithTombstone},
};
use backoff::BackoffConfig;
use data_types::{
    CompactionLevel, Namespace, NamespaceId, ParquetFile, ParquetFileId, Partition, PartitionId,
    PartitionParam, SequencerId, Table, TableId, TableSchema, Tombstone, TombstoneId,
};
use datafusion::error::DataFusionError;
use futures::stream::{FuturesUnordered, TryStreamExt};
use iox_catalog::interface::{get_schema_by_id, Catalog, Transaction};
use iox_query::{
    exec::{Executor, ExecutorType},
    frontend::reorg::ReorgPlanner,
    QueryChunk,
};
use iox_time::TimeProvider;
use metric::{Attributes, DurationHistogram, Metric, U64Counter, U64Gauge};
use observability_deps::tracing::{debug, info, trace, warn};
use parquet_file::{metadata::IoxMetadata, storage::ParquetStorage};
use snafu::{ensure, OptionExt, ResultExt, Snafu};
use std::{
    cmp::{max, min},
    collections::{BTreeMap, HashMap, HashSet},
    ops::DerefMut,
    sync::Arc,
};
use uuid::Uuid;

#[derive(Debug, Snafu)]
#[allow(missing_copy_implementations, missing_docs)]
pub enum Error {
    #[snafu(display(
        "The given parquet files are not in the same partition ({}, {}, {}), ({}, {}, {})",
        sequencer_id_1,
        table_id_1,
        partition_id_1,
        sequencer_id_2,
        table_id_2,
        partition_id_2
    ))]
    NotSamePartition {
        sequencer_id_1: SequencerId,
        table_id_1: TableId,
        partition_id_1: PartitionId,
        sequencer_id_2: SequencerId,
        table_id_2: TableId,
        partition_id_2: PartitionId,
    },
    #[snafu(display("Error building compact logical plan  {}", source))]
    CompactLogicalPlan {
        source: iox_query::frontend::reorg::Error,
    },

    #[snafu(display("Error building compact physical plan  {}", source))]
    CompactPhysicalPlan { source: DataFusionError },

    #[snafu(display("Error executing compact plan  {}", source))]
    ExecuteCompactPlan { source: DataFusionError },

    #[snafu(display("Error while starting catalog transaction {}", source))]
    Transaction {
        source: iox_catalog::interface::Error,
    },

    #[snafu(display("Error while committing catalog transaction {}", source))]
    TransactionCommit {
        source: iox_catalog::interface::Error,
    },

    #[snafu(display("Error while flagging a parquet file for deletion {}", source))]
    FlagForDelete {
        source: iox_catalog::interface::Error,
    },

    #[snafu(display("Error while requesting level 0 parquet files {}", source))]
    Level0 {
        source: iox_catalog::interface::Error,
    },

    #[snafu(display("Error updating catalog {}", source))]
    Update {
        source: iox_catalog::interface::Error,
    },

    #[snafu(display("Error removing tombstones {}", source))]
    RemoveTombstones {
        source: iox_catalog::interface::Error,
    },

    #[snafu(display("Error querying for tombstones for a parquet file {}", source))]
    QueryingTombstones {
        source: iox_catalog::interface::Error,
    },

    #[snafu(display("Error getting parquet files for partition: {}", source))]
    ListParquetFiles {
        source: iox_catalog::interface::Error,
    },

    #[snafu(display("Error querying partition {}", source))]
    QueryingPartition {
        source: iox_catalog::interface::Error,
    },

    #[snafu(display("Error querying table {}", source))]
    QueryingTable {
        source: iox_catalog::interface::Error,
    },

    #[snafu(display("Error querying namespace {}", source))]
    QueryingNamespace {
        source: iox_catalog::interface::Error,
    },

    #[snafu(display("Could not find partition {:?}", partition_id))]
    PartitionNotFound { partition_id: PartitionId },

    #[snafu(display("Could not find table {:?}", table_id))]
    TableNotFound { table_id: TableId },

    #[snafu(display("Could not find namespace {:?}", namespace_id))]
    NamespaceNotFound { namespace_id: NamespaceId },

    #[snafu(display("Could not serialize and persist record batches {}", source))]
    Persist {
        source: parquet_file::storage::UploadError,
    },

    #[snafu(display(
        "Two time-overlapped files with overlapped range sequence numbers. \
        File id 1: {}, file id 2: {}, sequence number range 1: [{}, {}], \
        sequence number range 2: [{}, {}], partition id: {}",
        file_id_1,
        file_id_2,
        min_seq_1,
        max_seq_1,
        min_seq_2,
        max_seq_2,
        partition_id,
    ))]
    OverlapTimeAndSequenceNumber {
        file_id_1: ParquetFileId,
        file_id_2: ParquetFileId,
        min_seq_1: i64,
        max_seq_1: i64,
        min_seq_2: i64,
        max_seq_2: i64,
        partition_id: PartitionId,
    },

    #[snafu(display(
        "Error getting the most recent highest ingested throughput partitions for sequencer {}. {}",
        sequencer_id,
        source
    ))]
    HighestThroughputPartitions {
        source: iox_catalog::interface::Error,
        sequencer_id: SequencerId,
    },

    #[snafu(display(
        "Error getting the most level 0 file partitions for sequencer {}. {}",
        sequencer_id,
        source
    ))]
    MostL0Partitions {
        source: iox_catalog::interface::Error,
        sequencer_id: SequencerId,
    },
}

/// A specialized `Error` for Compactor Data errors
pub type Result<T, E = Error> = std::result::Result<T, E>;

/// Data of parquet files to compact and upgrade
#[derive(Debug)]
pub struct CompactAndUpgrade {
    // sequencer ID of all files in this struct
    sequencer_id: Option<SequencerId>,
    // Each group will be compacted into one file
    groups_to_compact: Vec<GroupWithTombstones>,
    // level-0 files to be upgraded to level 1
    files_to_upgrade: Vec<ParquetFileId>,
}

impl CompactAndUpgrade {
    fn new(sequencer_id: Option<SequencerId>) -> Self {
        Self {
            sequencer_id,
            groups_to_compact: vec![],
            files_to_upgrade: vec![],
        }
    }

    /// Return true if there are files to compact and/or upgrade
    pub fn compactable(&self) -> bool {
        !self.groups_to_compact.is_empty() || !self.files_to_upgrade.is_empty()
    }

    /// Return sequncer ID
    pub fn sequencer_id(&self) -> Option<SequencerId> {
        self.sequencer_id
    }
}

/// Data points needed to run a compactor
#[derive(Debug)]
pub struct Compactor {
    /// Sequencers assigned to this compactor
    sequencers: Vec<SequencerId>,

    /// Object store for reading and persistence of parquet files
    store: ParquetStorage,

    /// The global catalog for schema, parquet files and tombstones
    pub(crate) catalog: Arc<dyn Catalog>,

    /// Executor for running queries, compacting, and persisting
    exec: Arc<Executor>,

    /// Time provider for all activities in this compactor
    pub time_provider: Arc<dyn TimeProvider>,

    /// Backoff config
    pub(crate) backoff_config: BackoffConfig,

    /// Configuration options for the compactor
    pub(crate) config: CompactorConfig,

    /// Counter for the number of files compacted
    compaction_counter: Metric<U64Counter>,

    /// Counter for level promotion from level 0 to 1
    /// These are large enough and non-overlapped file
    level_promotion_counter: Metric<U64Counter>,

    /// Counter for the number of output compacted files
    compaction_output_counter: Metric<U64Counter>,

    /// Gauge for the number of compaction partition candidates
    compaction_candidate_gauge: Metric<U64Gauge>,

    /// Gauge for the number of Parquet file candidates. The recorded values have attributes for
    /// the compaction level of the file and whether the file was selected for compaction or not.
    pub(crate) parquet_file_candidate_gauge: Metric<U64Gauge>,

    /// Gauge for the number of bytes of Parquet file candidates. The recorded values have
    /// attributes for  the compaction level of the file and whether the file was selected for
    /// compaction or not.
    pub(crate) parquet_file_candidate_bytes_gauge: Metric<U64Gauge>,

    /// Histogram for tracking the time to compact a partition
    compaction_duration: Metric<DurationHistogram>,
}

impl Compactor {
    /// Initialize the Compactor Data
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        sequencers: Vec<SequencerId>,
        catalog: Arc<dyn Catalog>,
        store: ParquetStorage,
        exec: Arc<Executor>,
        time_provider: Arc<dyn TimeProvider>,
        backoff_config: BackoffConfig,
        config: CompactorConfig,
        registry: Arc<metric::Registry>,
    ) -> Self {
        let compaction_counter = registry.register_metric(
            "compactor_compacted_files_total",
            "counter for the number of files compacted",
        );

        let compaction_output_counter = registry.register_metric(
            "compactor_output_compacted_files_total",
            "counter for the number of output compacted files",
        );

        let compaction_candidate_gauge = registry.register_metric(
            "compactor_candidates",
            "gauge for the number of compaction candidates that are found when checked",
        );

        let parquet_file_candidate_gauge = registry.register_metric(
            "parquet_file_candidates",
            "Number of Parquet file candidates",
        );

        let parquet_file_candidate_bytes_gauge = registry.register_metric(
            "parquet_file_candidate_bytes",
            "Number of bytes of Parquet file candidates",
        );

        let level_promotion_counter = registry.register_metric(
            "compactor_level_promotions_total",
            "Counter for level promotion from 0 to 1",
        );

        let compaction_duration: Metric<DurationHistogram> = registry.register_metric(
            "compactor_compact_partition_duration",
            "Compact partition duration",
        );

        Self {
            sequencers,
            catalog,
            store,
            exec,
            time_provider,
            backoff_config,
            config,
            compaction_counter,
            compaction_output_counter,
            level_promotion_counter,
            compaction_candidate_gauge,
            parquet_file_candidate_gauge,
            parquet_file_candidate_bytes_gauge,
            compaction_duration,
        }
    }

    async fn update_to_level_1(&self, parquet_file_ids: &[ParquetFileId]) -> Result<()> {
        let mut repos = self.catalog.repositories().await;

        let updated = repos
            .parquet_files()
            .update_to_level_1(parquet_file_ids)
            .await
            .context(UpdateSnafu)?;

        if updated.len() < parquet_file_ids.len() {
            let parquet_file_ids: HashSet<_> = parquet_file_ids.iter().collect();
            let updated: HashSet<_> = updated.iter().collect();
            let not_updated = parquet_file_ids.difference(&updated);

            warn!(
                "Unable to update to level 1 parquet files with IDs: {:?}",
                not_updated
            );
        }

        Ok(())
    }

    /// Return a list of the most recent highest ingested throughput partitions.
    /// The highest throughput partitions are prioritized as follows:
    ///  1. If there are partitions with new ingested files within the last 4 hours, pick them.
    ///  2. If no new ingested files in the last 4 hours, will look for partitions with new writes
    ///     within the last 24 hours.
    ///  3. If there are no ingested files within the last 24 hours, will look for partitions
    ///     with any new ingested files in the past.
    ///
    /// . New ingested files means non-deleted L0 files
    /// . In all cases above, for each sequencer, N partitions with the most new ingested files will be selected
    ///     and the return list will include at most, P = N * S, partitions where S is the number of
    ///     sequencers this compactor handles.
    pub async fn partitions_to_compact(
        &self,
        //  Max number of the most recent highest ingested throughput partitions
        // per sequencer we want to read
        max_num_partitions_per_sequencer: usize,
        // Minimum number of the most recent writes per partition we want to count
        // to prioritize partitions
        min_recent_ingested_files: usize,
    ) -> Result<Vec<PartitionParam>> {
        let mut candidates =
            Vec::with_capacity(self.sequencers.len() * max_num_partitions_per_sequencer);
        let mut repos = self.catalog.repositories().await;

        for sequencer_id in &self.sequencers {
            let attributes =
                Attributes::from([("sequencer_id", format!("{}", *sequencer_id).into())]);

            // Get the most recent highest ingested throughput partitions within
            // the last 4 hours. If nothing, increase to 24 hours
            let mut num_partitions = 0;
            for num_hours in [4, 24] {
                let mut partitions = repos
                    .parquet_files()
                    .recent_highest_throughput_partitions(
                        *sequencer_id,
                        num_hours,
                        min_recent_ingested_files,
                        max_num_partitions_per_sequencer,
                    )
                    .await
                    .context(HighestThroughputPartitionsSnafu {
                        sequencer_id: *sequencer_id,
                    })?;

                if !partitions.is_empty() {
                    num_partitions = partitions.len();
                    candidates.append(&mut partitions);
                    break;
                }
            }

            // No active ingesting partitions the last 24 hours,
            // get partition with the most level-0 files
            if num_partitions == 0 {
                let mut partitions = repos
                    .parquet_files()
                    .most_level_0_files_partitions(*sequencer_id, max_num_partitions_per_sequencer)
                    .await
                    .context(MostL0PartitionsSnafu {
                        sequencer_id: *sequencer_id,
                    })?;

                if !partitions.is_empty() {
                    num_partitions = partitions.len();
                    candidates.append(&mut partitions);
                }
            }

            // Record metric for candidates per sequencer
            let number_gauge = self.compaction_candidate_gauge.recorder(attributes.clone());
            number_gauge.set(num_partitions as u64);
        }

        Ok(candidates)
    }

    /// Add namespace and table information to partition candidates.
    pub async fn add_info_to_partitions(
        &self,
        partitions: &[PartitionParam],
    ) -> Result<Vec<PartitionCompactionCandidateWithInfo>> {
        let mut repos = self.catalog.repositories().await;

        let table_ids: HashSet<_> = partitions.iter().map(|p| p.table_id).collect();
        let namespace_ids: HashSet<_> = partitions.iter().map(|p| p.namespace_id).collect();

        let mut namespaces = HashMap::with_capacity(namespace_ids.len());
        for id in namespace_ids {
            let namespace = repos
                .namespaces()
                .get_by_id(id)
                .await
                .context(QueryingNamespaceSnafu)?
                .context(NamespaceNotFoundSnafu { namespace_id: id })?;
            let schema = get_schema_by_id(namespace.id, repos.as_mut())
                .await
                .context(QueryingNamespaceSnafu)?;
            namespaces.insert(id, (Arc::new(namespace), schema));
        }

        let mut tables = HashMap::with_capacity(table_ids.len());
        for id in table_ids {
            let table = repos
                .tables()
                .get_by_id(id)
                .await
                .context(QueryingTableSnafu)?
                .context(TableNotFoundSnafu { table_id: id })?;
            let schema = namespaces
                .get(&table.namespace_id)
                .expect("just queried")
                .1
                .tables
                .get(&table.name)
                .context(TableNotFoundSnafu { table_id: id })?
                .clone();
            tables.insert(id, (Arc::new(table), Arc::new(schema)));
        }

        Ok(partitions
            .iter()
            .map(|p| {
                let (table, table_schema) = tables.get(&p.table_id).expect("just queried");

                PartitionCompactionCandidateWithInfo {
                    table: Arc::clone(table),
                    table_schema: Arc::clone(table_schema),
                    namespace: Arc::clone(
                        &namespaces.get(&p.namespace_id).expect("just queried").0,
                    ),
                    candidate: *p,
                }
            })
            .collect())
    }

    /// Fetch the partition information stored in the catalog.
    async fn get_partition_from_catalog(&self, partition_id: PartitionId) -> Result<Partition> {
        let mut repos = self.catalog.repositories().await;

        let partition = repos
            .partitions()
            .get_by_id(partition_id)
            .await
            .context(QueryingPartitionSnafu)?
            .context(PartitionNotFoundSnafu { partition_id })?;

        Ok(partition)
    }

    /// Find the level-0 files to be compacted together and upgraded for a given partition.
    pub async fn groups_to_compact_and_files_to_upgrade(
        &self,
        partition_id: PartitionId,
        namespace_name: &str,
        table_name: &str,
    ) -> Result<CompactAndUpgrade> {
        let mut compact_and_upgrade = CompactAndUpgrade::new(None);

        // List all valid (not soft deleted) files of the partition
        let parquet_files = self
            .catalog
            .repositories()
            .await
            .parquet_files()
            .list_by_partition_not_to_delete(partition_id)
            .await
            .context(ListParquetFilesSnafu)?;
        if parquet_files.is_empty() {
            return Ok(compact_and_upgrade);
        }

        compact_and_upgrade.sequencer_id = Some(parquet_files[0].sequencer_id);

        // Attach appropriate tombstones to each file
        let files_with_tombstones = self.add_tombstones_to_files(parquet_files).await?;
        if let Some(files_with_tombstones) = files_with_tombstones {
            info!(
                partition_id = partition_id.get(),
                num_files = files_with_tombstones.parquet_files.len(),
                namespace = namespace_name,
                table = table_name,
                "compacting files",
            );

            // Only one file without tombstones, no need to compact.
            if files_with_tombstones.parquet_files.len() == 1
                && files_with_tombstones.tombstones.is_empty()
            {
                info!(
                    sequencer_id = files_with_tombstones.parquet_files[0].sequencer_id.get(),
                    partition_id = partition_id.get(),
                    parquet_file_id = files_with_tombstones.parquet_files[0]
                        .parquet_file_id()
                        .get(),
                    "no need to compact one file without tombstones"
                );
                // If it is level 0, upgrade it
                if files_with_tombstones.parquet_files[0].compaction_level
                    == CompactionLevel::Initial
                {
                    compact_and_upgrade
                        .files_to_upgrade
                        .push(files_with_tombstones.parquet_files[0].parquet_file_id())
                }
            } else {
                compact_and_upgrade
                    .groups_to_compact
                    .push(files_with_tombstones);
            }
        }

        Ok(compact_and_upgrade)
    }

    /// Runs compaction in a partition resolving any tombstones and deduplicating data
    ///
    /// Expectation: After a partition of a table has not received any writes for some
    /// amount of time, the compactor will ensure it is stored in object store as N parquet files
    /// which:
    ///   . have non overlapping time ranges
    ///   . each does not exceed a size specified by config param max_desired_file_size_bytes.
    ///
    /// TODO: will need some code changes until we reach the expectation
    pub async fn compact_partition(
        &self,
        namespace: &Namespace,
        table: &Table,
        table_schema: &TableSchema,
        partition_id: PartitionId,
        compact_and_upgrade: CompactAndUpgrade,
    ) -> Result<()> {
        if !compact_and_upgrade.compactable() {
            return Ok(());
        }

        info!("compacting partition {}", partition_id);
        let start_time = self.time_provider.now();

        let partition = self.get_partition_from_catalog(partition_id).await?;

        let mut files_by_level = BTreeMap::new();

        // Compact, persist,and update catalog accordingly for each overlapped file
        let mut tombstones = BTreeMap::new();
        let mut output_file_count = 0;
        let sequencer_id = compact_and_upgrade
            .sequencer_id()
            .expect("Should have sequencer ID");
        for group in compact_and_upgrade.groups_to_compact {
            for compaction_level in group.parquet_files.iter().map(|p| p.compaction_level) {
                *files_by_level.entry(compaction_level).or_default() += 1;
            }

            // keep tombstone ids
            tombstones = Self::union_tombstones(tombstones, &group);

            // Collect all the parquet file IDs, to be able to set their catalog records to be
            // deleted. These should already be unique, no need to dedupe.
            let original_parquet_file_ids: Vec<_> =
                group.parquet_files.iter().map(|f| f.id).collect();
            let size: i64 = group.parquet_files.iter().map(|f| f.file_size_bytes).sum();
            info!(
                partition_id = partition_id.get(),
                namespace = %namespace.name,
                table = %table.name,
                num_files=%group.parquet_files.len(),
                ?size,
                ?original_parquet_file_ids,
                "compacting group of files"
            );

            // Compact the files concurrently.
            //
            // This builds the StreamSplitExec plan & executes both partitions
            // concurrently, streaming the resulting record batches into the
            // Parquet serializer and directly uploads them to object store.
            //
            // If an non-object-store upload error occurs, all
            // executions/uploads are aborted and the error is returned to the
            // caller. If an error occurs during object store upload, it will be
            // retried indefinitely.
            let catalog_update_info = self
                .compact(
                    group.parquet_files,
                    namespace,
                    table,
                    table_schema,
                    &partition,
                )
                .await?
                .into_iter()
                .map(|v| async {
                    let CompactedData {
                        data,
                        meta,
                        tombstones,
                    } = v;
                    debug!(
                        ?partition_id,
                        ?meta,
                        "executing and uploading compaction StreamSplitExec"
                    );

                    let object_store_id = meta.object_store_id;
                    info!(?partition_id, %object_store_id, "streaming exec to object store");

                    // Stream the record batches from the compaction exec, serialize
                    // them, and directly upload the resulting Parquet files to
                    // object storage.
                    let (parquet_meta, file_size) =
                        self.store.upload(data, &meta).await.context(PersistSnafu)?;

                    debug!(?partition_id, %object_store_id, "file uploaded to object store");

                    Ok(CatalogUpdate::new(
                        partition_id,
                        meta,
                        file_size,
                        parquet_meta,
                        tombstones,
                        table_schema,
                    ))
                })
                .collect::<FuturesUnordered<_>>()
                .try_collect::<Vec<_>>()
                .await?;

            output_file_count += catalog_update_info.len();

            let mut txn = self
                .catalog
                .start_transaction()
                .await
                .context(TransactionSnafu)?;

            debug!(
                "updating catalog with {} updates",
                catalog_update_info.len()
            );
            self.update_catalog(
                catalog_update_info,
                original_parquet_file_ids,
                txn.deref_mut(),
            )
            .await?;

            txn.commit().await.context(TransactionCommitSnafu)?;
        }

        // Remove fully processed tombstones
        self.remove_fully_processed_tombstones(tombstones).await?;

        // Upgrade old level-0 to level 1
        self.update_to_level_1(&compact_and_upgrade.files_to_upgrade)
            .await?;

        let attributes = Attributes::from([("sequencer_id", format!("{}", sequencer_id).into())]);
        if !compact_and_upgrade.files_to_upgrade.is_empty() {
            let promotion_counter = self.level_promotion_counter.recorder(attributes.clone());
            promotion_counter.inc(compact_and_upgrade.files_to_upgrade.len() as u64);
        }

        if let Some(delta) = self.time_provider.now().checked_duration_since(start_time) {
            let duration = self.compaction_duration.recorder(attributes.clone());
            duration.record(delta);
        }

        for (compaction_level, file_count) in files_by_level {
            let mut attributes = attributes.clone();
            attributes.insert("compaction_level", format!("{}", compaction_level as i32));
            let compaction_counter = self.compaction_counter.recorder(attributes);
            compaction_counter.inc(file_count);
        }

        let compaction_output_counter = self.compaction_output_counter.recorder(attributes);
        compaction_output_counter.inc(output_file_count as u64);

        Ok(())
    }

    fn union_tombstones(
        mut tombstones: BTreeMap<TombstoneId, Tombstone>,
        group_with_tombstones: &GroupWithTombstones,
    ) -> BTreeMap<TombstoneId, Tombstone> {
        for ts in &group_with_tombstones.tombstones {
            tombstones.insert(ts.id, (*ts).clone());
        }
        tombstones
    }

    // Compact given files. The given files are all for the same partition.
    async fn compact(
        &self,
        overlapped_files: Vec<ParquetFileWithTombstone>,
        namespace: &Namespace,
        table: &Table,
        table_schema: &TableSchema,
        partition: &Partition,
    ) -> Result<Vec<CompactedData>> {
        debug!(num_files = overlapped_files.len(), "compact files");

        // Nothing to compact
        if overlapped_files.is_empty() {
            return Ok(vec![]);
        }

        // One file without tombstone, no need to compact
        if overlapped_files.len() == 1 && overlapped_files[0].tombstones().is_empty() {
            return Ok(vec![]);
        }

        // Hold onto various metadata items needed later.
        let sequencer_id = partition.sequencer_id;
        let namespace_id = table.namespace_id;
        let table_id = table.id;

        //  Collect all unique tombstones
        let mut tombstone_map = overlapped_files[0].tombstone_map();

        // Verify if the given files belong to the same partition and collect their tombstones.
        // One tombstone might be relevant to multiple parquet files in this set, so dedupe here.
        if let Some((head, tail)) = overlapped_files.split_first() {
            for file in tail {
                tombstone_map.append(&mut file.tombstone_map());

                let is_same = file.sequencer_id == head.sequencer_id
                    && file.table_id == head.table_id
                    && file.partition_id == head.partition_id;

                ensure!(
                    is_same,
                    NotSamePartitionSnafu {
                        sequencer_id_1: head.sequencer_id,
                        table_id_1: head.table_id,
                        partition_id_1: head.partition_id,
                        sequencer_id_2: file.sequencer_id,
                        table_id_2: file.table_id,
                        partition_id_2: file.partition_id
                    }
                );

                assert_eq!(file.sequencer_id, sequencer_id);
                assert_eq!(file.namespace_id, namespace_id);
                assert_eq!(file.table_id, table_id);
                assert_eq!(file.partition_id, partition.id);
            }
        }

        // Convert the input files into QueryableParquetChunk for making query plan
        let query_chunks: Vec<_> = overlapped_files
            .iter()
            .map(|f| {
                f.to_queryable_parquet_chunk(
                    self.store.clone(),
                    table.name.clone(),
                    table_schema,
                    partition.sort_key(),
                )
            })
            .collect();

        trace!(
            n_query_chunks = query_chunks.len(),
            "gathered parquet data to compact"
        );

        // Compute min & max sequence numbers and time
        // unwrap here will work becasue the len of the query_chunks already >= 1
        let (head, tail) = query_chunks.split_first().unwrap();
        let mut min_sequence_number = head.min_sequence_number();
        let mut max_sequence_number = head.max_sequence_number();
        let mut min_time = head.min_time();
        let mut max_time = head.max_time();
        for c in tail {
            min_sequence_number = min(min_sequence_number, c.min_sequence_number());
            max_sequence_number = max(max_sequence_number, c.max_sequence_number());
            min_time = min(min_time, c.min_time());
            max_time = max(max_time, c.max_time());
        }

        // Merge schema of the compacting chunks
        let query_chunks: Vec<_> = query_chunks
            .into_iter()
            .map(|c| Arc::new(c) as Arc<dyn QueryChunk>)
            .collect();
        let merged_schema = QueryableParquetChunk::merge_schemas(&query_chunks);
        debug!(
            num_cols = merged_schema.as_arrow().fields().len(),
            "Number of columns in the merged schema to build query plan"
        );

        // All partitions in the catalog MUST contain a sort key.
        let sort_key = partition
            .sort_key()
            .expect("no parittion sort key in catalog")
            .filter_to(&merged_schema.primary_key());

        // Build compact logical plan, compacting everything into one file
        let plan = ReorgPlanner::new()
            .compact_plan(Arc::clone(&merged_schema), query_chunks, sort_key.clone())
            .context(CompactLogicalPlanSnafu)?;

        let ctx = self.exec.new_context(ExecutorType::Reorg);
        let physical_plan = ctx
            .create_physical_plan(&plan)
            .await
            .context(CompactPhysicalPlanSnafu)?;

        // Run to collect each stream of the plan
        let stream_count = physical_plan.output_partitioning().partition_count();

        // Should be compacting to only one file
        assert_eq!(stream_count, 1);

        let mut compacted = Vec::with_capacity(stream_count);
        debug!("running plan with {} streams", stream_count);
        for i in 0..stream_count {
            trace!(partition = i, "executing datafusion partition");

            let stream = ctx
                .execute_stream_partitioned(Arc::clone(&physical_plan), i)
                .await
                .context(ExecuteCompactPlanSnafu)?;

            trace!(partition = i, "built result stream for partition");

            let meta = IoxMetadata {
                object_store_id: Uuid::new_v4(),
                creation_timestamp: self.time_provider.now(),
                sequencer_id: partition.sequencer_id,
                namespace_id: table.namespace_id,
                namespace_name: namespace.name.clone().into(),
                table_id: table.id,
                table_name: table.name.clone().into(),
                partition_id: partition.id,
                partition_key: partition.partition_key.clone(),
                min_sequence_number,
                max_sequence_number,
                compaction_level: CompactionLevel::FileNonOverlapped,
                sort_key: Some(sort_key.clone()),
            };

            let compacted_data = CompactedData::new(stream, meta, tombstone_map.clone());
            compacted.push(compacted_data);
        }

        Ok(compacted)
    }

    async fn update_catalog(
        &self,
        catalog_update_info: Vec<CatalogUpdate>,
        original_parquet_file_ids: Vec<ParquetFileId>,
        txn: &mut dyn Transaction,
    ) -> Result<()> {
        for catalog_update in catalog_update_info {
            // create a parquet file in the catalog first
            let parquet = txn
                .parquet_files()
                .create(catalog_update.parquet_file)
                .await
                .context(UpdateSnafu)?;

            // Now that the parquet file is available, create its processed tombstones
            for (_, tombstone) in catalog_update.tombstones {
                // Because data may get removed and split during compaction, a few new files
                // may no longer overlap with the delete tombstones. Need to verify whether
                // they overlap before adding process tombstones
                if (parquet.min_time <= tombstone.min_time
                    && parquet.max_time >= tombstone.min_time)
                    || (parquet.min_time > tombstone.min_time
                        && parquet.min_time <= tombstone.max_time)
                {
                    txn.processed_tombstones()
                        .create(parquet.id, tombstone.id)
                        .await
                        .context(UpdateSnafu)?;
                }
            }
        }

        for original_parquet_file_id in original_parquet_file_ids {
            txn.parquet_files()
                .flag_for_delete(original_parquet_file_id)
                .await
                .context(FlagForDeleteSnafu)?;
        }

        Ok(())
    }

    // remove fully processed tombstones
    async fn remove_fully_processed_tombstones(
        &self,
        tombstones: BTreeMap<TombstoneId, Tombstone>,
    ) -> Result<()> {
        // get fully proccessed ones
        let mut to_be_removed = Vec::with_capacity(tombstones.len());
        for (ts_id, ts) in tombstones {
            if self.fully_processed(ts).await {
                to_be_removed.push(ts_id);
            }
        }

        // Remove the tombstones
        // Note (todo maybe): if the list of deleted tombstone_ids is long and
        // make this transaction slow, we need to split them into many smaller lists
        // and each will be removed in its own transaction.
        let mut txn = self
            .catalog
            .start_transaction()
            .await
            .context(TransactionSnafu)?;

        txn.tombstones()
            .remove(&to_be_removed)
            .await
            .context(RemoveTombstonesSnafu)?;

        txn.commit().await.context(TransactionCommitSnafu)?;

        Ok(())
    }

    // Return true if the given tombstones is fully processed
    async fn fully_processed(&self, tombstone: Tombstone) -> bool {
        let mut repos = self.catalog.repositories().await;

        // Get number of non-deleted parquet files of the same table ID & sequencer ID that overlap
        // with the tombstone time range
        let count_pf = repos
            .parquet_files()
            .count_by_overlaps(
                tombstone.table_id,
                tombstone.sequencer_id,
                tombstone.min_time,
                tombstone.max_time,
                tombstone.sequence_number,
            )
            .await;
        let count_pf = match count_pf {
            Ok(count_pf) => count_pf,
            _ => {
                warn!(
                    "Error getting parquet file count for table ID {}, sequencer ID {}, min time \
                     {:?}, max time {:?}. Won't be able to verify whether its tombstone is fully \
                     processed",
                    tombstone.table_id,
                    tombstone.sequencer_id,
                    tombstone.min_time,
                    tombstone.max_time
                );
                return false;
            }
        };

        // Get number of the processed parquet file for this tombstone
        let count_pt = repos
            .processed_tombstones()
            .count_by_tombstone_id(tombstone.id)
            .await;
        let count_pt = match count_pt {
            Ok(count_pt) => count_pt,
            _ => {
                warn!(
                    "Error getting processed tombstone count for tombstone ID {}.
                    Won't be able to verify whether the tombstone is fully processed",
                    tombstone.id
                );
                return false;
            }
        };

        // Fully processed if two count the same
        count_pf == count_pt
    }

    async fn add_tombstones_to_files(
        &self,
        parquet_files: Vec<ParquetFile>,
    ) -> Result<Option<GroupWithTombstones>> {
        let mut repo = self.catalog.repositories().await;
        let tombstone_repo = repo.tombstones();

        // Skip over any empty groups
        if parquet_files.is_empty() {
            return Ok(None);
        }

        // Find the time range of the group
        let overall_min_time = parquet_files
            .iter()
            .map(|pf| pf.min_time)
            .min()
            .expect("The group was checked for emptiness above");
        let overall_max_time = parquet_files
            .iter()
            .map(|pf| pf.max_time)
            .max()
            .expect("The group was checked for emptiness above");
        // For a tombstone to be relevant to any parquet file, the tombstone must have a
        // sequence number greater than the parquet file's max_sequence_number. If we query
        // for all tombstones with a sequence number greater than the smallest parquet file
        // max_sequence_number in the group, we'll get all tombstones that could possibly
        // be relevant for this group.
        let overall_min_max_sequence_number = parquet_files
            .iter()
            .map(|pf| pf.max_sequence_number)
            .min()
            .expect("The group was checked for emptiness above");

        // Query the catalog for the tombstones that could be relevant to any parquet files
        // in this group.
        let tombstones = tombstone_repo
            .list_tombstones_for_time_range(
                // We've previously grouped the parquet files by sequence and table IDs, so
                // these values will be the same for all parquet files in the group.
                parquet_files[0].sequencer_id,
                parquet_files[0].table_id,
                overall_min_max_sequence_number,
                overall_min_time,
                overall_max_time,
            )
            .await
            .context(QueryingTombstonesSnafu)?;

        let parquet_files = parquet_files
            .into_iter()
            .map(|data| {
                // Filter the set of tombstones relevant to any file in the group to just those
                // relevant to this particular parquet file.
                let relevant_tombstones = tombstones
                    .iter()
                    .cloned()
                    .filter(|t| {
                        t.sequence_number > data.max_sequence_number
                            && ((t.min_time <= data.min_time && t.max_time >= data.min_time)
                                || (t.min_time > data.min_time && t.min_time <= data.max_time))
                    })
                    .collect();

                ParquetFileWithTombstone::new(Arc::new(data), relevant_tombstones)
            })
            .collect();

        Ok(Some(GroupWithTombstones {
            parquet_files,
            tombstones,
        }))
    }
}

/// [`PartitionParam`] with some information about its table and namespace.
#[derive(Debug)]
pub struct PartitionCompactionCandidateWithInfo {
    /// Partition compaction candidate.
    pub candidate: PartitionParam,

    /// Namespace.
    pub namespace: Arc<Namespace>,

    /// Table.
    pub table: Arc<Table>,

    /// Table schema
    pub table_schema: Arc<TableSchema>,
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;
    use arrow::record_batch::RecordBatch;
    use arrow_util::assert_batches_sorted_eq;
    use data_types::{
        ColumnId, ColumnSet, ColumnType, KafkaPartition, NamespaceId, ParquetFileParams,
        SequenceNumber, Timestamp,
    };
    use futures::{stream::FuturesOrdered, StreamExt, TryStreamExt};
    use iox_tests::util::{TestCatalog, TestParquetFileBuilder, TestTable};
    use iox_time::SystemProvider;
    use parquet_file::ParquetFilePath;
    use schema::{selection::Selection, sort::SortKey};
    use test_helpers::maybe_start_logging;

    #[tokio::test]
    // This is integration test to verify all pieces are put together correctly
    async fn test_compact_partition_one_file_one_tombstone() {
        test_helpers::maybe_start_logging();
        let catalog = TestCatalog::new();

        let lp = vec![
            "table,tag1=WA field_int=1000i 8000",
            "table,tag1=VT field_int=10i 10000",
            "table,tag1=UT field_int=70i 20000",
        ]
        .join("\n");
        let ns = catalog.create_namespace("ns").await;
        let sequencer = ns.create_sequencer(1).await;
        let table = ns.create_table("table").await;
        table.create_column("field_int", ColumnType::I64).await;
        table.create_column("tag1", ColumnType::Tag).await;
        table.create_column("time", ColumnType::Time).await;
        let table_schema = table.catalog_schema().await;

        let partition = table
            .with_sequencer(&sequencer)
            .create_partition("part")
            .await;

        // One parquet file
        let builder = TestParquetFileBuilder::default()
            .with_line_protocol(&lp)
            .with_min_seq(1)
            .with_max_seq(1)
            .with_min_time(8_000)
            .with_max_time(20_000)
            // file size > compaction_max_size_bytes to have it split into 2 files
            .with_file_size_bytes(120_000)
            .with_creation_time(catalog.time_provider.now().timestamp_nanos());
        partition.create_parquet_file(builder).await;

        // should have 1 level-0 file
        let count = catalog.count_level_0_files(sequencer.sequencer.id).await;
        assert_eq!(count, 1);

        // One overlapped tombstone
        let tombstone = table
            .with_sequencer(&sequencer)
            .create_tombstone(20, 6000, 12000, "tag1=VT")
            .await;
        // Should have 1 tombstone
        let count = catalog.count_tombstones_for_table(table.table.id).await;
        assert_eq!(count, 1);

        // ------------------------------------------------
        // Compact
        let config = make_compactor_config();
        let metrics = Arc::new(metric::Registry::new());
        let compactor = Compactor::new(
            vec![sequencer.sequencer.id],
            Arc::clone(&catalog.catalog),
            ParquetStorage::new(Arc::clone(&catalog.object_store)),
            Arc::new(Executor::new(1)),
            Arc::new(SystemProvider::new()),
            BackoffConfig::default(),
            config,
            Arc::clone(&metrics),
        );

        let compact_and_upgrade = compactor
            .groups_to_compact_and_files_to_upgrade(
                partition.partition.id,
                &ns.namespace.name,
                &table.table.name,
            )
            .await
            .unwrap();
        compactor
            .compact_partition(
                &ns.namespace,
                &table.table,
                &table_schema,
                partition.partition.id,
                compact_and_upgrade,
            )
            .await
            .unwrap();

        // should have 1 non-deleted level-1 file. The original file was marked deleted and not
        // counted.
        let mut files = catalog.list_by_table_not_to_delete(table.table.id).await;
        assert_eq!(files.len(), 1);
        // 1 newly created level-1 file as the result of compaction
        assert_eq!(
            (files[0].id.get(), files[0].compaction_level),
            (2, CompactionLevel::FileNonOverlapped)
        );

        // processed tombstones created and deleted inside compact_partition function
        let count = catalog
            .count_processed_tombstones(tombstone.tombstone.id)
            .await;
        assert_eq!(count, 0);
        // the tombstone is fully processed and should have been removed
        let count = catalog.count_tombstones_for_table(table.table.id).await;
        assert_eq!(count, 0);

        // Verify the file was pushed to the object store
        let object_store = catalog.object_store();
        let list = object_store.list(None).await.unwrap();
        let object_store_files: Vec<_> = list.try_collect().await.unwrap();
        // Original + 1 compacted
        assert_eq!(object_store_files.len(), 2);

        // ------------------------------------------------
        // Verify the parquet file content

        // query the chunks
        let files1 = files.pop().unwrap();
        let batches = read_parquet_file(&table, files1).await;
        assert_batches_sorted_eq!(
            &[
                "+-----------+------+-----------------------------+",
                "| field_int | tag1 | time                        |",
                "+-----------+------+-----------------------------+",
                "| 70        | UT   | 1970-01-01T00:00:00.000020Z |",
                "| 1000      | WA   | 1970-01-01T00:00:00.000008Z |",
                "+-----------+------+-----------------------------+",
            ],
            &batches
        );
        assert!(files.is_empty());

        // Verify the metrics
        let level_initial_file_counter = metrics
            .get_instrument::<Metric<U64Counter>>("compactor_compacted_files_total")
            .unwrap()
            .get_observer(&Attributes::from(&[
                ("sequencer_id", "1"),
                ("compaction_level", "0"),
            ]))
            .unwrap()
            .fetch();
        assert_eq!(level_initial_file_counter, 1);

        assert!(metrics
            .get_instrument::<Metric<U64Counter>>("compactor_compacted_files_total")
            .unwrap()
            .get_observer(&Attributes::from(&[
                ("sequencer_id", "1"),
                ("compaction_level", "2"),
            ]))
            .is_none());
    }

    // A quite sophisticated integration test
    // Beside lp data, every value min/max sequence numbers and min/max time are important
    // to have a combination of needed tests in this test function
    #[tokio::test]
    async fn test_compact_partition_many_files_many_tombstones() {
        maybe_start_logging();
        let catalog = TestCatalog::new();

        // lp1 does not overlap with any
        let lp1 = vec![
            "table,tag1=WA field_int=1000i 10",
            "table,tag1=VT field_int=10i 20",
        ]
        .join("\n");

        // lp2 overlaps with lp3
        let lp2 = vec![
            "table,tag1=WA field_int=1000i 8000", // will be eliminated due to duplicate
            "table,tag1=VT field_int=10i 10000",  // will be deleted by ts2
            "table,tag1=UT field_int=70i 20000",
        ]
        .join("\n");

        // lp3 overlaps with lp2
        let lp3 = vec![
            "table,tag1=WA field_int=1500i 8000", // latest duplicate and kept
            "table,tag1=VT field_int=10i 6000",
            "table,tag1=UT field_int=270i 25000",
        ]
        .join("\n");

        // lp4 does not overlap with any
        let lp4 = vec![
            "table,tag2=WA,tag3=10 field_int=1600i 28000",
            "table,tag2=VT,tag3=20 field_int=20i 26000",
        ]
        .join("\n");

        // lp5 does not overlap with any
        let lp5 = vec![
            "table,tag2=PA,tag3=15 field_int=1601i 30000",
            "table,tag2=OH,tag3=21 field_int=21i 36000",
        ]
        .join("\n");

        let ns = catalog.create_namespace("ns").await;
        let sequencer = ns.create_sequencer(1).await;
        let table = ns.create_table("table").await;
        table.create_column("field_int", ColumnType::I64).await;
        table.create_column("tag1", ColumnType::Tag).await;
        table.create_column("tag2", ColumnType::Tag).await;
        table.create_column("tag3", ColumnType::Tag).await;
        table.create_column("time", ColumnType::Time).await;
        let table_schema = table.catalog_schema().await;
        let partition = table
            .with_sequencer(&sequencer)
            .create_partition("part")
            .await;
        let time = Arc::new(SystemProvider::new());
        let config = make_compactor_config();
        let metrics = Arc::new(metric::Registry::new());
        let compactor = Compactor::new(
            vec![sequencer.sequencer.id],
            Arc::clone(&catalog.catalog),
            ParquetStorage::new(Arc::clone(&catalog.object_store)),
            Arc::new(Executor::new(1)),
            Arc::new(SystemProvider::new()),
            BackoffConfig::default(),
            config,
            Arc::clone(&metrics),
        );

        // parquet files that are all in the same partition and should all end up in the same
        // compacted file

        // pf1 does not overlap with any and is very large
        let builder = TestParquetFileBuilder::default()
            .with_line_protocol(&lp1)
            .with_min_seq(1)
            .with_max_seq(1)
            .with_min_time(10)
            .with_max_time(20)
            .with_file_size_bytes(compactor.config.max_desired_file_size_bytes() + 10)
            .with_creation_time(20);
        partition.create_parquet_file(builder).await;

        // pf2 overlaps with pf3
        let builder = TestParquetFileBuilder::default()
            .with_line_protocol(&lp2)
            .with_min_seq(4)
            .with_max_seq(5)
            .with_min_time(8_000)
            .with_max_time(20_000)
            .with_file_size_bytes(100) // small file
            .with_creation_time(time.now().timestamp_nanos());
        partition.create_parquet_file(builder).await;

        // pf3 overlaps with pf2
        let builder = TestParquetFileBuilder::default()
            .with_line_protocol(&lp3)
            .with_min_seq(8)
            .with_max_seq(10)
            .with_min_time(6_000)
            .with_max_time(25_000)
            .with_file_size_bytes(100) // small file
            .with_creation_time(time.now().timestamp_nanos());
        partition.create_parquet_file(builder).await;

        // pf4 does not overlap with any but is small
        let builder = TestParquetFileBuilder::default()
            .with_line_protocol(&lp4)
            .with_min_seq(18)
            .with_max_seq(18)
            .with_min_time(26_000)
            .with_max_time(28_000)
            .with_file_size_bytes(100) // small file
            .with_creation_time(time.now().timestamp_nanos());
        partition.create_parquet_file(builder).await;

        // pf5 was created in a previous compaction cycle
        let builder = TestParquetFileBuilder::default()
            .with_line_protocol(&lp5)
            .with_min_seq(20)
            .with_max_seq(20)
            .with_min_time(30_000)
            .with_max_time(36_000)
            .with_file_size_bytes(100) // small file
            .with_creation_time(time.now().timestamp_nanos())
            .with_compaction_level(CompactionLevel::FileNonOverlapped);
        partition.create_parquet_file(builder).await;

        // should have 4 level-0 files before compacting
        let count = catalog.count_level_0_files(sequencer.sequencer.id).await;
        assert_eq!(count, 4);

        // create 3 tombstones
        // ts1 overlaps with pf1 and pf2 but issued before pf1 and pf2 hence it won't be used
        let ts1 = table
            .with_sequencer(&sequencer)
            .create_tombstone(2, 6000, 21000, "tag1=UT")
            .await;
        // ts2 overlap with both pf1 and pf2 but issued before pf3 so only applied to pf2
        let ts2 = table
            .with_sequencer(&sequencer)
            .create_tombstone(6, 6000, 12000, "tag1=VT")
            .await;
        // ts3 does not overlap with any files
        let ts3 = table
            .with_sequencer(&sequencer)
            .create_tombstone(22, 1000, 2000, "tag1=VT")
            .await;
        // should have 3 tombstones
        let count = catalog.count_tombstones_for_table(table.table.id).await;
        assert_eq!(count, 3);
        // should not have any processed tombstones for any tombstones
        let count = catalog.count_processed_tombstones(ts1.tombstone.id).await;
        assert_eq!(count, 0);
        let count = catalog.count_processed_tombstones(ts2.tombstone.id).await;
        assert_eq!(count, 0);
        let count = catalog.count_processed_tombstones(ts3.tombstone.id).await;
        assert_eq!(count, 0);

        // ------------------------------------------------
        // Compact
        let compact_and_upgrade = compactor
            .groups_to_compact_and_files_to_upgrade(
                partition.partition.id,
                &ns.namespace.name,
                &table.table.name,
            )
            .await
            .unwrap();
        compactor
            .compact_partition(
                &ns.namespace,
                &table.table,
                &table_schema,
                partition.partition.id,
                compact_and_upgrade,
            )
            .await
            .unwrap();

        // Should have 1 non-soft-deleted files: the one newly created after compacting pf1, pf2,
        // pf3, pf4 all into one file
        let mut files = catalog.list_by_table_not_to_delete(table.table.id).await;
        assert_eq!(files.len(), 1);
        // 1 newly created CompactionLevel::FileNonOverlapped file as the result of
        // compaction
        assert_eq!(
            (files[0].id.get(), files[0].compaction_level),
            (6, CompactionLevel::FileNonOverlapped)
        );

        // should have ts1 and ts3 that not involved in the commpaction process
        // ts2 was removed because it was fully processed
        let tss = catalog.list_tombstones_by_table(table.table.id).await;
        assert_eq!(tss.len(), 2);
        assert_eq!(tss[0].id.get(), ts1.tombstone.id.get());
        assert_eq!(tss[1].id.get(), ts3.tombstone.id.get());

        // processed tombstones of ts2 was created and deleted inside compact_partition function
        let count = catalog.count_processed_tombstones(ts2.tombstone.id).await;
        assert_eq!(count, 0);

        // ------------------------------------------------
        // Verify the parquet file content

        // Compacted file
        let file1 = files.pop().unwrap();
        let batches = read_parquet_file(&table, file1).await;
        assert_batches_sorted_eq!(
            &[
                "+-----------+------+------+------+--------------------------------+",
                "| field_int | tag1 | tag2 | tag3 | time                           |",
                "+-----------+------+------+------+--------------------------------+",
                "| 10        | VT   |      |      | 1970-01-01T00:00:00.000000020Z |",
                "| 10        | VT   |      |      | 1970-01-01T00:00:00.000006Z    |",
                "| 1000      | WA   |      |      | 1970-01-01T00:00:00.000000010Z |",
                "| 1500      | WA   |      |      | 1970-01-01T00:00:00.000008Z    |",
                "| 270       | UT   |      |      | 1970-01-01T00:00:00.000025Z    |",
                "| 70        | UT   |      |      | 1970-01-01T00:00:00.000020Z    |",
                "| 1600      |      | WA   | 10   | 1970-01-01T00:00:00.000028Z    |",
                "| 20        |      | VT   | 20   | 1970-01-01T00:00:00.000026Z    |",
                "| 1601      |      | PA   | 15   | 1970-01-01T00:00:00.000030Z    |",
                "| 21        |      | OH   | 21   | 1970-01-01T00:00:00.000036Z    |",
                "+-----------+------+------+------+--------------------------------+",
            ],
            &batches
        );
        // No more files
        assert!(files.is_empty());

        // Verify the metrics
        let level_initial_file_counter = metrics
            .get_instrument::<Metric<U64Counter>>("compactor_compacted_files_total")
            .unwrap()
            .get_observer(&Attributes::from(&[
                ("sequencer_id", "1"),
                ("compaction_level", "0"),
            ]))
            .unwrap()
            .fetch();
        assert_eq!(level_initial_file_counter, 4);

        let level_non_overlapped_file_counter = metrics
            .get_instrument::<Metric<U64Counter>>("compactor_compacted_files_total")
            .unwrap()
            .get_observer(&Attributes::from(&[
                ("sequencer_id", "1"),
                ("compaction_level", "1"),
            ]))
            .unwrap()
            .fetch();
        assert_eq!(level_non_overlapped_file_counter, 1);
    }

    #[tokio::test]
    async fn test_compact_one_file_no_tombstones_is_complete() {
        let catalog = TestCatalog::new();

        let lp = vec![
            "table,tag1=WA field_int=1000i 8000",
            "table,tag1=VT field_int=10i 10000",
            "table,tag1=UT field_int=70i 20000",
        ]
        .join("\n");
        let ns = catalog.create_namespace("ns").await;
        let sequencer = ns.create_sequencer(1).await;
        let table = ns.create_table("table").await;
        table.create_column("tag1", ColumnType::Tag).await;
        table.create_column("field_int", ColumnType::I64).await;
        table.create_column("time", ColumnType::Time).await;
        let table_schema = table.catalog_schema().await;
        let partition = table
            .with_sequencer(&sequencer)
            .create_partition("part")
            .await;
        let builder = TestParquetFileBuilder::default()
            .with_line_protocol(&lp)
            .with_min_seq(1)
            .with_max_seq(1)
            .with_min_time(8_000)
            .with_max_time(20_000);
        let parquet_file = partition.create_parquet_file(builder).await.parquet_file;

        let config = make_compactor_config();
        let compactor = Compactor::new(
            vec![sequencer.sequencer.id],
            Arc::clone(&catalog.catalog),
            ParquetStorage::new(Arc::clone(&catalog.object_store)),
            Arc::new(Executor::new(1)),
            Arc::new(SystemProvider::new()),
            BackoffConfig::default(),
            config,
            Arc::new(metric::Registry::new()),
        );

        let sort_key = SortKey::from_columns(["tag1", "time"]);
        let partition = partition.update_sort_key(sort_key).await;

        // ------------------------------------------------
        // no files provided
        let result = compactor
            .compact(
                vec![],
                &ns.namespace,
                &table.table,
                &table_schema,
                &partition.partition,
            )
            .await
            .unwrap();
        assert!(result.is_empty());

        // ------------------------------------------------
        // File without tombstones
        let pf = ParquetFileWithTombstone::new(Arc::new(parquet_file), vec![]);
        // Nothing compacted for one file without tombstones
        let result = compactor
            .compact(
                vec![pf.clone()],
                &ns.namespace,
                &table.table,
                &table_schema,
                &partition.partition,
            )
            .await
            .unwrap();
        assert!(result.is_empty());
    }

    #[tokio::test]
    async fn test_compact_two_files() {
        let catalog = TestCatalog::new();

        let lp1 = vec![
            "table,tag1=WA field_int=1000i 8000", // will be eliminated due to duplicate
            "table,tag1=VT field_int=10i 10000",  // will be deleted
            "table,tag1=UT field_int=70i 20000",
        ]
        .join("\n");

        let lp2 = vec![
            "table,tag1=WA field_int=1500i 8000", // latest duplicate and kept
            "table,tag1=VT field_int=10i 6000",
            "table,tag1=UT field_int=270i 25000",
        ]
        .join("\n");

        let ns = catalog.create_namespace("ns").await;
        let sequencer = ns.create_sequencer(1).await;
        let table = ns.create_table("table").await;
        table.create_column("tag1", ColumnType::Tag).await;
        table.create_column("field_int", ColumnType::I64).await;
        table.create_column("time", ColumnType::Time).await;
        let table_schema = table.catalog_schema().await;
        let partition = table
            .with_sequencer(&sequencer)
            .create_partition("part")
            .await;

        // Create 2 parquet files in the same partition
        let builder = TestParquetFileBuilder::default()
            .with_line_protocol(&lp1)
            .with_min_seq(1)
            .with_max_seq(5)
            .with_min_time(8_000)
            .with_max_time(20_000)
            .with_file_size_bytes(140_000);
        let parquet_file1 = partition.create_parquet_file(builder).await.parquet_file;

        let builder = TestParquetFileBuilder::default()
            .with_line_protocol(&lp2)
            .with_min_seq(10)
            .with_max_seq(15)
            .with_min_time(6_000)
            .with_max_time(25_000)
            .with_file_size_bytes(100_000);
        let parquet_file2 = partition.create_parquet_file(builder).await.parquet_file;

        let config = make_compactor_config();
        let compactor = Compactor::new(
            vec![sequencer.sequencer.id],
            Arc::clone(&catalog.catalog),
            ParquetStorage::new(Arc::clone(&catalog.object_store)),
            Arc::new(Executor::new(1)),
            Arc::new(SystemProvider::new()),
            BackoffConfig::default(),
            config,
            Arc::new(metric::Registry::new()),
        );

        let sort_key = SortKey::from_columns(["tag1", "time"]);
        let partition = partition.update_sort_key(sort_key).await;

        // File 1 with tombstone
        let tombstone = table
            .with_sequencer(&sequencer)
            .create_tombstone(6, 6000, 12000, "tag1=VT")
            .await;
        let pf1 = ParquetFileWithTombstone::new(
            Arc::new(parquet_file1),
            vec![tombstone.tombstone.clone()],
        );
        // File 2 without tombstones
        let pf2 = ParquetFileWithTombstone::new(Arc::new(parquet_file2), vec![]);

        // Compact them into 1 batch/file
        let batches = compactor
            .compact(
                vec![pf1, pf2],
                &ns.namespace,
                &table.table,
                &table_schema,
                &partition.partition,
            )
            .await
            .unwrap();
        assert_eq!(batches.len(), 1);

        // Collect the results for inspection.
        let batches = batches
            .into_iter()
            .map(|v| async {
                datafusion::physical_plan::common::collect(v.data)
                    .await
                    .expect("failed to collect record batches")
            })
            .collect::<FuturesOrdered<_>>()
            .collect::<Vec<_>>()
            .await;

        // Data: Should have 4 rows left
        assert_batches_sorted_eq!(
            &[
                "+-----------+------+-----------------------------+",
                "| field_int | tag1 | time                        |",
                "+-----------+------+-----------------------------+",
                "| 70        | UT   | 1970-01-01T00:00:00.000020Z |",
                "| 270       | UT   | 1970-01-01T00:00:00.000025Z |",
                "| 10        | VT   | 1970-01-01T00:00:00.000006Z |",
                "| 1500      | WA   | 1970-01-01T00:00:00.000008Z |",
                "+-----------+------+-----------------------------+",
            ],
            &batches[0]
        );
    }

    #[tokio::test]
    async fn test_compact_three_files_different_cols() {
        let catalog = TestCatalog::new();

        let lp1 = vec![
            "table,tag1=WA field_int=1000i 8000", // will be eliminated due to duplicate
            "table,tag1=VT field_int=10i 10000",  // will be deleted
            "table,tag1=UT field_int=70i 20000",
        ]
        .join("\n");

        let lp2 = vec![
            "table,tag1=WA field_int=1500i 8000", // latest duplicate and kept
            "table,tag1=VT field_int=10i 6000",
            "table,tag1=UT field_int=270i 25000",
        ]
        .join("\n");

        let lp3 = vec![
            "table,tag2=WA,tag3=10 field_int=1500i 8000",
            "table,tag2=VT,tag3=20 field_int=10i 6000",
        ]
        .join("\n");

        let ns = catalog.create_namespace("ns").await;
        let sequencer = ns.create_sequencer(1).await;
        let table = ns.create_table("table").await;
        table.create_column("tag1", ColumnType::Tag).await;
        table.create_column("tag2", ColumnType::Tag).await;
        table.create_column("tag3", ColumnType::Tag).await;
        table.create_column("field_int", ColumnType::I64).await;
        table.create_column("time", ColumnType::Time).await;
        let table_schema = table.catalog_schema().await;
        let partition = table
            .with_sequencer(&sequencer)
            .create_partition("part")
            .await;

        // Sequence numbers are important here.
        // Time/sequence order from small to large: parquet_file_1, parquet_file_2, parquet_file_3
        // total file size = 50000 (file1) + 50000 (file2) + 20000 (file3)
        let builder = TestParquetFileBuilder::default()
            .with_line_protocol(&lp1)
            .with_min_seq(1)
            .with_max_seq(5)
            .with_min_time(8_000)
            .with_max_time(20_000)
            .with_file_size_bytes(50_000);
        let parquet_file1 = partition.create_parquet_file(builder).await.parquet_file;

        let builder = TestParquetFileBuilder::default()
            .with_line_protocol(&lp2)
            .with_min_seq(10)
            .with_max_seq(15)
            .with_min_time(6_000)
            .with_max_time(25_000)
            .with_file_size_bytes(50_000);
        let parquet_file2 = partition.create_parquet_file(builder).await.parquet_file;

        let builder = TestParquetFileBuilder::default()
            .with_line_protocol(&lp3)
            .with_min_seq(20)
            .with_max_seq(25)
            .with_min_time(6_000)
            .with_max_time(8_000)
            .with_file_size_bytes(20_000);
        let parquet_file3 = partition.create_parquet_file(builder).await.parquet_file;

        let config = make_compactor_config();
        let compactor = Compactor::new(
            vec![sequencer.sequencer.id],
            Arc::clone(&catalog.catalog),
            ParquetStorage::new(Arc::clone(&catalog.object_store)),
            Arc::new(Executor::new(1)),
            Arc::new(SystemProvider::new()),
            BackoffConfig::default(),
            config,
            Arc::new(metric::Registry::new()),
        );

        // The sort key comes from the catalog and should be the union of all tags the
        // ingester has seen
        let sort_key = SortKey::from_columns(["tag1", "tag2", "tag3", "time"]);
        let partition = partition.update_sort_key(sort_key.clone()).await;

        // File 1 with tombstone
        let tombstone = table
            .with_sequencer(&sequencer)
            .create_tombstone(6, 6000, 12000, "tag1=VT")
            .await;
        let pf1 = ParquetFileWithTombstone::new(
            Arc::new(parquet_file1),
            vec![tombstone.tombstone.clone()],
        );
        // File 2 without tombstones
        let pf2 = ParquetFileWithTombstone::new(Arc::new(parquet_file2), vec![]);
        // File 3 without tombstones
        let pf3 = ParquetFileWithTombstone::new(Arc::new(parquet_file3), vec![]);

        // Compact them into 1 batch/file
        let batches = compactor
            .compact(
                vec![pf1.clone(), pf2.clone(), pf3.clone()],
                &ns.namespace,
                &table.table,
                &table_schema,
                &partition.partition,
            )
            .await
            .unwrap();
        assert_eq!(batches.len(), 1);

        // Sort keys should be the same as was passed in to compact
        assert_eq!(batches[0].meta.sort_key.as_ref().unwrap(), &sort_key);

        // Collect the results for inspection.
        let batches = batches
            .into_iter()
            .map(|v| async {
                datafusion::physical_plan::common::collect(v.data)
                    .await
                    .expect("failed to collect record batches")
            })
            .collect::<FuturesOrdered<_>>()
            .collect::<Vec<_>>()
            .await;

        // Data: Should have 6 rows left
        assert_batches_sorted_eq!(
            &[
                "+-----------+------+------+------+-----------------------------+",
                "| field_int | tag1 | tag2 | tag3 | time                        |",
                "+-----------+------+------+------+-----------------------------+",
                "| 10        |      | VT   | 20   | 1970-01-01T00:00:00.000006Z |",
                "| 10        | VT   |      |      | 1970-01-01T00:00:00.000006Z |",
                "| 1500      |      | WA   | 10   | 1970-01-01T00:00:00.000008Z |",
                "| 1500      | WA   |      |      | 1970-01-01T00:00:00.000008Z |",
                "| 70        | UT   |      |      | 1970-01-01T00:00:00.000020Z |",
                "| 270       | UT   |      |      | 1970-01-01T00:00:00.000025Z |",
                "+-----------+------+------+------+-----------------------------+",
            ],
            &batches[0]
        );
    }

    #[tokio::test]
    async fn test_sort_queryable_parquet_chunk() {
        let catalog = TestCatalog::new();

        let lp1 = vec![
            "table,tag1=WA field_int=1000i 8000",
            "table,tag1=VT field_int=10i 10000",
            "table,tag1=UT field_int=70i 20000",
        ]
        .join("\n");

        let lp2 = vec![
            "table,tag1=WA field_int=1500i 28000",
            "table,tag1=UT field_int=270i 35000",
        ]
        .join("\n");

        let ns = catalog.create_namespace("ns").await;
        let sequencer = ns.create_sequencer(1).await;
        let table = ns.create_table("table").await;
        table.create_column("tag1", ColumnType::Tag).await;
        table.create_column("field_int", ColumnType::I64).await;
        table.create_column("time", ColumnType::Time).await;
        let table_schema = table.catalog_schema().await;
        let partition = table
            .with_sequencer(&sequencer)
            .create_partition("part")
            .await;
        // 2 files with same min_sequence_number
        let builder = TestParquetFileBuilder::default()
            .with_line_protocol(&lp1)
            .with_min_seq(1)
            .with_max_seq(5)
            .with_min_time(8_000)
            .with_max_time(20_000);
        let pf1 = partition.create_parquet_file(builder).await.parquet_file;
        let builder = TestParquetFileBuilder::default()
            .with_line_protocol(&lp2)
            .with_min_seq(1)
            .with_max_seq(5)
            .with_min_time(28_000)
            .with_max_time(35_000);
        let pf2 = partition.create_parquet_file(builder).await.parquet_file;

        // Build 2 QueryableParquetChunks
        let pt1 = ParquetFileWithTombstone::new(Arc::new(pf1), vec![]);
        let pt2 = ParquetFileWithTombstone::new(Arc::new(pf2), vec![]);

        let pc1 = pt1.to_queryable_parquet_chunk(
            ParquetStorage::new(Arc::clone(&catalog.object_store)),
            table.table.name.clone(),
            &table_schema,
            partition.partition.sort_key(),
        );
        let pc2 = pt2.to_queryable_parquet_chunk(
            ParquetStorage::new(Arc::clone(&catalog.object_store)),
            table.table.name.clone(),
            &table_schema,
            partition.partition.sort_key(),
        );

        // Vector of chunks
        let chunks = vec![pc2, pc1];
        // must same order/min_sequnce_number
        assert_eq!(chunks[0].order(), chunks[1].order());
        // different chunk ids
        assert!(chunks[0].id() != chunks[1].id());
    }

    #[tokio::test]
    async fn test_query_queryable_parquet_chunk() {
        let catalog = TestCatalog::new();

        let lp = vec![
            "table,tag1=WA field_int=1000i 8000",
            "table,tag1=VT field_int=10i 10000",
            "table,tag1=UT field_int=70i 20000",
        ]
        .join("\n");

        let ns = catalog.create_namespace("ns").await;
        let sequencer = ns.create_sequencer(1).await;
        let table = ns.create_table("table").await;
        table.create_column("field_int", ColumnType::I64).await;
        table.create_column("field_float", ColumnType::F64).await;
        table.create_column("tag1", ColumnType::Tag).await;
        table.create_column("tag2", ColumnType::Tag).await;
        table.create_column("tag3", ColumnType::Tag).await;
        table.create_column("time", ColumnType::Time).await;
        let table_schema = table.catalog_schema().await;
        let partition = table
            .with_sequencer(&sequencer)
            .create_partition("part")
            .await
            .update_sort_key(SortKey::from_columns(["tag1", "tag2", "time"]))
            .await;

        let builder = TestParquetFileBuilder::default().with_line_protocol(&lp);
        let pf = partition.create_parquet_file(builder).await.parquet_file;

        let pt = ParquetFileWithTombstone::new(Arc::new(pf), vec![]);

        let pc = pt.to_queryable_parquet_chunk(
            ParquetStorage::new(Arc::clone(&catalog.object_store)),
            table.table.name.clone(),
            &table_schema,
            partition.partition.sort_key(),
        );
        let stream = pc
            .read_filter(
                catalog.exec().new_context(ExecutorType::Query),
                &predicate::Predicate::new(),
                Selection::All,
            )
            .unwrap();
        let batches = datafusion::physical_plan::common::collect(stream)
            .await
            .unwrap();
        assert_eq!(batches.len(), 1);
        assert_batches_sorted_eq!(
            &[
                "+-----------+------+-----------------------------+",
                "| field_int | tag1 | time                        |",
                "+-----------+------+-----------------------------+",
                "| 10        | VT   | 1970-01-01T00:00:00.000010Z |",
                "| 1000      | WA   | 1970-01-01T00:00:00.000008Z |",
                "| 70        | UT   | 1970-01-01T00:00:00.000020Z |",
                "+-----------+------+-----------------------------+",
            ],
            &batches
        );
    }

    #[tokio::test]
    async fn test_add_parquet_file_with_tombstones() {
        let catalog = TestCatalog::new();

        let config = make_compactor_config();
        let compactor = Compactor::new(
            vec![],
            Arc::clone(&catalog.catalog),
            ParquetStorage::new(Arc::clone(&catalog.object_store)),
            Arc::new(Executor::new(1)),
            Arc::new(SystemProvider::new()),
            BackoffConfig::default(),
            config,
            Arc::new(metric::Registry::new()),
        );

        let mut txn = catalog.catalog.start_transaction().await.unwrap();
        let kafka = txn.kafka_topics().create_or_get("foo").await.unwrap();
        let pool = txn.query_pools().create_or_get("foo").await.unwrap();
        let namespace = txn
            .namespaces()
            .create(
                "namespace_parquet_file_with_tombstones_test",
                "inf",
                kafka.id,
                pool.id,
            )
            .await
            .unwrap();
        let table = txn
            .tables()
            .create_or_get("test_table", namespace.id)
            .await
            .unwrap();
        let sequencer = txn
            .sequencers()
            .create_or_get(&kafka, KafkaPartition::new(1))
            .await
            .unwrap();
        let partition = txn
            .partitions()
            .create_or_get("one".into(), sequencer.id, table.id)
            .await
            .unwrap();

        // Add tombstones
        let min_time = Timestamp::new(1);
        let max_time = Timestamp::new(10);
        let t1 = txn
            .tombstones()
            .create_or_get(
                table.id,
                sequencer.id,
                SequenceNumber::new(1),
                min_time,
                max_time,
                "whatevs",
            )
            .await
            .unwrap();
        let t2 = txn
            .tombstones()
            .create_or_get(
                table.id,
                sequencer.id,
                SequenceNumber::new(2),
                min_time,
                max_time,
                "bleh",
            )
            .await
            .unwrap();
        let t3 = txn
            .tombstones()
            .create_or_get(
                table.id,
                sequencer.id,
                SequenceNumber::new(3),
                min_time,
                max_time,
                "meh",
            )
            .await
            .unwrap();

        let meta = IoxMetadata {
            object_store_id: Uuid::new_v4(),
            creation_timestamp: compactor.time_provider.now(),
            namespace_id: NamespaceId::new(1),
            namespace_name: "mydata".into(),
            sequencer_id: sequencer.id,
            table_id: table.id,
            table_name: "temperature".into(),
            partition_id: PartitionId::new(4),
            partition_key: "somehour".into(),
            min_sequence_number: SequenceNumber::new(5),
            max_sequence_number: SequenceNumber::new(6),
            // TODO: consider to add level-1 tests before merging
            compaction_level: CompactionLevel::FileNonOverlapped,
            sort_key: None,
        };

        // Prepare metadata in form of ParquetFileParams to get added with tombstone
        let parquet = ParquetFileParams {
            sequencer_id: sequencer.id,
            namespace_id: namespace.id,
            table_id: table.id,
            partition_id: partition.id,
            object_store_id: Uuid::new_v4(),
            min_sequence_number: SequenceNumber::new(4),
            max_sequence_number: SequenceNumber::new(10),
            min_time,
            max_time,
            file_size_bytes: 1337,
            compaction_level: CompactionLevel::Initial, // level of file of new writes
            row_count: 0,
            created_at: Timestamp::new(1),
            column_set: ColumnSet::new([ColumnId::new(1), ColumnId::new(2)]),
        };
        let other_parquet = ParquetFileParams {
            object_store_id: Uuid::new_v4(),
            min_sequence_number: SequenceNumber::new(11),
            max_sequence_number: SequenceNumber::new(20),
            ..parquet.clone()
        };
        let another_parquet = ParquetFileParams {
            object_store_id: Uuid::new_v4(),
            min_sequence_number: SequenceNumber::new(21),
            max_sequence_number: SequenceNumber::new(30),
            ..parquet.clone()
        };

        let parquet_file_count_before = txn.parquet_files().count().await.unwrap();
        let pt_count_before = txn.processed_tombstones().count().await.unwrap();
        txn.commit().await.unwrap();

        // Add parquet and processed tombstone in one transaction
        let mut txn = catalog.catalog.start_transaction().await.unwrap();
        let catalog_updates = vec![CatalogUpdate {
            meta: meta.clone(),
            tombstones: BTreeMap::from([(t1.id, t1.clone()), (t2.id, t2.clone())]),
            parquet_file: parquet.clone(),
        }];
        compactor
            .update_catalog(catalog_updates, vec![], txn.deref_mut())
            .await
            .unwrap();
        txn.commit().await.unwrap();

        // verify the catalog
        let mut txn = catalog.catalog.start_transaction().await.unwrap();
        let parquet_file_count_after = txn.parquet_files().count().await.unwrap();
        let pt_count_after = txn.processed_tombstones().count().await.unwrap();
        let catalog_parquet_files = txn
            .parquet_files()
            .list_by_namespace_not_to_delete(namespace.id)
            .await
            .unwrap();
        assert_eq!(catalog_parquet_files.len(), 1);

        assert_eq!(pt_count_after - pt_count_before, 2);
        assert_eq!(parquet_file_count_after - parquet_file_count_before, 1);
        let pt_count_before = pt_count_after;
        let parquet_file_count_before = parquet_file_count_after;
        txn.commit().await.unwrap();

        // Error due to duplicate parquet file
        let mut txn = catalog.catalog.start_transaction().await.unwrap();
        let catalog_updates = vec![CatalogUpdate {
            meta: meta.clone(),
            tombstones: BTreeMap::from([(t3.id, t3.clone()), (t1.id, t1.clone())]),
            parquet_file: parquet.clone(),
        }];
        compactor
            .update_catalog(catalog_updates, vec![], txn.deref_mut())
            .await
            .unwrap_err();
        txn.abort().await.unwrap();

        // Since the transaction is rolled back, t3 is not yet added
        let mut txn = catalog.catalog.start_transaction().await.unwrap();
        let pt_count_after = txn.processed_tombstones().count().await.unwrap();
        assert_eq!(pt_count_after, pt_count_before);

        // Add new parquet and new tombstone, plus pretend this file was compacted from the first
        // parquet file so that one should now be deleted. Should go through
        let catalog_updates = vec![CatalogUpdate {
            meta: meta.clone(),
            tombstones: BTreeMap::from([(t3.id, t3.clone())]),
            parquet_file: other_parquet.clone(),
        }];
        compactor
            .update_catalog(
                catalog_updates,
                vec![catalog_parquet_files[0].id],
                txn.deref_mut(),
            )
            .await
            .unwrap();

        let pt_count_after = txn.processed_tombstones().count().await.unwrap();
        let parquet_file_count_after = txn.parquet_files().count().await.unwrap();
        assert_eq!(pt_count_after - pt_count_before, 1);
        assert_eq!(parquet_file_count_after - parquet_file_count_before, 1);
        let pt_count_before = pt_count_after;
        let parquet_file_count_before = parquet_file_count_after;
        let catalog_parquet_files = txn
            .parquet_files()
            .list_by_namespace_not_to_delete(namespace.id)
            .await
            .unwrap();
        // There should still only be one non-deleted file because we added one new one and marked
        // the existing one as deleted
        assert_eq!(catalog_parquet_files.len(), 1);
        txn.commit().await.unwrap();

        // Add non-exist tombstone t4 and should fail
        let mut txn = catalog.catalog.start_transaction().await.unwrap();
        let mut t4 = t3.clone();
        t4.id = TombstoneId::new(t4.id.get() + 10);
        let catalog_updates = vec![CatalogUpdate {
            meta: meta.clone(),
            tombstones: BTreeMap::from([(t4.id, t4.clone())]),
            parquet_file: another_parquet.clone(),
        }];
        compactor
            .update_catalog(catalog_updates, vec![], txn.deref_mut())
            .await
            .unwrap_err();
        txn.abort().await.unwrap();

        // Still same count as before
        let mut txn = catalog.catalog.start_transaction().await.unwrap();
        let pt_count_after = txn.processed_tombstones().count().await.unwrap();
        let parquet_file_count_after = txn.parquet_files().count().await.unwrap();
        assert_eq!(pt_count_after - pt_count_before, 0);
        assert_eq!(parquet_file_count_after - parquet_file_count_before, 0);
        txn.commit().await.unwrap();
    }

    #[tokio::test]
    async fn test_candidate_partitions() {
        let catalog = TestCatalog::new();

        // Create a db with 2 sequencers, one with 4 emppty partitions and the other one with one empty partition
        let mut txn = catalog.catalog.start_transaction().await.unwrap();

        let kafka = txn.kafka_topics().create_or_get("foo").await.unwrap();
        let pool = txn.query_pools().create_or_get("foo").await.unwrap();
        let namespace = txn
            .namespaces()
            .create("namespace_candidate_partitions", "inf", kafka.id, pool.id)
            .await
            .unwrap();
        let table = txn
            .tables()
            .create_or_get("test_table", namespace.id)
            .await
            .unwrap();
        let sequencer = txn
            .sequencers()
            .create_or_get(&kafka, KafkaPartition::new(1))
            .await
            .unwrap();
        let partition1 = txn
            .partitions()
            .create_or_get("one".into(), sequencer.id, table.id)
            .await
            .unwrap();
        let partition2 = txn
            .partitions()
            .create_or_get("two".into(), sequencer.id, table.id)
            .await
            .unwrap();
        let partition3 = txn
            .partitions()
            .create_or_get("three".into(), sequencer.id, table.id)
            .await
            .unwrap();
        let partition4 = txn
            .partitions()
            .create_or_get("four".into(), sequencer.id, table.id)
            .await
            .unwrap();
        // other sequencer
        let another_table = txn
            .tables()
            .create_or_get("another_test_table", namespace.id)
            .await
            .unwrap();
        let another_sequencer = txn
            .sequencers()
            .create_or_get(&kafka, KafkaPartition::new(2))
            .await
            .unwrap();
        let another_partition = txn
            .partitions()
            .create_or_get(
                "another_partition".into(),
                another_sequencer.id,
                another_table.id,
            )
            .await
            .unwrap();
        txn.commit().await.unwrap();

        // Create a compactor
        let time_provider = Arc::new(SystemProvider::new());
        let config = make_compactor_config();
        let compactor = Compactor::new(
            vec![sequencer.id, another_sequencer.id],
            Arc::clone(&catalog.catalog),
            ParquetStorage::new(Arc::clone(&catalog.object_store)),
            Arc::new(Executor::new(1)),
            time_provider,
            BackoffConfig::default(),
            config,
            Arc::new(metric::Registry::new()),
        );

        // Some times in the past to set to created_at of the files
        let time_now = Timestamp::new(compactor.time_provider.now().timestamp_nanos());
        let _time_one_hour_ago = Timestamp::new(
            (compactor.time_provider.now() - Duration::from_secs(60 * 60)).timestamp_nanos(),
        );
        let time_three_hour_ago = Timestamp::new(
            (compactor.time_provider.now() - Duration::from_secs(60 * 60 * 3)).timestamp_nanos(),
        );
        let time_five_hour_ago = Timestamp::new(
            (compactor.time_provider.now() - Duration::from_secs(60 * 60 * 5)).timestamp_nanos(),
        );
        let time_38_hour_ago = Timestamp::new(
            (compactor.time_provider.now() - Duration::from_secs(60 * 60 * 38)).timestamp_nanos(),
        );

        // Basic parquet info
        let p1 = ParquetFileParams {
            sequencer_id: sequencer.id,
            namespace_id: namespace.id,
            table_id: table.id,
            partition_id: partition1.id,
            object_store_id: Uuid::new_v4(),
            min_sequence_number: SequenceNumber::new(4),
            max_sequence_number: SequenceNumber::new(100),
            min_time: Timestamp::new(1),
            max_time: Timestamp::new(5),
            file_size_bytes: 1337,
            row_count: 0,
            compaction_level: CompactionLevel::Initial, // level of file of new writes
            created_at: time_now,
            column_set: ColumnSet::new([ColumnId::new(1), ColumnId::new(2)]),
        };

        // Note: The order of the test cases below is important and should not be changed
        // becasue they depend on the  order of the writes and their content. For example,
        // in order to test `Case 3`, we do not need to add asserts for `Case 1` and `Case 2`,
        // but all the writes, deletes and updates in Cases 1 and 2 are a must for testing Case 3.
        // In order words, the last Case needs all content of previous tests.
        // This shows the priority of selecting compaction candidates

        // --------------------------------------
        // Case 1: no files yet --> no partition candidates
        //
        let candidates = compactor.partitions_to_compact(1, 1).await.unwrap();
        assert!(candidates.is_empty());

        // --------------------------------------
        // Case 2: no non-deleleted L0 files -->  no partition candidates
        //
        // partition1 has a deleted L0
        let mut txn = catalog.catalog.start_transaction().await.unwrap();
        let pf1 = txn.parquet_files().create(p1.clone()).await.unwrap();
        txn.parquet_files().flag_for_delete(pf1.id).await.unwrap();
        //
        // partition2 has a non-L0 file
        let p2 = ParquetFileParams {
            object_store_id: Uuid::new_v4(),
            partition_id: partition2.id,
            ..p1.clone()
        };
        let pf2 = txn.parquet_files().create(p2).await.unwrap();
        txn.parquet_files()
            .update_to_level_1(&[pf2.id])
            .await
            .unwrap();
        txn.commit().await.unwrap();
        // No non-deleted level 0 files yet --> no candidates
        let candidates = compactor.partitions_to_compact(1, 1).await.unwrap();
        assert!(candidates.is_empty());

        // --------------------------------------
        // Case 3: no new recent writes (within the last 24 hours) --> return candidates with the most L0
        //
        // partition2 has an old (more 24 hours ago) non-deleted level 0 file
        let mut txn = catalog.catalog.start_transaction().await.unwrap();
        let p3 = ParquetFileParams {
            object_store_id: Uuid::new_v4(),
            partition_id: partition2.id,
            created_at: time_38_hour_ago,
            ..p1.clone()
        };
        let _pf3 = txn.parquet_files().create(p3).await.unwrap();
        txn.commit().await.unwrap();
        //
        // Has at least one partition with a L0 file --> make it a candidate
        let candidates = compactor.partitions_to_compact(1, 1).await.unwrap();
        assert_eq!(candidates.len(), 1);
        assert_eq!(candidates[0].partition_id, partition2.id);

        // --------------------------------------
        // Case 4: has one partition with recent writes (5 hours ago) --> return that partition
        //
        // partition4 has a new write 5 hours ago
        let mut txn = catalog.catalog.start_transaction().await.unwrap();
        let p4 = ParquetFileParams {
            object_store_id: Uuid::new_v4(),
            partition_id: partition4.id,
            created_at: time_five_hour_ago,
            ..p1.clone()
        };
        let _pf4 = txn.parquet_files().create(p4).await.unwrap();
        txn.commit().await.unwrap();
        //
        // Has at least one partition with a recent write --> make it a candidate
        let candidates = compactor.partitions_to_compact(1, 1).await.unwrap();
        assert_eq!(candidates.len(), 1);
        assert_eq!(candidates[0].partition_id, partition4.id);

        // --------------------------------------
        // Case 5: has 2 partitions with 2 different groups of recent writes:
        //  1. Within the last 4 hours
        //  2. Within the last 24 hours but older than 4 hours ago
        // When we have group 1, we will ignore partitions in group 2
        //
        // partition3 has a new write 3 hours ago
        let mut txn = catalog.catalog.start_transaction().await.unwrap();
        let p5 = ParquetFileParams {
            object_store_id: Uuid::new_v4(),
            partition_id: partition3.id,
            created_at: time_three_hour_ago,
            ..p1.clone()
        };
        let _pf5 = txn.parquet_files().create(p5).await.unwrap();
        txn.commit().await.unwrap();
        //
        // make partitions in the most recent group candidates
        let candidates = compactor.partitions_to_compact(1, 1).await.unwrap();
        assert_eq!(candidates.len(), 1);
        assert_eq!(candidates[0].partition_id, partition3.id);

        // --------------------------------------
        // Case 6: has partittion candidates for 2 sequecers
        //
        // The another_sequencer now has non-deleted level-0 file ingested 38 hours ago
        let mut txn = catalog.catalog.start_transaction().await.unwrap();
        let p6 = ParquetFileParams {
            object_store_id: Uuid::new_v4(),
            sequencer_id: another_sequencer.id,
            table_id: another_table.id,
            partition_id: another_partition.id,
            created_at: time_38_hour_ago,
            ..p1.clone()
        };
        let _pf6 = txn.parquet_files().create(p6).await.unwrap();
        txn.commit().await.unwrap();
        //
        // Will have 2 candidates, one for each sequencer
        let mut candidates = compactor.partitions_to_compact(1, 1).await.unwrap();
        candidates.sort();
        assert_eq!(candidates.len(), 2);
        assert_eq!(candidates[0].partition_id, partition3.id);
        assert_eq!(candidates[0].sequencer_id, sequencer.id);
        assert_eq!(candidates[1].partition_id, another_partition.id);
        assert_eq!(candidates[1].sequencer_id, another_sequencer.id);
    }

    #[tokio::test]
    async fn test_compact_two_big_files() {
        test_helpers::maybe_start_logging();
        let catalog = TestCatalog::new();

        let lp1 = (1..1000) // total 999 rows
            .into_iter()
            .map(|i| format!("table,tag1=foo ifield=1i {}", i))
            .collect::<Vec<_>>()
            .join("\n");

        let lp2 = (500..1500) // total 1000 rows
            .into_iter()
            .map(|i| format!("table,tag1=foo ifield=1i {}", i))
            .collect::<Vec<_>>()
            .join("\n");

        let ns = catalog.create_namespace("ns").await;
        let sequencer = ns.create_sequencer(1).await;
        let table = ns.create_table("table").await;
        table.create_column("tag1", ColumnType::Tag).await;
        table.create_column("ifield", ColumnType::I64).await;
        table.create_column("time", ColumnType::Time).await;
        let table_schema = table.catalog_schema().await;
        let partition = table
            .with_sequencer(&sequencer)
            .create_partition("part")
            .await;

        // total file size = 60000 + 60000 = 120000
        let builder = TestParquetFileBuilder::default()
            .with_line_protocol(&lp1)
            .with_min_seq(1)
            .with_max_seq(1)
            .with_min_time(1)
            .with_max_time(1_000)
            .with_file_size_bytes(60_000);
        let parquet_file1 = partition.create_parquet_file(builder).await.parquet_file;

        let builder = TestParquetFileBuilder::default()
            .with_line_protocol(&lp2)
            .with_min_seq(10)
            .with_max_seq(15)
            .with_min_time(500)
            .with_max_time(1_500)
            .with_file_size_bytes(60_000);
        let parquet_file2 = partition.create_parquet_file(builder).await.parquet_file;

        let config = make_compactor_config();
        let compactor = Compactor::new(
            vec![sequencer.sequencer.id],
            Arc::clone(&catalog.catalog),
            ParquetStorage::new(Arc::clone(&catalog.object_store)),
            Arc::new(Executor::new(1)),
            Arc::new(SystemProvider::new()),
            BackoffConfig::default(),
            config,
            Arc::new(metric::Registry::new()),
        );

        let pf1 = ParquetFileWithTombstone::new(Arc::new(parquet_file1), vec![]);
        // File 2 without tombstones
        let pf2 = ParquetFileWithTombstone::new(Arc::new(parquet_file2), vec![]);

        // Compact them
        let sort_key = SortKey::from_columns(["tag1", "time"]);
        let partition = partition.update_sort_key(sort_key).await;

        let batches = compactor
            .compact(
                vec![pf1, pf2],
                &ns.namespace,
                &table.table,
                &table_schema,
                &partition.partition,
            )
            .await
            .unwrap();

        assert_eq!(batches.len(), 1);

        let batches = batches
            .into_iter()
            .map(|v| async {
                datafusion::physical_plan::common::collect(v.data)
                    .await
                    .expect("failed to collect record batches")
            })
            .collect::<FuturesOrdered<_>>()
            .collect::<Vec<_>>()
            .await;

        // Verify number of output rows
        // There should be total 1499 output rows (999 from lp1 + 100 from lp2 - 500 duplicates)
        let num_rows: usize = batches[0].iter().map(|rb| rb.num_rows()).sum();
        assert_eq!(num_rows, 1499);
    }

    async fn read_parquet_file(table: &Arc<TestTable>, file: ParquetFile) -> Vec<RecordBatch> {
        let storage = ParquetStorage::new(table.catalog.object_store());

        // get schema
        let table_catalog_schema = table.catalog_schema().await;
        let column_id_lookup = table_catalog_schema.column_id_map();
        let table_schema = table.schema().await;
        let selection: Vec<_> = file
            .column_set
            .iter()
            .map(|id| *column_id_lookup.get(id).unwrap())
            .collect();
        let schema = table_schema.select_by_names(&selection).unwrap();

        let path: ParquetFilePath = (&file).into();
        let rx = storage.read_all(schema.as_arrow(), &path).unwrap();
        datafusion::physical_plan::common::collect(rx)
            .await
            .unwrap()
    }

    fn make_compactor_config() -> CompactorConfig {
        let max_desired_file_size_bytes = 10_000;
        let percentage_max_file_size = 30;
        let split_percentage = 80;
        let max_concurrent_size_bytes = 100_000;
        let max_number_partitions_per_sequencer = 1;
        let min_number_recent_ingested_per_partition = 1;
        let input_size_threshold_bytes = 300 * 1024 * 1024;
        let input_file_count_threshold = 100;
        CompactorConfig::new(
            max_desired_file_size_bytes,
            percentage_max_file_size,
            split_percentage,
            max_concurrent_size_bytes,
            max_number_partitions_per_sequencer,
            min_number_recent_ingested_per_partition,
            input_size_threshold_bytes,
            input_file_count_threshold,
        )
    }
}
