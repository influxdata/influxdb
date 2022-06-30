//! Data Points for the lifecycle of the Compactor

use crate::{
    handler::CompactorConfig,
    query::QueryableParquetChunk,
    utils::{
        CatalogUpdate, CompactedData, GroupWithMinTimeAndSize, GroupWithTombstones,
        ParquetFileWithTombstone,
    },
};
use backoff::BackoffConfig;
use data_types::{
    Namespace, NamespaceId, ParquetFile, ParquetFileId, Partition, PartitionId, SequencerId, Table,
    TableId, TableSchema, Timestamp, Tombstone, TombstoneId, FILE_NON_OVERLAPPED_COMAPCTION_LEVEL,
    INITIAL_COMPACTION_LEVEL,
};
use datafusion::error::DataFusionError;
use futures::stream::{FuturesUnordered, TryStreamExt};
use iox_catalog::interface::{get_schema_by_id, Catalog, Transaction};
use iox_query::{
    exec::{Executor, ExecutorType},
    frontend::reorg::ReorgPlanner,
    provider::overlap::group_potential_duplicates,
    QueryChunk,
};
use iox_time::TimeProvider;
use metric::{Attributes, DurationHistogram, Metric, U64Counter, U64Gauge};
use observability_deps::tracing::{debug, info, trace, warn};
use parquet_file::{metadata::IoxMetadata, storage::ParquetStorage};
use snafu::{ensure, OptionExt, ResultExt, Snafu};
use std::{
    cmp::{max, min, Ordering},
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

/// Data points need to run a compactor
#[derive(Debug)]
pub struct Compactor {
    /// Sequencers assigned to this compactor
    sequencers: Vec<SequencerId>,
    /// Object store for reading and persistence of parquet files
    store: ParquetStorage,
    /// The global catalog for schema, parquet files and tombstones
    catalog: Arc<dyn Catalog>,

    /// Executor for running queries and compacting and persisting
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

    /// Gauge for the number of bytes of level-0 files across compaction partition candidates
    compaction_candidate_bytes_gauge: Metric<U64Gauge>,

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
        let compaction_candidate_bytes_gauge = registry.register_metric(
            "compactor_candidate_bytes",
            "gauge for the total bytes for compaction candidates",
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
            // compaction_actual_counter,
            compaction_output_counter,
            level_promotion_counter,
            compaction_candidate_gauge,
            compaction_candidate_bytes_gauge,
            compaction_duration,
        }
    }

    async fn level_0_parquet_files(&self, sequencer_id: SequencerId) -> Result<Vec<ParquetFile>> {
        let mut repos = self.catalog.repositories().await;

        repos
            .parquet_files()
            .level_0(sequencer_id)
            .await
            .context(Level0Snafu)
    }

    async fn update_to_level_2(&self, parquet_file_ids: &[ParquetFileId]) -> Result<()> {
        let mut repos = self.catalog.repositories().await;

        let updated = repos
            .parquet_files()
            .update_to_level_2(parquet_file_ids)
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

    /// Returns a list of partitions that have level 0 files to compact along with some summary
    /// statistics that the scheduler can use to decide which partitions to prioritize. Orders
    /// them by the number of level 0 files and then size.
    pub async fn partitions_to_compact(&self) -> Result<Vec<PartitionCompactionCandidate>> {
        let mut candidates = vec![];

        for sequencer_id in &self.sequencers {
            // Read level-0 parquet files
            let level_0_files = self.level_0_parquet_files(*sequencer_id).await?;

            let mut partitions = BTreeMap::new();
            for f in level_0_files {
                let mut p = partitions.entry(f.partition_id).or_insert_with(|| {
                    PartitionCompactionCandidate {
                        sequencer_id: *sequencer_id,
                        namespace_id: f.namespace_id,
                        table_id: f.table_id,
                        partition_id: f.partition_id,
                        level_0_file_count: 0,
                        file_size_bytes: 0,
                        oldest_file: f.created_at,
                    }
                });
                p.file_size_bytes += f.file_size_bytes;
                p.level_0_file_count += 1;
                p.oldest_file = p.oldest_file.min(f.created_at);
            }

            let mut partitions: Vec<_> = partitions.into_values().collect();

            let total_size = partitions.iter().fold(0, |t, p| t + p.file_size_bytes);
            let attributes =
                Attributes::from([("sequencer_id", format!("{}", *sequencer_id).into())]);

            let number_gauge = self.compaction_candidate_gauge.recorder(attributes.clone());
            number_gauge.set(partitions.len() as u64);
            let size_gauge = self.compaction_candidate_bytes_gauge.recorder(attributes);
            size_gauge.set(total_size as u64);

            candidates.append(&mut partitions);
        }

        candidates.sort_by(
            |a, b| match b.level_0_file_count.cmp(&a.level_0_file_count) {
                Ordering::Equal => b.file_size_bytes.cmp(&a.file_size_bytes),
                o => o,
            },
        );

        debug!(
            candidate_num = candidates.len(),
            "Number of candidate partitions to be considered to compact"
        );

        Ok(candidates)
    }

    /// Add namespace and table information to partition candidates.
    pub async fn add_info_to_partitions(
        &self,
        partitions: &[PartitionCompactionCandidate],
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
                    candidate: p.clone(),
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

    /// Group files to be compacted together and level-0 files that will get upgraded
    /// for a given partition.
    /// The number of compacting files per group will be limited by thier total size and number of files
    pub async fn groups_to_compact_and_files_to_upgrade(
        &self,
        partition_id: PartitionId,
        compaction_max_size_bytes: i64, // max size of files to get compacted
        compaction_max_file_count: i64, // max number of files to get compacted
    ) -> Result<CompactAndUpgrade> {
        let mut compact_and_upgrade = CompactAndUpgrade::new(None);

        // List all valid (not soft deletded) files of the partition
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

        // Group overlapped files
        // Each group will be limited by their size and number of files
        let overlapped_file_groups = Self::overlapped_groups(
            parquet_files,
            compaction_max_size_bytes,
            compaction_max_file_count,
        );

        // Group time-contiguous non-overlapped groups if their total size is smaller than a threshold
        // If their
        let compact_file_groups = Self::group_small_contiguous_groups(
            overlapped_file_groups,
            compaction_max_size_bytes,
            compaction_max_file_count,
        );

        // Attach appropriate tombstones to each file
        let groups_with_tombstones = self.add_tombstones_to_groups(compact_file_groups).await?;
        info!("compacting {} groups", groups_with_tombstones.len());

        // File groups to compact and files to upgrade
        for group in groups_with_tombstones {
            // Only one file without tombstones, no need to compact.
            if group.parquet_files.len() == 1 && group.tombstones.is_empty() {
                // If it is level 0, upgrade it since it is non-overlapping
                if group.parquet_files[0].compaction_level == INITIAL_COMPACTION_LEVEL {
                    compact_and_upgrade
                        .files_to_upgrade
                        .push(group.parquet_files[0].parquet_file_id())
                }
            } else {
                compact_and_upgrade.groups_to_compact.push(group);
            }
        }

        Ok(compact_and_upgrade)
    }

    /// Runs compaction in a partition resolving any tombstones and compacting data so that parquet
    /// files will be non-overlapping in time.
    /// If the compacted result is too large, it will be plit into many non-overlapped files, each is
    /// likely smaller than max_desired_file_size. There is still possibility the file sizes are larger
    /// than the desired becasue the process to split the result is estimated using percentage size based
    /// on the input time range
    pub async fn compact_partition(
        &self,
        namespace: &Namespace,
        table: &Table,
        table_schema: &TableSchema,
        partition_id: PartitionId,
        compact_and_upgrade: CompactAndUpgrade,
        max_desired_file_size: i64,
    ) -> Result<()> {
        if !compact_and_upgrade.compactable() {
            return Ok(());
        }

        info!("compacting partition {}", partition_id);
        let start_time = self.time_provider.now();

        let partition = self.get_partition_from_catalog(partition_id).await?;

        let mut file_count = 0;

        // Compact, persist,and update catalog accordingly for each overlapped file
        let mut tombstones = BTreeMap::new();
        let mut output_file_count = 0;
        let sequencer_id = compact_and_upgrade
            .sequencer_id()
            .expect("Should have sequencer ID");
        for group in compact_and_upgrade.groups_to_compact {
            file_count += group.parquet_files.len();

            // keep tombstone ids
            tombstones = Self::union_tombstones(tombstones, &group);

            // Collect all the parquet file IDs, to be able to set their catalog records to be
            // deleted. These should already be unique, no need to dedupe.
            let original_parquet_file_ids: Vec<_> =
                group.parquet_files.iter().map(|f| f.id).collect();
            let size: i64 = group.parquet_files.iter().map(|f| f.file_size_bytes).sum();
            info!(num_files=%group.parquet_files.len(), ?size, ?original_parquet_file_ids, "compacting group of files");

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
                    max_desired_file_size,
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
        self.update_to_level_2(&compact_and_upgrade.files_to_upgrade)
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

        let compaction_counter = self.compaction_counter.recorder(attributes.clone());
        compaction_counter.inc(file_count as u64);

        let compaction_output_counter = self.compaction_output_counter.recorder(attributes);
        compaction_output_counter.inc(output_file_count as u64);

        Ok(())
    }

    // Group time-contiguous non-overlapped groups if their total size is smaller than a threshold
    fn group_small_contiguous_groups(
        mut file_groups: Vec<GroupWithMinTimeAndSize>,
        compaction_max_size_bytes: i64,
        compaction_max_file_count: i64,
    ) -> Vec<Vec<ParquetFile>> {
        let mut groups = Vec::with_capacity(file_groups.len());
        if file_groups.is_empty() {
            return groups;
        }

        // Sort the groups by their min_time
        file_groups.sort_by_key(|a| a.min_time);

        let mut current_group = vec![];
        let mut current_size = 0;
        let mut current_num_files = 0;
        for g in file_groups {
            if !g.overlapped_with_other_groups // only group non-overlapped ones
                && current_size + g.total_file_size_bytes < compaction_max_size_bytes
                && current_num_files + g.parquet_files.len()
                    < compaction_max_file_count.try_into().unwrap()
            {
                // Group this one with the current_group
                current_num_files += g.parquet_files.len();
                current_group.extend(g.parquet_files);
                current_size += g.total_file_size_bytes;
            } else {
                // Current group  cannot combine with it next one
                if !current_group.is_empty() {
                    groups.push(current_group);
                }
                current_num_files = g.parquet_files.len();
                current_group = g.parquet_files;
                current_size = g.total_file_size_bytes;
            }
        }

        // push the last one
        groups.push(current_group);

        groups
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

    // Compact given files. The given files are either overlapped or contiguous in time.
    // The output will include 2 CompactedData sets, one contains a large amount of data of
    // least recent time and the other has a small amount of data of most recent time. Each
    // will be persisted in its own file. The idea is when new writes come, they will
    // mostly overlap with the most recent data only.
    async fn compact(
        &self,
        overlapped_files: Vec<ParquetFileWithTombstone>,
        namespace: &Namespace,
        table: &Table,
        table_schema: &TableSchema,
        partition: &Partition,
        max_desired_file_size: i64,
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

        // Total size of all files
        let total_size = overlapped_files
            .iter()
            .map(|f| f.file_size_bytes)
            .sum::<i64>();

        //  Collect all unique tombstone
        let mut tombstone_map = overlapped_files[0].tombstone_map();

        // Verify if the given files belong to the same partition and collect their tombstones
        //  One tombstone might be relevant to multiple parquet files in this set, so dedupe here.
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

        // Convert the input files into QueryableParquetChunk for making query
        // plan
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

        // Identify split time
        let split_times =
            Self::compute_split_time(min_time, max_time, total_size, max_desired_file_size);

        // Build compact logical plan
        let plan = {
            // split data to compact data into 2 files
            if split_times.len() == 1 && split_times[0] == max_time {
                // compact everything into one file
                ReorgPlanner::new()
                    .compact_plan(Arc::clone(&merged_schema), query_chunks, sort_key.clone())
                    .context(CompactLogicalPlanSnafu)?
            } else {
                // split compact query plan
                ReorgPlanner::new()
                    .split_plan(
                        Arc::clone(&merged_schema),
                        query_chunks,
                        sort_key.clone(),
                        split_times,
                    )
                    .context(CompactLogicalPlanSnafu)?
            }
        };

        let ctx = self.exec.new_context(ExecutorType::Reorg);
        let physical_plan = ctx
            .create_physical_plan(&plan)
            .await
            .context(CompactPhysicalPlanSnafu)?;

        // Run to collect each stream of the plan
        let stream_count = physical_plan.output_partitioning().partition_count();
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
                // TODO before merging: this can either level-1 or level-2
                compaction_level: FILE_NON_OVERLAPPED_COMAPCTION_LEVEL,
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
                // Becasue data may get removed and split during compaction, a few new files
                // may no longer overlap with the delete tombstones. Need to verify whether
                // they are overlap before adding process tombstones
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

    // Split overlapped groups into smaller groups if there are so mnay files in each group or
    // their total size is too large.
    //
    // Invariants
    //  1. Input `groups`, each contains overlapped files but the groups do not overlap with each other.
    //     As a result, a group can include files whose total size is larger than our maximum allocated memory.
    //  2. Output will be in OverlappedGroups type that include 2 different groups of files:
    //      . internal_overlapped_groups: groups of the input that are neither contains too many files
    //        nor their total file size is too large. Each of these groups do not overlap with any other groups.
    //      . external_overlapped_groups: subgroups of splitting too-large-or-too-many-file input groups.
    //        Each of these subgroups will overlap with at least one of the other subgroups. However, to correclty
    //        deduplicate data, each subgroup only include contiguous sequence numbers.
    //
    // Example:
    // The input `groups` consists of 4 groups of 9 chunks with time ranges as below.
    // The chunks are named using their sequence number. `C4` means this chunk inlcudes
    // data of sequence numner 4 (Note: a chunk can include data of a range of sequence numbers
    // but to simplify the example, we make chunk contain only one sequence number)
    //   . G1: includes one chunk C4
    //   . G2: inlcudes 4 overlapped C1, C2, C3, C6
    //   . G3: inlcudes 3 overlapped C5, C6, C9
    //   . G4: inlcudes C7
    //                                                                                 Time
    //  ────────────────────────────────────────────────────────────────────────────────────▶
    //
    // ┌───────┐     ┌───────────┐ ┌───────┐       ┌────────┐                 ┌──────────┐
    // │  C4   │     │    C1     │ │  C2   │       │   C5   │                 │    C7    │
    // └───────┘     └───────────┘ └───────┘       └────────┘                 └──────────┘
    //                         ┌───────┐                  ┌────────────┐
    //                         │  C3   │                  │     C8     │
    //                         └───────┘                  └────────────┘
    //                    ┌─────────────┐             ┌───────────┐
    //                    │     C6      │             │    C9     │
    //                    └─────────────┘             └───────────┘
    //
    //  ─ ─ ─ ─ ─     ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─       ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─        ─ ─ ─ ─ ─ ─
    //   G1[C4]         G2[C1, C2, C3, C6]           G3[C5, C8, C9]              G4[C7]
    //
    //
    // Since total size of files in G2 and G3 is over the limit `max_size_bytes`, they are split further,
    // each into 2 groups. The output will inlcude 6 groups as follows:
    //                                                                                 Time
    //  ────────────────────────────────────────────────────────────────────────────────────▶
    //
    // ┌───────┐     ┌───────────┐ ┌───────┐       ┌────────┐                 ┌──────────┐
    // │  C4   │     │    C1     │ │  C2   │       │   C5   │                 │    C7    │
    // └───────┘     └───────────┘ └───────┘       └────────┘                 └──────────┘
    //                         ┌───────┐                  ┌────────────┐
    //                         │  C3   │                  │     C8     │
    //                         └───────┘                  └────────────┘
    //                    ┌─────────────┐             ┌───────────┐
    //                    │     C6      │             │    C9     │
    //                    └─────────────┘             └───────────┘
    //
    //  ─ ─ ─ ─ ─     ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─       ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─        ─ ─ ─ ─ ─ ─
    //   G1[C4]            G2[C1, C2]*                G4[C5, C8]*                G6[C7]
    //                     G3[C3, C6]*                  G5[C9]*
    //
    // The newly split group are G2, G3, G4 and G5 and marked with a star (*) at the end.
    // Before splitting, the chunks are sorted by their sequence numbers to guarantee their data
    // will be deduplicated correctly in this and future compaction cycles. As a consequence,
    // new group may inlcude non-overlapped chunks, e.g. G2. But this is not an issue becasue:
    //   . The compaction plan will discover whether the chunks overlap or not to avoid the
    //     actual deduplication work
    //   . We do want to compact/concat small continuous non-overlapped chunks (in later steps) so
    //     this can be considered as a part of that optimization
    fn split_overlapped_groups(
        groups: &mut Vec<Vec<ParquetFile>>,
        max_size_bytes: i64,
        max_file_count: i64,
    ) -> OverlappedGroups {
        let mut overlapped_groups = OverlappedGroups::new(groups.len() * 2);
        let max_count = max_file_count.try_into().unwrap();
        for group in groups {
            let total_size_bytes: i64 = group.iter().map(|f| f.file_size_bytes).sum();
            if group.len() == 1 || (group.len() <= max_count && total_size_bytes <= max_size_bytes)
            {
                overlapped_groups.add_internal_overalapped_group(group.to_vec());
            } else {
                // Sort overlapped files on their min sequence number to ensure their split subgroups
                // contain contiguous sequnce numbers
                group.sort_by_key(|f| f.min_sequence_number);

                // Verify that the sorted ranges of [min_sequence_number, max_sequence_number] do not overlap
                for i in 1..group.len() {
                    if group[i - 1].max_sequence_number >= group[i].min_sequence_number {
                        panic!(
                            "Two files with overlapped range sequence numbers. \
                            File id 1: {}, file id 2: {}, sequence number range 1: [{}, {}], \
                            sequence number range 2: [{}, {}], partition id: {}",
                            group[i - 1].id,
                            group[i].id,
                            group[i - 1].min_sequence_number.get(),
                            group[i - 1].max_sequence_number.get(),
                            group[i].min_sequence_number.get(),
                            group[i].max_sequence_number.get(),
                            group[i].partition_id,
                        )
                    }
                }

                while !group.is_empty() {
                    // limit file num
                    let mut count = max_count;

                    // limit total file size
                    let mut size = 0;
                    for (i, item) in group.iter().enumerate() {
                        if i >= max_count {
                            count = max_count;
                            break;
                        }

                        size += item.file_size_bytes;
                        if size >= max_size_bytes {
                            count = i + 1;
                            break;
                        }
                    }

                    if count > group.len() {
                        count = group.len();
                    }
                    let group_new = group.split_off(count);
                    overlapped_groups.add_external_overalapped_group(group.to_vec());
                    *group = group_new;
                }
            }
        }

        overlapped_groups
    }

    // Given a list of parquet files that come from the same Table Partition, group files together
    // if their (min_time, max_time) ranges overlap. Does not preserve or guarantee any ordering.
    // If there are so many files in an overlapped group, the group will be split to ensure each
    // group contains limited number of files
    fn overlapped_groups(
        parquet_files: Vec<ParquetFile>,
        max_size_bytes: i64,
        max_file_count: i64,
    ) -> Vec<GroupWithMinTimeAndSize> {
        // group overlap files
        let mut overlapped_groups =
            group_potential_duplicates(parquet_files).expect("Error grouping overlapped chunks");

        // split overlapped groups into smaller groups if they include so many files
        let overlapped_groups =
            Self::split_overlapped_groups(&mut overlapped_groups, max_size_bytes, max_file_count);

        overlapped_groups.groups_with_min_time_and_size()
    }

    // Compute time to split data
    // Return a list of times at which we want data to be split. The times are computed
    // based on the max_desired_file_size each file should not exceed and the total_size this input
    // time range [min_time, max_time] contains.
    // The split times assume that the data is evenly distributed in the time range and if
    // that is not the case the resulting files are not guaranteed to be below max_desired_file_size
    // Hence, the range between two contiguous returned time is pecentage of
    // max_desired_file_size/total_size of the time range
    // Example:
    //  . Input
    //      min_time = 1
    //      max_time = 21
    //      total_size = 100
    //      max_desired_file_size = 30
    //
    //  . Pecentage = 70/100 = 0.3
    //  . Time range between 2 times = (21 - 1) * 0.3 = 6
    //
    //  . Output = [7, 13, 19] in which
    //     7 = 1 (min_time) + 6 (time range)
    //     13 = 7 (previous time) + 6 (time range)
    //     19 = 13 (previous time) + 6 (time range)
    fn compute_split_time(
        min_time: i64,
        max_time: i64,
        total_size: i64,
        max_desired_file_size: i64,
    ) -> Vec<i64> {
        // Too small to split
        if total_size <= max_desired_file_size {
            return vec![max_time];
        }

        let mut split_times = vec![];
        let percentage = max_desired_file_size as f64 / total_size as f64;
        let mut min = min_time;
        loop {
            let split_time = min + ((max_time - min_time) as f64 * percentage).floor() as i64;
            if split_time < max_time {
                split_times.push(split_time);
                min = split_time;
            } else {
                break;
            }
        }

        split_times
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

        // Get number of non-deleted parquet files of the same tableId & sequencerId that overlap with the tombstone time range
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
                    "Error getting parquet file count for table ID {}, sequencer ID {}, min time {:?}, max time {:?}.
                    Won't be able to verify whether its tombstone is fully processed",
                    tombstone.table_id, tombstone.sequencer_id, tombstone.min_time, tombstone.max_time
                );
                return false;
            }
        };

        // Get number of the processed parquet file for this tombstones
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

    async fn add_tombstones_to_groups(
        &self,
        groups: Vec<Vec<ParquetFile>>,
    ) -> Result<Vec<GroupWithTombstones>> {
        let mut repo = self.catalog.repositories().await;
        let tombstone_repo = repo.tombstones();

        let mut overlapped_file_with_tombstones_groups = Vec::with_capacity(groups.len());

        // For each group of overlapping parquet files,
        for parquet_files in groups {
            // Skip over any empty groups
            if parquet_files.is_empty() {
                continue;
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

            overlapped_file_with_tombstones_groups.push(GroupWithTombstones {
                parquet_files,
                tombstones,
            });
        }

        Ok(overlapped_file_with_tombstones_groups)
    }
}

#[derive(Debug, Clone, PartialEq)]
struct OverlappedGroups {
    // Groups that contain overlapped files but the groups do not overlapp
    // with other groups
    internal_overlapped_groups: Vec<Vec<ParquetFile>>,

    // Groups that overlap with at least one of other groups
    external_overlapped_groups: Vec<Vec<ParquetFile>>,
}

impl OverlappedGroups {
    pub fn new(len: usize) -> Self {
        Self {
            internal_overlapped_groups: Vec::with_capacity(len),
            external_overlapped_groups: Vec::with_capacity(len),
        }
    }

    pub fn add_internal_overalapped_group(&mut self, group: Vec<ParquetFile>) {
        self.internal_overlapped_groups.push(group);
    }

    pub fn add_external_overalapped_group(&mut self, group: Vec<ParquetFile>) {
        self.external_overlapped_groups.push(group);
    }

    pub fn groups_with_min_time_and_size(self) -> Vec<GroupWithMinTimeAndSize> {
        let mut groups = Vec::with_capacity(
            self.internal_overlapped_groups.len() + self.external_overlapped_groups.len(),
        );

        for group in self.internal_overlapped_groups {
            groups.push(GroupWithMinTimeAndSize::new(group, false));
        }
        for group in self.external_overlapped_groups {
            groups.push(GroupWithMinTimeAndSize::new(group, true));
        }

        groups
    }
}

/// Summary information for a partition that is a candidate for compaction.
#[derive(Debug, Clone, Eq, PartialEq)]
#[allow(missing_copy_implementations)] // shouldn't be silently cloned
pub struct PartitionCompactionCandidate {
    /// the sequencer the partition is in
    pub sequencer_id: SequencerId,
    /// the table the partition is in
    pub table_id: TableId,
    /// the partition for compaction
    pub partition_id: PartitionId,
    /// namespace ID
    pub namespace_id: NamespaceId,
    /// the number of level 0 files in the partition
    pub level_0_file_count: usize,
    /// the total bytes of the level 0 files to compact
    pub file_size_bytes: i64,
    /// the created_at time of the oldest level 0 file in the partition
    pub oldest_file: Timestamp,
}

/// [`PartitionCompactionCandidate`] with some information about its table and namespace.
#[derive(Debug)]
pub struct PartitionCompactionCandidateWithInfo {
    /// Partition compaction candidate.
    pub candidate: PartitionCompactionCandidate,

    /// Namespace.
    pub namespace: Arc<Namespace>,

    /// Table.
    pub table: Arc<Table>,

    /// Table schema
    pub table_schema: Arc<TableSchema>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::record_batch::RecordBatch;
    use arrow_util::assert_batches_sorted_eq;
    use data_types::{
        ColumnId, ColumnSet, ColumnType, KafkaPartition, NamespaceId, ParquetFileParams,
        SequenceNumber,
    };
    use futures::{stream::FuturesOrdered, StreamExt, TryStreamExt};
    use iox_tests::util::{TestCatalog, TestTable};
    use iox_time::SystemProvider;
    use parquet_file::ParquetFilePath;
    use schema::{selection::Selection, sort::SortKey};
    use std::sync::atomic::{AtomicI64, Ordering};
    use test_helpers::maybe_start_logging;

    // Simulate unique ID generation
    static NEXT_ID: AtomicI64 = AtomicI64::new(0);
    static TEST_MAX_SIZE_BYTES: i64 = 100000;
    static TEST_MAX_FILE_COUNT: i64 = 10;

    #[tokio::test]
    async fn test_compute_split_time() {
        let min_time = 1;
        let max_time = 11;
        let total_size = 100;
        let max_desired_file_size = 100;

        // no split
        let result =
            Compactor::compute_split_time(min_time, max_time, total_size, max_desired_file_size);
        assert_eq!(result.len(), 1);
        assert_eq!(result[0], max_time);

        // split 70% and 30%
        let max_desired_file_size = 70;
        let result =
            Compactor::compute_split_time(min_time, max_time, total_size, max_desired_file_size);
        // only need to store the last split time
        assert_eq!(result.len(), 1);
        assert_eq!(result[0], 8); // = 1 (min_time) + 7

        // split 40%, 40%, 20%
        let max_desired_file_size = 40;
        let result =
            Compactor::compute_split_time(min_time, max_time, total_size, max_desired_file_size);
        // store first and second split time
        assert_eq!(result.len(), 2);
        assert_eq!(result[0], 5); // = 1 (min_time) + 4
        assert_eq!(result[1], 9); // = 5 (previous split_time) + 4
    }

    #[tokio::test]
    // This is integration test to verify all pieces are put together correctly
    async fn test_compact_partition() {
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

        // One parquet file
        let partition = table
            .with_sequencer(&sequencer)
            .create_partition("part")
            .await;
        partition
            .create_parquet_file_with_min_max_size_and_creation_time(
                &lp,
                1,
                1,
                8000,
                20000,
                120000, // file size > compaction_max_size_bytes to have it split into 2 files
                catalog.time_provider.now().timestamp_nanos(),
            )
            .await;
        // should have 1 level-0 file
        let count = catalog.count_level_0_files(sequencer.sequencer.id).await;
        assert_eq!(count, 1);

        // One overlapped tombstone
        let tombstone = table
            .with_sequencer(&sequencer)
            .create_tombstone(20, 6000, 12000, "tag1=VT")
            .await;
        // Should have 1 tomstone
        let count = catalog.count_tombstones_for_table(table.table.id).await;
        assert_eq!(count, 1);

        // ------------------------------------------------
        // Compact
        let split_percentage = 90;
        let max_concurrent_compaction_size_bytes = 100000;
        let compaction_max_size_bytes = 100000;
        let compaction_max_file_count = 10;
        let compactor = Compactor::new(
            vec![sequencer.sequencer.id],
            Arc::clone(&catalog.catalog),
            ParquetStorage::new(Arc::clone(&catalog.object_store)),
            Arc::new(Executor::new(1)),
            Arc::new(SystemProvider::new()),
            BackoffConfig::default(),
            CompactorConfig::new(
                split_percentage,
                max_concurrent_compaction_size_bytes,
                compaction_max_size_bytes,
                compaction_max_file_count,
            ),
            Arc::new(metric::Registry::new()),
        );

        let compact_and_upgrade = compactor
            .groups_to_compact_and_files_to_upgrade(
                partition.partition.id,
                compactor.config.compaction_max_size_bytes(),
                compactor.config.compaction_max_file_count(),
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
                compaction_max_size_bytes,
            )
            .await
            .unwrap();

        // should have 2 non-deleted level_0 files. The original file was marked deleted and not counted
        let mut files = catalog.list_by_table_not_to_delete(table.table.id).await;
        assert_eq!(files.len(), 2);
        // 2 newly created level-1 files as the result of compaction
        assert_eq!(
            (files[0].id.get(), files[0].compaction_level),
            (2, FILE_NON_OVERLAPPED_COMAPCTION_LEVEL)
        );
        assert_eq!(
            (files[1].id.get(), files[1].compaction_level),
            (3, FILE_NON_OVERLAPPED_COMAPCTION_LEVEL)
        );

        // processed tombstones created and deleted inside compact_partition function
        let count = catalog
            .count_processed_tombstones(tombstone.tombstone.id)
            .await;
        assert_eq!(count, 0);
        // the tombstone is fully processed and should have been removed
        let count = catalog.count_tombstones_for_table(table.table.id).await;
        assert_eq!(count, 0);

        // Verify the files were pushed to the object store
        let object_store = catalog.object_store();
        let list = object_store.list(None).await.unwrap();
        let object_store_files: Vec<_> = list.try_collect().await.unwrap();
        // Original + 2 compacted
        assert_eq!(object_store_files.len(), 3);

        // ------------------------------------------------
        // Verify the parquet file content

        // query the chunks
        // most recent compacted second half (~10%)
        let files1 = files.pop().unwrap();
        let batches = read_parquet_file(&table, files1).await;
        assert_batches_sorted_eq!(
            &[
                "+-----------+------+-----------------------------+",
                "| field_int | tag1 | time                        |",
                "+-----------+------+-----------------------------+",
                "| 70        | UT   | 1970-01-01T00:00:00.000020Z |",
                "+-----------+------+-----------------------------+",
            ],
            &batches
        );
        // least recent compacted first half (~90%)
        let files2 = files.pop().unwrap();
        let batches = read_parquet_file(&table, files2).await;
        assert_batches_sorted_eq!(
            &[
                "+-----------+------+-----------------------------+",
                "| field_int | tag1 | time                        |",
                "+-----------+------+-----------------------------+",
                "| 1000      | WA   | 1970-01-01T00:00:00.000008Z |",
                "+-----------+------+-----------------------------+",
            ],
            &batches
        );
        assert!(files.is_empty());
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

        // lp4 does not overlapp with any
        let lp4 = vec![
            "table,tag2=WA,tag3=10 field_int=1600i 28000",
            "table,tag2=VT,tag3=20 field_int=20i 26000",
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
        let split_percentage = 90;
        let max_concurrent_compaction_size_bytes = 100000;
        let compaction_max_size_bytes = 100000;
        let compaction_max_file_count = 10;
        let compactor = Compactor::new(
            vec![sequencer.sequencer.id],
            Arc::clone(&catalog.catalog),
            ParquetStorage::new(Arc::clone(&catalog.object_store)),
            Arc::new(Executor::new(1)),
            Arc::new(SystemProvider::new()),
            BackoffConfig::default(),
            CompactorConfig::new(
                split_percentage,
                max_concurrent_compaction_size_bytes,
                compaction_max_size_bytes,
                compaction_max_file_count,
            ),
            Arc::new(metric::Registry::new()),
        );

        // parquet files
        // pf1 does not overlap with any and very large ==> will be upgraded to level 1 during compaction
        partition
            .create_parquet_file_with_min_max_size_and_creation_time(
                &lp1,
                1,
                1,
                10,
                20,
                compactor.config.compaction_max_size_bytes() + 10,
                20,
            )
            .await;
        // pf2 overlaps with pf3 ==> compacted and marked to_delete with a timestamp
        partition
            .create_parquet_file_with_min_max_size_and_creation_time(
                &lp2,
                4,
                5,
                8000,
                20000,
                100, // smal file
                time.now().timestamp_nanos(),
            )
            .await;
        // pf3 overlaps with pf2 ==> compacted and marked to_delete with a timestamp
        partition
            .create_parquet_file_with_min_max_size_and_creation_time(
                &lp3,
                8,
                10,
                6000,
                25000,
                100, // small file
                time.now().timestamp_nanos(),
            )
            .await;
        // pf4 does not overlap with any but small => will also be compacted with pf2 and pf3
        partition
            .create_parquet_file_with_min_max_size_and_creation_time(
                &lp4,
                18,
                18,
                26000,
                28000,
                100, // small file
                time.now().timestamp_nanos(),
            )
            .await;
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
        // should have 3 tomstones
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
                compactor.config.compaction_max_size_bytes(),
                compactor.config.compaction_max_file_count(),
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
                compactor.config.compaction_max_size_bytes(),
            )
            .await
            .unwrap();

        // Should have 2 non-soft-deleted files: pf1 not compacted and stay, and 1 newly created
        // after compacting pf2, pf3, pf4 all very small into one file
        let mut files = catalog.list_by_table_not_to_delete(table.table.id).await;
        assert_eq!(files.len(), 2);
        // pf1 upgraded to level FILE_NON_OVERLAPPED_COMAPCTION_LEVEL
        assert_eq!(
            (files[0].id.get(), files[0].compaction_level),
            (1, FILE_NON_OVERLAPPED_COMAPCTION_LEVEL)
        );
        // 1 newly created level FILE_NON_OVERLAPPED_COMAPCTION_LEVEL files as the result of compaction
        assert_eq!(
            (files[1].id.get(), files[1].compaction_level),
            (5, FILE_NON_OVERLAPPED_COMAPCTION_LEVEL)
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
        let file2 = files.pop().unwrap();
        let batches = read_parquet_file(&table, file2).await;
        assert_batches_sorted_eq!(
            &[
                "+-----------+------+------+------+-----------------------------+",
                "| field_int | tag1 | tag2 | tag3 | time                        |",
                "+-----------+------+------+------+-----------------------------+",
                "| 10        | VT   |      |      | 1970-01-01T00:00:00.000006Z |",
                "| 1500      | WA   |      |      | 1970-01-01T00:00:00.000008Z |",
                "| 270       | UT   |      |      | 1970-01-01T00:00:00.000025Z |",
                "| 70        | UT   |      |      | 1970-01-01T00:00:00.000020Z |",
                "| 1600      |      | WA   | 10   | 1970-01-01T00:00:00.000028Z |",
                "| 20        |      | VT   | 20   | 1970-01-01T00:00:00.000026Z |",
                "+-----------+------+------+------+-----------------------------+",
            ],
            &batches
        );
        // Non-compacted file
        let file1 = files.pop().unwrap();
        let batches = read_parquet_file(&table, file1).await;
        assert_batches_sorted_eq!(
            &[
                "+-----------+------+--------------------------------+",
                "| field_int | tag1 | time                           |",
                "+-----------+------+--------------------------------+",
                "| 10        | VT   | 1970-01-01T00:00:00.000000020Z |",
                "| 1000      | WA   | 1970-01-01T00:00:00.000000010Z |",
                "+-----------+------+--------------------------------+",
            ],
            &batches
        );
        // No more files
        assert!(files.is_empty());
    }

    #[tokio::test]
    async fn test_compact_one_file() {
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
        let parquet_file = partition
            .create_parquet_file_with_min_max(&lp, 1, 1, 8000, 20000)
            .await
            .parquet_file;

        let split_percentage = 90;
        let max_concurrent_compaction_size_bytes = 100000;
        let compaction_max_size_bytes = 100000;
        let compaction_max_file_count = 10;
        let compactor = Compactor::new(
            vec![sequencer.sequencer.id],
            Arc::clone(&catalog.catalog),
            ParquetStorage::new(Arc::clone(&catalog.object_store)),
            Arc::new(Executor::new(1)),
            Arc::new(SystemProvider::new()),
            BackoffConfig::default(),
            CompactorConfig::new(
                split_percentage,
                max_concurrent_compaction_size_bytes,
                compaction_max_size_bytes,
                compaction_max_file_count,
            ),
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
                compactor.config.compaction_max_size_bytes(),
            )
            .await
            .unwrap();
        assert!(result.is_empty());

        // ------------------------------------------------
        // File without tombstones
        let mut pf = ParquetFileWithTombstone::new(Arc::new(parquet_file), vec![]);
        // Nothing compacted for one file without tombstones
        let result = compactor
            .compact(
                vec![pf.clone()],
                &ns.namespace,
                &table.table,
                &table_schema,
                &partition.partition,
                compactor.config.compaction_max_size_bytes(),
            )
            .await
            .unwrap();
        assert!(result.is_empty());

        // ------------------------------------------------
        // Let add a tombstone
        let tombstone = table
            .with_sequencer(&sequencer)
            .create_tombstone(20, 6000, 12000, "tag1=VT")
            .await;
        pf.add_tombstones(vec![tombstone.tombstone.clone()]);

        // should have compacted data
        let batches = compactor
            .compact(
                vec![pf],
                &ns.namespace,
                &table.table,
                &table_schema,
                &partition.partition,
                compactor.config.compaction_max_size_bytes(),
            )
            .await
            .unwrap();
        // One output batch because the input is too small to split
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

        // Data: row tag1=VT was removed
        assert_batches_sorted_eq!(
            &[
                "+-----------+------+-----------------------------+",
                "| field_int | tag1 | time                        |",
                "+-----------+------+-----------------------------+",
                "| 1000      | WA   | 1970-01-01T00:00:00.000008Z |",
                "| 70        | UT   | 1970-01-01T00:00:00.000020Z |",
                "+-----------+------+-----------------------------+",
            ],
            &batches[0]
        );
    }

    #[tokio::test]
    async fn test_compact_one_file_no_split() {
        maybe_start_logging();
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
        let parquet_file = partition
            .create_parquet_file_with_min_max(&lp, 1, 1, 8000, 20000)
            .await
            .parquet_file;

        let split_percentage = 100;
        let max_concurrent_compaction_size_bytes = 100000;
        let compaction_max_size_bytes = 100000;
        let compaction_max_file_count = 10;
        let compactor = Compactor::new(
            vec![sequencer.sequencer.id],
            Arc::clone(&catalog.catalog),
            ParquetStorage::new(Arc::clone(&catalog.object_store)),
            Arc::new(Executor::new(1)),
            Arc::new(SystemProvider::new()),
            BackoffConfig::default(),
            // split_percentage = 100 which means no split
            CompactorConfig::new(
                split_percentage,
                max_concurrent_compaction_size_bytes,
                compaction_max_size_bytes,
                compaction_max_file_count,
            ),
            Arc::new(metric::Registry::new()),
        );

        let sort_key = SortKey::from_columns(["tag1", "time"]);
        let partition = partition.update_sort_key(sort_key).await;

        // ------------------------------------------------
        // Let add a tombstone
        let tombstone = table
            .with_sequencer(&sequencer)
            .create_tombstone(20, 6000, 12000, "tag1=VT")
            .await;
        let pf = ParquetFileWithTombstone::new(
            Arc::new(parquet_file),
            vec![tombstone.tombstone.clone()],
        );

        // should have compacted datas
        let batches = compactor
            .compact(
                vec![pf],
                &ns.namespace,
                &table.table,
                &table_schema,
                &partition.partition,
                compactor.config.compaction_max_size_bytes(),
            )
            .await
            .unwrap();
        // 1 output set becasue split rule = 100%
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

        // Data: row tag1=VT was removed
        assert_batches_sorted_eq!(
            &[
                "+-----------+------+-----------------------------+",
                "| field_int | tag1 | time                        |",
                "+-----------+------+-----------------------------+",
                "| 1000      | WA   | 1970-01-01T00:00:00.000008Z |",
                "| 70        | UT   | 1970-01-01T00:00:00.000020Z |",
                "+-----------+------+-----------------------------+",
            ],
            &batches[0]
        );
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
        // Create 2 parquet files with total size = 140000 (file1) + 100000 (file2) = 240000
        let parquet_file1 = partition
            .create_parquet_file_with_min_max_size(&lp1, 1, 5, 8000, 20000, 140000)
            .await
            .parquet_file;
        let parquet_file2 = partition
            .create_parquet_file_with_min_max_size(&lp2, 10, 15, 6000, 25000, 100000)
            .await
            .parquet_file;

        let split_percentage = 90;
        let max_concurrent_compaction_size_bytes = 100000;
        let compaction_max_size_bytes = 100000;
        let compaction_max_file_count = 10;
        let compactor = Compactor::new(
            vec![sequencer.sequencer.id],
            Arc::clone(&catalog.catalog),
            ParquetStorage::new(Arc::clone(&catalog.object_store)),
            Arc::new(Executor::new(1)),
            Arc::new(SystemProvider::new()),
            BackoffConfig::default(),
            CompactorConfig::new(
                split_percentage,
                max_concurrent_compaction_size_bytes,
                compaction_max_size_bytes,
                compaction_max_file_count,
            ),
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

        // Compact them
        let batches = compactor
            .compact(
                vec![pf1, pf2],
                &ns.namespace,
                &table.table,
                &table_schema,
                &partition.partition,
                compactor.config.compaction_max_size_bytes(),
            )
            .await
            .unwrap();
        // 3 sets based on 42% split rule = 100000 (max_desired_file_size) / 240000 (total_size of 2 files)
        assert_eq!(batches.len(), 3);

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
        // first set contains least recent 2 rows
        assert_batches_sorted_eq!(
            &[
                "+-----------+------+-----------------------------+",
                "| field_int | tag1 | time                        |",
                "+-----------+------+-----------------------------+",
                "| 10        | VT   | 1970-01-01T00:00:00.000006Z |",
                "| 1500      | WA   | 1970-01-01T00:00:00.000008Z |",
                "+-----------+------+-----------------------------+",
            ],
            &batches[0]
        );
        // second set contains the next least recent one row
        assert_batches_sorted_eq!(
            &[
                "+-----------+------+-----------------------------+",
                "| field_int | tag1 | time                        |",
                "+-----------+------+-----------------------------+",
                "| 70        | UT   | 1970-01-01T00:00:00.000020Z |",
                "+-----------+------+-----------------------------+",
            ],
            &batches[1]
        );
        // third set contains most recent one row
        assert_batches_sorted_eq!(
            &[
                "+-----------+------+-----------------------------+",
                "| field_int | tag1 | time                        |",
                "+-----------+------+-----------------------------+",
                "| 270       | UT   | 1970-01-01T00:00:00.000025Z |",
                "+-----------+------+-----------------------------+",
            ],
            &batches[2]
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
        let parquet_file1 = partition
            .create_parquet_file_with_min_max_size(&lp1, 1, 5, 8000, 20000, 50000)
            .await
            .parquet_file;
        let parquet_file2 = partition
            .create_parquet_file_with_min_max_size(&lp2, 10, 15, 6000, 25000, 50000)
            .await
            .parquet_file;
        let parquet_file3 = partition
            .create_parquet_file_with_min_max_size(&lp3, 20, 25, 6000, 8000, 20000)
            .await
            .parquet_file;

        let split_percentage = 90;
        let max_concurrent_compaction_size_bytes = 100000;
        let compaction_max_size_bytes = 100000;
        let compaction_max_file_count = 10;
        let compactor = Compactor::new(
            vec![sequencer.sequencer.id],
            Arc::clone(&catalog.catalog),
            ParquetStorage::new(Arc::clone(&catalog.object_store)),
            Arc::new(Executor::new(1)),
            Arc::new(SystemProvider::new()),
            BackoffConfig::default(),
            CompactorConfig::new(
                split_percentage,
                max_concurrent_compaction_size_bytes,
                compaction_max_size_bytes,
                compaction_max_file_count,
            ),
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

        // Compact them
        let batches = compactor
            .compact(
                vec![pf1.clone(), pf2.clone(), pf3.clone()],
                &ns.namespace,
                &table.table,
                &table_schema,
                &partition.partition,
                compactor.config.compaction_max_size_bytes(),
            )
            .await
            .unwrap();

        // 2 sets based on the 83% split rule: 100000 (max_desired_file_size) / 120000 (total fle size)
        assert_eq!(batches.len(), 2);

        // Sort keys should be the same as was passed in to compact
        assert_eq!(batches[0].meta.sort_key.as_ref().unwrap(), &sort_key);
        assert_eq!(batches[1].meta.sort_key.as_ref().unwrap(), &sort_key);

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
        // first set contains least recent 5 rows
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
                "+-----------+------+------+------+-----------------------------+",
            ],
            &batches[0]
        );
        // second set contains most recent one row
        assert_batches_sorted_eq!(
            &[
                "+-----------+------+------+------+-----------------------------+",
                "| field_int | tag1 | tag2 | tag3 | time                        |",
                "+-----------+------+------+------+-----------------------------+",
                "| 270       | UT   |      |      | 1970-01-01T00:00:00.000025Z |",
                "+-----------+------+------+------+-----------------------------+",
            ],
            &batches[1]
        );
    }

    /// A test utility function to make minimially-viable ParquetFile records with particular
    /// min/max times. Does not involve the catalog at all.
    fn arbitrary_parquet_file(min_time: i64, max_time: i64) -> ParquetFile {
        arbitrary_parquet_file_with_size(min_time, max_time, 100)
    }

    fn arbitrary_parquet_file_with_size_and_sequence_number(
        min_time: i64,
        max_time: i64,
        min_seq: i64,
        max_seq: i64,
        file_size_bytes: i64,
    ) -> ParquetFile {
        let id = NEXT_ID.fetch_add(1, Ordering::SeqCst);
        ParquetFile {
            id: ParquetFileId::new(id),
            sequencer_id: SequencerId::new(0),
            namespace_id: NamespaceId::new(0),
            table_id: TableId::new(0),
            partition_id: PartitionId::new(0),
            object_store_id: Uuid::new_v4(),
            min_sequence_number: SequenceNumber::new(min_seq),
            max_sequence_number: SequenceNumber::new(max_seq),
            min_time: Timestamp::new(min_time),
            max_time: Timestamp::new(max_time),
            to_delete: None,
            file_size_bytes,
            row_count: 0,
            compaction_level: INITIAL_COMPACTION_LEVEL, // level of file of new writes
            created_at: Timestamp::new(1),
            column_set: ColumnSet::new([ColumnId::new(1), ColumnId::new(2)]),
        }
    }

    fn arbitrary_parquet_file_with_size(
        min_time: i64,
        max_time: i64,
        file_size_bytes: i64,
    ) -> ParquetFile {
        arbitrary_parquet_file_with_size_and_sequence_number(
            min_time,
            max_time,
            0,
            1,
            file_size_bytes,
        )
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
        let pf1 = partition
            .create_parquet_file_with_min_max(&lp1, 1, 5, 8000, 20000)
            .await
            .parquet_file;
        let pf2 = partition
            .create_parquet_file_with_min_max(&lp2, 1, 5, 28000, 35000)
            .await
            .parquet_file;

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

        let pf = partition.create_parquet_file(&lp).await.parquet_file;

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

    #[test]
    fn test_overlapped_groups_no_overlap() {
        // Given two files that don't overlap,
        let pf1 = arbitrary_parquet_file(1, 2);
        let pf2 = arbitrary_parquet_file(3, 4);

        let groups = Compactor::overlapped_groups(
            vec![pf1.clone(), pf2.clone()],
            TEST_MAX_SIZE_BYTES,
            TEST_MAX_FILE_COUNT,
        );

        // They should be 2 groups
        assert_eq!(groups.len(), 2, "There should have been two group");

        assert!(groups[0].parquet_files.contains(&pf1));
        assert!(groups[1].parquet_files.contains(&pf2));
    }

    #[test]
    fn test_overlapped_groups_with_overlap() {
        // Given two files that do overlap,
        let pf1 = arbitrary_parquet_file(1, 3);
        let pf2 = arbitrary_parquet_file(2, 4);

        let groups = Compactor::overlapped_groups(
            vec![pf1.clone(), pf2.clone()],
            TEST_MAX_SIZE_BYTES,
            TEST_MAX_FILE_COUNT,
        );

        // They should be in one group (order not guaranteed)
        assert_eq!(groups.len(), 1, "There should have only been one group");

        let group = &groups[0];
        assert_eq!(
            group.parquet_files.len(),
            2,
            "The one group should have contained 2 items"
        );
        assert!(group.parquet_files.contains(&pf1));
        assert!(group.parquet_files.contains(&pf2));
    }

    #[test]
    fn test_overlapped_groups_many_groups() {
        let overlaps_many = arbitrary_parquet_file(5, 10);
        let contained_completely_within = arbitrary_parquet_file(6, 7);
        let max_equals_min = arbitrary_parquet_file(3, 5);
        let min_equals_max = arbitrary_parquet_file(10, 12);

        let alone = arbitrary_parquet_file(30, 35);

        let another = arbitrary_parquet_file(13, 15);
        let partial_overlap = arbitrary_parquet_file(14, 16);

        // Given a bunch of files in an arbitrary order,
        let all = vec![
            min_equals_max.clone(),
            overlaps_many.clone(),
            alone.clone(),
            another.clone(),
            max_equals_min.clone(),
            contained_completely_within.clone(),
            partial_overlap.clone(),
        ];

        let mut groups =
            Compactor::overlapped_groups(all, TEST_MAX_SIZE_BYTES, TEST_MAX_FILE_COUNT);
        dbg!(&groups);

        assert_eq!(groups.len(), 3);

        // Order of the groups is not guaranteed; sort by min_time of group so we can test membership
        groups.sort_by_key(|g| g.min_time);

        let alone_group = &groups[2];
        assert_eq!(alone_group.min_time, Timestamp::new(30));
        assert!(
            alone_group.parquet_files.contains(&alone),
            "Actually contains: {:#?}",
            alone_group
        );

        let another_group = &groups[1];
        assert_eq!(another_group.min_time, Timestamp::new(13));
        assert!(
            another_group.parquet_files.contains(&another),
            "Actually contains: {:#?}",
            another_group
        );
        assert!(
            another_group.parquet_files.contains(&partial_overlap),
            "Actually contains: {:#?}",
            another_group
        );

        let many_group = &groups[0];
        assert_eq!(many_group.min_time, Timestamp::new(3));
        assert!(
            many_group.parquet_files.contains(&overlaps_many),
            "Actually contains: {:#?}",
            many_group
        );
        assert!(
            many_group
                .parquet_files
                .contains(&contained_completely_within),
            "Actually contains: {:#?}",
            many_group
        );
        assert!(
            many_group.parquet_files.contains(&max_equals_min),
            "Actually contains: {:#?}",
            many_group
        );
        assert!(
            many_group.parquet_files.contains(&min_equals_max),
            "Actually contains: {:#?}",
            many_group
        );
    }

    #[test]
    fn test_group_small_contiguous_overlapped_groups() {
        // Given two files that don't overlap,
        let pf1 = arbitrary_parquet_file_with_size(1, 2, 100);
        let pf2 = arbitrary_parquet_file_with_size(3, 4, 200);

        let overlapped_groups = Compactor::overlapped_groups(
            vec![pf1.clone(), pf2.clone()],
            TEST_MAX_SIZE_BYTES,
            TEST_MAX_FILE_COUNT,
        );
        // 2 overlapped groups
        assert_eq!(overlapped_groups.len(), 2);
        let g1 = GroupWithMinTimeAndSize {
            parquet_files: vec![pf1.clone()],
            min_time: Timestamp::new(1),
            total_file_size_bytes: 100,
            overlapped_with_other_groups: false,
        };
        let g2 = GroupWithMinTimeAndSize {
            parquet_files: vec![pf2.clone()],
            min_time: Timestamp::new(3),
            total_file_size_bytes: 200,
            overlapped_with_other_groups: false,
        };
        // They should each be in their own group
        assert_eq!(overlapped_groups, vec![g1, g2]);

        // Group them by size
        let compaction_max_size_bytes = 100000;
        let groups = Compactor::group_small_contiguous_groups(
            overlapped_groups,
            compaction_max_size_bytes,
            TEST_MAX_FILE_COUNT,
        );
        // 2 small groups should be grouped in one
        assert_eq!(groups.len(), 1);
        assert_eq!(groups, vec![vec![pf1, pf2]]);
    }

    #[test]
    #[should_panic]
    fn test_split_overlapped_groups_panic() {
        let max_size_bytes = 1000;
        let max_file_count = 2;

        // 2 files, each smaller than max_size_bytes but total is larger --> will be split
        // files have overlapped range of sequence numbers: [5, 11] and [7, 15]
        let f1 =
            arbitrary_parquet_file_with_size_and_sequence_number(5, 10, 5, 11, max_size_bytes - 10);
        let f2 =
            arbitrary_parquet_file_with_size_and_sequence_number(7, 11, 7, 15, max_size_bytes - 10);
        let mut groups = vec![vec![f1, f2]];

        // will panic becasue of
        Compactor::split_overlapped_groups(&mut groups, max_size_bytes, max_file_count);
    }

    // This is specific unit test for split_overlapped_groups but it is a subtest of test_limit_size_and_num_files
    // Keep all the variables the same names in both tests for us to follow them easily
    #[test]
    fn test_split_overlapped_groups_sequence_number_same_order_with_time() {
        let compaction_max_size_bytes = 100000;

        // oldest overlapped and very small
        let overlaps_many = arbitrary_parquet_file_with_size_and_sequence_number(5, 10, 5, 5, 400);
        let contained_completely_within =
            arbitrary_parquet_file_with_size_and_sequence_number(6, 7, 6, 6, 500);
        let max_equals_min = arbitrary_parquet_file_with_size_and_sequence_number(3, 5, 3, 3, 400);
        let min_equals_max =
            arbitrary_parquet_file_with_size_and_sequence_number(10, 12, 10, 10, 500);
        let oldest_overlapped_group = vec![
            overlaps_many.clone(),
            contained_completely_within.clone(),
            max_equals_min.clone(),
            min_equals_max.clone(),
        ];

        // newest files and very large
        let alone = arbitrary_parquet_file_with_size_and_sequence_number(
            30,
            35,
            30,
            30,
            compaction_max_size_bytes + 200,
        ); // too large to group
        let newest_overlapped_group = vec![alone.clone()];

        // small files in  the middle
        let another = arbitrary_parquet_file_with_size_and_sequence_number(13, 15, 13, 13, 1000);
        let partial_overlap =
            arbitrary_parquet_file_with_size_and_sequence_number(14, 16, 14, 14, 2000);
        let middle_overlapped_group = vec![another.clone(), partial_overlap.clone()];

        let mut overlapped_groups = vec![
            oldest_overlapped_group,
            newest_overlapped_group,
            middle_overlapped_group,
        ];

        let max_size_bytes = 1000;
        let max_file_count = 2;

        // Three input groups but will produce 5 output ones  becasue of limit in size and file count.
        // Note that the 3 input groups each includes overlapped files but the groups do not overlap.
        // The function split_overlapped_groups is to split each overlapped group. It does not merge any groups.
        let groups = Compactor::split_overlapped_groups(
            &mut overlapped_groups,
            max_size_bytes,
            max_file_count,
        );

        // one intact group
        assert_eq!(groups.internal_overlapped_groups.len(), 1);
        // four newly splitted groups from two large groups
        assert_eq!(groups.external_overlapped_groups.len(), 4);

        // oldest_overlapped_group was split into external_overlapped_groups[0] and [1] due to file count limit
        // the chunks split by their min_seq order
        assert_eq!(groups.external_overlapped_groups[0].len(), 2); // reach limit file count
        assert!(groups.external_overlapped_groups[0].contains(&max_equals_min)); // min_time = 3, min_seq = 3
        assert!(groups.external_overlapped_groups[0].contains(&overlaps_many)); // min_time = 5, min_seq = 5
        assert_eq!(groups.external_overlapped_groups[1].len(), 2); // reach limit file count
        assert!(groups.external_overlapped_groups[1].contains(&contained_completely_within)); // min_time = 6, min_seq = 6
        assert!(groups.external_overlapped_groups[1].contains(&min_equals_max)); // min_time = 10, min_seq = 10

        // newest_overlapped_group stays the same length one file and the corresponding output is
        // groups.internal_overlapped_groups[0]
        assert_eq!(groups.internal_overlapped_groups[0].len(), 1); // reach limit file size
        assert!(groups.internal_overlapped_groups[0].contains(&alone));

        // middle_overlapped_group was split into groups.external_overlapped_groups[2] and [3] due to size limit
        // the chunks split by their min_seq order
        assert_eq!(groups.external_overlapped_groups[2].len(), 1); // reach limit file size
        assert!(groups.external_overlapped_groups[2].contains(&another)); // min_time = 13, min_seq = 13
        assert_eq!(groups.external_overlapped_groups[3].len(), 1); // reach limit file size
        assert!(groups.external_overlapped_groups[3].contains(&partial_overlap));
        // min_time = 14, min_seq = 14
    }

    // This is specific unit test for split_overlapped_groups but it is a subtest of test_limit_size_and_num_files
    // Keep all the variables the same names in both tests for us to follow them easily
    #[test]
    fn test_split_overlapped_groups_sequence_number_different_order_from_time() {
        let compaction_max_size_bytes = 100000;

        // oldest overlapped and very small
        let overlaps_many =
            arbitrary_parquet_file_with_size_and_sequence_number(5, 10, 10, 10, 400);
        let contained_completely_within =
            arbitrary_parquet_file_with_size_and_sequence_number(6, 7, 6, 6, 500);
        let max_equals_min = arbitrary_parquet_file_with_size_and_sequence_number(3, 5, 3, 3, 400);
        let min_equals_max =
            arbitrary_parquet_file_with_size_and_sequence_number(10, 12, 5, 5, 500);
        let oldest_overlapped_group = vec![
            overlaps_many.clone(),
            contained_completely_within.clone(),
            max_equals_min.clone(),
            min_equals_max.clone(),
        ];

        // newest files and very large
        let alone = arbitrary_parquet_file_with_size_and_sequence_number(
            30,
            35,
            30,
            30,
            compaction_max_size_bytes + 200,
        ); // too large to group
        let newest_overlapped_group = vec![alone.clone()];

        // small files in  the middle
        let another = arbitrary_parquet_file_with_size_and_sequence_number(13, 15, 14, 14, 1000);
        let partial_overlap =
            arbitrary_parquet_file_with_size_and_sequence_number(14, 16, 13, 13, 2000);
        let middle_overlapped_group = vec![another.clone(), partial_overlap.clone()];

        let mut overlapped_groups = vec![
            oldest_overlapped_group,
            newest_overlapped_group,
            middle_overlapped_group,
        ];

        let max_size_bytes = 1000;
        let max_file_count = 2;

        // Three input groups but will produce 5 output ones becasue of limit in size and file count.
        // Note that the 3 input groups each includes overlapped files but the groups do not overlap.
        // The function split_overlapped_groups is to split each overlapped group. It does not merge any groups.
        let groups = Compactor::split_overlapped_groups(
            &mut overlapped_groups,
            max_size_bytes,
            max_file_count,
        );

        // one intact group
        assert_eq!(groups.internal_overlapped_groups.len(), 1);
        // four newly splitted groups from two large groups
        assert_eq!(groups.external_overlapped_groups.len(), 4);

        // oldest_overlapped_group was split into external_overlapped_groups[0] and [1] due to file count limit
        // the chunks split by their min_seq order
        assert_eq!(groups.external_overlapped_groups[0].len(), 2); // reach limit file count
        assert!(groups.external_overlapped_groups[0].contains(&max_equals_min)); // min_time = 3, min_seq = 3
        assert!(groups.external_overlapped_groups[0].contains(&min_equals_max)); // min_time = 10, min_seq = 5
        assert_eq!(groups.external_overlapped_groups[1].len(), 2); // reach limit file count
        assert!(groups.external_overlapped_groups[1].contains(&contained_completely_within)); // min_time = 6, min_seq = 6
        assert!(groups.external_overlapped_groups[1].contains(&overlaps_many)); // min_time = 5, min_seq = 10

        // newest_overlapped_group stays the same length one file and the corresponding output is
        // groups.internal_overlapped_groups[0]
        assert_eq!(groups.internal_overlapped_groups[0].len(), 1); // reach limit file size
        assert!(groups.internal_overlapped_groups[0].contains(&alone));

        // middle_overlapped_group was split into groups.external_overlapped_groups[2] and [3] due to size limit
        // the chunks split by their min_seq order
        assert_eq!(groups.external_overlapped_groups[2].len(), 1); // reach limit file size
        assert!(groups.external_overlapped_groups[2].contains(&partial_overlap)); // min_time = 14, min_seq = 13
        assert_eq!(groups.external_overlapped_groups[3].len(), 1); // reach limit file size
        assert!(groups.external_overlapped_groups[3].contains(&another)); // min_time = 13, min_seq = 14
    }

    // This tests
    //   1. overlapped_groups which focuses on the detail of both its children:
    //      1.a. group_potential_duplicates that groups files into overlapped groups
    //      1.b. split_overlapped_groups that splits each overlapped group further to meet size and/or file limit
    //   2. group_small_contiguous_groups that merges non-overlapped group into a larger one if they meet size and file limit
    #[test]
    fn test_limit_size_and_num_files() {
        let compaction_max_size_bytes = 100000;

        // oldest overlapped and very small
        let overlaps_many =
            arbitrary_parquet_file_with_size_and_sequence_number(5, 10, 10, 10, 400);
        let contained_completely_within =
            arbitrary_parquet_file_with_size_and_sequence_number(6, 7, 6, 6, 500);
        let max_equals_min = arbitrary_parquet_file_with_size_and_sequence_number(3, 5, 3, 3, 400);
        let min_equals_max =
            arbitrary_parquet_file_with_size_and_sequence_number(10, 12, 5, 5, 500);

        // newest files and very large
        let alone = arbitrary_parquet_file_with_size_and_sequence_number(
            30,
            35,
            30,
            30,
            compaction_max_size_bytes + 200,
        ); // too large to group

        // small files in  the middle
        let another = arbitrary_parquet_file_with_size_and_sequence_number(13, 15, 14, 14, 1000);
        let partial_overlap =
            arbitrary_parquet_file_with_size_and_sequence_number(14, 16, 13, 13, 2000);

        // Given a bunch of files in an arbitrary order,
        let all = vec![
            min_equals_max.clone(),
            overlaps_many.clone(),
            alone.clone(),
            another.clone(),
            max_equals_min.clone(),
            contained_completely_within.clone(),
            partial_overlap.clone(),
        ];

        // Group into overlapped groups
        let max_size_bytes = 1000;
        let max_file_count = 2;
        let overlapped_groups = Compactor::overlapped_groups(all, max_size_bytes, max_file_count);
        // Must be 5
        assert_eq!(overlapped_groups.len(), 5);
        // Non-overlapped with other groups always first
        // Group 0: newest files and very large
        assert_eq!(overlapped_groups[0].parquet_files.len(), 1); // reach limit file size
        assert!(overlapped_groups[0].parquet_files.contains(&alone)); // min_time = 30
                                                                      // The rest of the groups, each overlapp with another group
                                                                      // Groups 1 & 2: split from `oldest overlapped and very small` ordering by sequence numbers
        assert_eq!(overlapped_groups[1].parquet_files.len(), 2); // reach limit file count
        assert!(overlapped_groups[1].parquet_files.contains(&max_equals_min)); // min_time = 3, min_seq = 3
        assert!(overlapped_groups[1].parquet_files.contains(&min_equals_max)); // min_time = 10, min_seq = 5
        assert_eq!(overlapped_groups[2].parquet_files.len(), 2); // reach limit file count
        assert!(overlapped_groups[2]
            .parquet_files
            .contains(&contained_completely_within)); // min_time = 6, min_seq = 6
        assert!(overlapped_groups[2].parquet_files.contains(&overlaps_many)); // min_time = 5, min_seq = 10
                                                                              // Groups 3 & 4: split from `small files in  the middle` ordering by sequence numbers
        assert_eq!(overlapped_groups[3].parquet_files.len(), 1); // reach limit file size
        assert!(overlapped_groups[3]
            .parquet_files
            .contains(&partial_overlap)); // min_time = 14, min_seq = 13
        assert_eq!(overlapped_groups[4].parquet_files.len(), 1); // reach limit file size
        assert!(overlapped_groups[4].parquet_files.contains(&another)); // min_time = 13, min_seq = 14

        // Group further into group by size and file count limit
        // Due to the merge with correct time range, this function has to sort the groups hence output data will be
        // in time order between each group and in sequence number order within a each group
        let groups = Compactor::group_small_contiguous_groups(
            overlapped_groups,
            compaction_max_size_bytes,
            max_file_count,
        );

        // Still 5 groups. Nothing is merged due to the limit of size and file num
        assert_eq!(groups.len(), 5);

        assert_eq!(groups[0].len(), 2); // reach file num limit
        assert!(groups[0].contains(&max_equals_min)); // min_time = 3, min_seq = 3
        assert!(groups[0].contains(&min_equals_max)); // min_time = 10, min_seq = 5

        assert_eq!(groups[1].len(), 2); // reach file num limit
        assert!(groups[1].contains(&contained_completely_within)); // min_time = 6, min_seq = 6
        assert!(groups[1].contains(&overlaps_many)); // min_time = 5, min_seq = 10

        assert_eq!(groups[2].len(), 1); // reach size limit
        assert!(groups[2].contains(&another)); // min_time = 13, min_seq = 14

        assert_eq!(groups[3].len(), 1); // reach size limit
        assert!(groups[3].contains(&partial_overlap)); // min_time = 14, min_seq = 13

        assert_eq!(groups[4].len(), 1); // reach size limit
        assert!(groups[4].contains(&alone)); // min_time=30
    }

    // This tests
    //   1. overlapped_groups
    //   2. group_small_contiguous_groups that merges non-overlapped group into a larger one if they meet size and file limit
    #[test]
    fn test_group_small_contiguous_overlapped_groups_no_group() {
        let compaction_max_size_bytes = 100000;

        // Given two files that don't overlap,
        let pf1 = arbitrary_parquet_file_with_size(1, 2, 100);
        let pf2 = arbitrary_parquet_file_with_size(3, 4, compaction_max_size_bytes); // too large to group

        let overlapped_groups = Compactor::overlapped_groups(
            vec![pf1.clone(), pf2.clone()],
            TEST_MAX_SIZE_BYTES,
            TEST_MAX_FILE_COUNT,
        );
        // 2 overlapped groups
        assert_eq!(overlapped_groups.len(), 2);
        let g1 = GroupWithMinTimeAndSize {
            parquet_files: vec![pf1.clone()],
            min_time: Timestamp::new(1),
            total_file_size_bytes: 100,
            overlapped_with_other_groups: false,
        };
        let g2 = GroupWithMinTimeAndSize {
            parquet_files: vec![pf2.clone()],
            min_time: Timestamp::new(3),
            total_file_size_bytes: compaction_max_size_bytes,
            overlapped_with_other_groups: false,
        };
        // They should each be in their own group
        assert_eq!(overlapped_groups, vec![g1, g2]);

        // Group them by size
        let groups = Compactor::group_small_contiguous_groups(
            overlapped_groups,
            compaction_max_size_bytes,
            TEST_MAX_FILE_COUNT,
        );
        // Files too big to group further
        assert_eq!(groups.len(), 2);
        assert_eq!(groups, vec![vec![pf1], vec![pf2]]);
    }

    // This tests
    //   1. overlapped_groups which focuses on the detail of both its children:
    //      1.a. group_potential_duplicates that groups files into overlapped groups
    //      1.b. split_overlapped_groups that splits each overlapped group further to meet size and/or file limit
    //   2. group_small_contiguous_groups that merges non-overlapped group into a larger one if they meet size and file limit
    #[test]
    fn test_group_small_contiguous_overlapped_groups_many_files() {
        let compaction_max_size_bytes = 100000;

        // oldest overlapped and very small
        let overlaps_many = arbitrary_parquet_file_with_size(5, 10, 200);
        let contained_completely_within = arbitrary_parquet_file_with_size(6, 7, 300);
        let max_equals_min = arbitrary_parquet_file_with_size(3, 5, 400);
        let min_equals_max = arbitrary_parquet_file_with_size(10, 12, 500);

        // newest files and very large
        let alone = arbitrary_parquet_file_with_size(30, 35, compaction_max_size_bytes + 200); // too large to group

        // small files in  the middle
        let another = arbitrary_parquet_file_with_size(13, 15, 1000);
        let partial_overlap = arbitrary_parquet_file_with_size(14, 16, 2000);

        // Given a bunch of files in an arbitrary order,
        let all = vec![
            min_equals_max.clone(),
            overlaps_many.clone(),
            alone.clone(),
            another.clone(),
            max_equals_min.clone(),
            contained_completely_within.clone(),
            partial_overlap.clone(),
        ];

        // Group into overlapped groups
        let overlapped_groups =
            Compactor::overlapped_groups(all, TEST_MAX_SIZE_BYTES, TEST_MAX_FILE_COUNT);
        // There are 3 overlapped groups but each group does not overlap with the other
        assert_eq!(overlapped_groups.len(), 3);

        // group further into group by size
        let groups = Compactor::group_small_contiguous_groups(
            overlapped_groups,
            compaction_max_size_bytes,
            TEST_MAX_FILE_COUNT,
        );
        // should be 2 groups
        assert_eq!(groups.len(), 2);
        // first group includes 6 oldest files in 2 overlapped groups
        assert_eq!(groups[0].len(), 6);
        assert!(groups[0].contains(&overlaps_many));
        assert!(groups[0].contains(&contained_completely_within));
        assert!(groups[0].contains(&max_equals_min));
        assert!(groups[0].contains(&min_equals_max));
        assert!(groups[0].contains(&another));
        assert!(groups[0].contains(&partial_overlap));
        // second group includes the one newest file
        assert_eq!(groups[1].len(), 1);
        assert!(groups[1].contains(&alone));
    }

    // This tests
    //   1. overlapped_groups which focuses on the detail of both its children:
    //      1.a. group_potential_duplicates that groups files into overlapped groups
    //      1.b. split_overlapped_groups that splits each overlapped group further to meet size and/or file limit
    //   2. group_small_contiguous_groups that merges non-overlapped group into a larger one if they meet size and file limit
    #[test]
    fn test_group_small_contiguous_overlapped_groups_many_files_too_large() {
        // oldest overlapped and very small
        let overlaps_many = arbitrary_parquet_file_with_size_and_sequence_number(5, 10, 5, 5, 200);
        let contained_completely_within =
            arbitrary_parquet_file_with_size_and_sequence_number(6, 7, 6, 6, 300);
        let max_equals_min = arbitrary_parquet_file_with_size_and_sequence_number(
            3,
            5,
            3,
            3,
            TEST_MAX_SIZE_BYTES + 400,
        ); // too large to group
        let min_equals_max =
            arbitrary_parquet_file_with_size_and_sequence_number(10, 12, 10, 10, 500);

        // newest files and small
        let alone = arbitrary_parquet_file_with_size_and_sequence_number(
            30,
            35,
            30,
            30,
            TEST_MAX_SIZE_BYTES,
        ); // too large to group

        // large files in  the middle
        let another = arbitrary_parquet_file_with_size_and_sequence_number(
            13,
            15,
            13,
            13,
            TEST_MAX_SIZE_BYTES,
        ); // too large to group
        let partial_overlap =
            arbitrary_parquet_file_with_size_and_sequence_number(14, 16, 14, 14, 2000);

        // Given a bunch of files in an arbitrary order
        let all = vec![
            min_equals_max.clone(),
            overlaps_many.clone(),
            alone.clone(),
            another.clone(),
            max_equals_min.clone(),
            contained_completely_within.clone(),
            partial_overlap.clone(),
        ];

        // Group into overlapped groups
        let overlapped_groups =
            Compactor::overlapped_groups(all, TEST_MAX_SIZE_BYTES, TEST_MAX_FILE_COUNT);
        assert_eq!(overlapped_groups.len(), 5);

        // 5 input groups and 5 output groups because they are too large to group further
        let groups = Compactor::group_small_contiguous_groups(
            overlapped_groups,
            TEST_MAX_SIZE_BYTES,
            TEST_MAX_FILE_COUNT,
        );
        assert_eq!(groups.len(), 5);

        // first group includes oldest and large file
        assert_eq!(groups[0].len(), 1);
        assert!(groups[0].contains(&max_equals_min)); // min_time = 3
                                                      // second group
        assert_eq!(groups[1].len(), 3);
        assert!(groups[1].contains(&overlaps_many)); // min_time = 5
        assert!(groups[1].contains(&contained_completely_within)); // min_time = 6
        assert!(groups[1].contains(&min_equals_max)); // min_time = 10
                                                      // third group
        assert_eq!(groups[2].len(), 1);
        assert!(groups[2].contains(&another)); // min_time = 13
                                               // forth group
        assert_eq!(groups[3].len(), 1); // min_time = 14
        assert!(groups[3].contains(&partial_overlap));
        // fifth group
        assert_eq!(groups[4].len(), 1);
        assert!(groups[4].contains(&alone)); // min_time = 30
    }

    #[test]
    fn test_group_small_contiguous_overlapped_groups_many_files_middle_too_large() {
        // oldest overlapped and very small
        let overlaps_many = arbitrary_parquet_file_with_size_and_sequence_number(5, 10, 5, 5, 200);
        let contained_completely_within =
            arbitrary_parquet_file_with_size_and_sequence_number(6, 7, 6, 6, 300);
        let max_equals_min = arbitrary_parquet_file_with_size_and_sequence_number(3, 5, 3, 3, 400);
        let min_equals_max =
            arbitrary_parquet_file_with_size_and_sequence_number(10, 12, 10, 10, 500);

        // newest files and small
        let alone = arbitrary_parquet_file_with_size_and_sequence_number(30, 35, 30, 30, 200);

        // large files in  the middle
        let another = arbitrary_parquet_file_with_size_and_sequence_number(
            13,
            15,
            13,
            13,
            TEST_MAX_SIZE_BYTES,
        ); // too large to group
        let partial_overlap =
            arbitrary_parquet_file_with_size_and_sequence_number(14, 16, 14, 14, 2000);

        // Given a bunch of files in an arbitrary order
        let all = vec![
            min_equals_max.clone(),
            overlaps_many.clone(),
            alone.clone(),
            another.clone(),
            max_equals_min.clone(),
            contained_completely_within.clone(),
            partial_overlap.clone(),
        ];

        // Group into overlapped groups
        let overlapped_groups =
            Compactor::overlapped_groups(all, TEST_MAX_SIZE_BYTES, TEST_MAX_FILE_COUNT);
        assert_eq!(overlapped_groups.len(), 4);

        // 4 input groups but 3 output groups
        // The last 2 groups will be grouped together because they are small
        let groups = Compactor::group_small_contiguous_groups(
            overlapped_groups,
            TEST_MAX_SIZE_BYTES,
            TEST_MAX_FILE_COUNT,
        );
        assert_eq!(groups.len(), 3);

        // first group includes 4 oldest files
        assert_eq!(groups[0].len(), 4);
        assert!(groups[0].contains(&max_equals_min)); // min _time = 3
        assert!(groups[0].contains(&overlaps_many)); // min _time = 5
        assert!(groups[0].contains(&contained_completely_within)); // min _time = 6
        assert!(groups[0].contains(&min_equals_max)); // min _time = 10
                                                      // second group
        assert_eq!(groups[1].len(), 1);
        assert!(groups[1].contains(&another)); // min _time = 13
                                               // third group
        assert_eq!(groups[2].len(), 2);
        assert!(groups[2].contains(&partial_overlap)); // min _time = 3
        assert!(groups[2].contains(&alone)); // min _time = 30
    }

    #[test]
    fn test_group_small_contiguous_overlapped_groups_with_split_overlap_groups() {
        // oldest overlapped
        // These 4 overlapped files exceed max count --> will be split into 2 overlapped groups
        let overlaps_many = arbitrary_parquet_file_with_size_and_sequence_number(5, 10, 5, 5, 400);
        let contained_completely_within =
            arbitrary_parquet_file_with_size_and_sequence_number(6, 7, 6, 6, 500);
        let max_equals_min = arbitrary_parquet_file_with_size_and_sequence_number(3, 5, 3, 3, 400);
        let min_equals_max =
            arbitrary_parquet_file_with_size_and_sequence_number(10, 12, 10, 10, 500);

        // newest files
        // one and large file that does not overlapped with any other files
        //   --> will be in its own overlapped group after Compactor::overlapped_groups
        // Since it is small --> it will be grouped with the middle_overlapped_group after Compactor::group_small_contiguous_groups
        let alone = arbitrary_parquet_file_with_size_and_sequence_number(30, 35, 30, 30, 50);

        // very small files in  the middle
        // these 2 files overlap but small so no further split
        let another = arbitrary_parquet_file_with_size_and_sequence_number(13, 15, 13, 13, 10);
        let partial_overlap =
            arbitrary_parquet_file_with_size_and_sequence_number(14, 16, 14, 14, 20);

        // Given a bunch of files in an arbitrary order
        let all = vec![
            min_equals_max.clone(),
            overlaps_many.clone(),
            alone.clone(),
            another.clone(),
            max_equals_min.clone(),
            contained_completely_within.clone(),
            partial_overlap.clone(),
        ];

        let max_size_bytes = 1000;
        let max_file_count = 2;

        // Group into overlapped groups
        let overlapped_groups = Compactor::overlapped_groups(all, max_size_bytes, max_file_count);
        assert_eq!(overlapped_groups.len(), 4);

        // Must be 4
        assert_eq!(overlapped_groups.len(), 4);
        // 2 groups that do not overlapped wih other groups are always first
        // Group 0: `very small files in  the middle`
        assert_eq!(overlapped_groups[0].parquet_files.len(), 2);
        assert!(overlapped_groups[0].parquet_files.contains(&another)); // min_time = 13, min_seq = 14
        assert!(overlapped_groups[0]
            .parquet_files
            .contains(&partial_overlap)); // min_time = 14, min_seq = 13
                                          // Group 1: `newest files`
        assert_eq!(overlapped_groups[1].parquet_files.len(), 1);
        assert!(overlapped_groups[1].parquet_files.contains(&alone)); // min_time = 30
                                                                      // Next 2 groups are split from `oldest overlapped`
        assert_eq!(overlapped_groups[2].parquet_files.len(), 2);
        assert!(overlapped_groups[2].parquet_files.contains(&max_equals_min)); // min_time = 3, min_seq = 3
        assert!(overlapped_groups[2].parquet_files.contains(&overlaps_many)); // min_time = 5, min_seq = 5
        assert_eq!(overlapped_groups[3].parquet_files.len(), 2);
        assert!(overlapped_groups[3]
            .parquet_files
            .contains(&contained_completely_within)); // min_time = 6, min_seq = 6
        assert!(overlapped_groups[3].parquet_files.contains(&min_equals_max)); // min_time = 10, min_seq = 10

        // 4 input groups but 3 output groups. Group 0 and Group 1 above are further grouped together
        let groups = Compactor::group_small_contiguous_groups(
            overlapped_groups,
            max_size_bytes,
            max_file_count + 5, // increase limit file count ot get them grouped
        );
        assert_eq!(groups.len(), 3);

        // The output group is in time order
        // groups[0] the same as overlapped_groups[2]
        assert_eq!(groups[0].len(), 2);
        assert!(groups[0].contains(&max_equals_min)); // min_time = 3, min_seq = 3
        assert!(groups[0].contains(&overlaps_many)); // min_time = 5, min_seq = 5
                                                     // groups[1] the same as overlapped_groups[3]
        assert_eq!(groups[1].len(), 2);
        assert!(groups[1].contains(&contained_completely_within)); // min_time = 6, min_seq = 6
        assert!(groups[1].contains(&min_equals_max)); // min_time = 10, min_seq = 10
                                                      // group[2] is the result of grouping overlapped_groups[0] and overlapped_groups[1]
        assert_eq!(groups[2].len(), 3);
        assert!(groups[2].contains(&another)); // min_time = 13, min_seq = 1
        assert!(groups[2].contains(&partial_overlap)); // min_time = 14, min_seq = 13
        assert!(groups[2].contains(&alone)); // min_time = 30
    }

    #[tokio::test]
    async fn add_tombstones_to_parquet_files_in_groups() {
        let catalog = TestCatalog::new();

        let split_percentage = 90;
        let max_concurrent_compaction_size_bytes = 100000;
        let compaction_max_size_bytes = 100000;
        let compaction_max_file_count = 10;
        let compactor = Compactor::new(
            vec![],
            Arc::clone(&catalog.catalog),
            ParquetStorage::new(Arc::clone(&catalog.object_store)),
            Arc::new(Executor::new(1)),
            Arc::new(SystemProvider::new()),
            BackoffConfig::default(),
            CompactorConfig::new(
                split_percentage,
                max_concurrent_compaction_size_bytes,
                compaction_max_size_bytes,
                compaction_max_file_count,
            ),
            Arc::new(metric::Registry::new()),
        );

        let mut txn = catalog.catalog.start_transaction().await.unwrap();

        let kafka = txn.kafka_topics().create_or_get("foo").await.unwrap();
        let pool = txn.query_pools().create_or_get("foo").await.unwrap();
        let namespace = txn
            .namespaces()
            .create(
                "namespace_add_tombstones_to_parquet_files_in_groups",
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

        let p1 = ParquetFileParams {
            sequencer_id: sequencer.id,
            namespace_id: namespace.id,
            table_id: table.id,
            partition_id: partition.id,
            object_store_id: Uuid::new_v4(),
            min_sequence_number: SequenceNumber::new(4),
            max_sequence_number: SequenceNumber::new(100),
            min_time: Timestamp::new(1),
            max_time: Timestamp::new(5),
            file_size_bytes: 1337,
            row_count: 0,
            created_at: Timestamp::new(1),
            compaction_level: INITIAL_COMPACTION_LEVEL,
            column_set: ColumnSet::new([ColumnId::new(1), ColumnId::new(2)]),
        };

        let p2 = ParquetFileParams {
            object_store_id: Uuid::new_v4(),
            max_sequence_number: SequenceNumber::new(200),
            min_time: Timestamp::new(4),
            max_time: Timestamp::new(7),

            ..p1.clone()
        };
        let pf1 = txn.parquet_files().create(p1).await.unwrap();
        let pf2 = txn.parquet_files().create(p2).await.unwrap();

        let parquet_files = vec![pf1.clone(), pf2.clone()];
        let groups = vec![
            vec![], // empty group should get filtered out
            parquet_files,
        ];

        // Tombstone with a sequence number that's too low for both files, even though it overlaps
        // with the time range. Shouldn't be included in the group
        let _t1 = txn
            .tombstones()
            .create_or_get(
                table.id,
                sequencer.id,
                SequenceNumber::new(1),
                Timestamp::new(3),
                Timestamp::new(6),
                "whatevs",
            )
            .await
            .unwrap();

        // Tombstone with a sequence number too low for one file but not the other, time range
        // overlaps both files
        let t2 = txn
            .tombstones()
            .create_or_get(
                table.id,
                sequencer.id,
                SequenceNumber::new(150),
                Timestamp::new(3),
                Timestamp::new(6),
                "whatevs",
            )
            .await
            .unwrap();

        // Tombstone with a time range that only overlaps with one file
        let t3 = txn
            .tombstones()
            .create_or_get(
                table.id,
                sequencer.id,
                SequenceNumber::new(300),
                Timestamp::new(6),
                Timestamp::new(8),
                "whatevs",
            )
            .await
            .unwrap();

        // Tombstone with a time range that overlaps both files and has a sequence number large
        // enough for both files
        let t4 = txn
            .tombstones()
            .create_or_get(
                table.id,
                sequencer.id,
                SequenceNumber::new(400),
                Timestamp::new(1),
                Timestamp::new(10),
                "whatevs",
            )
            .await
            .unwrap();
        txn.commit().await.unwrap();

        let groups_with_tombstones = compactor.add_tombstones_to_groups(groups).await.unwrap();

        assert_eq!(groups_with_tombstones.len(), 1);
        let group_with_tombstones = &groups_with_tombstones[0];

        let actual_group_tombstone_ids = group_with_tombstones.tombstone_ids();
        assert_eq!(
            actual_group_tombstone_ids,
            HashSet::from([t2.id, t3.id, t4.id])
        );

        let actual_pf1 = group_with_tombstones
            .parquet_files
            .iter()
            .find(|pf| pf.parquet_file_id() == pf1.id)
            .unwrap();
        let mut actual_pf1_tombstones: Vec<_> =
            actual_pf1.tombstones().iter().map(|t| t.id).collect();
        actual_pf1_tombstones.sort();
        assert_eq!(actual_pf1_tombstones, &[t2.id, t4.id]);

        let actual_pf2 = group_with_tombstones
            .parquet_files
            .iter()
            .find(|pf| pf.parquet_file_id() == pf2.id)
            .unwrap();
        let mut actual_pf2_tombstones: Vec<_> =
            actual_pf2.tombstones().iter().map(|t| t.id).collect();
        actual_pf2_tombstones.sort();
        assert_eq!(actual_pf2_tombstones, &[t3.id, t4.id]);
    }

    #[tokio::test]
    async fn test_overlap_group_edge_case() {
        let one = arbitrary_parquet_file(0, 3);
        let two = arbitrary_parquet_file(5, 10);
        let three = arbitrary_parquet_file(2, 6);

        // Given a bunch of files in a particular order to exercise the algorithm:
        let all = vec![one, two, three];

        let groups = Compactor::overlapped_groups(all, TEST_MAX_SIZE_BYTES, TEST_MAX_FILE_COUNT);
        dbg!(&groups);

        // All should be in the same group.
        assert_eq!(groups.len(), 1);

        let one = arbitrary_parquet_file(0, 3);
        let two = arbitrary_parquet_file(5, 10);
        let three = arbitrary_parquet_file(2, 6);
        let four = arbitrary_parquet_file(8, 11);

        // Given a bunch of files in a particular order to exercise the algorithm:
        let all = vec![one, two, three, four];

        let groups = Compactor::overlapped_groups(all, TEST_MAX_SIZE_BYTES, TEST_MAX_FILE_COUNT);
        dbg!(&groups);

        // All should be in the same group.
        assert_eq!(groups.len(), 1);
    }

    #[tokio::test]
    async fn test_add_parquet_file_with_tombstones() {
        let catalog = TestCatalog::new();

        let split_percentage = 90;
        let max_concurrent_compaction_size_bytes = 100000;
        let compaction_max_size_bytes = 100000;
        let compaction_max_file_count = 10;
        let compactor = Compactor::new(
            vec![],
            Arc::clone(&catalog.catalog),
            ParquetStorage::new(Arc::clone(&catalog.object_store)),
            Arc::new(Executor::new(1)),
            Arc::new(SystemProvider::new()),
            BackoffConfig::default(),
            CompactorConfig::new(
                split_percentage,
                max_concurrent_compaction_size_bytes,
                compaction_max_size_bytes,
                compaction_max_file_count,
            ),
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
            // TODO: cpnsider to add level-1 tests before merging
            compaction_level: FILE_NON_OVERLAPPED_COMAPCTION_LEVEL,
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
            compaction_level: INITIAL_COMPACTION_LEVEL, // level of file of new writes
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
        let partition = txn
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

        let p1 = ParquetFileParams {
            sequencer_id: sequencer.id,
            namespace_id: namespace.id,
            table_id: table.id,
            partition_id: partition.id,
            object_store_id: Uuid::new_v4(),
            min_sequence_number: SequenceNumber::new(4),
            max_sequence_number: SequenceNumber::new(100),
            min_time: Timestamp::new(1),
            max_time: Timestamp::new(5),
            file_size_bytes: 1337,
            row_count: 0,
            compaction_level: INITIAL_COMPACTION_LEVEL, // level of file of new writes
            created_at: Timestamp::new(1),
            column_set: ColumnSet::new([ColumnId::new(1), ColumnId::new(2)]),
        };

        let p2 = ParquetFileParams {
            object_store_id: Uuid::new_v4(),
            ..p1.clone()
        };
        let p3 = ParquetFileParams {
            object_store_id: Uuid::new_v4(),
            partition_id: partition2.id,
            ..p1.clone()
        };
        let p4 = ParquetFileParams {
            object_store_id: Uuid::new_v4(),
            partition_id: partition3.id,
            file_size_bytes: 20000,
            ..p1.clone()
        };
        let p5 = ParquetFileParams {
            object_store_id: Uuid::new_v4(),
            partition_id: partition4.id,
            ..p1.clone()
        };
        let pf1 = txn.parquet_files().create(p1).await.unwrap();
        let _pf2 = txn.parquet_files().create(p2).await.unwrap();
        let pf3 = txn.parquet_files().create(p3).await.unwrap();
        let pf4 = txn.parquet_files().create(p4).await.unwrap();
        let pf5 = txn.parquet_files().create(p5).await.unwrap();
        txn.parquet_files()
            .update_to_level_2(&[pf5.id])
            .await
            .unwrap();
        txn.commit().await.unwrap();

        let split_percentage = 90;
        let max_concurrent_compaction_size_bytes = 100000;
        let compaction_max_size_bytes = 100000;
        let compaction_max_file_count = 10;
        let compactor = Compactor::new(
            vec![sequencer.id],
            Arc::clone(&catalog.catalog),
            ParquetStorage::new(Arc::clone(&catalog.object_store)),
            Arc::new(Executor::new(1)),
            Arc::new(SystemProvider::new()),
            BackoffConfig::default(),
            CompactorConfig::new(
                split_percentage,
                max_concurrent_compaction_size_bytes,
                compaction_max_size_bytes,
                compaction_max_file_count,
            ),
            Arc::new(metric::Registry::new()),
        );

        let candidates = compactor.partitions_to_compact().await.unwrap();
        let expect: Vec<PartitionCompactionCandidate> = vec![
            PartitionCompactionCandidate {
                sequencer_id: sequencer.id,
                table_id: table.id,
                partition_id: partition.id,
                namespace_id: namespace.id,
                level_0_file_count: 2,
                file_size_bytes: 1337 * 2,
                oldest_file: pf1.created_at,
            },
            PartitionCompactionCandidate {
                sequencer_id: sequencer.id,
                table_id: table.id,
                partition_id: partition3.id,
                namespace_id: namespace.id,
                level_0_file_count: 1,
                file_size_bytes: 20000,
                oldest_file: pf4.created_at,
            },
            PartitionCompactionCandidate {
                sequencer_id: sequencer.id,
                table_id: table.id,
                partition_id: partition2.id,
                namespace_id: namespace.id,
                level_0_file_count: 1,
                file_size_bytes: 1337,
                oldest_file: pf3.created_at,
            },
        ];
        assert_eq!(expect, candidates);
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
        let parquet_file1 = partition
            .create_parquet_file_with_min_max_size(&lp1, 1, 5, 1, 1000, 60000)
            .await
            .parquet_file;
        let parquet_file2 = partition
            .create_parquet_file_with_min_max_size(&lp2, 10, 15, 500, 1500, 60000)
            .await
            .parquet_file;

        let split_percentage = 90;
        let max_concurrent_compaction_size_bytes = 100000;
        let compaction_max_size_bytes = 100000;
        let compaction_max_file_count = 10;
        let compactor = Compactor::new(
            vec![sequencer.sequencer.id],
            Arc::clone(&catalog.catalog),
            ParquetStorage::new(Arc::clone(&catalog.object_store)),
            Arc::new(Executor::new(1)),
            Arc::new(SystemProvider::new()),
            BackoffConfig::default(),
            CompactorConfig::new(
                split_percentage,
                max_concurrent_compaction_size_bytes,
                compaction_max_size_bytes,
                compaction_max_file_count,
            ),
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
                compactor.config.compaction_max_size_bytes(),
            )
            .await
            .unwrap();

        // 2 sets based on 83% split rule = 100000 (max file size) / 120000 (total file size)
        assert_eq!(batches.len(), 2);

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
        let mut num_rows: usize = batches[0].iter().map(|rb| rb.num_rows()).sum();
        num_rows += batches[1].iter().map(|rb| rb.num_rows()).sum::<usize>();
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
}
