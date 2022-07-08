//! Data Points for the lifecycle of the Compactor

use crate::{
    handler::CompactorConfig,
    query::QueryableParquetChunk,
    utils::{CatalogUpdate, CompactedData, GroupWithTombstones, ParquetFileWithTombstone},
};
use backoff::BackoffConfig;
use data_types::{
    Namespace, NamespaceId, ParquetFile, ParquetFileId, Partition, PartitionId, SequencerId, Table,
    TableId, TableSchema, Timestamp, Tombstone, TombstoneId, FILE_NON_OVERLAPPED_COMPACTION_LEVEL,
    INITIAL_COMPACTION_LEVEL,
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
    catalog: Arc<dyn Catalog>,

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
                    == INITIAL_COMPACTION_LEVEL
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
                // TODO before merging: this can either level-1 or level-2
                compaction_level: FILE_NON_OVERLAPPED_COMPACTION_LEVEL,
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
        // Should have 1 tombstone
        let count = catalog.count_tombstones_for_table(table.table.id).await;
        assert_eq!(count, 1);

        // ------------------------------------------------
        // Compact
        let split_percentage = 90;
        let max_concurrent_compaction_size_bytes = 100000;
        let compaction_max_size_bytes = 100000;
        let compaction_max_file_count = 10;
        let compaction_max_desired_file_size_bytes = 30000;
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
                compaction_max_desired_file_size_bytes,
            ),
            Arc::new(metric::Registry::new()),
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
            (2, FILE_NON_OVERLAPPED_COMPACTION_LEVEL)
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
        let compaction_max_desired_file_size_bytes = 30000;
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
                compaction_max_desired_file_size_bytes,
            ),
            Arc::new(metric::Registry::new()),
        );

        // parquet files that are all in the same partition and should all end up in the same
        // compacted file

        // pf1 does not overlap with any and is very large
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
        // pf2 overlaps with pf3
        partition
            .create_parquet_file_with_min_max_size_and_creation_time(
                &lp2,
                4,
                5,
                8000,
                20000,
                100, // small file
                time.now().timestamp_nanos(),
            )
            .await;
        // pf3 overlaps with pf2
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

        // pf4 does not overlap with any but is small
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
        // 1 newly created level-FILE_NON_OVERLAPPED_COMAPCTION_LEVEL file as the result of
        // compaction
        assert_eq!(
            (files[0].id.get(), files[0].compaction_level),
            (5, FILE_NON_OVERLAPPED_COMPACTION_LEVEL)
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
                "+-----------+------+------+------+--------------------------------+",
            ],
            &batches
        );
        // No more files
        assert!(files.is_empty());
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
        let parquet_file = partition
            .create_parquet_file_with_min_max(&lp, 1, 1, 8000, 20000)
            .await
            .parquet_file;

        let split_percentage = 90;
        let max_concurrent_compaction_size_bytes = 100000;
        let compaction_max_size_bytes = 100000;
        let compaction_max_file_count = 10;
        let compaction_max_desired_file_size_bytes = 30000;
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
                compaction_max_desired_file_size_bytes,
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
        let compaction_max_desired_file_size_bytes = 30000;
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
                compaction_max_desired_file_size_bytes,
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
        let compaction_max_desired_file_size_bytes = 30000;
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
                compaction_max_desired_file_size_bytes,
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

    #[tokio::test]
    async fn test_add_parquet_file_with_tombstones() {
        let catalog = TestCatalog::new();

        let split_percentage = 90;
        let max_concurrent_compaction_size_bytes = 100000;
        let compaction_max_size_bytes = 100000;
        let compaction_max_file_count = 10;
        let compaction_max_desired_file_size_bytes = 30000;
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
                compaction_max_desired_file_size_bytes,
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
            compaction_level: FILE_NON_OVERLAPPED_COMPACTION_LEVEL,
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
        let compaction_max_desired_file_size_bytes = 30000;
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
                compaction_max_desired_file_size_bytes,
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
        let compaction_max_desired_file_size_bytes = 30000;
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
                compaction_max_desired_file_size_bytes,
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
}
