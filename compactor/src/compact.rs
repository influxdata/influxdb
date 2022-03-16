//! Data Points for the lifecycle of the Compactor

use crate::{
    query::QueryableParquetChunk,
    utils::{CatalogUpdate, CompactedData, ParquetFileWithTombstone},
};
use arrow::record_batch::RecordBatch;
use backoff::{Backoff, BackoffConfig};
use bytes::Bytes;
use data_types2::{
    ParquetFile, ParquetFileId, PartitionId, SequencerId, TableId, TablePartition, Timestamp,
    TombstoneId,
};
use datafusion::error::DataFusionError;
use iox_catalog::interface::{Catalog, Transaction};
use iox_object_store::ParquetFilePath;
use object_store::DynObjectStore;
use observability_deps::tracing::warn;
use parquet_file::metadata::{IoxMetadata, IoxParquetMetaData};
use query::{
    compute_sort_key_for_chunks, exec::ExecutorType, frontend::reorg::ReorgPlanner,
    util::compute_timenanosecond_min_max,
};
use query::{exec::Executor, QueryChunk};
use snafu::{ensure, ResultExt, Snafu};
use std::{
    cmp::{max, min},
    collections::{BTreeMap, HashSet},
    ops::DerefMut,
    sync::Arc,
};
use time::{Time, TimeProvider};
use uuid::Uuid;

/// 24 hours in nanoseconds
// TODO: make this a config parameter
pub const LEVEL_UPGRADE_THRESHOLD_NANO: i64 = 60 * 60 * 24 * 1000000000;

/// Percentage of least recent data we want to split to reduce compacting non-overlapped data
/// Must be between 0 and 100
// TODO: make this a config parameter
pub const SPLIT_PERCENTAGE: i64 = 90;

#[derive(Debug, Snafu)]
#[allow(missing_copy_implementations, missing_docs)]
pub enum Error {
    #[snafu(display(
        "Cannot compact parquet files for unassigned sequencer ID {}",
        sequencer_id
    ))]
    SequencerNotFound { sequencer_id: SequencerId },

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

    #[snafu(display(
        "Cannot compact parquet files for table ID {} due to an internal error: {}",
        table_id,
        source
    ))]
    TableNotFound {
        source: iox_catalog::interface::Error,
        table_id: TableId,
    },

    #[snafu(display(
        "Cannot compact parquet files for an non-existing table ID {}",
        table_id
    ))]
    TableNotExist { table_id: TableId },

    #[snafu(display("Error building compact logical plan  {}", source))]
    CompactLogicalPlan {
        source: query::frontend::reorg::Error,
    },

    #[snafu(display("Error building compact physical plan  {}", source))]
    CompactPhysicalPlan { source: DataFusionError },

    #[snafu(display("Error executing compact plan  {}", source))]
    ExecuteCompactPlan { source: DataFusionError },

    #[snafu(display("Error collecting stream yto record batches  {}", source))]
    CollectStream { source: DataFusionError },

    #[snafu(display("Could not convert row count to i64"))]
    RowCountTypeConversion { source: std::num::TryFromIntError },

    #[snafu(display("Error computing min and max for record batches: {}", source))]
    MinMax { source: query::util::Error },

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

    #[snafu(display("Error while requesting level 1 parquet files {}", source))]
    Level1 {
        source: iox_catalog::interface::Error,
    },

    #[snafu(display("Error converting the parquet stream to bytes: {}", source))]
    ConvertingToBytes {
        source: parquet_file::storage::Error,
    },

    #[snafu(display("Error writing to object store: {}", source))]
    WritingToObjectStore { source: object_store::Error },

    #[snafu(display("Error updating catalog {}", source))]
    Update {
        source: iox_catalog::interface::Error,
    },

    #[snafu(display("Error querying for tombstones for a parquet file {}", source))]
    QueryingTombstones {
        source: iox_catalog::interface::Error,
    },
}

/// A specialized `Error` for Compactor Data errors
pub type Result<T, E = Error> = std::result::Result<T, E>;

/// Data points need to run a compactor
#[derive(Debug)]
pub struct Compactor {
    /// Sequencers assigned to this compactor
    sequencers: Vec<SequencerId>,
    /// Object store for reading and persistence of parquet files
    object_store: Arc<DynObjectStore>,
    /// The global catalog for schema, parquet files and tombstones
    catalog: Arc<dyn Catalog>,

    /// Executor for running queries and compacting and persisting
    exec: Arc<Executor>,
    /// Time provider for all activities in this compactor
    pub time_provider: Arc<dyn TimeProvider>,

    /// Backoff config
    backoff_config: BackoffConfig,
}

impl Compactor {
    /// Initialize the Compactor Data
    pub fn new(
        sequencers: Vec<SequencerId>,
        catalog: Arc<dyn Catalog>,
        object_store: Arc<DynObjectStore>,
        exec: Arc<Executor>,
        time_provider: Arc<dyn TimeProvider>,
        backoff_config: BackoffConfig,
    ) -> Self {
        Self {
            sequencers,
            catalog,
            object_store,
            exec,
            time_provider,
            backoff_config,
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

    async fn level_1_parquet_files(
        &self,
        table_partition: TablePartition,
        min_time: Timestamp,
        max_time: Timestamp,
    ) -> Result<Vec<ParquetFile>> {
        let mut repos = self.catalog.repositories().await;

        repos
            .parquet_files()
            .level_1(table_partition, min_time, max_time)
            .await
            .context(Level1Snafu)
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

    // TODO: this function should be invoked in a backround loop
    /// Find and compact parquet files for a given sequencer
    pub async fn find_and_compact(&self, sequencer_id: SequencerId) -> Result<()> {
        if !self.sequencers.contains(&sequencer_id) {
            return Err(Error::SequencerNotFound { sequencer_id });
        }

        // Read level-0 parquet files
        let level_0_files = self.level_0_parquet_files(sequencer_id).await?;

        // If there are no level-0 parquet files, return because there's nothing to do
        if level_0_files.is_empty() {
            return Ok(());
        }

        // Group files into table partition
        let mut partitions = Self::group_parquet_files_into_partition(level_0_files);

        // Get level-1 files overlapped in time with level-0
        for (key, val) in &mut partitions.iter_mut() {
            let overall_min_time = val
                .iter()
                .map(|pf| pf.min_time)
                .min()
                .expect("The list of files was checked for emptiness above");
            let overall_max_time = val
                .iter()
                .map(|pf| pf.max_time)
                .max()
                .expect("The list of files was checked for emptiness above");
            let level_1_files = self
                .level_1_parquet_files(*key, overall_min_time, overall_max_time)
                .await?;
            val.extend(level_1_files);
        }

        // Each partition may contain non-overlapped files. Group overlapped files in each
        // partition. Once the groups are created, the partitions aren't needed.
        let overlapped_file_groups: Vec<Vec<ParquetFile>> = partitions
            .into_iter()
            .flat_map(|(_key, parquet_files)| Self::overlapped_groups(parquet_files))
            .collect();

        let mut repo = self.catalog.repositories().await;
        let tombstone_repo = repo.tombstones();
        let mut overlapped_file_with_tombstones_groups =
            Vec::with_capacity(overlapped_file_groups.len());

        // For each group of overlapping parquet files,
        for parquet_files in overlapped_file_groups {
            let mut parquet_files_with_tombstones = Vec::with_capacity(parquet_files.len());
            // Attach to each individual parquet file the relevant tombstones.
            for parquet_file in parquet_files {
                let tombstones = tombstone_repo
                    .list_tombstones_for_parquet_file(&parquet_file)
                    .await
                    .context(QueryingTombstonesSnafu)?;
                parquet_files_with_tombstones.push(ParquetFileWithTombstone {
                    data: Arc::new(parquet_file),
                    tombstones,
                });
            }

            overlapped_file_with_tombstones_groups.push(parquet_files_with_tombstones);
        }

        // Compact, persist,and update catalog accordingly for each overlaped file
        let mut tombstones: HashSet<TombstoneId> = HashSet::new();
        let mut upgrade_level_list: Vec<ParquetFileId> = vec![];
        for overlapped_files in overlapped_file_with_tombstones_groups {
            // keep tombstone ids
            tombstones = Self::union_tombstone_ids(tombstones, &overlapped_files);

            // Only one file without tombstones, no need to compact
            if overlapped_files.len() == 1 && overlapped_files[0].no_tombstones() {
                // If the file is old enough, it would not have any overlaps. Add it
                // to the list to be upgraded to level 1
                if overlapped_files[0].level_upgradable(Arc::clone(&self.time_provider)) {
                    upgrade_level_list.push(overlapped_files[0].parquet_file_id());
                }
                continue;
            }

            // Collect all the parquet file IDs, to be able to set their catalog records to be
            // deleted. These should already be unique, no need to dedupe.
            let original_parquet_file_ids: Vec<_> =
                overlapped_files.iter().map(|f| f.data.id).collect();

            // compact
            let split_compacted_files = self.compact(overlapped_files).await?;
            let mut catalog_update_info = Vec::with_capacity(split_compacted_files.len());

            for split_file in split_compacted_files {
                let CompactedData {
                    data,
                    meta,
                    tombstone_ids,
                } = split_file;

                let file_size_and_md = Backoff::new(&self.backoff_config)
                    .retry_all_errors("persist to object store", || {
                        Self::persist(&meta, data.clone(), Arc::clone(&self.object_store))
                    })
                    .await
                    .expect("retry forever");

                if let Some((file_size, md)) = file_size_and_md {
                    catalog_update_info.push(CatalogUpdate::new(
                        meta,
                        file_size,
                        md,
                        tombstone_ids,
                    ));
                }
            }
            let mut txn = self
                .catalog
                .start_transaction()
                .await
                .context(TransactionSnafu)?;

            self.update_catalog(
                catalog_update_info,
                original_parquet_file_ids,
                txn.deref_mut(),
            )
            .await?;

            txn.commit().await.context(TransactionCommitSnafu)?;
        }

        // Remove fully processed tombstones
        // TODO: #3953 - remove_fully_processed_tombstones(tombstones)

        // Upgrade old level-0 to level 1
        self.update_to_level_1(&upgrade_level_list).await?;

        Ok(())
    }

    // Group given parquet files into partition of the same (sequencer_id, table_id, partition_id)
    fn group_parquet_files_into_partition(
        parquet_files: Vec<ParquetFile>,
    ) -> BTreeMap<TablePartition, Vec<ParquetFile>> {
        let mut groups: BTreeMap<TablePartition, Vec<ParquetFile>> = BTreeMap::default();
        for file in parquet_files {
            let key = TablePartition::new(file.sequencer_id, file.table_id, file.partition_id);
            if let Some(val) = groups.get_mut(&key) {
                val.push(file);
            } else {
                groups.insert(key, vec![file]);
            }
        }

        groups
    }

    // Extract tombstones id
    fn union_tombstone_ids(
        mut tombstones: HashSet<TombstoneId>,
        parquet_with_tombstones: &[ParquetFileWithTombstone],
    ) -> HashSet<TombstoneId> {
        for file in parquet_with_tombstones {
            for id in file.tombstone_ids() {
                tombstones.insert(id);
            }
        }
        tombstones
    }

    // Compact given files. Assume the given files are overlaped in time.
    // If the assumption does not meet, we will spend time not to compact anything but put data
    // together
    // The output will include 2 CompactedData sets, one contains a large amount of data of
    // least recent time and the other has a small amount of data of most recent time. Each
    // will be persisted in its own file. The idea is when new writes come, they will
    // mostly overlap with the most recent data only.
    async fn compact(
        &self,
        overlapped_files: Vec<ParquetFileWithTombstone>,
    ) -> Result<Vec<CompactedData>> {
        let mut compacted = vec![];
        // Nothing to compact
        if overlapped_files.is_empty() {
            return Ok(compacted);
        }

        // One file without tombstone, no need to compact
        if overlapped_files.len() == 1 && overlapped_files[0].tombstones.is_empty() {
            return Ok(compacted);
        }

        // Collect all the tombstone IDs. One tombstone might be relevant to multiple parquet
        // files in this set, so dedupe here.
        let tombstone_ids: HashSet<_> = overlapped_files
            .iter()
            .flat_map(|f| f.tombstone_ids())
            .collect();

        // Keep the fist IoxMetadata to reuse same IDs and names
        let iox_metadata = overlapped_files[0].iox_metadata();

        // Verify if the given files belong to the same partition
        // Note: we can ignore this verification if we assume this is a must-have condition
        if let Some((head, tail)) = overlapped_files.split_first() {
            for file in tail {
                let is_same = file.data.sequencer_id == head.data.sequencer_id
                    && file.data.table_id == head.data.table_id
                    && file.data.partition_id == head.data.partition_id;

                ensure!(
                    is_same,
                    NotSamePartitionSnafu {
                        sequencer_id_1: head.data.sequencer_id,
                        table_id_1: head.data.table_id,
                        partition_id_1: head.data.partition_id,
                        sequencer_id_2: file.data.sequencer_id,
                        table_id_2: file.data.table_id,
                        partition_id_2: file.data.partition_id
                    }
                )
            }
        }

        // Convert the input files into QueryableParquetChunk for making query plan
        let query_chunks: Vec<_> = overlapped_files
            .iter()
            .map(|f| {
                f.to_queryable_parquet_chunk(
                    Arc::clone(&self.object_store),
                    iox_metadata.table_name.to_string(),
                    iox_metadata.partition_key.to_string(),
                )
            })
            .collect();

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

        // Compute the sorted output of the compacting result
        let sort_key = compute_sort_key_for_chunks(&merged_schema, &query_chunks);

        // Identify split time
        let split_time = Self::compute_split_time(min_time, max_time);

        // Build compact & split query plan
        let plan = ReorgPlanner::new()
            .split_plan(
                Arc::clone(&merged_schema),
                query_chunks,
                sort_key.clone(),
                split_time,
            )
            .context(CompactLogicalPlanSnafu)?;

        let ctx = self.exec.new_context(ExecutorType::Reorg);
        let physical_plan = ctx
            .prepare_plan(&plan)
            .await
            .context(CompactPhysicalPlanSnafu)?;

        // Run to collect each stream of the plan
        let stream_count = physical_plan.output_partitioning().partition_count();
        for i in 0..stream_count {
            let stream = ctx
                .execute_stream_partitioned(Arc::clone(&physical_plan), i)
                .await
                .context(ExecuteCompactPlanSnafu)?;

            // Collect compacted data into record batches for computing statistics
            let output_batches = datafusion::physical_plan::common::collect(stream)
                .await
                .context(CollectStreamSnafu)?;

            // Filter empty record batches
            let output_batches: Vec<_> = output_batches
                .into_iter()
                .filter(|b| b.num_rows() != 0)
                .collect();

            let row_count: usize = output_batches.iter().map(|b| b.num_rows()).sum();
            let row_count = row_count.try_into().context(RowCountTypeConversionSnafu)?;

            if row_count == 0 {
                continue;
            }

            // Compute min and max of the `time` column
            let (min_time, max_time) =
                compute_timenanosecond_min_max(&output_batches).context(MinMaxSnafu)?;

            let meta = IoxMetadata {
                object_store_id: Uuid::new_v4(),
                creation_timestamp: self.time_provider.now(),
                sequencer_id: iox_metadata.sequencer_id,
                namespace_id: iox_metadata.namespace_id,
                namespace_name: Arc::<str>::clone(&iox_metadata.namespace_name),
                table_id: iox_metadata.table_id,
                table_name: Arc::<str>::clone(&iox_metadata.table_name),
                partition_id: iox_metadata.partition_id,
                partition_key: Arc::<str>::clone(&iox_metadata.partition_key),
                time_of_first_write: Time::from_timestamp_nanos(min_time),
                time_of_last_write: Time::from_timestamp_nanos(max_time),
                min_sequence_number,
                max_sequence_number,
                row_count,
            };

            let compacted_data = CompactedData::new(output_batches, meta, tombstone_ids.clone());
            compacted.push(compacted_data);
        }

        Ok(compacted)
    }

    /// Write the given data to the given location in the given object storage.
    ///
    /// Returns the persisted file size (in bytes) and metadata if a file was created.
    async fn persist(
        metadata: &IoxMetadata,
        record_batches: Vec<RecordBatch>,
        object_store: Arc<DynObjectStore>,
    ) -> Result<Option<(usize, IoxParquetMetaData)>> {
        if record_batches.is_empty() {
            return Ok(None);
        }
        // All record batches have the same schema.
        let schema = record_batches
            .first()
            .expect("record_batches.is_empty was just checked")
            .schema();

        // Make a fake IOx object store to conform to the parquet file
        // interface, but note this isn't actually used to find parquet
        // paths to write to
        use iox_object_store::IoxObjectStore;
        let iox_object_store = Arc::new(IoxObjectStore::existing(
            Arc::clone(&object_store),
            IoxObjectStore::root_path_for(&*object_store, uuid::Uuid::new_v4()),
        ));

        let data = parquet_file::storage::Storage::new(Arc::clone(&iox_object_store))
            .parquet_bytes(record_batches, schema, metadata)
            .await
            .context(ConvertingToBytesSnafu)?;

        if data.is_empty() {
            return Ok(None);
        }

        // extract metadata
        let data = Arc::new(data);
        let md = IoxParquetMetaData::from_file_bytes(Arc::clone(&data))
            .expect("cannot read parquet file metadata")
            .expect("no metadata in parquet file");
        let data = Arc::try_unwrap(data).expect("dangling reference to data");

        let file_size = data.len();
        let bytes = Bytes::from(data);

        let path = ParquetFilePath::new_new_gen(
            metadata.namespace_id,
            metadata.table_id,
            metadata.sequencer_id,
            metadata.partition_id,
            metadata.object_store_id,
        );

        iox_object_store
            .put_parquet_file(&path, bytes)
            .await
            .context(WritingToObjectStoreSnafu)?;

        Ok(Some((file_size, md)))
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
            for tombstone_id in catalog_update.tombstone_ids {
                txn.processed_tombstones()
                    .create(parquet.id, tombstone_id)
                    .await
                    .context(UpdateSnafu)?;
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

    /// Given a list of parquet files that come from the same Table Partition, group files together
    /// if their (min_time, max_time) ranges overlap. Does not preserve or guarantee any ordering.
    fn overlapped_groups(mut parquet_files: Vec<ParquetFile>) -> Vec<Vec<ParquetFile>> {
        let mut groups = Vec::with_capacity(parquet_files.len());

        // While there are still files not in any group
        while !parquet_files.is_empty() {
            // Start a group containing only the first file
            let mut in_group = Vec::with_capacity(parquet_files.len());
            in_group.push(parquet_files.swap_remove(0));

            // Start a group for the remaining files that don't overlap
            let mut out_group = Vec::with_capacity(parquet_files.len());

            // Consider each file; if it overlaps with any file in the current group, add it to
            // the group. If not, add it to the non-overlapping group.
            for file in parquet_files {
                if in_group.iter().any(|group_file| {
                    (file.min_time <= group_file.min_time && file.max_time >= group_file.min_time)
                        || (file.min_time > group_file.min_time
                            && file.min_time <= group_file.max_time)
                }) {
                    in_group.push(file);
                } else {
                    out_group.push(file);
                }
            }

            groups.push(in_group);
            parquet_files = out_group;
        }

        groups
    }

    // Compute time to split data
    fn compute_split_time(min_time: i64, max_time: i64) -> i64 {
        min_time + (max_time - min_time) * SPLIT_PERCENTAGE / 100
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_util::assert_batches_sorted_eq;
    use data_types2::{KafkaPartition, NamespaceId, ParquetFileParams, SequenceNumber};
    use futures::{stream, StreamExt, TryStreamExt};
    use iox_tests::util::TestCatalog;
    use object_store::path::Path;
    use query::test::{raw_data, TestChunk};
    use time::SystemProvider;

    #[tokio::test]
    async fn test_compact_one_file() {
        let catalog = TestCatalog::new();

        let lp = vec![
            "table,tag1=WA field_int=1000 8000",
            "table,tag1=VT field_int=10 10000",
            "table,tag1=UT field_int=70 20000",
        ]
        .join("\n");
        let ns = catalog.create_namespace("ns").await;
        let sequencer = ns.create_sequencer(1).await;
        let table = ns.create_table("table").await;
        let parquet_file = table
            .with_sequencer(&sequencer)
            .create_partition("part")
            .await
            .create_parquet_file_with_min_max(&lp, 1, 1, 8000, 20000)
            .await
            .parquet_file
            .clone();

        let compactor = Compactor {
            sequencers: vec![sequencer.sequencer.id],
            object_store: Arc::clone(&catalog.object_store),
            catalog: Arc::clone(&catalog.catalog),
            exec: Arc::new(Executor::new(1)),
            time_provider: Arc::new(SystemProvider::new()),
            backoff_config: BackoffConfig::default(),
        };

        // ------------------------------------------------
        // no files provided
        let result = compactor.compact(vec![]).await.unwrap();
        assert!(result.is_empty());

        // ------------------------------------------------
        // File without tombstones
        let mut pf = ParquetFileWithTombstone {
            data: Arc::new(parquet_file),
            tombstones: vec![],
        };
        // Nothing compacted for one file without tombstones
        let result = compactor.compact(vec![pf.clone()]).await.unwrap();
        assert!(result.is_empty());

        // ------------------------------------------------
        // Let add a tombstone
        let tombstone = table
            .with_sequencer(&sequencer)
            .create_tombstone(20, 6000, 12000, "tag1=VT")
            .await;
        pf.add_tombstones(vec![tombstone.tombstone.clone()]);
        // should have compacted data
        let batches = compactor.compact(vec![pf]).await.unwrap();
        // 2 sets based on the split rule
        assert_eq!(batches.len(), 2);
        // Data: row tag1=VT was removed
        // first set contains least recent data
        assert_batches_sorted_eq!(
            &[
                "+-----------+------+-----------------------------+",
                "| field_int | tag1 | time                        |",
                "+-----------+------+-----------------------------+",
                "| 1000      | WA   | 1970-01-01T00:00:00.000008Z |",
                "+-----------+------+-----------------------------+",
            ],
            &batches[0].data
        );
        // second set contains most recent data
        assert_batches_sorted_eq!(
            &[
                "+-----------+------+-----------------------------+",
                "| field_int | tag1 | time                        |",
                "+-----------+------+-----------------------------+",
                "| 70        | UT   | 1970-01-01T00:00:00.000020Z |",
                "+-----------+------+-----------------------------+",
            ],
            &batches[1].data
        );
    }

    #[tokio::test]
    async fn test_compact_two_files() {
        let catalog = TestCatalog::new();

        let lp1 = vec![
            "table,tag1=WA field_int=1000 8000", // will be eliminated due to duplicate
            "table,tag1=VT field_int=10 10000",  // will be deleted
            "table,tag1=UT field_int=70 20000",
        ]
        .join("\n");

        let lp2 = vec![
            "table,tag1=WA field_int=1500 8000", // latest duplicate and kept
            "table,tag1=VT field_int=10 6000",
            "table,tag1=UT field_int=270 25000",
        ]
        .join("\n");

        let ns = catalog.create_namespace("ns").await;
        let sequencer = ns.create_sequencer(1).await;
        let table = ns.create_table("table").await;
        let partition = table
            .with_sequencer(&sequencer)
            .create_partition("part")
            .await;
        let parquet_file1 = partition
            .create_parquet_file_with_min_max(&lp1, 1, 5, 8000, 20000)
            .await
            .parquet_file
            .clone();
        let parquet_file2 = partition
            .create_parquet_file_with_min_max(&lp2, 10, 15, 6000, 25000)
            .await
            .parquet_file
            .clone();

        let compactor = Compactor {
            sequencers: vec![sequencer.sequencer.id],
            object_store: Arc::clone(&catalog.object_store),
            catalog: Arc::clone(&catalog.catalog),
            exec: Arc::new(Executor::new(1)),
            time_provider: Arc::new(SystemProvider::new()),
            backoff_config: BackoffConfig::default(),
        };

        // File 1 with tombstone
        let tombstone = table
            .with_sequencer(&sequencer)
            .create_tombstone(6, 6000, 12000, "tag1=VT")
            .await;
        let pf1 = ParquetFileWithTombstone {
            data: Arc::new(parquet_file1),
            tombstones: vec![tombstone.tombstone.clone()],
        };
        // File 2 without tombstones
        let pf2 = ParquetFileWithTombstone {
            data: Arc::new(parquet_file2),
            tombstones: vec![],
        };

        // Compact them
        let batches = compactor.compact(vec![pf1, pf2]).await.unwrap();
        // 2 sets based on the split rule
        assert_eq!(batches.len(), 2);

        // Data: Should have 4 rows left
        // first set contains least recent 3 rows
        assert_batches_sorted_eq!(
            &[
                "+-----------+------+-----------------------------+",
                "| field_int | tag1 | time                        |",
                "+-----------+------+-----------------------------+",
                "| 10        | VT   | 1970-01-01T00:00:00.000006Z |",
                "| 1500      | WA   | 1970-01-01T00:00:00.000008Z |",
                "| 70        | UT   | 1970-01-01T00:00:00.000020Z |",
                "+-----------+------+-----------------------------+",
            ],
            &batches[0].data
        );
        // second set contains most recent one row
        assert_batches_sorted_eq!(
            &[
                "+-----------+------+-----------------------------+",
                "| field_int | tag1 | time                        |",
                "+-----------+------+-----------------------------+",
                "| 270       | UT   | 1970-01-01T00:00:00.000025Z |",
                "+-----------+------+-----------------------------+",
            ],
            &batches[1].data
        );
    }

    #[tokio::test]
    async fn test_compact_three_files_different_cols() {
        let catalog = TestCatalog::new();

        let lp1 = vec![
            "table,tag1=WA field_int=1000 8000", // will be eliminated due to duplicate
            "table,tag1=VT field_int=10 10000",  // will be deleted
            "table,tag1=UT field_int=70 20000",
        ]
        .join("\n");

        let lp2 = vec![
            "table,tag1=WA field_int=1500 8000", // latest duplicate and kept
            "table,tag1=VT field_int=10 6000",
            "table,tag1=UT field_int=270 25000",
        ]
        .join("\n");

        let lp3 = vec![
            "table,tag2=WA,tag3=10 field_int=1500 8000",
            "table,tag2=VT,tag3=20 field_int=10 6000",
        ]
        .join("\n");

        let ns = catalog.create_namespace("ns").await;
        let sequencer = ns.create_sequencer(1).await;
        let table = ns.create_table("table").await;
        let partition = table
            .with_sequencer(&sequencer)
            .create_partition("part")
            .await;
        // Sequence numbers are important here.
        // Time/sequence order from small to large: parquet_file_1, parquet_file_2, parquet_file_3
        let parquet_file1 = partition
            .create_parquet_file_with_min_max(&lp1, 1, 5, 8000, 20000)
            .await
            .parquet_file
            .clone();
        let parquet_file2 = partition
            .create_parquet_file_with_min_max(&lp2, 10, 15, 6000, 25000)
            .await
            .parquet_file
            .clone();
        let parquet_file3 = partition
            .create_parquet_file_with_min_max(&lp3, 20, 25, 6000, 8000)
            .await
            .parquet_file
            .clone();

        let compactor = Compactor {
            sequencers: vec![sequencer.sequencer.id],
            object_store: Arc::clone(&catalog.object_store),
            catalog: Arc::clone(&catalog.catalog),
            exec: Arc::new(Executor::new(1)),
            time_provider: Arc::new(SystemProvider::new()),
            backoff_config: BackoffConfig::default(),
        };

        // File 1 with tombstone
        let tombstone = table
            .with_sequencer(&sequencer)
            .create_tombstone(6, 6000, 12000, "tag1=VT")
            .await;
        let pf1 = ParquetFileWithTombstone {
            data: Arc::new(parquet_file1),
            tombstones: vec![tombstone.tombstone.clone()],
        };
        // File 2 without tombstones
        let pf2 = ParquetFileWithTombstone {
            data: Arc::new(parquet_file2),
            tombstones: vec![],
        };
        // File 3 without tombstones
        let pf3 = ParquetFileWithTombstone {
            data: Arc::new(parquet_file3),
            tombstones: vec![],
        };

        // Compact them
        let batches = compactor
            .compact(vec![pf1.clone(), pf2.clone(), pf3.clone()])
            .await
            .unwrap();

        // 2 sets based on the split rule
        assert_eq!(batches.len(), 2);

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
            &batches[0].data
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
            &batches[1].data
        );
    }

    /// A test utility function to make minimially-viable ParquetFile records with particular
    /// min/max times. Does not involve the catalog at all.
    fn arbitrary_parquet_file(min_time: i64, max_time: i64) -> ParquetFile {
        arbitrary_parquet_file_with_creation_time(min_time, max_time, 1)
    }

    fn arbitrary_parquet_file_with_creation_time(
        min_time: i64,
        max_time: i64,
        created_at: i64,
    ) -> ParquetFile {
        ParquetFile {
            id: ParquetFileId::new(0),
            sequencer_id: SequencerId::new(0),
            table_id: TableId::new(0),
            partition_id: PartitionId::new(0),
            object_store_id: Uuid::new_v4(),
            min_sequence_number: SequenceNumber::new(0),
            max_sequence_number: SequenceNumber::new(1),
            min_time: Timestamp::new(min_time),
            max_time: Timestamp::new(max_time),
            to_delete: false,
            file_size_bytes: 0,
            parquet_metadata: vec![],
            row_count: 0,
            compaction_level: 0,
            created_at: Timestamp::new(created_at),
        }
    }

    #[test]
    fn test_level_upgradable() {
        let time = Arc::new(SystemProvider::new());
        // file older than one day
        let pf_old = arbitrary_parquet_file_with_creation_time(1, 10, 1);
        let pt_old = ParquetFileWithTombstone {
            data: Arc::new(pf_old),
            tombstones: vec![],
        };
        // upgradable
        assert!(pt_old.level_upgradable(Arc::<time::SystemProvider>::clone(&time)));

        // file is created today
        let pf_new = arbitrary_parquet_file_with_creation_time(1, 10, time.now().timestamp_nanos());
        let pt_new = ParquetFileWithTombstone {
            data: Arc::new(pf_new),
            tombstones: vec![],
        };
        // not upgradable
        assert!(!pt_new.level_upgradable(Arc::<time::SystemProvider>::clone(&time)));
    }

    #[test]
    fn test_overlapped_groups_no_overlap() {
        // Given two files that don't overlap,
        let pf1 = arbitrary_parquet_file(1, 2);
        let pf2 = arbitrary_parquet_file(3, 4);

        let groups = Compactor::overlapped_groups(vec![pf1.clone(), pf2.clone()]);

        // They should each be in their own group
        assert_eq!(groups, vec![vec![pf1], vec![pf2]]);
    }

    #[test]
    fn test_overlapped_groups_with_overlap() {
        // Given two files that do overlap,
        let pf1 = arbitrary_parquet_file(1, 3);
        let pf2 = arbitrary_parquet_file(2, 4);

        let groups = Compactor::overlapped_groups(vec![pf1.clone(), pf2.clone()]);

        // They should be in one group (order not guaranteed)
        assert_eq!(groups.len(), 1, "There should have only been one group");

        let group = &groups[0];
        assert_eq!(
            group.len(),
            2,
            "The one group should have contained 2 items"
        );
        assert!(group.contains(&pf1));
        assert!(group.contains(&pf2));
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

        let mut groups = Compactor::overlapped_groups(all);
        dbg!(&groups);

        assert_eq!(groups.len(), 3);

        // Order of the groups is not guaranteed; sort by length of group so we can test membership
        groups.sort_by_key(|g| g.len());

        let alone_group = &groups[0];
        assert_eq!(alone_group.len(), 1);
        assert!(
            alone_group.contains(&alone),
            "Actually contains: {:#?}",
            alone_group
        );

        let another_group = &groups[1];
        assert_eq!(another_group.len(), 2);
        assert!(
            another_group.contains(&another),
            "Actually contains: {:#?}",
            another_group
        );
        assert!(
            another_group.contains(&partial_overlap),
            "Actually contains: {:#?}",
            another_group
        );

        let many_group = &groups[2];
        assert_eq!(many_group.len(), 4);
        assert!(
            many_group.contains(&overlaps_many),
            "Actually contains: {:#?}",
            many_group
        );
        assert!(
            many_group.contains(&contained_completely_within),
            "Actually contains: {:#?}",
            many_group
        );
        assert!(
            many_group.contains(&max_equals_min),
            "Actually contains: {:#?}",
            many_group
        );
        assert!(
            many_group.contains(&min_equals_max),
            "Actually contains: {:#?}",
            many_group
        );
    }

    async fn list_all(object_store: &DynObjectStore) -> Result<Vec<Path>, object_store::Error> {
        object_store
            .list(None)
            .await?
            .map_ok(|v| stream::iter(v).map(Ok))
            .try_flatten()
            .try_collect()
            .await
    }

    #[tokio::test]
    async fn persist_adds_to_object_store() {
        let catalog = TestCatalog::new();

        let compactor = Compactor {
            sequencers: vec![],
            object_store: Arc::clone(&catalog.object_store),
            catalog: Arc::clone(&catalog.catalog),
            exec: Arc::new(Executor::new(1)),
            time_provider: Arc::new(SystemProvider::new()),
            backoff_config: BackoffConfig::default(),
        };

        let table_id = TableId::new(3);
        let sequencer_id = SequencerId::new(2);

        let meta = IoxMetadata {
            object_store_id: Uuid::new_v4(),
            creation_timestamp: compactor.time_provider.now(),
            namespace_id: NamespaceId::new(1),
            namespace_name: "mydata".into(),
            sequencer_id,
            table_id,
            table_name: "temperature".into(),
            partition_id: PartitionId::new(4),
            partition_key: "somehour".into(),
            time_of_first_write: compactor.time_provider.now(),
            time_of_last_write: compactor.time_provider.now(),
            min_sequence_number: SequenceNumber::new(5),
            max_sequence_number: SequenceNumber::new(6),
            row_count: 3,
        };

        let chunk1 = Arc::new(
            TestChunk::new("t")
                .with_id(1)
                .with_time_column() //_with_full_stats(
                .with_tag_column("tag1")
                .with_i64_field_column("field_int")
                .with_three_rows_of_data(),
        );
        let data = raw_data(&[chunk1]).await;

        let t1 = catalog
            .catalog
            .repositories()
            .await
            .tombstones()
            .create_or_get(
                table_id,
                sequencer_id,
                SequenceNumber::new(1),
                Timestamp::new(1),
                Timestamp::new(1),
                "whatevs",
            )
            .await
            .unwrap();

        let compacted_data = CompactedData::new(data, meta, HashSet::from([t1.id]));

        Compactor::persist(
            &compacted_data.meta,
            compacted_data.data,
            Arc::clone(&compactor.object_store),
        )
        .await
        .unwrap();

        let object_store_files = list_all(&*compactor.object_store).await.unwrap();
        assert_eq!(object_store_files.len(), 1);
    }

    #[tokio::test]
    async fn test_add_parquet_file_with_tombstones() {
        let catalog = TestCatalog::new();

        let compactor = Compactor {
            sequencers: vec![],
            object_store: Arc::clone(&catalog.object_store),
            catalog: Arc::clone(&catalog.catalog),
            exec: Arc::new(Executor::new(1)),
            time_provider: Arc::new(SystemProvider::new()),
            backoff_config: BackoffConfig::default(),
        };

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
            .create_or_get("one", sequencer.id, table.id)
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
            time_of_first_write: compactor.time_provider.now(),
            time_of_last_write: compactor.time_provider.now(),
            min_sequence_number: SequenceNumber::new(5),
            max_sequence_number: SequenceNumber::new(6),
            row_count: 3,
        };

        // Prepare metadata in form of ParquetFileParams to get added with tombstone
        let parquet = ParquetFileParams {
            sequencer_id: sequencer.id,
            table_id: table.id,
            partition_id: partition.id,
            object_store_id: Uuid::new_v4(),
            min_sequence_number: SequenceNumber::new(4),
            max_sequence_number: SequenceNumber::new(10),
            min_time,
            max_time,
            file_size_bytes: 1337,
            parquet_metadata: b"md1".to_vec(),
            row_count: 0,
            created_at: Timestamp::new(1),
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
            tombstone_ids: HashSet::from([t1.id, t2.id]),
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
            tombstone_ids: HashSet::from([t3.id, t1.id]),
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
            tombstone_ids: HashSet::from([t3.id]),
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
            tombstone_ids: HashSet::from([t4.id]),
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
}
