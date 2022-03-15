//! Data Points for the lifecycle of the Compactor

use crate::{
    query::QueryableParquetChunk,
    utils::{CompactedData, ParquetFileWithTombstone},
};
use backoff::BackoffConfig;
use data_types2::{
    ParquetFile, ParquetFileId, PartitionId, SequencerId, TableId, TablePartition, Timestamp,
    TombstoneId,
};
use datafusion::error::DataFusionError;
use iox_catalog::interface::Catalog;
use object_store::DynObjectStore;
use observability_deps::tracing::warn;
use parquet_file::metadata::IoxMetadata;
use query::exec::Executor;
use query::{
    compute_sort_key_for_chunks, exec::ExecutorType, frontend::reorg::ReorgPlanner,
    util::compute_timenanosecond_min_max,
};
use snafu::{ensure, ResultExt, Snafu};
use std::{
    cmp::{max, min},
    collections::{BTreeMap, HashSet},
    sync::Arc,
};
use time::{Time, TimeProvider};
use uuid::Uuid;

/// 24 hours in nanoseconds
// TODO: make this a config parameter
pub const LEVEL_UPGRADE_THRESHOLD_NANO: u64 = 60 * 60 * 24 * 1000000000;

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

    #[snafu(display("Error while requesting level 0 parquet files {}", source))]
    Level0 {
        source: iox_catalog::interface::Error,
    },

    #[snafu(display("Error while requesting level 1 parquet files {}", source))]
    Level1 {
        source: iox_catalog::interface::Error,
    },

    #[snafu(display("Error updating catalog {}", source))]
    Update {
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

        // Each partition may contain non-overlapped files,
        // groups overlapped files in each partition
        let mut overlapped_file_groups = vec![];
        for _val in partitions.values_mut() {
            let overlapped_files: Vec<Vec<ParquetFile>> = vec![]; // TODO: #3949
            overlapped_file_groups.extend(overlapped_files);
        }

        // Find and attach tombstones to each parquet file
        let mut overlapped_file_with_tombstones_groups = vec![];
        for _files in overlapped_file_groups {
            let overlapped_file_with_tombstones: Vec<ParquetFileWithTombstone> = vec![]; // TODO: #3948
            overlapped_file_with_tombstones_groups.push(overlapped_file_with_tombstones);
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
                if overlapped_files[0].level_upgradable() {
                    upgrade_level_list.push(overlapped_files[0].parquet_file_id());
                }
                continue;
            }

            // compact
            let _compacted_data = self.compact(overlapped_files).await;

            // split the compacted data into 2 files 90/10
            let output_parquet_files: Vec<ParquetFile> = vec![]; // TODO: #3999

            for _file in output_parquet_files {
                // persist the file
                // TODO: #3951

                // update the catalog
                // TODO: #3952
            }
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
    async fn compact(
        &self,
        overlapped_files: Vec<ParquetFileWithTombstone>,
    ) -> Result<Option<CompactedData>> {
        // Nothing to compact
        if overlapped_files.is_empty() {
            return Ok(None);
        }

        // One file without tombstone, no need to compact
        if overlapped_files.len() == 1 && overlapped_files[0].tombstones.is_empty() {
            return Ok(None);
        }

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

        // Compute min & max sequence numbers
        // unwrap here will work becasue the len of the query_chunks already >= 1
        let (head, tail) = query_chunks.split_first().unwrap();
        let mut min_sequence_number = head.min_sequence_number();
        let mut max_sequence_number = head.max_sequence_number();
        for c in tail {
            min_sequence_number = min(min_sequence_number, c.min_sequence_number());
            max_sequence_number = max(max_sequence_number, c.max_sequence_number());
        }

        // Merge schema of the compacting chunks
        let merged_schema = QueryableParquetChunk::merge_schemas(&query_chunks);

        // Compute the sorted output of the compacting result
        let sort_key = compute_sort_key_for_chunks(&merged_schema, &query_chunks);

        // Build compact query plan
        let plan = ReorgPlanner::new()
            .compact_plan(
                Arc::clone(&merged_schema),
                query_chunks.into_iter().map(Arc::new),
                sort_key.clone(),
            )
            .context(CompactLogicalPlanSnafu)?;
        let ctx = self.exec.new_context(ExecutorType::Reorg);
        let physical_plan = ctx
            .prepare_plan(&plan)
            .await
            .context(CompactPhysicalPlanSnafu)?;

        // Run the plan
        let stream = ctx
            .execute_stream(physical_plan)
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
            return Ok(None);
        }

        // Compute min and max of the `time` column
        let (min_time, max_time) =
            compute_timenanosecond_min_max(&output_batches).context(MinMaxSnafu)?;

        let meta = IoxMetadata {
            object_store_id: Uuid::new_v4(),
            creation_timestamp: self.time_provider.now(),
            sequencer_id: iox_metadata.sequencer_id,
            namespace_id: iox_metadata.namespace_id,
            namespace_name: iox_metadata.namespace_name,
            table_id: iox_metadata.table_id,
            table_name: iox_metadata.table_name,
            partition_id: iox_metadata.partition_id,
            partition_key: iox_metadata.partition_key,
            time_of_first_write: Time::from_timestamp_nanos(min_time),
            time_of_last_write: Time::from_timestamp_nanos(max_time),
            min_sequence_number,
            max_sequence_number,
            row_count,
        };

        let compacted_data = CompactedData::new(output_batches, meta);

        Ok(Some(compacted_data))
    }
}

#[cfg(test)]
mod tests {

    use super::*;
    use arrow_util::assert_batches_sorted_eq;
    use iox_tests::util::TestCatalog;
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
            .create_parquet_file(&lp)
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
        assert!(result.is_none());

        // ------------------------------------------------
        // File without tombstones
        let mut pf = ParquetFileWithTombstone {
            data: Arc::new(parquet_file),
            tombstones: vec![],
        };
        // Nothing compacted for one file without tombstones
        let result = compactor.compact(vec![pf.clone()]).await.unwrap();
        assert!(result.is_none());

        // ------------------------------------------------
        // Let add a tombstone
        let tombstone = table
            .with_sequencer(&sequencer)
            .create_tombstone(20, 6000, 12000, "tag1=VT")
            .await;
        pf.add_tombstones(vec![tombstone.tombstone.clone()]);
        // should have compacted data
        let batches = compactor.compact(vec![pf]).await.unwrap().unwrap().data;
        // one row tag1=VT was removed
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
            .create_parquet_file_with_sequence_numbers(&lp1, 1, 5)
            .await
            .parquet_file
            .clone();
        let parquet_file2 = partition
            .create_parquet_file_with_sequence_numbers(&lp2, 10, 15)
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
        let batches = compactor
            .compact(vec![pf1, pf2])
            .await
            .unwrap()
            .unwrap()
            .data;
        // Should have 4 rows left
        assert_batches_sorted_eq!(
            &[
                "+-----------+------+-----------------------------+",
                "| field_int | tag1 | time                        |",
                "+-----------+------+-----------------------------+",
                "| 10        | VT   | 1970-01-01T00:00:00.000006Z |",
                "| 1500      | WA   | 1970-01-01T00:00:00.000008Z |",
                "| 270       | UT   | 1970-01-01T00:00:00.000025Z |",
                "| 70        | UT   | 1970-01-01T00:00:00.000020Z |",
                "+-----------+------+-----------------------------+",
            ],
            &batches
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
            .create_parquet_file_with_sequence_numbers(&lp1, 1, 5)
            .await
            .parquet_file
            .clone();
        let parquet_file2 = partition
            .create_parquet_file_with_sequence_numbers(&lp2, 10, 15)
            .await
            .parquet_file
            .clone();
        let parquet_file3 = partition
            .create_parquet_file_with_sequence_numbers(&lp3, 20, 25)
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
            .unwrap()
            .unwrap()
            .data;
        // Should have 6 rows
        let expected = vec![
            "+-----------+------+------+------+-----------------------------+",
            "| field_int | tag1 | tag2 | tag3 | time                        |",
            "+-----------+------+------+------+-----------------------------+",
            "| 10        |      | VT   | 20   | 1970-01-01T00:00:00.000006Z |",
            "| 10        | VT   |      |      | 1970-01-01T00:00:00.000006Z |",
            "| 1500      |      | WA   | 10   | 1970-01-01T00:00:00.000008Z |",
            "| 1500      | WA   |      |      | 1970-01-01T00:00:00.000008Z |",
            "| 270       | UT   |      |      | 1970-01-01T00:00:00.000025Z |",
            "| 70        | UT   |      |      | 1970-01-01T00:00:00.000020Z |",
            "+-----------+------+------+------+-----------------------------+",
        ];
        assert_batches_sorted_eq!(&expected, &batches);

        // Make a vector of different file order but the result is still the same
        // becasue the actual order for deduplication does not rely on their order in the vector
        // Compact them
        let batches = compactor
            .compact(vec![pf2, pf3, pf1]) // different order in the vector
            .await
            .unwrap()
            .unwrap()
            .data;
        // Should have 6 rows
        assert_batches_sorted_eq!(&expected, &batches);
    }
}
