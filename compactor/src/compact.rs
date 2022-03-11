//! Data Points for the lifecycle of the Compactor

use std::{
    collections::{BTreeMap, HashSet},
    sync::Arc,
};

use backoff::BackoffConfig;
use data_types2::{
    ParquetFile, ParquetFileId, PartitionId, SequencerId, TableId, Tombstone, TombstoneId,
};
use iox_catalog::interface::Catalog;
use object_store::ObjectStore;
use query::exec::Executor;
use snafu::Snafu;

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
}

/// A specialized `Error` for Compactor Data errors
pub type Result<T, E = Error> = std::result::Result<T, E>;

/// Data points need to run a compactor
#[derive(Debug)]
pub struct Compactor {
    /// Sequencers assigned to this compactor
    sequencers: Vec<SequencerId>,
    /// Object store for reading and persistence of parquet files
    object_store: Arc<ObjectStore>,
    /// The global catalog for schema, parquet files and tombstones
    catalog: Arc<dyn Catalog>,

    /// Executor for running queries and compacting and persisting
    exec: Arc<Executor>,

    /// Backoff config
    backoff_config: BackoffConfig,
}

impl Compactor {
    /// Initialize the Compactor Data
    pub fn new(
        sequencers: Vec<SequencerId>,
        catalog: Arc<dyn Catalog>,
        object_store: Arc<ObjectStore>,
        exec: Arc<Executor>,
        backoff_config: BackoffConfig,
    ) -> Self {
        Self {
            sequencers,
            catalog,
            object_store,
            exec,
            backoff_config,
        }
    }

    // TODO: this function should be invoked in a backround loop
    /// Find and compact parquet files for a given sequencer
    pub async fn find_and_compact(&self, sequencer_id: SequencerId) -> Result<()> {
        if !self.sequencers.contains(&sequencer_id) {
            return Err(Error::SequencerNotFound { sequencer_id });
        }

        // Read level-0 parquet files
        let level_0_files: Vec<Arc<ParquetFile>> = vec![]; // TODO: #3946

        // Group files into table partition
        let mut partitions = Self::group_parquet_files_into_partition(level_0_files);

        // Get level-1 files overlapped with level-0
        for (_key, val) in &mut partitions.iter_mut() {
            let level_1_files: Vec<Arc<ParquetFile>> = vec![]; // TODO: #3946
            val.extend(level_1_files);
        }

        // Each partition may contain non-overlapped files,
        // groups overlapped files in each partition
        let mut overlapped_file_groups = vec![];
        for _val in partitions.values_mut() {
            let overlapped_files: Vec<Vec<Arc<ParquetFile>>> = vec![]; // TODO: #3949
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

            // Only one file, no need to compact
            if overlapped_files.len() == 1 && overlapped_files[0].no_tombstones() {
                // If the file is old enough, it would not have any overlaps. Add it
                // to the list to be upgraded to level 1
                if overlapped_files[0].level_upgradable() {
                    upgrade_level_list.push(overlapped_files[0].parquet_file_id());
                }
                continue;
            }

            // compact
            let output_parquet_files: Vec<ParquetFile> = vec![]; // TODO: #3907

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
        // TODO: #3950 - update_to_level_1(upgrade_level_list)

        Ok(())
    }

    // Group given parquet files into parittion of the same (sequencer_id, table_id, partition_id)
    fn group_parquet_files_into_partition(
        parquet_files: Vec<Arc<ParquetFile>>,
    ) -> BTreeMap<TablePartition, Vec<Arc<ParquetFile>>> {
        let mut groups: BTreeMap<TablePartition, Vec<Arc<ParquetFile>>> = BTreeMap::default();
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
}

#[derive(Debug, Clone, Copy, PartialEq, PartialOrd, Eq, Ord)]
struct TablePartition {
    sequencer_id: SequencerId,
    table_id: TableId,
    partition_id: PartitionId,
}

impl TablePartition {
    fn new(sequencer_id: SequencerId, table_id: TableId, partition_id: PartitionId) -> Self {
        Self {
            sequencer_id,
            table_id,
            partition_id,
        }
    }
}

struct ParquetFileWithTombstone {
    data: Arc<ParquetFile>,
    tombstones: Vec<Tombstone>,
}

impl ParquetFileWithTombstone {
    fn tombstone_ids(&self) -> HashSet<TombstoneId> {
        self.tombstones.iter().map(|t| t.id).collect::<HashSet<_>>()
    }

    fn no_tombstones(&self) -> bool {
        self.tombstones.is_empty()
    }

    // Check if the parquet file is old enough to upgarde its level
    fn level_upgradable(&self) -> bool {
        // TODO: need to wait for creation_time added
        // if time_provider.now() - self.data.creation_time > LEVEL_UPGRADE_THRESHOLD_NANO
        true
    }

    fn parquet_file_id(&self) -> ParquetFileId {
        self.data.id
    }
}
