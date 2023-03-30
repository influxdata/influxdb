//! Information of a partition for compaction

use std::{collections::HashSet, sync::Arc};

use data_types::{
    CompactionLevel, NamespaceId, ParquetFile, ParquetFileId, PartitionId, PartitionKey, Table,
    TableSchema,
};
use schema::sort::SortKey;

/// Information about the Partition being compacted
#[derive(Debug, PartialEq, Eq)]
pub struct PartitionInfo {
    /// the partition
    pub partition_id: PartitionId,

    /// Namespace ID
    pub namespace_id: NamespaceId,

    /// Namespace name
    pub namespace_name: String,

    /// Table.
    pub table: Arc<Table>,

    /// Table schema
    pub table_schema: Arc<TableSchema>,

    /// Sort key of the partition
    pub sort_key: Option<SortKey>,

    /// partition_key
    pub partition_key: PartitionKey,
}

impl PartitionInfo {
    /// Returns number of columns in the table
    pub fn column_count(&self) -> usize {
        self.table_schema.column_count()
    }
}

/// Saved snapshot of a partition's Parquet files' IDs and compaction levels. Save this state at the beginning of a
/// compaction operation, then just before committing ask for the state again. If the two saved states are identical,
/// we assume no other compactor instance has compacted this partition and this compactor instance should commit its
/// work. If the two saved states differ, throw away the work and do not commit as the Parquet files have been changed
/// by some other process while this compactor instance was working.
#[derive(Debug, Clone)]
pub(crate) struct SavedParquetFileState {
    ids_and_levels: HashSet<(ParquetFileId, CompactionLevel)>,
}

impl<'a, T> From<T> for SavedParquetFileState
where
    T: IntoIterator<Item = &'a ParquetFile>,
{
    fn from(parquet_files: T) -> Self {
        let ids_and_levels = parquet_files
            .into_iter()
            .map(|pf| (pf.id, pf.compaction_level))
            .collect();

        Self { ids_and_levels }
    }
}

impl SavedParquetFileState {
    pub fn existing_files_modified(&self, new: &SavedParquetFileState) -> bool {
        let old = self;
        let mut missing = old.ids_and_levels.difference(&new.ids_and_levels);

        // If there are any files in `self`/`old` that are not present in `new`, that means some files were marked
        // to delete by some other process.
        missing.next().is_some()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use iox_tests::ParquetFileBuilder;

    #[test]
    fn saved_state_sorts_by_parquet_file_id() {
        let pf_id1_level_0 = ParquetFileBuilder::new(1)
            .with_compaction_level(CompactionLevel::Initial)
            .build();
        let pf_id2_level_2 = ParquetFileBuilder::new(2)
            .with_compaction_level(CompactionLevel::Final)
            .build();
        let pf_id3_level_1 = ParquetFileBuilder::new(3)
            .with_compaction_level(CompactionLevel::FileNonOverlapped)
            .build();

        let saved_state_1 =
            SavedParquetFileState::from([&pf_id1_level_0, &pf_id2_level_2, &pf_id3_level_1]);
        let saved_state_2 =
            SavedParquetFileState::from([&pf_id3_level_1, &pf_id1_level_0, &pf_id2_level_2]);

        assert!(!saved_state_1.existing_files_modified(&saved_state_2));
    }

    #[test]
    fn both_empty_parquet_files() {
        let saved_state_1 = SavedParquetFileState::from([]);
        let saved_state_2 = SavedParquetFileState::from([]);

        assert!(!saved_state_1.existing_files_modified(&saved_state_2));
    }

    #[test]
    fn missing_files_indicates_modifications() {
        let pf_id1_level_0 = ParquetFileBuilder::new(1)
            .with_compaction_level(CompactionLevel::Initial)
            .build();

        let saved_state_1 = SavedParquetFileState::from([&pf_id1_level_0]);
        let saved_state_2 = SavedParquetFileState::from([]);

        assert!(saved_state_1.existing_files_modified(&saved_state_2));
    }

    #[test]
    fn disregard_new_files() {
        let pf_id1_level_0 = ParquetFileBuilder::new(1)
            .with_compaction_level(CompactionLevel::Initial)
            .build();

        // New files of any level don't affect whether the old saved state is considered modified
        let pf_id2_level_2 = ParquetFileBuilder::new(2)
            .with_compaction_level(CompactionLevel::Final)
            .build();
        let pf_id3_level_1 = ParquetFileBuilder::new(3)
            .with_compaction_level(CompactionLevel::FileNonOverlapped)
            .build();
        let pf_id4_level_0 = ParquetFileBuilder::new(1)
            .with_compaction_level(CompactionLevel::Initial)
            .build();

        let saved_state_1 = SavedParquetFileState::from([&pf_id1_level_0]);

        let saved_state_2 = SavedParquetFileState::from([&pf_id1_level_0, &pf_id2_level_2]);
        assert!(!saved_state_1.existing_files_modified(&saved_state_2));

        let saved_state_2 = SavedParquetFileState::from([&pf_id1_level_0, &pf_id3_level_1]);
        assert!(!saved_state_1.existing_files_modified(&saved_state_2));

        let saved_state_2 = SavedParquetFileState::from([&pf_id1_level_0, &pf_id4_level_0]);
        assert!(!saved_state_1.existing_files_modified(&saved_state_2));

        let saved_state_2 = SavedParquetFileState::from([
            &pf_id1_level_0,
            &pf_id2_level_2,
            &pf_id4_level_0,
            &pf_id4_level_0,
        ]);
        assert!(!saved_state_1.existing_files_modified(&saved_state_2));
    }

    #[test]
    fn changed_compaction_level_indicates_modification() {
        let pf_id1_level_0 = ParquetFileBuilder::new(1)
            .with_compaction_level(CompactionLevel::Initial)
            .build();
        let pf_id1_level_1 = ParquetFileBuilder::new(1)
            .with_compaction_level(CompactionLevel::FileNonOverlapped)
            .build();
        let pf_id2_level_2 = ParquetFileBuilder::new(2)
            .with_compaction_level(CompactionLevel::Final)
            .build();

        let saved_state_1 = SavedParquetFileState::from([&pf_id1_level_0, &pf_id2_level_2]);
        let saved_state_2 = SavedParquetFileState::from([&pf_id1_level_1, &pf_id2_level_2]);

        assert!(saved_state_1.existing_files_modified(&saved_state_2));
    }

    #[test]
    fn same_number_of_files_different_ids_indicates_modification() {
        let pf_id1_level_0 = ParquetFileBuilder::new(1)
            .with_compaction_level(CompactionLevel::Initial)
            .build();
        let pf_id2_level_0 = ParquetFileBuilder::new(2)
            .with_compaction_level(CompactionLevel::Initial)
            .build();
        let pf_id3_level_2 = ParquetFileBuilder::new(3)
            .with_compaction_level(CompactionLevel::Final)
            .build();

        let saved_state_1 = SavedParquetFileState::from([&pf_id1_level_0, &pf_id3_level_2]);
        let saved_state_2 = SavedParquetFileState::from([&pf_id2_level_0, &pf_id3_level_2]);

        assert!(saved_state_1.existing_files_modified(&saved_state_2));
    }
}
