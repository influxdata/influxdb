use std::{
    collections::HashSet,
    fmt::{Debug, Display},
};

use async_trait::async_trait;
use data_types::{CompactionLevel, ParquetFile, ParquetFileId};

pub mod logging;

/// Returns `true` if the files in the saved state have been changed according to the current state.
#[async_trait]
pub trait ChangedFilesFilter: Debug + Display + Send + Sync {
    /// Return `true` if some other process modified the files in `old` such that they don't appear or appear with a
    /// different compaction level than `new`, and thus we should stop compacting.
    async fn apply(&self, old: &SavedParquetFileState, new: &SavedParquetFileState) -> bool;
}

/// Saved snapshot of a partition's Parquet files' IDs and compaction levels. Save this state at the beginning of a
/// compaction operation, then just before committing ask for the catalog state again. If the ID+compaction level pairs
/// in the initial saved state still appear in the latest catalog state (disregarding any new files that may appear in
/// the latest catalog state) we assume no other compactor instance has compacted the relevant files and this compactor
/// instance should commit its work. If any old ID+compaction level pairs are missing from the latest catalog state
/// (and thus show up in a set difference operation of `old - current`), throw away the work and do not commit as the
/// relevant Parquet files have been changed by some other process while this compactor instance was working.
#[derive(Debug, Clone)]
pub struct SavedParquetFileState {
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
    fn missing<'a>(
        &'a self,
        new: &'a Self,
    ) -> impl Iterator<Item = &'a (ParquetFileId, CompactionLevel)> {
        let old = self;
        old.ids_and_levels.difference(&new.ids_and_levels)
    }

    pub fn existing_files_modified(&self, new: &Self) -> bool {
        let mut missing = self.missing(new);
        // If there are any `(ParquetFileId, CompactionLevel)` pairs in `self` that are not present in `new`, that
        // means some files were marked to delete or had their compaction level changed by some other process.
        missing.next().is_some()
    }

    pub fn modified_ids_and_levels(&self, new: &Self) -> Vec<(ParquetFileId, CompactionLevel)> {
        self.missing(new).cloned().collect()
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
        assert!(saved_state_1
            .modified_ids_and_levels(&saved_state_2)
            .is_empty());
    }

    #[test]
    fn both_empty_parquet_files() {
        let saved_state_1 = SavedParquetFileState::from([]);
        let saved_state_2 = SavedParquetFileState::from([]);

        assert!(!saved_state_1.existing_files_modified(&saved_state_2));
        assert!(saved_state_1
            .modified_ids_and_levels(&saved_state_2)
            .is_empty());
    }

    #[test]
    fn missing_files_indicates_modifications() {
        let pf_id1_level_0 = ParquetFileBuilder::new(1)
            .with_compaction_level(CompactionLevel::Initial)
            .build();

        let saved_state_1 = SavedParquetFileState::from([&pf_id1_level_0]);
        let saved_state_2 = SavedParquetFileState::from([]);

        assert!(saved_state_1.existing_files_modified(&saved_state_2));
        assert_eq!(
            saved_state_1.modified_ids_and_levels(&saved_state_2),
            &[(ParquetFileId::new(1), CompactionLevel::Initial)]
        );
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
        let pf_id4_level_0 = ParquetFileBuilder::new(4)
            .with_compaction_level(CompactionLevel::Initial)
            .build();

        let saved_state_1 = SavedParquetFileState::from([&pf_id1_level_0]);

        let saved_state_2 = SavedParquetFileState::from([&pf_id1_level_0, &pf_id2_level_2]);
        assert!(!saved_state_1.existing_files_modified(&saved_state_2));
        assert!(saved_state_1
            .modified_ids_and_levels(&saved_state_2)
            .is_empty());

        let saved_state_2 = SavedParquetFileState::from([&pf_id1_level_0, &pf_id3_level_1]);
        assert!(!saved_state_1.existing_files_modified(&saved_state_2));
        assert!(saved_state_1
            .modified_ids_and_levels(&saved_state_2)
            .is_empty());

        let saved_state_2 = SavedParquetFileState::from([&pf_id1_level_0, &pf_id4_level_0]);
        assert!(!saved_state_1.existing_files_modified(&saved_state_2));
        assert!(saved_state_1
            .modified_ids_and_levels(&saved_state_2)
            .is_empty());

        let saved_state_2 = SavedParquetFileState::from([
            &pf_id1_level_0,
            &pf_id2_level_2,
            &pf_id4_level_0,
            &pf_id4_level_0,
        ]);
        assert!(!saved_state_1.existing_files_modified(&saved_state_2));
        assert!(saved_state_1
            .modified_ids_and_levels(&saved_state_2)
            .is_empty());
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
        assert_eq!(
            saved_state_1.modified_ids_and_levels(&saved_state_2),
            &[(ParquetFileId::new(1), CompactionLevel::Initial)]
        );
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
        assert_eq!(
            saved_state_1.modified_ids_and_levels(&saved_state_2),
            &[(ParquetFileId::new(1), CompactionLevel::Initial)]
        );
    }
}
