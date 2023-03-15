use std::fmt::Display;

use data_types::{CompactionLevel, ParquetFile};

use crate::{
    components::{files_split::FilesSplit, split_or_compact::SplitOrCompact},
    file_classification::{FileClassification, FilesToCompactOrSplit},
    partition_info::PartitionInfo,
    RoundInfo,
};

use super::FileClassifier;

/// Use [`FilesSplit`] to build a [`FileClassification`].
///
/// Uses the target_level from the `round_info` in the following data flow:
///
/// ```text
/// (files+target_level)-+.......................................
///                      |                                      :
///                      |                                      :
///                      |     +................................+
///                      |     :                                :
///                      |     :                                :
///                      V     V                                :
///             [target level split (FT)]                       :
///                      |     |                                :
///                      |     |                                :
///                      |     +------------+                   :
///                      |                  |                   :
///                      |                  |                   :
///                      |     +............|...................+
///                      |     :            |                   :
///                      V     V            |                   :
///             [non overlap split (FO)]    |                   :
///                      |     |            |                   :
///                      |     |            |                   :
///                      |     +------------+------+            :
///                      |                         |            :
///                      |                         |            :
///                      |     +................................+
///                      |     :                   |            :
///                      V     V                   |            :
///             [upgrade split (FU)]               |            :
///                      |     |                   |            :
///                      |     |                   |            :
///                      |     V                   |            :
///                      |  (files upgrade)        |            :
///                      |                         |            :
///                      |     +................................+
///                      |     |                   |
///                      V     V                   |
///            [split or compact (FSC)]            |
///                      |     |                   |
///                      |     +-------------------+
///                      |                         |
///                      V                         V
///      (files compact or split)            (files keep)
/// ```
#[derive(Debug)]
pub struct SplitBasedFileClassifier<FT, FO, FU, FSC>
where
    FT: FilesSplit,
    FO: FilesSplit,
    FU: FilesSplit,
    FSC: SplitOrCompact,
{
    target_level_split: FT,
    non_overlap_split: FO,
    upgrade_split: FU,
    split_or_compact: FSC,
}

impl<FT, FO, FU, FSC> SplitBasedFileClassifier<FT, FO, FU, FSC>
where
    FT: FilesSplit,
    FO: FilesSplit,
    FU: FilesSplit,
    FSC: SplitOrCompact,
{
    pub fn new(
        target_level_split: FT,
        non_overlap_split: FO,
        upgrade_split: FU,
        split_or_compact: FSC,
    ) -> Self {
        Self {
            target_level_split,
            non_overlap_split,
            upgrade_split,
            split_or_compact,
        }
    }
}

impl<FT, FO, FU, FSC> Display for SplitBasedFileClassifier<FT, FO, FU, FSC>
where
    FT: FilesSplit,
    FO: FilesSplit,
    FU: FilesSplit,
    FSC: SplitOrCompact,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "split_based(target_level_split={}, non_overlap_split={}, upgrade_split={})",
            self.target_level_split, self.non_overlap_split, self.upgrade_split,
        )
    }
}

impl<FT, FO, FU, FSC> FileClassifier for SplitBasedFileClassifier<FT, FO, FU, FSC>
where
    FT: FilesSplit,
    FO: FilesSplit,
    FU: FilesSplit,
    FSC: SplitOrCompact,
{
    fn classify(
        &self,
        partition_info: &PartitionInfo,
        round_info: &RoundInfo,
        files: Vec<ParquetFile>,
    ) -> FileClassification {
        let files_to_compact = files;
        let target_level = round_info.target_level();

        if round_info.is_many_small_files() {
            return file_classification_for_many_files(files_to_compact, target_level);
        }

        // Split files into files_to_compact, files_to_upgrade, and files_to_keep
        //
        // Since output of one compaction is used as input of next compaction, all files that are not
        // compacted or upgraded are still kept to consider in next round of compaction

        // Split actual files to compact from its higher-target-level files
        // The higher-target-level files are kept for next round of compaction
        let (files_to_compact, mut files_to_keep) = self
            .target_level_split
            .apply(files_to_compact, target_level);

        // To have efficient compaction performance, we do not need to compact eligible non-overlapped files
        // Find eligible non-overlapped files and keep for next round of compaction
        let (files_to_compact, non_overlapping_files) =
            self.non_overlap_split.apply(files_to_compact, target_level);
        files_to_keep.extend(non_overlapping_files);

        // To have efficient compaction performance, we only need to upgrade (catalog update only) eligible files
        let (files_to_compact, files_to_upgrade) =
            self.upgrade_split.apply(files_to_compact, target_level);

        // See if we need to split start-level files due to over compaction size limit
        let (files_to_compact_or_split, other_files) =
            self.split_or_compact
                .apply(partition_info, files_to_compact, target_level);
        files_to_keep.extend(other_files);

        FileClassification {
            target_level,
            files_to_compact_or_split,
            files_to_upgrade,
            files_to_keep,
        }
    }
}

fn file_classification_for_many_files(
    files_to_compact: Vec<ParquetFile>,
    target_level: CompactionLevel,
) -> FileClassification {
    // Verify all input files are in the target_level
    let err_msg = format!(
        "All files to compact must be in {target_level} level, but found files in other levels",
    );

    assert!(
        files_to_compact
            .iter()
            .all(|f| f.compaction_level == target_level),
        "{err_msg}"
    );

    FileClassification {
        target_level,
        files_to_compact_or_split: FilesToCompactOrSplit::FilesToCompact(files_to_compact),
        files_to_upgrade: vec![],
        files_to_keep: vec![],
    }
}
