use std::fmt::Display;

use data_types::ParquetFile;

use crate::{
    components::{files_split::FilesSplit, target_level_chooser::TargetLevelChooser},
    file_classification::FileClassification,
    partition_info::PartitionInfo,
};

use super::FileClassifier;

/// Use a combination of [`TargetLevelChooser`] and [`FilesSplit`] to build a [`FileClassification`].
///
/// This uses the following data flow:
///
/// ```text
/// (files)--------------+->[target level chooser (T)]--->(target level)
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
///                      |     +------------+-->(files keep)    :
///                      |                                      :
///                      |                                      :
///                      |     +................................+
///                      |     :
///                      V     V
///             [upgrade split (FU)]
///                      |     |
///                      |     |
///                      V     V
///           (file compact)  (file upgrade)
/// ```
#[derive(Debug)]
pub struct SplitBasedFileClassifier<T, FT, FO, FU>
where
    T: TargetLevelChooser,
    FT: FilesSplit,
    FO: FilesSplit,
    FU: FilesSplit,
{
    target_level_chooser: T,
    target_level_split: FT,
    non_overlap_split: FO,
    upgrade_split: FU,
}

impl<T, FT, FO, FU> SplitBasedFileClassifier<T, FT, FO, FU>
where
    T: TargetLevelChooser,
    FT: FilesSplit,
    FO: FilesSplit,
    FU: FilesSplit,
{
    pub fn new(
        target_level_chooser: T,
        target_level_split: FT,
        non_overlap_split: FO,
        upgrade_split: FU,
    ) -> Self {
        Self {
            target_level_chooser,
            target_level_split,
            non_overlap_split,
            upgrade_split,
        }
    }
}

impl<T, FT, FO, FU> Display for SplitBasedFileClassifier<T, FT, FO, FU>
where
    T: TargetLevelChooser,
    FT: FilesSplit,
    FO: FilesSplit,
    FU: FilesSplit,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "split_based(target_level_chooser={}, target_level_split={}, non_overlap_split={}, upgrade_split={})",
            self.target_level_chooser,
            self.target_level_split,
            self.non_overlap_split,
            self.upgrade_split,
        )
    }
}

impl<T, FT, FO, FU> FileClassifier for SplitBasedFileClassifier<T, FT, FO, FU>
where
    T: TargetLevelChooser,
    FT: FilesSplit,
    FO: FilesSplit,
    FU: FilesSplit,
{
    fn classify(
        &self,
        _partition_info: &PartitionInfo,
        files: Vec<ParquetFile>,
    ) -> FileClassification {
        let files_to_compact = files;

        // Detect target level to compact to
        let target_level = self.target_level_chooser.detect(&files_to_compact);

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

        // To have efficient compaction performance, we only need to uprade (catalog update only) eligible files
        let (files_to_compact, files_to_upgrade) =
            self.upgrade_split.apply(files_to_compact, target_level);

        FileClassification {
            target_level,
            files_to_compact,
            files_to_upgrade,
            files_to_keep,
        }
    }
}
