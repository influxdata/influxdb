use std::fmt::Display;

use data_types::{CompactionLevel, ParquetFile};

use crate::{
    components::{
        divide_initial::multiple_branches::order_files, files_split::FilesSplit,
        split_or_compact::SplitOrCompact,
    },
    file_classification::{
        CompactReason, FileClassification, FileToSplit, FilesForProgress, FilesToSplitOrCompact,
        SplitReason,
    },
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

// TODO(joe): maintain this comment through the next few PRs; see how true the comment is/remains.
// classify is the third of three file list manipulation layers.  It is given a list of files
// for a single branch and decides which files should be upgraded, split (with split times) or compacted.
// classification can decide to ignore some files for this round (putting them in files_to_keep), but
// that's generally the result of split|compact decisions.  Files not needing considered for action in
// this round were already filtered out before we get to classification.
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

        match round_info {
            RoundInfo::ManySmallFiles {
                start_level,
                max_num_files_to_group,
                max_total_file_size_to_group,
            } => file_classification_for_many_files(
                *max_total_file_size_to_group,
                *max_num_files_to_group,
                files_to_compact,
                *start_level,
            ),

            RoundInfo::SimulatedLeadingEdge { .. } => {
                // file division already done in round_info_source
                FileClassification {
                    target_level: round_info.target_level(),
                    files_to_make_progress_on: FilesForProgress {
                        upgrade: vec![],
                        split_or_compact: FilesToSplitOrCompact::Compact(
                            files_to_compact,
                            CompactReason::TotalSizeLessThanMaxCompactSize,
                        ),
                    },
                    files_to_keep: vec![],
                }
            }

            RoundInfo::VerticalSplit { split_times } => {
                file_classification_for_vertical_split(split_times, files_to_compact)
            }

            RoundInfo::TargetLevel { target_level, .. } => {
                // Split files into files_to_compact, files_to_upgrade, and files_to_keep
                //
                // Since output of one compaction is used as input of next compaction, all files that are not
                // compacted or upgraded are still kept to consider in next round of compaction

                // Split actual files to compact from its higher-target-level files
                // The higher-target-level files are kept for next round of compaction
                let target_level = *target_level;
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
                let (files_to_split_or_compact, other_files) =
                    self.split_or_compact
                        .apply(partition_info, files_to_compact, target_level);
                files_to_keep.extend(other_files);

                let files_to_make_progress_on = FilesForProgress {
                    upgrade: files_to_upgrade,
                    split_or_compact: files_to_split_or_compact,
                };

                FileClassification {
                    target_level,
                    files_to_make_progress_on,
                    files_to_keep,
                }
            }
        }
    }
}

// ManySmallFiles assumes the L0 files are tiny and aims to do L0-> L0 compaction to reduce the number of tiny files.
fn file_classification_for_many_files(
    max_total_file_size_to_group: usize,
    max_num_files_to_group: usize,
    files: Vec<ParquetFile>,
    target_level: CompactionLevel,
) -> FileClassification {
    // Verify all input files are in the target_level
    let err_msg = format!(
        "All files to compact must be in {target_level} level, but found files in other levels",
    );

    assert!(
        files.iter().all(|f| f.compaction_level == target_level),
        "{err_msg}"
    );

    let mut files_to_compact = vec![];
    let mut files_to_keep: Vec<ParquetFile> = vec![];

    // Enforce max_num_files_to_group
    if files.len() > max_num_files_to_group {
        let ordered_files = order_files(files, target_level.prev());

        ordered_files
            .chunks(max_num_files_to_group)
            .for_each(|chunk| {
                let this_chunk_bytes: usize =
                    chunk.iter().map(|f| f.file_size_bytes as usize).sum();
                if this_chunk_bytes > max_total_file_size_to_group {
                    // This chunk of files are plenty big and don't fit the ManySmallFiles characteristics.
                    // If we let ManySmallFiles handle them, it may get stuck with unproductive compactions.
                    // So set them aside for later (when we're not in ManySmallFiles mode).
                    files_to_keep.append(chunk.to_vec().as_mut());
                } else if files_to_compact.is_empty() {
                    files_to_compact = chunk.to_vec();
                } else {
                    // We've already got a batch of files to compact, these can wait.
                    files_to_keep.append(chunk.to_vec().as_mut());
                }
            });
    } else {
        files_to_compact = files;
    }

    let files_to_make_progress_on = FilesForProgress {
        upgrade: vec![],
        split_or_compact: FilesToSplitOrCompact::Compact(
            files_to_compact,
            CompactReason::ManySmallFiles,
        ),
    };

    FileClassification {
        target_level,
        files_to_make_progress_on,
        files_to_keep,
    }
}

// VerticalSplit splits the given files at the given split_times.
// All files given here must be L0 files overlapping at least one of the split_times.
fn file_classification_for_vertical_split(
    split_times: &[i64],
    files: Vec<ParquetFile>,
) -> FileClassification {
    let target_level = CompactionLevel::Initial;
    let files_to_keep: Vec<ParquetFile> = vec![];
    let mut files_to_split: Vec<FileToSplit> = Vec::with_capacity(files.len());

    // Determine the necessary splits for each file.
    // split time is the last ns included in the 'left' file in the split.  So if the the split time matches max time
    // of a file, that file does not need split.
    for f in files {
        let this_file_splits: Vec<i64> = split_times
            .iter()
            .filter(|split| split >= &&f.min_time.get() && split < &&f.max_time.get())
            .cloned()
            .collect();

        assert!(
            !this_file_splits.is_empty(),
            "files not needing split should be filtered out"
        );

        let file_to_split = FileToSplit {
            file: f,
            split_times: this_file_splits,
        };
        files_to_split.push(file_to_split);
    }

    let files_to_make_progress_on = FilesForProgress {
        upgrade: vec![],
        split_or_compact: FilesToSplitOrCompact::Split(files_to_split, SplitReason::VerticalSplit),
    };

    FileClassification {
        target_level,
        files_to_make_progress_on,
        files_to_keep,
    }
}
