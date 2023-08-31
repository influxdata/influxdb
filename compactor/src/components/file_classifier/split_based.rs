use std::fmt::Display;

use data_types::{CompactionLevel, ParquetFile, TransitionPartitionId};

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
                partition_info.partition_id(),
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

            RoundInfo::VerticalSplit { split_times } => file_classification_for_vertical_split(
                split_times,
                files_to_compact,
                partition_info.partition_id(),
            ),

            RoundInfo::TargetLevel { target_level, .. } => {
                let partition_id = partition_info.partition_id();

                // Split files into files_to_compact, files_to_upgrade, and files_to_keep
                //
                // Since output of one compaction is used as input of next compaction, all files that are not
                // compacted or upgraded are still kept to consider in next round of compaction

                // Split actual files to compact from its higher-target-level files
                // The higher-target-level files are kept for next round of compaction
                let target_level = *target_level;
                let (files_to_compact, mut files_to_keep) = self.target_level_split.apply(
                    files_to_compact,
                    target_level,
                    partition_id.clone(),
                );

                // To have efficient compaction performance, we do not need to compact eligible non-overlapped files
                // Find eligible non-overlapped files and keep for next round of compaction
                let (files_to_compact, non_overlapping_files) = self.non_overlap_split.apply(
                    files_to_compact,
                    target_level,
                    partition_id.clone(),
                );
                files_to_keep.extend(non_overlapping_files);

                // To have efficient compaction performance, we only need to upgrade (catalog update only) eligible files
                let (files_to_compact, files_to_upgrade) =
                    self.upgrade_split
                        .apply(files_to_compact, target_level, partition_id);

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

            RoundInfo::CompactRanges {
                max_num_files_to_group,
                max_total_file_size_to_group,
                ..
            } => {
                let partition_id = partition_info.partition_id();

                let l0_count = files_to_compact
                    .iter()
                    .filter(|f| f.compaction_level == CompactionLevel::Initial)
                    .count();

                if l0_count > *max_num_files_to_group {
                    // Too many L0s, do manySmallFiles within this range.
                    let (files_to_compact, mut files_to_keep) = files_to_compact
                        .into_iter()
                        .partition(|f| f.compaction_level == CompactionLevel::Initial);

                    let l0_classification = file_classification_for_many_files(
                        partition_id.clone(),
                        *max_total_file_size_to_group,
                        *max_num_files_to_group,
                        files_to_compact,
                        CompactionLevel::Initial,
                    );

                    files_to_keep.extend(l0_classification.files_to_keep);

                    assert!(
                        !l0_classification.files_to_make_progress_on.is_empty(),
                        "L0 files_to_make_progress_on should not be empty, for partition {}",
                        partition_id
                    );
                    FileClassification {
                        target_level: l0_classification.target_level,
                        files_to_make_progress_on: l0_classification.files_to_make_progress_on,
                        files_to_keep,
                    }
                } else {
                    // There's not too many L0s, so upgrade/split/compact as required to get L0s->L1.
                    let target_level = CompactionLevel::FileNonOverlapped;
                    let (files_to_compact, mut files_to_keep) = self.target_level_split.apply(
                        files_to_compact,
                        target_level,
                        partition_id.clone(),
                    );

                    // To have efficient compaction performance, we do not need to compact eligible non-overlapped files
                    // Find eligible non-overlapped files and keep for next round of compaction
                    let (files_to_compact, non_overlapping_files) = self.non_overlap_split.apply(
                        files_to_compact,
                        target_level,
                        partition_id.clone(),
                    );
                    files_to_keep.extend(non_overlapping_files);

                    // To have efficient compaction performance, we only need to upgrade (catalog update only) eligible files
                    let (files_to_compact, files_to_upgrade) =
                        self.upgrade_split
                            .apply(files_to_compact, target_level, partition_id);

                    // See if we need to split start-level files due to over compaction size limit
                    let (files_to_split_or_compact, other_files) =
                        self.split_or_compact
                            .apply(partition_info, files_to_compact, target_level);
                    files_to_keep.extend(other_files);

                    let files_to_make_progress_on = FilesForProgress {
                        upgrade: files_to_upgrade,
                        split_or_compact: files_to_split_or_compact,
                    };

                    assert!(
                        !files_to_make_progress_on.is_empty(),
                        "files_to_make_progress_on should not be empty, for partition {}",
                        partition_info.partition_id()
                    );
                    FileClassification {
                        target_level,
                        files_to_make_progress_on,
                        files_to_keep,
                    }
                }
            }
        }
    }
}

// ManySmallFiles assumes the L0 files are tiny and aims to do L0-> L0 compaction to reduce the number of tiny files.
// With vertical splitting, this only operates on a CompactionRange that's up to the max_compact_size.  So while there's
// many files, we know there's not many bytes (in total).  Because of this, We can skip anything that's a sizeable portion
// of the max_compact_size, knowing that we can still get the L0 quantity down to max_num_files_to_group.
fn file_classification_for_many_files(
    partition: TransitionPartitionId,
    max_total_file_size_to_group: usize,
    max_num_files_to_group: usize,
    files: Vec<ParquetFile>,
    target_level: CompactionLevel,
) -> FileClassification {
    // Verify all input files are in the target_level
    let err_msg = format!(
        "all files to compact must be in {target_level} level, but found files in other levels",
    );

    assert!(
        files.iter().all(|f| f.compaction_level == target_level),
        "{err_msg}, partition_id {}",
        partition,
    );

    let mut files_to_compact = vec![];
    let mut files_to_keep: Vec<ParquetFile> = vec![];

    // The goal is to compact the small files without repeately rewriting the non-small files (that hurts write amp).
    // Assume tiny files separated by non-tiny files, we need to get down to max_num_files_to_group.
    // So compute the biggest files we can skip, and still be guaranteed to get down to max_num_files_to_group.
    let skip_size = max_total_file_size_to_group * 2 / max_num_files_to_group;

    // Enforce max_num_files_to_group
    if files.len() > max_num_files_to_group {
        let ordered_files = order_files(files, target_level.prev());

        let mut chunk_bytes: usize = 0;
        let mut chunk: Vec<ParquetFile> = Vec::with_capacity(max_num_files_to_group);
        for f in ordered_files {
            if !files_to_compact.is_empty() {
                // We've already got a batch of files to compact, this can wait.
                files_to_keep.push(f);
            } else if chunk_bytes + f.file_size_bytes as usize > max_total_file_size_to_group
                || chunk.len() + 1 > max_num_files_to_group
                || f.file_size_bytes >= skip_size as i64
            {
                // This file will not be included in this compaction.
                files_to_keep.push(f);
                if chunk.len() > 1 {
                    // Several files; we'll do an L0->L0 comapction on them.
                    files_to_compact = chunk.to_vec();
                    chunk = Vec::with_capacity(max_num_files_to_group);
                } else if !chunk.is_empty() {
                    // Just one file, and we don't want to compact it with 'f', so skip it.
                    files_to_keep.append(chunk.to_vec().as_mut());
                    chunk = Vec::with_capacity(max_num_files_to_group);
                }
            } else {
                // This files goes in our draft chunk to compact
                chunk_bytes += f.file_size_bytes as usize;
                chunk.push(f);
            }
        }
        if !chunk.is_empty() {
            assert!(files_to_compact.is_empty(), "we shouldn't accumulate multiple non-contiguous chunks to compact, but we found non-contiguous chunks in compaction job for partition_id={}", partition);
            if chunk.len() > 1 {
                // We need to compact what comes before f
                files_to_compact = chunk.to_vec();
            } else if !chunk.is_empty() {
                files_to_keep.append(chunk.to_vec().as_mut());
            }
        }

        assert!(
            chunk.is_empty() || chunk.len() > 1,
            "should not have only 1 chunk, for partition {}",
            partition
        );
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
    partition: TransitionPartitionId,
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
            "files not needing split should be filtered out, instead found to-compact file (not to-split) in partition {}", partition
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
