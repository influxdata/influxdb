//! Information about the current compaction round

use std::fmt::Display;

use data_types::{CompactionLevel, FileRange};

/// Information about the current compaction round (see driver.rs for
/// more details about a round)
#[derive(Debug, PartialEq, Eq, Clone)]
pub enum RoundInfo {
    /// compacting to target level
    TargetLevel {
        /// compaction level of target fles
        target_level: CompactionLevel,
        /// max total size limit of files to group in each plan
        max_total_file_size_to_group: usize,
    },
    /// In many small files mode
    ManySmallFiles {
        /// start level of files in this round
        start_level: CompactionLevel,
        /// max number of files to group in each plan
        max_num_files_to_group: usize,
        /// max total size limit of files to group in each plan
        max_total_file_size_to_group: usize,
    },

    /// This scenario is not 'leading edge', but we'll process it like it is.
    /// We'll start with the L0 files we must start with (the first by max_l0_created_at),
    /// and take as many as we can (up to max files | bytes), and compact them down as if
    /// that's the only L0s there are.  This will be very much like if we got the chance
    /// to compact a while ago, when those were the only files in L0.
    /// Why would we do this:
    /// The diagnosis of various scenarios (vertical splitting, ManySmallFiles, etc)
    /// sometimes get into conflict with each other.  When we're having trouble with
    /// an efficient "big picture" approach, this is a way to get some progress.
    /// Its sorta like pushing the "easy button".
    SimulatedLeadingEdge {
        // level: always Initial
        /// max number of files to group in each plan
        max_num_files_to_group: usize,
        /// max total size limit of files to group in each plan
        max_total_file_size_to_group: usize,
    },

    /// Vertical Split always applies to L0.  This is triggered when we have too many overlapping L0s to
    /// compact in one batch, so we'll split files so they don't all overlap.  Its called "vertical" because
    /// if the L0s were drawn on a timeline, we'd then draw some vertical lines across L0s, and every place
    /// a line crosses a file, its split there.
    VerticalSplit {
        /// split_times are the exact times L0 files will be split at.  Only L0 files overlapping these times
        /// need split.
        split_times: Vec<i64>,
    },

    /// CompactRanges are overlapping chains of L0s are less than max_compact_size, with no L0 or L1 overlaps
    /// between ranges.
    CompactRanges {
        /// Ranges describing distinct chains of L0s to be compacted.
        ranges: Vec<FileRange>,
        /// max number of files to group in each plan
        max_num_files_to_group: usize,
        /// max total size limit of files to group in each plan
        max_total_file_size_to_group: usize,
    },
}

impl Display for RoundInfo {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::TargetLevel { target_level, max_total_file_size_to_group  } => write!(f, "TargetLevel: {target_level} {max_total_file_size_to_group}"),
            Self::ManySmallFiles {
                start_level,
                max_num_files_to_group,
                max_total_file_size_to_group,
            } => write!(f, "ManySmallFiles: {start_level}, {max_num_files_to_group}, {max_total_file_size_to_group}",),
            Self::SimulatedLeadingEdge {
                max_num_files_to_group,
                max_total_file_size_to_group,
            } => write!(f, "SimulatedLeadingEdge: {max_num_files_to_group}, {max_total_file_size_to_group}",),
            Self::VerticalSplit  { split_times } => write!(f, "VerticalSplit: {split_times:?}"),
            Self::CompactRanges { ranges, max_num_files_to_group, max_total_file_size_to_group } => write!(f, "{:?}, {max_num_files_to_group}, {max_total_file_size_to_group}", ranges)
        }
    }
}

impl RoundInfo {
    /// what levels should the files in this round be?
    pub fn target_level(&self) -> CompactionLevel {
        match self {
            Self::TargetLevel { target_level, .. } => *target_level,
            // For many files, start level is the target level
            Self::ManySmallFiles { start_level, .. } => *start_level,
            Self::SimulatedLeadingEdge { .. } => CompactionLevel::FileNonOverlapped,
            Self::VerticalSplit { .. } => CompactionLevel::Initial,
            Self::CompactRanges { .. } => CompactionLevel::Initial,
        }
    }

    /// Is this round in many small files mode?
    pub fn is_many_small_files(&self) -> bool {
        matches!(self, Self::ManySmallFiles { .. })
    }

    /// Is this round in simulated leading edge mode?
    pub fn is_simulated_leading_edge(&self) -> bool {
        matches!(self, Self::SimulatedLeadingEdge { .. })
    }

    /// return max_num_files_to_group, when available.
    pub fn max_num_files_to_group(&self) -> Option<usize> {
        match self {
            Self::TargetLevel { .. } => None,
            Self::ManySmallFiles {
                max_num_files_to_group,
                ..
            } => Some(*max_num_files_to_group),
            Self::SimulatedLeadingEdge {
                max_num_files_to_group,
                ..
            } => Some(*max_num_files_to_group),
            Self::VerticalSplit { .. } => None,
            Self::CompactRanges {
                max_num_files_to_group,
                ..
            } => Some(*max_num_files_to_group),
        }
    }

    /// return max_total_file_size_to_group, when available.
    pub fn max_total_file_size_to_group(&self) -> Option<usize> {
        match self {
            Self::TargetLevel { .. } => None,
            Self::ManySmallFiles {
                max_total_file_size_to_group,
                ..
            } => Some(*max_total_file_size_to_group),
            Self::SimulatedLeadingEdge {
                max_total_file_size_to_group,
                ..
            } => Some(*max_total_file_size_to_group),
            Self::VerticalSplit { .. } => None,
            Self::CompactRanges {
                max_total_file_size_to_group,
                ..
            } => Some(*max_total_file_size_to_group),
        }
    }

    /// return compaction ranges, when available.
    /// We could generate ranges from VerticalSplit split times, but that asssumes the splits resulted in
    /// no ranges > max_compact_size, which is not guaranteed.  Instead, we'll detect the ranges the first
    /// time after VerticalSplit, and may decide to resplit subset of the files again if data was more
    /// non-linear than expected.
    pub fn ranges(&self) -> Option<Vec<FileRange>> {
        match self {
            Self::TargetLevel { .. } => None,
            Self::ManySmallFiles { .. } => None,
            Self::SimulatedLeadingEdge { .. } => None,
            Self::VerticalSplit { .. } => None,
            Self::CompactRanges { ranges, .. } => Some(ranges.clone()),
        }
    }
}
