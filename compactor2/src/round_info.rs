//! Information about the current compaction round

use std::fmt::Display;

use data_types::CompactionLevel;

/// Information about the current compaction round (see driver.rs for
/// more details about a round)
#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub enum RoundInfo {
    /// compacting to target level
    TargetLevel {
        /// compaction level of target fles
        target_level: CompactionLevel,
    },
    /// In many small files mode
    ManySmallFiles {
        /// start level of files in this round
        start_level: CompactionLevel,
        /// max number of files to group in each plan
        max_num_files_to_group: usize,
    },
}

impl Display for RoundInfo {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::TargetLevel { target_level } => write!(f, "TargetLevel: {target_level}"),
            Self::ManySmallFiles {
                start_level,
                max_num_files_to_group,
            } => write!(f, "ManySmallFiles: {start_level}, {max_num_files_to_group}",),
        }
    }
}

impl RoundInfo {
    /// what levels should the files in this round be?
    pub fn target_level(&self) -> CompactionLevel {
        match self {
            Self::TargetLevel { target_level } => *target_level,
            // For many files, start level is the target level
            Self::ManySmallFiles { start_level, .. } => *start_level,
        }
    }

    /// Is this round in many small files mode?
    pub fn is_many_small_files(&self) -> bool {
        matches!(self, Self::ManySmallFiles { .. })
    }
}
