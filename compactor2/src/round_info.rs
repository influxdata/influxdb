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
    ManySmallFiles,
}

impl Display for RoundInfo {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::TargetLevel { target_level } => write!(f, "TargetLevel: {target_level}"),
            Self::ManySmallFiles => write!(f, "ManySmallFiles"),
        }
    }
}

impl RoundInfo {
    /// what levels should the files in this round be?
    pub fn target_level(&self) -> CompactionLevel {
        match self {
            Self::TargetLevel { target_level } => *target_level,
            Self::ManySmallFiles => CompactionLevel::Initial,
        }
    }
}
