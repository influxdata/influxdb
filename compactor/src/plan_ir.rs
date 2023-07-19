use std::fmt::Display;

use data_types::{ChunkOrder, CompactionLevel, ParquetFile};
use parquet_file::ParquetFilePath;

use crate::file_classification::{CompactReason, NoneReason, SplitReason};

#[derive(Debug)]
/// Describes a specific compactor plan to create.
pub enum PlanIR {
    /// Compact `files` into a single large output file
    Compact {
        /// The files to be compacted
        files: Vec<FileIR>,
        /// The level the compacted file will be
        target_level: CompactionLevel,
        /// The reason compact was chosen
        reason: CompactReason,
    },
    /// Compact `files` into multiple files, for each entry in
    /// `split_times`
    Split {
        /// The files to be compacted.
        files: Vec<FileIR>,
        /// The timestamps at which to split the data
        ///
        /// If there are n split entries in split_times,
        /// there will be `n+1` output files.
        ///
        /// The distribution of times is described on
        /// [`iox_query::frontend::reorg::ReorgPlanner::split_plan`]
        split_times: Vec<i64>,
        /// The level the split files will be
        target_level: CompactionLevel,
        /// The reason split was chosen
        reason: SplitReason,
    },
    /// Nothing to do, but communicate why
    None {
        /// The reason there's nothing to do
        reason: NoneReason,
    },
}

impl PlanIR {
    /// Return the target level for this plan
    pub fn target_level(&self) -> CompactionLevel {
        match *self {
            Self::Compact { target_level, .. } => target_level,
            Self::Split { target_level, .. } => target_level,
            Self::None { .. } => unreachable!("filter out None plans before calling target_level"),
        }
    }

    /// Return the number of output files produced
    pub fn n_output_files(&self) -> usize {
        match self {
            Self::Compact { .. } => 1,
            Self::Split { split_times, .. } => split_times.len() + 1,
            Self::None { .. } => 0,
        }
    }

    /// return the number of input files that will be compacted together
    pub fn n_input_files(&self) -> usize {
        match self {
            Self::Compact { files, .. } => files.len(),
            Self::Split { files, .. } => files.len(),
            Self::None { .. } => 0,
        }
    }

    /// return the input files that will be compacted together
    pub fn input_files(&self) -> &[FileIR] {
        match self {
            Self::Compact { files, .. } => files,
            Self::Split { files, .. } => files,
            Self::None { .. } => &[],
        }
    }

    /// return the ParquetFiles that will be compacted together
    pub fn input_parquet_files(&self) -> Vec<ParquetFile> {
        self.input_files()
            .iter()
            .map(|ir| ir.file.clone())
            .collect::<Vec<_>>()
    }

    /// return the paths of the input files that will be compacted together
    pub fn input_paths(&self) -> Vec<ParquetFilePath> {
        self.input_files()
            .iter()
            .map(|ir| ir.path.clone())
            .collect::<Vec<_>>()
    }

    /// return the total bytes of the input files that will be compacted together
    pub fn input_bytes(&self) -> i64 {
        self.input_files()
            .iter()
            .map(|ir| ir.file.file_size_bytes)
            .sum::<i64>()
    }

    /// return a string describing the reason for this plan.
    pub fn reason(&self) -> String {
        match self {
            Self::Compact { reason, .. } => format!("compact({reason:?})"),
            Self::Split { reason, .. } => format!("split({reason:?})"),
            Self::None { reason, .. } => format!("none({reason:?})"),
        }
    }
}

impl Display for PlanIR {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Compact { reason, .. } => write!(f, "compact({reason:?})"),
            Self::Split { reason, .. } => write!(f, "split({reason:?})"),
            Self::None { reason, .. } => write!(f, "none({reason:?})"),
        }
    }
}

#[derive(Debug)]
pub struct FileIR {
    pub file: ParquetFile,
    pub path: ParquetFilePath,
    pub order: ChunkOrder,
}
