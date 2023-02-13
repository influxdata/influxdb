use std::fmt::Display;

use data_types::{ChunkOrder, ParquetFile};

#[derive(Debug)]
/// Describes a specific compactor plan to create.
pub enum PlanIR {
    /// Compact `files` into a single large output file
    Compact {
        /// The files to be compacted
        files: Vec<FileIR>,
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
        /// The contents of each file:
        /// * `0`: Rows that have `time` *on or before* the `split_times[0]`
        /// * `i (0 < i < split_times's length)`: Rows that have  `time` in range `(split_times[i-1], split_times[i]]`
        /// * `n (n = split_times.len())`: Rows that have `time` *after* all the `split_times` and NULL rows
        split_times: Vec<i64>,
    },
}

impl PlanIR {
    /// Return the number of output files produced
    pub fn n_output_files(&self) -> usize {
        match self {
            Self::Compact { .. } => 1,
            Self::Split { split_times, .. } => split_times.len() + 1,
        }
    }

    /// return the input files that will be compacted together
    pub fn input_files(&self) -> &[FileIR] {
        match self {
            Self::Compact { files } => files,
            Self::Split { files, .. } => files,
        }
    }
}

impl Display for PlanIR {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Compact { .. } => write!(f, "compact"),
            Self::Split { .. } => write!(f, "split"),
        }
    }
}

#[derive(Debug)]
pub struct FileIR {
    pub file: ParquetFile,
    pub order: ChunkOrder,
}
