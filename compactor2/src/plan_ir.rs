use std::fmt::Display;

use data_types::{ChunkOrder, ParquetFile};

#[derive(Debug)]
/// Describes a specific compactor plan to create.
pub enum PlanIR {
    /// Compact `files` into a single large output file
    Compact { files: Vec<FileIR> },
    /// Compact `files` into one file for each entry in `split_times`.
    ///
    // The contents of each file:
    /// * `0`: Rows that have `time` *on or before* the `split_times[0]`
    /// * `i (0 < i < split_times's length)`: Rows that have  `time` in range `(split_times[i-1], split_times[i]]`
    /// * `n (n = split_times.len())`: Rows that have `time` *after* all the `split_times` and NULL rows
    Split {
        files: Vec<FileIR>,
        split_times: Vec<i64>,
    },
}

impl PlanIR {
    pub fn n_output_files(&self) -> usize {
        match self {
            Self::Compact { .. } => 1,
            Self::Split { split_times, .. } => split_times.len() + 1,
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
