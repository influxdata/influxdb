use data_types::{CompactionLevel, ParquetFile};

/// A file classification specifies the parameters for a single compaction branch.
///
/// This may
/// generate one or more new parquet files. It includes the target
/// [`CompactionLevel`], the specific files that should be compacted
/// together to form new file(s), files that should be upgraded
/// without chainging, files that should be left unmodified.
#[derive(Debug, PartialEq, Eq)]
pub struct FileClassification {
    /// The target level of file resulting from compaction
    pub target_level: CompactionLevel,

    /// Decision on what files should be split or compacted. See [`FilesToSplitOrCompact`] for more details.
    pub files_to_split_or_compact: FilesToSplitOrCompact,

    /// Non-overlapped files that should be upgraded to the target
    /// level without rewriting (for example they are of sufficient
    /// size)
    pub files_to_upgrade: Vec<ParquetFile>,

    /// files which should not be modified. For example,
    /// non-overlapped or higher-target-level files
    pub files_to_keep: Vec<ParquetFile>,
}

impl FileClassification {
    pub fn files_to_compact_len(&self) -> usize {
        match &self.files_to_split_or_compact {
            FilesToSplitOrCompact::Compact(files) => files.len(),
            FilesToSplitOrCompact::Split(_) => 0,
        }
    }

    pub fn files_to_split_len(&self) -> usize {
        match &self.files_to_split_or_compact {
            FilesToSplitOrCompact::Compact(_files) => 0,
            FilesToSplitOrCompact::Split(files) => files.len(),
        }
    }
}

/// Files to compact or to split
#[derive(Debug, PartialEq, Eq)]
pub enum FilesToSplitOrCompact {
    /// These files should be compacted together, ideally forming a single output file.
    /// Due to constraints such as the maximum desired output file size and the "leading edge" optimization
    ///  `FilesToCompact` may actually produce multiple output files.
    Compact(Vec<ParquetFile>),
    /// The input files should be split into multiple output files, at the specified times
    Split(Vec<FileToSplit>),
}

impl FilesToSplitOrCompact {
    // Return true if thelist is empty
    pub fn is_empty(&self) -> bool {
        match self {
            Self::Compact(files) => files.is_empty(),
            Self::Split(files) => files.is_empty(),
        }
    }

    /// Return lentgh of files to compact
    pub fn files_to_compact_len(&self) -> usize {
        match self {
            Self::Compact(files) => files.len(),
            Self::Split(_) => 0,
        }
    }

    /// Return lentgh of files to split
    pub fn files_to_split_len(&self) -> usize {
        match self {
            Self::Compact(_) => 0,
            Self::Split(files) => files.len(),
        }
    }

    /// Return files to compact
    pub fn files_to_compact(&self) -> Vec<ParquetFile> {
        match self {
            Self::Compact(files) => files.clone(),
            Self::Split(_) => vec![],
        }
    }

    /// Return files to split
    pub fn files_to_split(&self) -> Vec<ParquetFile> {
        match self {
            Self::Compact(_) => vec![],
            Self::Split(files) => {
                let files: Vec<ParquetFile> = files.iter().map(|f| f.file.clone()).collect();
                files
            }
        }
    }

    // return split times of files to split
    pub fn split_times(&self) -> Vec<Vec<i64>> {
        match self {
            Self::Compact(_) => vec![],
            Self::Split(files) => files.iter().map(|f| f.split_times.clone()).collect(),
        }
    }

    /// Return files of either type
    pub fn files(&self) -> Vec<ParquetFile> {
        match self {
            Self::Compact(files) => files.clone(),
            Self::Split(files) => files.iter().map(|f| f.file.clone()).collect(),
        }
    }
}

/// File to split and their split times
#[derive(Debug, PartialEq, Eq)]
pub struct FileToSplit {
    pub file: ParquetFile,
    pub split_times: Vec<i64>,
}
