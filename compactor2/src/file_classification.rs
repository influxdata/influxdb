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

    /// Decision on what files should be compacted or split. See [`FilesToCompactOrSplit`] for more details.
    pub files_to_compact_or_split: FilesToCompactOrSplit,

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
        match &self.files_to_compact_or_split {
            FilesToCompactOrSplit::FilesToCompact(files) => files.len(),
            FilesToCompactOrSplit::FilesToSplit(_) => 0,
        }
    }

    pub fn files_to_split_len(&self) -> usize {
        match &self.files_to_compact_or_split {
            FilesToCompactOrSplit::FilesToCompact(_files) => 0,
            FilesToCompactOrSplit::FilesToSplit(files) => files.len(),
        }
    }

    pub fn has_upgrade_files(&self) -> bool {
        !self.files_to_upgrade.is_empty()
    }
}

/// Files to compact or to split
#[derive(Debug, PartialEq, Eq)]
pub enum FilesToCompactOrSplit {
    /// These files should be compacted together, ideally forming a single output file.
    /// Due to constraints such as the maximum desired output file size and the "leading edge" optimization
    ///  `FilesToCompact` may actually produce multiple output files.
    FilesToCompact(Vec<ParquetFile>),
    /// The input files should be split into multiple output files, at the specified times
    FilesToSplit(Vec<FileToSplit>),
}

impl FilesToCompactOrSplit {
    // Return true if thelist is empty
    pub fn is_empty(&self) -> bool {
        match self {
            Self::FilesToCompact(files) => files.is_empty(),
            Self::FilesToSplit(files) => files.is_empty(),
        }
    }

    /// Return lentgh of files to compact
    pub fn files_to_compact_len(&self) -> usize {
        match self {
            Self::FilesToCompact(files) => files.len(),
            Self::FilesToSplit(_) => 0,
        }
    }

    /// Return lentgh of files to split
    pub fn files_to_split_len(&self) -> usize {
        match self {
            Self::FilesToCompact(_) => 0,
            Self::FilesToSplit(files) => files.len(),
        }
    }

    /// Return files to compact
    pub fn files_to_compact(&self) -> Vec<ParquetFile> {
        match self {
            Self::FilesToCompact(files) => files.clone(),
            Self::FilesToSplit(_) => vec![],
        }
    }

    /// Return files to split
    pub fn files_to_split(&self) -> Vec<ParquetFile> {
        match self {
            Self::FilesToCompact(_) => vec![],
            Self::FilesToSplit(files) => {
                let files: Vec<ParquetFile> = files.iter().map(|f| f.file.clone()).collect();
                files
            }
        }
    }

    /// Return files of either type
    pub fn files(&self) -> Vec<ParquetFile> {
        match self {
            Self::FilesToCompact(files) => files.clone(),
            Self::FilesToSplit(files) => files.iter().map(|f| f.file.clone()).collect(),
        }
    }

    // Returns target level of the files which the compaction level of spit files if any
    // or the given target level
    pub fn target_level(&self, target_level: CompactionLevel) -> CompactionLevel {
        match self {
            Self::FilesToCompact(_) => target_level,
            Self::FilesToSplit(files) => files[0].file.compaction_level,
        }
    }
}

/// File to split and their split times
#[derive(Debug, PartialEq, Eq)]
pub struct FileToSplit {
    pub file: ParquetFile,
    pub split_times: Vec<i64>,
}
