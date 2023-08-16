use data_types::{CompactionLevel, ParquetFile};
use parquet_file::ParquetFilePath;

/// A file classification specifies the parameters for a single compaction branch.
///
/// This may generate one or more new Parquet files. It includes the target [`CompactionLevel`],
/// the specific files that should be compacted together to form new file(s), files that should be
/// upgraded without changing, and files that should be left unmodified.
#[derive(Debug, PartialEq, Eq)]
pub struct FileClassification {
    /// The target level of file resulting from compaction
    pub target_level: CompactionLevel,

    /// All files that should make some sort of progress in this round of compaction, including
    /// upgrading, splitting, and compacting. See [`FilesForProgress`] for more details.
    pub files_to_make_progress_on: FilesForProgress,

    /// Files which should not be modified. For example, non-overlapped or higher-target-level
    /// files.
    pub files_to_keep: Vec<ParquetFile>,
}

impl FileClassification {
    /// Number of files to upgrade; useful for logging
    pub fn num_files_to_upgrade(&self) -> usize {
        self.files_to_make_progress_on.upgrade.len()
    }

    /// Number of files to compact; useful for logging
    pub fn num_files_to_compact(&self) -> usize {
        match &self.files_to_make_progress_on.split_or_compact {
            FilesToSplitOrCompact::Compact(files, ..) => files.len(),
            _ => 0,
        }
    }

    /// Number of files to split; useful for logging
    pub fn num_files_to_split(&self) -> usize {
        match &self.files_to_make_progress_on.split_or_compact {
            FilesToSplitOrCompact::Split(files, ..) => files.len(),
            _ => 0,
        }
    }

    /// Number of files to keep; useful for logging
    pub fn num_files_to_keep(&self) -> usize {
        self.files_to_keep.len()
    }
}

#[derive(Debug, PartialEq, Eq)]
pub struct FilesForProgress {
    pub upgrade: Vec<ParquetFile>,
    pub split_or_compact: FilesToSplitOrCompact,
}

impl FilesForProgress {
    // If there are neither files to upgrade nor files to split/compact, there's nothing to do.
    pub fn is_empty(&self) -> bool {
        self.upgrade.is_empty() && matches!(self.split_or_compact, FilesToSplitOrCompact::None(..))
    }

    /// Create an empty instance; useful for tests.
    #[cfg(test)]
    pub fn empty() -> Self {
        Self {
            upgrade: vec![],
            split_or_compact: FilesToSplitOrCompact::None(NoneReason::NoInputFiles),
        }
    }
}

/// Files to split or to compact
#[derive(Debug, PartialEq, Eq, Clone)]
pub enum FilesToSplitOrCompact {
    /// Nothing to do.
    None(NoneReason),
    /// The input files should be split into multiple output files, at the specified times
    Split(Vec<FileToSplit>, SplitReason),
    /// These files should be compacted together, ideally forming a single output file.
    /// Due to constraints such as the maximum desired output file size and the "leading edge" optimization
    ///  `FilesToCompact` may actually produce multiple output files.
    Compact(Vec<ParquetFile>, CompactReason),
}

/// Reasons why there's nothing to split or compact
#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub enum NoneReason {
    NoInputFiles,
    NoFilesToSplitFound,
}

/// Reasons why there are files to split
#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub enum SplitReason {
    ReduceOverlap,
    ReduceLargeFileSize,
    CompactAndSplitOutput(CompactReason),
    HighL0OverlapSingleFile,
    HighL0OverlapTotalBacklog,
    StartLevelOverlapsTooBig,
}

/// Reasons why there are files to compact
#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub enum CompactReason {
    ManySmallFiles,
    TotalSizeLessThanMaxCompactSize,
    FoundSubsetLessThanMaxCompactSize,
}

impl FilesToSplitOrCompact {
    /// Number of files to compact; useful for logging
    pub fn num_files_to_compact(&self) -> usize {
        match &self {
            Self::Compact(files, ..) => files.len(),
            _ => 0,
        }
    }

    /// Number of files to split; useful for logging
    pub fn num_files_to_split(&self) -> usize {
        match &self {
            Self::Split(files, ..) => files.len(),
            _ => 0,
        }
    }

    /// Paths to the files for giving to the scratchpad.
    pub fn file_input_paths(&self) -> Vec<ParquetFilePath> {
        self.files().iter().map(|f| (*f).into()).collect()
    }

    /// References to the inner Parquet files
    pub fn files(&self) -> Vec<&ParquetFile> {
        match self {
            Self::None(..) => vec![],
            Self::Split(files, ..) => files.iter().map(|f| &f.file).collect(),
            Self::Compact(files, ..) => files.iter().collect(),
        }
    }

    /// Return files of either type
    pub fn into_files(self) -> Vec<ParquetFile> {
        match self {
            Self::None(..) => vec![],
            Self::Split(files, ..) => files.into_iter().map(|f| f.file).collect(),
            Self::Compact(files, ..) => files,
        }
    }
}

/// File to split and their split times
#[derive(Debug, PartialEq, Eq, Clone)]
pub struct FileToSplit {
    pub file: ParquetFile,
    pub split_times: Vec<i64>,
}

#[cfg(test)]
mod tests {
    use super::*;

    use iox_tests::ParquetFileBuilder;

    fn create_test_files_for_progress(
        num_to_upgrade: usize,
        num_to_compact: usize,
        num_to_split: usize,
    ) -> FilesForProgress {
        let file = ParquetFileBuilder::new(1).build();

        let split_or_compact = match (num_to_compact, num_to_split) {
            (0, 0) => FilesToSplitOrCompact::None(NoneReason::NoInputFiles),
            (n, 0) => FilesToSplitOrCompact::Compact(
                (0..n).map(|_| file.clone()).collect(),
                CompactReason::ManySmallFiles,
            ),
            (0, n) => FilesToSplitOrCompact::Split(
                (0..n)
                    .map(|_| FileToSplit {
                        file: file.clone(),
                        split_times: vec![],
                    })
                    .collect(),
                SplitReason::ReduceLargeFileSize,
            ),
            // Make sure this is a valid case; splitting and compacting are mutually exclusive
            _ => unreachable!("num_to_compact and num_to_split can't both be nonzero"),
        };

        FilesForProgress {
            upgrade: (0..num_to_upgrade).map(|_| file.clone()).collect(),
            split_or_compact,
        }
    }

    #[test]
    fn progress_is_empty_expectations() {
        let cases = [
            // (num to upgrade, num to compact, num to split, expected_empty_value)
            (0, 0, 0, true),
            (1, 0, 0, false),
            (0, 1, 0, false),
            (1, 1, 0, false),
            (0, 0, 1, false),
            (1, 0, 1, false),
            // These two cases don't make sense because splitting and compacting are mutually exclusive:
            // (0, 1, 1, invalid),
            // (1, 1, 1, invalid),
        ];

        for (num_to_upgrade, num_to_compact, num_to_split, expected_empty_value) in cases {
            let files_for_progress =
                create_test_files_for_progress(num_to_upgrade, num_to_compact, num_to_split);
            let actual_empty_value = files_for_progress.is_empty();
            assert_eq!(
                actual_empty_value, expected_empty_value,
                "In case {num_to_upgrade} files to upgrade, \
                {num_to_compact} files to compact, \
                {num_to_split} files to split, \
                got {actual_empty_value}, expected {expected_empty_value}"
            )
        }
    }
}
