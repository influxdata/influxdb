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

    /// Files which should be compacted into a new single parquet
    /// file, often the small and/or overlapped files
    pub files_to_compact: Vec<ParquetFile>,

    /// Non-overlapped files that should be upgraded to the target
    /// level without rewriting (for example they are of sufficient
    /// size)
    pub files_to_upgrade: Vec<ParquetFile>,

    /// files which should not be modified. For example,
    /// non-overlapped or higher-target-level files
    pub files_to_keep: Vec<ParquetFile>,
}
