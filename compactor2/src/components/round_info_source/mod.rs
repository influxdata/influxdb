use std::{
    fmt::{Debug, Display},
    sync::Arc,
};

use async_trait::async_trait;
use data_types::{CompactionLevel, ParquetFile, Timestamp};
use observability_deps::tracing::debug;

use crate::{error::DynError, PartitionInfo, RoundInfo};

/// Calculates information about what this compaction round does
#[async_trait]
pub trait RoundInfoSource: Debug + Display + Send + Sync {
    async fn calculate(
        &self,
        partition_info: &PartitionInfo,
        files: &[ParquetFile],
    ) -> Result<Arc<RoundInfo>, DynError>;
}

#[derive(Debug)]
pub struct LoggingRoundInfoWrapper {
    inner: Arc<dyn RoundInfoSource>,
}

impl LoggingRoundInfoWrapper {
    pub fn new(inner: Arc<dyn RoundInfoSource>) -> Self {
        Self { inner }
    }
}

impl Display for LoggingRoundInfoWrapper {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "LoggingRoundInfoWrapper({})", self.inner)
    }
}

#[async_trait]
impl RoundInfoSource for LoggingRoundInfoWrapper {
    async fn calculate(
        &self,
        partition_info: &PartitionInfo,
        files: &[ParquetFile],
    ) -> Result<Arc<RoundInfo>, DynError> {
        let res = self.inner.calculate(partition_info, files).await;
        if let Ok(round_info) = &res {
            debug!(round_info_source=%self.inner, %round_info, "running round");
        }
        res
    }
}

/// Computes the type of round based on the levels of the input files
#[derive(Debug)]
pub struct LevelBasedRoundInfo {
    pub max_num_files_per_plan: usize,
}

impl Display for LevelBasedRoundInfo {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "LevelBasedRoundInfo {}", self.max_num_files_per_plan)
    }
}
impl LevelBasedRoundInfo {
    pub fn new(max_num_files_per_plan: usize) -> Self {
        Self {
            max_num_files_per_plan,
        }
    }

    /// Returns true if number of files of the given start_level and
    /// their overlapped files in next level is over limit
    ///
    /// over the limit means that the maximum number of files that a subsequent compaction
    /// branch may choose to compact in a single plan would exceed `max_num_files_per_plan`
    pub fn too_many_files_to_compact(
        &self,
        files: &[ParquetFile],
        start_level: CompactionLevel,
    ) -> bool {
        let start_level_files = files
            .iter()
            .filter(|f| f.compaction_level == start_level)
            .collect::<Vec<_>>();
        let num_start_level = start_level_files.len();

        let next_level_files = files
            .iter()
            .filter(|f| f.compaction_level == start_level.next())
            .collect::<Vec<_>>();

        // The compactor may compact all the target level and next level together in one
        // branch in the worst case, thus if that would result in too many files to compact in a single
        // plan, run a pre-phase to reduce the number of files first
        let num_overlapped_files = get_num_overlapped_files(start_level_files, next_level_files);
        if num_start_level + num_overlapped_files > self.max_num_files_per_plan {
            return true;
        }

        false
    }
}

#[async_trait]
impl RoundInfoSource for LevelBasedRoundInfo {
    async fn calculate(
        &self,
        _partition_info: &PartitionInfo,
        files: &[ParquetFile],
    ) -> Result<Arc<RoundInfo>, DynError> {
        let start_level = get_start_level(files);

        if self.too_many_files_to_compact(files, start_level) {
            return Ok(Arc::new(RoundInfo::ManySmallFiles {
                start_level,
                max_num_files_to_group: self.max_num_files_per_plan,
            }));
        }

        let target_level = pick_level(files);
        Ok(Arc::new(RoundInfo::TargetLevel { target_level }))
    }
}

fn get_start_level(files: &[data_types::ParquetFile]) -> CompactionLevel {
    // panic if the files are empty
    assert!(!files.is_empty());

    // Start with initial level
    // If there are files in  this level, itis the start level
    // Otherwise repeat until reaching the final level.
    let mut level = CompactionLevel::Initial;
    while level != CompactionLevel::Final {
        if files.iter().any(|f| f.compaction_level == level) {
            return level;
        }

        level = level.next();
    }

    level
}

fn get_num_overlapped_files(
    start_level_files: Vec<&ParquetFile>,
    next_level_files: Vec<&ParquetFile>,
) -> usize {
    // min_time and max_time of files in start_level
    let (min_time, max_time) =
        start_level_files
            .iter()
            .fold((None, None), |(min_time, max_time), f| {
                let min_time = min_time
                    .map(|v: Timestamp| v.min(f.min_time))
                    .unwrap_or(f.min_time);
                let max_time = max_time
                    .map(|v: Timestamp| v.max(f.max_time))
                    .unwrap_or(f.max_time);
                (Some(min_time), Some(max_time))
            });

    // There must be values, otherwise panic
    let min_time = min_time.unwrap();
    let max_time = max_time.unwrap();

    // number of files in next level that overlap with files in start_level
    let count_overlapped = next_level_files
        .iter()
        .filter(|f| f.min_time <= max_time && f.max_time >= min_time)
        .count();

    count_overlapped
}

fn pick_level(files: &[data_types::ParquetFile]) -> CompactionLevel {
    // Start with initial level
    // If there are files in  this level, the compaction's target level will be the next level.
    // Otherwise repeat until reaching the final level.
    let mut level = CompactionLevel::Initial;
    while level != CompactionLevel::Final {
        if files.iter().any(|f| f.compaction_level == level) {
            return level.next();
        }

        level = level.next();
    }

    level
}

#[cfg(test)]
mod tests {
    use data_types::CompactionLevel;
    use iox_tests::ParquetFileBuilder;

    use crate::components::round_info_source::LevelBasedRoundInfo;

    #[test]
    fn test_too_many_files_to_compact() {
        // L0 files
        let f1 = ParquetFileBuilder::new(1)
            .with_time_range(0, 100)
            .with_compaction_level(CompactionLevel::Initial)
            .build();
        let f2 = ParquetFileBuilder::new(2)
            .with_time_range(0, 100)
            .with_compaction_level(CompactionLevel::Initial)
            .build();
        // non overlapping L1 file
        let f3 = ParquetFileBuilder::new(3)
            .with_time_range(101, 200)
            .with_compaction_level(CompactionLevel::FileNonOverlapped)
            .build();
        // overlapping L1 file
        let f4 = ParquetFileBuilder::new(4)
            .with_time_range(50, 150)
            .with_compaction_level(CompactionLevel::FileNonOverlapped)
            .build();

        // max 2 files per plan
        let round_info = LevelBasedRoundInfo {
            max_num_files_per_plan: 2,
        };

        // f1 and f2 are not over limit
        assert!(!round_info
            .too_many_files_to_compact(&[f1.clone(), f2.clone()], CompactionLevel::Initial));
        // f1, f2 and f3 are not over limit
        assert!(!round_info.too_many_files_to_compact(
            &[f1.clone(), f2.clone(), f3.clone()],
            CompactionLevel::Initial
        ));
        // f1, f2 and f4 are over limit
        assert!(round_info.too_many_files_to_compact(
            &[f1.clone(), f2.clone(), f4.clone()],
            CompactionLevel::Initial
        ));
        // f1, f2, f3 and f4 are over limit
        assert!(round_info.too_many_files_to_compact(&[f1, f2, f3, f4], CompactionLevel::Initial));
    }
}
