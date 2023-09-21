use std::{fmt::Display, sync::Arc};

use data_types::{ChunkOrder, CompactionLevel, ParquetFile, Timestamp, TimestampMinMax};
use parquet_file::ParquetFilePath;
use uuid::Uuid;

use crate::{
    file_classification::{CompactReason, FileToSplit, FilesToSplitOrCompact, SplitReason},
    partition_info::PartitionInfo,
    plan_ir::{FileIR, PlanIR},
};

use super::IRPlanner;

/// Builder for compaction plans.
///
/// This uses the first draft / version of how the compactor splits files / time ranges. There will probably future
/// implementations (maybe called V2, but maybe it also gets a proper name).
#[derive(Debug)]
pub struct V1IRPlanner {
    max_desired_file_size_bytes: u64,
    percentage_max_file_size: u16,
    split_percentage: u16,
}

impl V1IRPlanner {
    /// Create a new compact plan builder.
    pub fn new(
        max_desired_file_size_bytes: u64,
        percentage_max_file_size: u16,
        split_percentage: u16,
    ) -> Self {
        Self {
            max_desired_file_size_bytes,
            percentage_max_file_size,
            split_percentage,
        }
    }

    // compute cut off bytes for files
    fn cutoff_bytes(max_desired_file_size_bytes: u64, percentage_max_file_size: u16) -> (u64, u64) {
        (
            (max_desired_file_size_bytes * percentage_max_file_size as u64) / 100,
            (max_desired_file_size_bytes * (100 + percentage_max_file_size as u64)) / 100,
        )
    }

    // Compute time to split data
    // Return a list of times at which we want data to be split. The times are computed
    // based on the max_desired_file_size each file should not exceed and the total_size this input
    // time range [min_time, max_time] contains.
    // The split times assume that the data is evenly distributed in the time range and if
    // that is not the case the resulting files are not guaranteed to be below max_desired_file_size
    // Hence, the range between two contiguous returned time is percentage of
    // max_desired_file_size/total_size of the time range
    // Example:
    //  . Input
    //      min_time = 1
    //      max_time = 21
    //      total_size = 100
    //      max_desired_file_size = 30
    //
    //  . Pecentage = 70/100 = 0.3
    //  . Time range between 2 times = (21 - 1) * 0.3 = 6
    //
    //  . Output = [7, 13, 19] in which
    //     7 = 1 (min_time) + 6 (time range)
    //     13 = 7 (previous time) + 6 (time range)
    //     19 = 13 (previous time) + 6 (time range)
    pub fn compute_split_time(
        chunk_times: Vec<TimestampMinMax>,
        min_time: i64,
        max_time: i64,
        total_size: u64,
        max_desired_file_size: u64,
    ) -> Vec<i64> {
        // Too small to split
        if total_size <= max_desired_file_size {
            return vec![max_time];
        }

        // Same min and max time, nothing to split
        if min_time == max_time {
            return vec![max_time];
        }

        let mut split_times = vec![];

        let percentage = max_desired_file_size as f64 / total_size as f64;
        let interval = ((max_time - min_time) as f64 * percentage).ceil() as i64;

        let mut min = min_time;
        loop {
            let split_time = min + interval;

            if split_time >= max_time {
                break;
            } else if Self::time_range_present(&chunk_times, min, split_time) {
                split_times.push(split_time);
            }
            min = split_time;
        }

        split_times
    }

    // time_range_present returns true if the given time range is included in any of the chunks.
    fn time_range_present(chunk_times: &[TimestampMinMax], min_time: i64, max_time: i64) -> bool {
        chunk_times
            .iter()
            .any(|&chunk| chunk.max >= min_time && chunk.min <= max_time)
    }
}

impl Display for V1IRPlanner {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "v1")
    }
}

impl IRPlanner for V1IRPlanner {
    /// Build compact or split plans as appropriate
    fn create_plans(
        &self,
        partition: Arc<PartitionInfo>,
        target_level: CompactionLevel,
        split_or_compact: FilesToSplitOrCompact,
        object_store_ids: Vec<Uuid>,
        object_store_paths: Vec<ParquetFilePath>,
    ) -> Vec<PlanIR> {
        match split_or_compact {
            FilesToSplitOrCompact::Compact(files, reason) => {
                vec![self.compact_plan(
                    files,
                    object_store_paths,
                    object_store_ids,
                    reason,
                    partition,
                    target_level,
                )]
            }
            FilesToSplitOrCompact::Split(files, reason) => {
                files
                    .into_iter()
                    .zip(object_store_ids)
                    .zip(object_store_paths)
                    .map(|((file_to_split, object_store_id), object_store_path)| {
                        // target level of a split file is the same as its level
                        let target_level = file_to_split.file.compaction_level;

                        self.split_plan(
                            file_to_split,
                            object_store_path,
                            object_store_id,
                            reason,
                            Arc::clone(&partition),
                            target_level,
                        )
                    })
                    .collect()
            }
            FilesToSplitOrCompact::None(reason) => vec![PlanIR::None { reason }], // Nothing to do
        }
    }

    /// Build a plan to compact many files into a single file. Since we limit the size of the files,
    /// if the compact result is larger than that limit, we will split the output into many files
    fn compact_plan(
        &self,
        files: Vec<ParquetFile>,
        paths: Vec<ParquetFilePath>,
        object_store_ids: Vec<Uuid>,
        reason: CompactReason,
        _partition: Arc<PartitionInfo>,
        target_level: CompactionLevel,
    ) -> PlanIR {
        // gather data
        // total file size is the sum of the file sizes of the files to compact
        let total_size = files.iter().map(|f| f.file_size_bytes).sum::<i64>() as u64;
        let chunk_times = files
            .iter()
            .map(|f| TimestampMinMax::new(f.min_time.get(), f.max_time.get()))
            .collect::<Vec<_>>();
        let min_time = files
            .iter()
            .map(|f| f.min_time.get())
            .min()
            .expect("at least one file");
        let max_time = files
            .iter()
            .map(|f| f.max_time.get())
            .max()
            .expect("at least one file");

        let (small_cutoff_bytes, large_cutoff_bytes) = Self::cutoff_bytes(
            self.max_desired_file_size_bytes,
            self.percentage_max_file_size,
        );

        let files = files
            .into_iter()
            .zip(object_store_ids)
            .zip(paths)
            .map(|((file, object_store_id), path)| {
                let order = order(file.compaction_level, target_level, file.max_l0_created_at);
                FileIR {
                    file: ParquetFile {
                        object_store_id,
                        ..file
                    },
                    path,
                    order,
                }
            })
            .collect::<Vec<_>>();

        // Build logical compact plan
        if total_size <= small_cutoff_bytes || reason == CompactReason::ManySmallFiles {
            PlanIR::Compact {
                files,
                target_level,
                reason,
            }
        } else {
            let split_times = if small_cutoff_bytes < total_size && total_size <= large_cutoff_bytes
            {
                // Split compaction into two files, the earlier of split_percentage amount of
                // max_desired_file_size_bytes, the later of the rest
                vec![min_time + ((max_time - min_time) * self.split_percentage as i64) / 100]
            } else {
                // Split compaction into multiple files
                Self::compute_split_time(
                    chunk_times,
                    min_time,
                    max_time,
                    total_size,
                    self.max_desired_file_size_bytes,
                )
            };

            if split_times.is_empty() || (split_times.len() == 1 && split_times[0] == max_time) {
                // The split times might not have actually split anything, so in this case, compact
                // everything into one file
                PlanIR::Compact {
                    files,
                    target_level,
                    reason,
                }
            } else {
                // split compact query plan to split the result into multiple files
                PlanIR::Split {
                    files,
                    split_times,
                    target_level,
                    reason: SplitReason::CompactAndSplitOutput(reason),
                }
            }
        }
    }

    /// Build a plan to split a file into multiple files based on the given split times
    fn split_plan(
        &self,
        file_to_split: FileToSplit,
        path: ParquetFilePath,
        object_store_id: Uuid,
        reason: SplitReason,
        _partition: Arc<PartitionInfo>,
        target_level: CompactionLevel,
    ) -> PlanIR {
        let FileToSplit { file, split_times } = file_to_split;
        let order = order(file.compaction_level, target_level, file.max_l0_created_at);

        let file = FileIR {
            file: ParquetFile {
                object_store_id,
                ..file
            },
            path,
            order,
        };

        PlanIR::Split {
            files: vec![file],
            split_times,
            target_level,
            reason,
        }
    }
}

// Order of the chunk so they can be deduplicated correctly
fn order(
    compaction_level: CompactionLevel,
    target_level: CompactionLevel,
    max_l0_created_at: Timestamp,
) -> ChunkOrder {
    // TODO: If we chnage this design specified in driver.rs's compact functions, we will need to refine this
    // Currently, we only compact files of level_n with level_n+1 and produce level_n+1 files,
    // and with the strictly design that:
    //    . Level-0 files can overlap with any files.
    //    . Level-N files (N > 0) cannot overlap with any files in the same level.
    //    . For Level-0 files, we always pick the smaller `max_l0_created_at` files to compact (with
    //      each other and overlapped L1 files) first. `max_l0_created_at` is the max created time of all L0 files
    //      that were compacted into this given file. This value is used to order chunk for deduplication.
    //    . Level-N+1 files are results of compacting Level-N and/or Level-N+1 files, their `created_at`
    //      can be after the `created_at` of other Level-N files but they may include data loaded before
    //      the other Level-N files. Hence we should never use `created_at` of Level-N+1 files to order
    //      them with Level-N files.
    //    . We can only compact different sets of files of the same partition concurrently into the same target_level.
    // We can use the following rules to set order of the chunk of its (compaction_level, target_level) as follows:
    //    . compaction_level < target_level : the order is `created_at`
    //    . compaction_level == target_level : order is 0 to make sure it is in the front of the ordered list.
    //      This means that the chunk of `compaction_level == target_level` will be in arbitrary order and will be
    //      fine as long as they are in front of the chunks of `compaction_level < target_level`

    match (compaction_level, target_level) {
        (CompactionLevel::Initial, CompactionLevel::Initial)
        | (CompactionLevel::Initial, CompactionLevel::FileNonOverlapped)
        | (CompactionLevel::FileNonOverlapped, CompactionLevel::Final) => {
            ChunkOrder::new(max_l0_created_at.get())
        }
        (CompactionLevel::FileNonOverlapped, CompactionLevel::FileNonOverlapped)
        | (CompactionLevel::Final, CompactionLevel::Final) => ChunkOrder::new(0),
        _ => {
            panic!(
                "Invalid compaction level combination: ({compaction_level:?}, {target_level:?})",
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use data_types::TimestampMinMax;

    #[test]
    fn test_cutoff_bytes() {
        let (small, large) = V1IRPlanner::cutoff_bytes(100, 30);
        assert_eq!(small, 30);
        assert_eq!(large, 130);

        let (small, large) = V1IRPlanner::cutoff_bytes(100 * 1024 * 1024, 30);
        assert_eq!(small, 30 * 1024 * 1024);
        assert_eq!(large, 130 * 1024 * 1024);

        let (small, large) = V1IRPlanner::cutoff_bytes(100, 60);
        assert_eq!(small, 60);
        assert_eq!(large, 160);
    }

    #[test]
    fn test_compute_split_time() {
        let min_time = 1;
        let max_time = 11;
        let total_size = 100;
        let max_desired_file_size = 100;
        let chunk_times = vec![TimestampMinMax {
            min: min_time,
            max: max_time,
        }];

        // no split
        let result = V1IRPlanner::compute_split_time(
            chunk_times.clone(),
            min_time,
            max_time,
            total_size,
            max_desired_file_size,
        );
        assert_eq!(result.len(), 1);
        assert_eq!(result[0], max_time);

        // split 70% and 30%
        let max_desired_file_size = 70;
        let result = V1IRPlanner::compute_split_time(
            chunk_times.clone(),
            min_time,
            max_time,
            total_size,
            max_desired_file_size,
        );
        // only need to store the last split time
        assert_eq!(result.len(), 1);
        assert_eq!(result[0], 8); // = 1 (min_time) + 7

        // split 40%, 40%, 20%
        let max_desired_file_size = 40;
        let result = V1IRPlanner::compute_split_time(
            chunk_times,
            min_time,
            max_time,
            total_size,
            max_desired_file_size,
        );
        // store first and second split time
        assert_eq!(result.len(), 2);
        assert_eq!(result[0], 5); // = 1 (min_time) + 4
        assert_eq!(result[1], 9); // = 5 (previous split_time) + 4
    }

    #[test]
    fn compute_split_time_when_min_time_equals_max() {
        // Imagine a customer is backfilling a large amount of data and for some reason, all the
        // times on the data are exactly the same. That means the min_time and max_time will be the
        // same, but the total_size will be greater than the desired size.
        // We will not split it because the split has to stick to non-overlapped time range

        let min_time = 1;
        let max_time = 1;

        let total_size = 200;
        let max_desired_file_size = 100;
        let chunk_times = vec![TimestampMinMax {
            min: min_time,
            max: max_time,
        }];

        let result = V1IRPlanner::compute_split_time(
            chunk_times,
            min_time,
            max_time,
            total_size,
            max_desired_file_size,
        );

        // must return vector of one containing max_time
        assert_eq!(result.len(), 1);
        assert_eq!(result[0], 1);
    }

    #[test]
    fn compute_split_time_please_dont_explode() {
        // degenerated case where the step size is so small that it is < 1 (but > 0). In this case we shall still
        // not loop forever.
        let min_time = 10;
        let max_time = 20;

        let total_size = 600000;
        let max_desired_file_size = 10000;
        let chunk_times = vec![TimestampMinMax {
            min: min_time,
            max: max_time,
        }];

        let result = V1IRPlanner::compute_split_time(
            chunk_times,
            min_time,
            max_time,
            total_size,
            max_desired_file_size,
        );
        assert_eq!(result.len(), 9);
    }

    #[test]
    fn compute_split_time_chunk_gaps() {
        // When the chunks have large gaps, we should not introduce a splits that cause time ranges
        // known to be empty.  Split T2 below should not exist.
        //                   │               │
        //┌────────────────┐                   ┌──────────────┐
        //│    Chunk 1     │ │               │ │   Chunk 2    │
        //└────────────────┘                   └──────────────┘
        //                   │               │
        //                Split T1       Split T2

        // Create a scenario where naive splitting would produce 2 splits (3 chunks) as shown above, but
        // the only chunk data present is in the highest and lowest quarters, similar to what's shown above.
        let min_time = 1;
        let max_time = 100;

        let total_size = 200;
        let max_desired_file_size = total_size / 3;
        let chunk_times = vec![
            TimestampMinMax { min: 1, max: 24 },
            TimestampMinMax { min: 75, max: 100 },
        ];

        let result = V1IRPlanner::compute_split_time(
            chunk_times,
            min_time,
            max_time,
            total_size,
            max_desired_file_size,
        );

        // must return vector of one, containing a Split T1 shown above.
        assert_eq!(result.len(), 1);
        assert_eq!(result[0], 34);
    }
}
