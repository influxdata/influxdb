//! Select partitions with small, adjacent L1 files and compact them

use crate::{
    compact::Compactor, compact_candidates_with_memory_budget, compact_in_parallel,
    parquet_file_lookup::CompactionType, utils::get_candidates_with_retry,
};
use data_types::CompactionLevel;
use metric::Attributes;
use observability_deps::tracing::*;
use std::sync::Arc;

/// Warm compaction. Returns the number of compacted partitions.
pub async fn compact(compactor: Arc<Compactor>) -> usize {
    let compaction_type = CompactionType::Warm;

    // https://github.com/influxdata/influxdb_iox/issues/6518 to remove the use of shard_id and
    // simplify this
    let max_num_partitions =
        compactor.shards.len() * compactor.config.max_number_partitions_per_shard;

    let hours_threshold = compactor.config.warm_partition_candidates_hours_threshold;
    let candidates = get_candidates_with_retry(
        Arc::clone(&compactor),
        compaction_type,
        move |compactor_for_retry| async move {
            compactor_for_retry
                .partitions_to_compact(compaction_type, vec![hours_threshold], max_num_partitions)
                .await
        },
    )
    .await;

    let n_candidates = candidates.len();
    if n_candidates == 0 {
        debug!(%compaction_type, "no compaction candidates found");
        return 0;
    } else {
        debug!(n_candidates, %compaction_type, "found compaction candidates");
    }

    let start_time = compactor.time_provider.now();

    compact_candidates_with_memory_budget(
        Arc::clone(&compactor),
        CompactionType::Warm,
        CompactionLevel::FileNonOverlapped,
        // we are compacting L1 files into other L1 files
        CompactionLevel::FileNonOverlapped,
        compact_in_parallel,
        true, // split
        candidates.into(),
    )
    .await;

    // Done compacting all candidates in the cycle, record its time
    if let Some(delta) = compactor
        .time_provider
        .now()
        .checked_duration_since(start_time)
    {
        let attributes = Attributes::from([("partition_type", compaction_type.to_string().into())]);
        let duration = compactor.compaction_cycle_duration.recorder(attributes);
        duration.record(delta);
    }

    n_candidates
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        compact::{Compactor, ShardAssignment},
        handler::CompactorConfig,
    };
    use arrow_util::assert_batches_sorted_eq;
    use backoff::BackoffConfig;
    use data_types::{ColumnType, CompactionLevel, TablePartition, Timestamp};
    use futures::{stream::FuturesUnordered, StreamExt};
    use iox_tests::{TestCatalog, TestParquetFileBuilder};
    use iox_time::{SystemProvider, Time, TimeProvider};
    use parquet_file::storage::{ParquetStorage, StorageId};
    use std::sync::Arc;

    const DEFAULT_MAX_NUM_PARTITION_CANDIDATES: usize = 100;
    const DEFAULT_HOT_COMPACTION_HOURS_THRESHOLD_1: u64 = 4;
    const DEFAULT_HOT_COMPACTION_HOURS_THRESHOLD_2: u64 = 24;
    const DEFAULT_WARM_PARTITION_CANDIDATES_HOURS_THRESHOLD: u64 = 24;
    const DEFAULT_COLD_PARTITION_CANDIDATES_HOURS_THRESHOLD: u64 = 24;
    const DEFAULT_MAX_PARALLEL_PARTITIONS: u64 = 20;
    const DEFAULT_MINUTES_WITHOUT_NEW_WRITES: u64 = 8 * 60;
    const DEFAULT_MIN_ROWS_ALLOCATED: u64 = 100;

    fn make_compactor_config(
        max_desired_file_size_bytes: u64,
        warm_compaction_small_size_threshold_bytes: i64,
        warm_compaction_min_small_file_count: usize,
        percentage_max_file_size: u16,
    ) -> CompactorConfig {
        CompactorConfig {
            max_desired_file_size_bytes,
            percentage_max_file_size,
            split_percentage: 80,
            max_number_partitions_per_shard: DEFAULT_MAX_NUM_PARTITION_CANDIDATES,
            min_number_recent_ingested_files_per_partition: 1,
            hot_multiple: 4,
            warm_multiple: 1,
            memory_budget_bytes: 10 * 1024 * 1024,
            min_num_rows_allocated_per_record_batch_to_datafusion_plan: DEFAULT_MIN_ROWS_ALLOCATED,
            max_num_compacting_files: 20,
            max_num_compacting_files_first_in_partition: 40,
            minutes_without_new_writes_to_be_cold: DEFAULT_MINUTES_WITHOUT_NEW_WRITES,
            cold_partition_candidates_hours_threshold:
                DEFAULT_COLD_PARTITION_CANDIDATES_HOURS_THRESHOLD,
            hot_compaction_hours_threshold_1: DEFAULT_HOT_COMPACTION_HOURS_THRESHOLD_1,
            hot_compaction_hours_threshold_2: DEFAULT_HOT_COMPACTION_HOURS_THRESHOLD_2,
            max_parallel_partitions: DEFAULT_MAX_PARALLEL_PARTITIONS,
            warm_partition_candidates_hours_threshold:
                DEFAULT_WARM_PARTITION_CANDIDATES_HOURS_THRESHOLD,
            warm_compaction_small_size_threshold_bytes,
            warm_compaction_min_small_file_count,
        }
    }

    struct TestFileToCreate {
        lp: String,
        max_seq: i64,
        min_time: i64,
        max_time: i64,
        creation_time: Time,
        compaction_level: CompactionLevel,
    }

    #[tokio::test]
    async fn test_compact_small_l1_files_below_max_into_larger_l1() {
        test_helpers::maybe_start_logging();
        let catalog = TestCatalog::new();
        let time = Arc::new(SystemProvider::new());

        // four contiguous, non-overlapping small files
        let files = vec![
            TestFileToCreate {
                lp: vec![
                    "dog_speed,breed=greyhound speed=64i 10",
                    "dog_speed,breed=whippet speed=56i 20",
                ]
                .join("\n"),
                max_seq: 5,
                min_time: 10,
                max_time: 20,
                creation_time: time.minutes_ago(30),
                compaction_level: CompactionLevel::FileNonOverlapped,
            },
            TestFileToCreate {
                lp: vec![
                    "dog_speed,breed=greyhound speed=63i 30",
                    "dog_speed,breed=whippet speed=55i 40",
                ]
                .join("\n"),
                max_seq: 10,
                min_time: 30,
                max_time: 40,
                creation_time: time.minutes_ago(25),
                compaction_level: CompactionLevel::FileNonOverlapped,
            },
            TestFileToCreate {
                lp: vec![
                    "dog_speed,breed=greyhound speed=62i 50",
                    "dog_speed,breed=whippet speed=54i 60",
                ]
                .join("\n"),
                max_seq: 15,
                min_time: 50,
                max_time: 60,
                creation_time: time.minutes_ago(20),
                compaction_level: CompactionLevel::FileNonOverlapped,
            },
            TestFileToCreate {
                lp: vec![
                    "dog_speed,breed=greyhound speed=61i 70",
                    "dog_speed,breed=whippet speed=53i 80",
                ]
                .join("\n"),
                max_seq: 20,
                min_time: 70,
                max_time: 80,
                creation_time: time.minutes_ago(15),
                compaction_level: CompactionLevel::FileNonOverlapped,
            },
        ];

        let ns = catalog.create_namespace_1hr_retention("ns").await;
        let shard = ns.create_shard(1).await;
        let table = ns.create_table("dog_speed").await;
        table.create_column("speed", ColumnType::I64).await;
        table.create_column("breed", ColumnType::Tag).await;
        table.create_column("time", ColumnType::Time).await;

        let partition = table.with_shard(&shard).create_partition("part").await;
        let config = make_compactor_config(100_000, 5_000, 4, 30);
        let metrics = Arc::new(metric::Registry::new());
        let compactor = Compactor::new(
            ShardAssignment::Only(vec![shard.shard.id]),
            Arc::clone(&catalog.catalog),
            ParquetStorage::new(Arc::clone(&catalog.object_store), StorageId::from("iox")),
            catalog.exec(),
            time,
            BackoffConfig::default(),
            config,
            Arc::clone(&metrics),
        );

        files
            .into_iter()
            .map(|file| {
                let partition = Arc::clone(&partition);
                async move {
                    let builder = TestParquetFileBuilder::default()
                        .with_line_protocol(&file.lp)
                        .with_max_seq(file.max_seq)
                        .with_min_time(file.min_time)
                        .with_max_time(file.max_time)
                        .with_creation_time(file.creation_time)
                        .with_compaction_level(file.compaction_level);
                    let _pf = partition.create_parquet_file(builder).await;
                }
            })
            .collect::<FuturesUnordered<_>>()
            .collect::<Vec<_>>()
            .await;

        // should have 4 level-1 files before compacting
        let count = catalog
            .count_level_1_files(
                TablePartition {
                    table_id: table.table.id,
                    shard_id: shard.shard.id,
                    partition_id: partition.partition.id,
                },
                Timestamp::new(0),
                Timestamp::new(100),
            )
            .await;
        assert_eq!(count, 4);

        // there will only be 1 because all files in the same partition
        let compactor = Arc::new(compactor);
        let compaction_type = CompactionType::Warm;
        let hour_threshold = compactor.config.warm_partition_candidates_hours_threshold;
        let max_num_partitions = compactor.config.max_number_partitions_per_shard;
        let partition_candidates = compactor
            .partitions_to_compact(compaction_type, vec![hour_threshold], max_num_partitions)
            .await
            .unwrap();
        assert_eq!(partition_candidates.len(), 1);

        compact_candidates_with_memory_budget(
            Arc::clone(&compactor),
            CompactionType::Warm,
            CompactionLevel::FileNonOverlapped,
            // we are compacting L1 files into other L1 files
            CompactionLevel::FileNonOverlapped,
            compact_in_parallel,
            true, // split
            partition_candidates.into(),
        )
        .await;

        // should have 1 non-soft-deleted L1 file with the next ID, which is 5
        let mut files = catalog.list_by_table_not_to_delete(table.table.id).await;
        assert_eq!(files.len(), 1, "{files:?}");
        let files_and_levels: Vec<_> = files
            .iter()
            .map(|f| (f.id.get(), f.compaction_level))
            .collect();
        assert_eq!(
            files_and_levels,
            vec![(5, CompactionLevel::FileNonOverlapped),]
        );

        // ------------------------------------------------
        // Verify the parquet file contents

        let file = files.pop().unwrap();
        let batches = table.read_parquet_file(file).await;
        assert_batches_sorted_eq!(
            &[
                "+-------+-----------+--------------------------------+",
                "| speed | breed     | time                           |",
                "+-------+-----------+--------------------------------+",
                "| 53    | whippet   | 1970-01-01T00:00:00.000000080Z |",
                "| 54    | whippet   | 1970-01-01T00:00:00.000000060Z |",
                "| 55    | whippet   | 1970-01-01T00:00:00.000000040Z |",
                "| 56    | whippet   | 1970-01-01T00:00:00.000000020Z |",
                "| 61    | greyhound | 1970-01-01T00:00:00.000000070Z |",
                "| 62    | greyhound | 1970-01-01T00:00:00.000000050Z |",
                "| 63    | greyhound | 1970-01-01T00:00:00.000000030Z |",
                "| 64    | greyhound | 1970-01-01T00:00:00.000000010Z |",
                "+-------+-----------+--------------------------------+",
            ],
            &batches
        );
    }

    #[tokio::test]
    async fn test_compact_small_l1_files_ignores_l0_files() {
        test_helpers::maybe_start_logging();
        let catalog = TestCatalog::new();
        let time = Arc::new(SystemProvider::new());

        let files = vec![
            // two contiguous, non-overlapping small L1 files
            TestFileToCreate {
                lp: vec![
                    "dog_speed,breed=greyhound speed=64i 10",
                    "dog_speed,breed=whippet speed=56i 20",
                ]
                .join("\n"),
                max_seq: 5,
                min_time: 10,
                max_time: 20,
                creation_time: time.minutes_ago(30),
                compaction_level: CompactionLevel::FileNonOverlapped,
            },
            TestFileToCreate {
                lp: vec![
                    "dog_speed,breed=greyhound speed=63i 30",
                    "dog_speed,breed=whippet speed=55i 40",
                ]
                .join("\n"),
                max_seq: 10,
                min_time: 30,
                max_time: 40,
                creation_time: time.minutes_ago(25),
                compaction_level: CompactionLevel::FileNonOverlapped,
            },
            // one non-overlapping L0 file that shouldn't be compacted with the L1s
            TestFileToCreate {
                lp: vec![
                    "dog_speed,breed=greyhound speed=62i 50",
                    "dog_speed,breed=saluki speed=60i 60",
                ]
                .join("\n"),
                max_seq: 15,
                min_time: 50,
                max_time: 60,
                creation_time: time.minutes_ago(20),
                compaction_level: CompactionLevel::Initial,
            },
            // one overlapping L0 file that shouldn't be compacted with the L1s
            TestFileToCreate {
                lp: vec![
                    "dog_speed,breed=greyhound speed=61i 30",
                    "dog_speed,breed=saluki speed=59i 40",
                ]
                .join("\n"),
                max_seq: 20,
                min_time: 30,
                max_time: 40,
                creation_time: time.minutes_ago(20),
                compaction_level: CompactionLevel::Initial,
            },
            // two more contiguous, non-overlapping small L1 files
            TestFileToCreate {
                lp: vec![
                    "dog_speed,breed=greyhound speed=60i 70",
                    "dog_speed,breed=whippet speed=54i 80",
                ]
                .join("\n"),
                max_seq: 25,
                min_time: 70,
                max_time: 80,
                creation_time: time.minutes_ago(15),
                compaction_level: CompactionLevel::FileNonOverlapped,
            },
            TestFileToCreate {
                lp: vec![
                    "dog_speed,breed=greyhound speed=59i 90",
                    "dog_speed,breed=whippet speed=53i 100",
                ]
                .join("\n"),
                max_seq: 30,
                min_time: 90,
                max_time: 100,
                creation_time: time.minutes_ago(10),
                compaction_level: CompactionLevel::FileNonOverlapped,
            },
        ];

        let ns = catalog.create_namespace_1hr_retention("ns").await;
        let shard = ns.create_shard(1).await;
        let table = ns.create_table("dog_speed").await;
        table.create_column("speed", ColumnType::I64).await;
        table.create_column("breed", ColumnType::Tag).await;
        table.create_column("time", ColumnType::Time).await;

        let partition = table.with_shard(&shard).create_partition("part").await;
        let config = make_compactor_config(100_000, 5_000, 4, 30);
        let metrics = Arc::new(metric::Registry::new());
        let compactor = Compactor::new(
            ShardAssignment::Only(vec![shard.shard.id]),
            Arc::clone(&catalog.catalog),
            ParquetStorage::new(Arc::clone(&catalog.object_store), StorageId::from("iox")),
            catalog.exec(),
            time,
            BackoffConfig::default(),
            config,
            Arc::clone(&metrics),
        );

        files
            .into_iter()
            .map(|file| {
                let partition = Arc::clone(&partition);
                async move {
                    let builder = TestParquetFileBuilder::default()
                        .with_line_protocol(&file.lp)
                        .with_max_seq(file.max_seq)
                        .with_min_time(file.min_time)
                        .with_max_time(file.max_time)
                        .with_creation_time(file.creation_time)
                        .with_compaction_level(file.compaction_level);
                    let _pf = partition.create_parquet_file(builder).await;
                }
            })
            .collect::<FuturesUnordered<_>>()
            .collect::<Vec<_>>()
            .await;

        // should have 4 level-1 files and 2 level-0 files before compacting
        let count = catalog
            .count_level_1_files(
                TablePartition {
                    table_id: table.table.id,
                    shard_id: shard.shard.id,
                    partition_id: partition.partition.id,
                },
                Timestamp::new(0),
                Timestamp::new(100),
            )
            .await;
        assert_eq!(count, 4);
        let count = catalog.count_level_0_files(shard.shard.id).await;
        assert_eq!(count, 2);

        // there will only be 1 because all files in the same partition
        let compactor = Arc::new(compactor);
        let compaction_type = CompactionType::Warm;
        let hour_threshold = compactor.config.warm_partition_candidates_hours_threshold;
        let max_num_partitions = compactor.config.max_number_partitions_per_shard;
        let partition_candidates = compactor
            .partitions_to_compact(compaction_type, vec![hour_threshold], max_num_partitions)
            .await
            .unwrap();
        assert_eq!(partition_candidates.len(), 1);

        compact_candidates_with_memory_budget(
            Arc::clone(&compactor),
            CompactionType::Warm,
            CompactionLevel::FileNonOverlapped,
            // we are compacting L1 files into other L1 files
            CompactionLevel::FileNonOverlapped,
            compact_in_parallel,
            true, // split
            partition_candidates.into(),
        )
        .await;

        // this compaction outputs 3 files: the two L0 files left alone and the four L1
        // files compacted together that got the next ID.
        let files = catalog.list_by_table_not_to_delete(table.table.id).await;
        assert_eq!(files.len(), 3, "{files:?}");
        let files_and_levels: Vec<_> = files
            .iter()
            .map(|f| (f.id.get(), f.compaction_level))
            .collect();
        assert_eq!(
            files_and_levels,
            vec![
                (3, CompactionLevel::Initial),
                (4, CompactionLevel::Initial),
                (7, CompactionLevel::FileNonOverlapped),
            ]
        );
    }

    #[tokio::test]
    async fn test_compact_small_l1_files_ignores_non_overlapping_l2_files() {
        test_helpers::maybe_start_logging();
        let catalog = TestCatalog::new();
        let time = Arc::new(SystemProvider::new());

        let files = vec![
            // two contiguous, non-overlapping small L1 files
            TestFileToCreate {
                lp: vec![
                    "dog_speed,breed=greyhound speed=64i 10",
                    "dog_speed,breed=whippet speed=56i 20",
                ]
                .join("\n"),
                max_seq: 5,
                min_time: 10,
                max_time: 20,
                creation_time: time.minutes_ago(30),
                compaction_level: CompactionLevel::FileNonOverlapped,
            },
            TestFileToCreate {
                lp: vec![
                    "dog_speed,breed=greyhound speed=63i 30",
                    "dog_speed,breed=whippet speed=55i 40",
                ]
                .join("\n"),
                max_seq: 10,
                min_time: 30,
                max_time: 40,
                creation_time: time.minutes_ago(25),
                compaction_level: CompactionLevel::FileNonOverlapped,
            },
            // one non-overlapping L2 file that shouldn't be compacted with the L1s
            TestFileToCreate {
                lp: vec![
                    "dog_speed,breed=greyhound speed=62i 50",
                    "dog_speed,breed=saluki speed=60i 60",
                ]
                .join("\n"),
                max_seq: 15,
                min_time: 50,
                max_time: 60,
                creation_time: time.minutes_ago(20),
                compaction_level: CompactionLevel::Final,
            },
            // one overlapping L2 file that should be compacted with the L1s
            TestFileToCreate {
                lp: vec![
                    "dog_speed,breed=greyhound speed=61i 30",
                    "dog_speed,breed=saluki speed=59i 40",
                ]
                .join("\n"),
                max_seq: 20,
                min_time: 30,
                max_time: 40,
                creation_time: time.minutes_ago(20),
                compaction_level: CompactionLevel::Final,
            },
            // two more contiguous, non-overlapping small L1 files
            TestFileToCreate {
                lp: vec![
                    "dog_speed,breed=greyhound speed=60i 70",
                    "dog_speed,breed=whippet speed=54i 80",
                ]
                .join("\n"),
                max_seq: 25,
                min_time: 70,
                max_time: 80,
                creation_time: time.minutes_ago(15),
                compaction_level: CompactionLevel::FileNonOverlapped,
            },
            TestFileToCreate {
                lp: vec![
                    "dog_speed,breed=greyhound speed=59i 90",
                    "dog_speed,breed=whippet speed=53i 100",
                ]
                .join("\n"),
                max_seq: 30,
                min_time: 90,
                max_time: 100,
                creation_time: time.minutes_ago(10),
                compaction_level: CompactionLevel::FileNonOverlapped,
            },
        ];

        let ns = catalog.create_namespace_1hr_retention("ns").await;
        let shard = ns.create_shard(1).await;
        let table = ns.create_table("dog_speed").await;
        table.create_column("speed", ColumnType::I64).await;
        table.create_column("breed", ColumnType::Tag).await;
        table.create_column("time", ColumnType::Time).await;

        let partition = table.with_shard(&shard).create_partition("part").await;
        let config = make_compactor_config(100_000, 10_000, 4, 30);
        let metrics = Arc::new(metric::Registry::new());
        let compactor = Compactor::new(
            ShardAssignment::Only(vec![shard.shard.id]),
            Arc::clone(&catalog.catalog),
            ParquetStorage::new(Arc::clone(&catalog.object_store), StorageId::from("iox")),
            catalog.exec(),
            time,
            BackoffConfig::default(),
            config,
            Arc::clone(&metrics),
        );

        files
            .into_iter()
            .map(|file| {
                let partition = Arc::clone(&partition);
                async move {
                    let builder = TestParquetFileBuilder::default()
                        .with_line_protocol(&file.lp)
                        .with_max_seq(file.max_seq)
                        .with_min_time(file.min_time)
                        .with_max_time(file.max_time)
                        .with_creation_time(file.creation_time)
                        .with_compaction_level(file.compaction_level);
                    let _pf = partition.create_parquet_file(builder).await;
                }
            })
            .collect::<FuturesUnordered<_>>()
            .collect::<Vec<_>>()
            .await;

        // should have four level-1 files and two level-2 files before compacting
        let l1_count = catalog
            .count_level_1_files(
                TablePartition {
                    table_id: table.table.id,
                    shard_id: shard.shard.id,
                    partition_id: partition.partition.id,
                },
                Timestamp::new(0),
                Timestamp::new(100),
            )
            .await;
        assert_eq!(l1_count, 4);
        let files = catalog.list_by_table_not_to_delete(table.table.id).await;
        let total_count = files.len();
        // by deduction this is l2 count (wasn't worth adding a count fn to the catalog!)
        assert_eq!(total_count - l1_count, 2);

        // there will only be 1 because all files in the same partition
        let compactor = Arc::new(compactor);
        let compaction_type = CompactionType::Warm;
        let hour_threshold = compactor.config.warm_partition_candidates_hours_threshold;
        let max_num_partitions = compactor.config.max_number_partitions_per_shard;
        let partition_candidates = compactor
            .partitions_to_compact(compaction_type, vec![hour_threshold], max_num_partitions)
            .await
            .unwrap();
        assert_eq!(partition_candidates.len(), 1);

        compact_candidates_with_memory_budget(
            Arc::clone(&compactor),
            CompactionType::Warm,
            CompactionLevel::FileNonOverlapped,
            // we are compacting L1 files into other L1 files
            CompactionLevel::FileNonOverlapped,
            compact_in_parallel,
            true, // split
            partition_candidates.into(),
        )
        .await;

        // this compaction outputs three files: the two L2 files are left alone, regardless of whether
        // or not they overlap with L1 files, and the remaining L1s are compacted into one file.
        // the important thing for us here is that it didn't do anything with the L2's- the current
        // compactor algorithm won't compact L2s down into L1s. the L2 that overlaps with L1 will
        // be dealt with in a round of cold compaction later.
        let files = catalog.list_by_table_not_to_delete(table.table.id).await;
        assert_eq!(files.len(), 3, "{files:?}");
        let files_and_levels: Vec<_> = files
            .iter()
            .map(|f| (f.id.get(), f.compaction_level))
            .collect();
        assert_eq!(
            files_and_levels,
            vec![
                (3, CompactionLevel::Final),
                (4, CompactionLevel::Final),
                (7, CompactionLevel::FileNonOverlapped),
            ]
        );
    }
}
