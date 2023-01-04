//! Select partitions with small, adjacent L1 files and compact them

use crate::{
    compact::{self, Compactor, ShardAssignment},
    compact_candidates_with_memory_budget, compact_in_parallel,
    utils::get_candidates_with_retry,
    PartitionCompactionCandidateWithInfo,
};
use data_types::{CompactionLevel, PartitionParam, ShardId};
use iox_catalog::interface::Catalog;
use metric::Attributes;
use observability_deps::tracing::*;
use std::sync::Arc;

/// Warm compaction. Returns the number of compacted partitions.
pub async fn compact(compactor: Arc<Compactor>) -> usize {
    let compaction_type = "warm";

    let candidates = get_candidates_with_retry(
        Arc::clone(&compactor),
        compaction_type,
        |compactor_for_retry| async move { warm_partitions_to_compact(compactor_for_retry).await },
    )
    .await;

    let n_candidates = candidates.len();
    if n_candidates == 0 {
        debug!(compaction_type, "no compaction candidates found");
        return 0;
    } else {
        debug!(n_candidates, compaction_type, "found compaction candidates");
    }

    let start_time = compactor.time_provider.now();

    compact_candidates_with_memory_budget(
        Arc::clone(&compactor),
        compaction_type,
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
        let attributes = Attributes::from(&[("partition_type", compaction_type)]);
        let duration = compactor.compaction_cycle_duration.recorder(attributes);
        duration.record(delta);
    }

    n_candidates
}

/// Return a list of partitions that have enough small L1 files that make warm compaction
/// worthwhile. This works by counting the number of L1 files that are smaller than
/// `max_desired_file_size_bytes / 2`
pub(crate) async fn warm_partitions_to_compact(
    compactor: Arc<Compactor>,
) -> Result<Vec<Arc<PartitionCompactionCandidateWithInfo>>, compact::Error> {
    let compaction_type = "warm";

    let max_number_partitions_per_shard = compactor.config.max_number_partitions_per_shard;
    let mut candidates =
        Vec::with_capacity(compactor.shards.len() * max_number_partitions_per_shard);

    match &compactor.shards {
        ShardAssignment::All => {
            let mut partitions = warm_partitions_for_shard(
                Arc::clone(&compactor.catalog),
                None,
                compactor.config.warm_compaction_small_size_threshold_bytes,
                compactor.config.warm_compaction_min_small_file_count,
                max_number_partitions_per_shard,
            )
            .await?;

            // Record metric for candidates per shard
            let num_partitions = partitions.len();
            debug!(n = num_partitions, compaction_type, "compaction candidates",);
            let attributes = Attributes::from([("partition_type", compaction_type.into())]);
            let number_gauge = compactor.compaction_candidate_gauge.recorder(attributes);
            number_gauge.set(num_partitions as u64);

            candidates.append(&mut partitions);
        }
        ShardAssignment::Only(shards) => {
            for &shard_id in shards {
                let mut partitions = warm_partitions_for_shard(
                    Arc::clone(&compactor.catalog),
                    Some(shard_id),
                    compactor.config.warm_compaction_small_size_threshold_bytes,
                    compactor.config.warm_compaction_min_small_file_count,
                    max_number_partitions_per_shard,
                )
                .await?;

                // Record metric for candidates per shard
                let num_partitions = partitions.len();
                debug!(
                    shard_id = shard_id.get(),
                    n = num_partitions,
                    compaction_type,
                    "compaction candidates",
                );
                let attributes = Attributes::from([
                    ("shard_id", format!("{}", shard_id).into()),
                    ("partition_type", compaction_type.into()),
                ]);
                let number_gauge = compactor.compaction_candidate_gauge.recorder(attributes);
                number_gauge.set(num_partitions as u64);

                candidates.append(&mut partitions);
            }
        }
    }

    // Get extra needed information for selected partitions
    let start_time = compactor.time_provider.now();

    // Column types and their counts of the tables of the partition candidates
    debug!(
        num_candidates=?candidates.len(),
        compaction_type,
        "start getting column types for the partition candidates"
    );
    let table_columns = compactor.table_columns(&candidates).await?;

    // Add other compaction-needed info into selected partitions
    debug!(
        num_candidates=?candidates.len(),
        compaction_type,
        "start getting additional info for the partition candidates"
    );
    let candidates = compactor
        .add_info_to_partitions(&candidates, &table_columns)
        .await?;

    if let Some(delta) = compactor
        .time_provider
        .now()
        .checked_duration_since(start_time)
    {
        let attributes = Attributes::from(&[("partition_type", compaction_type)]);
        let duration = compactor
            .partitions_extra_info_reading_duration
            .recorder(attributes);
        duration.record(delta);
    }

    Ok(candidates)
}

async fn warm_partitions_for_shard(
    catalog: Arc<dyn Catalog>,
    shard_id: Option<ShardId>,
    // Upper bound on file size to be counted as "small" for warm compaction.
    small_size_threshold_bytes: i64,
    // Minimum number of small files a partition must have to be selected as a candidate for warm
    // compaction.
    min_small_file_count: usize,
    // Max number of partitions per shard we want to read
    max_number_partitions_per_shard: usize,
) -> Result<Vec<PartitionParam>, compact::Error> {
    let mut repos = catalog.repositories().await;

    let partitions = repos
        .parquet_files()
        .partitions_with_small_l1_file_count(
            shard_id,
            small_size_threshold_bytes,
            min_small_file_count,
            max_number_partitions_per_shard,
        )
        .await
        .map_err(|e| compact::Error::PartitionsWithSmallL1Files {
            shard_id,
            source: e,
        })?;
    if !partitions.is_empty() {
        debug!(
            ?shard_id,
            small_size_threshold_bytes,
            min_small_file_count,
            n = partitions.len(),
            "found some partitions with small L1 files"
        );
        return Ok(partitions);
    }

    Ok(Vec::new())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{compact::Compactor, handler::CompactorConfig};
    use arrow_util::assert_batches_sorted_eq;
    use backoff::BackoffConfig;
    use data_types::{ColumnType, CompactionLevel, TablePartition, Timestamp};
    use futures::{stream::FuturesUnordered, StreamExt};
    use iox_tests::util::{TestCatalog, TestParquetFileBuilder, TestShard, TestTable};
    use iox_time::{SystemProvider, Time, TimeProvider};
    use parquet_file::storage::{ParquetStorage, StorageId};
    use std::sync::Arc;

    struct TestSetup {
        catalog: Arc<TestCatalog>,
        shard1: Arc<TestShard>,
        table1: Arc<TestTable>,
        shard2: Arc<TestShard>,
    }

    async fn test_setup() -> TestSetup {
        let catalog = TestCatalog::new();
        let namespace = catalog
            .create_namespace_1hr_retention("namespace_warm_compaction")
            .await;
        let shard1 = namespace.create_shard(1).await;
        let table1 = namespace.create_table("test_table1").await;
        let shard2 = namespace.create_shard(2).await;

        TestSetup {
            catalog,
            shard1,
            table1,
            shard2,
        }
    }

    #[tokio::test]
    async fn no_partitions_no_candidates() {
        let TestSetup {
            catalog, shard1, ..
        } = test_setup().await;
        let candidates = warm_partitions_for_shard(
            Arc::clone(&catalog.catalog),
            Some(shard1.shard.id),
            100,
            10,
            1,
        )
        .await
        .unwrap();
        assert!(candidates.is_empty());
    }

    #[tokio::test]
    async fn no_files_no_candidates() {
        let TestSetup {
            catalog,
            shard1,
            table1,
            ..
        } = test_setup().await;
        table1.with_shard(&shard1).create_partition("one").await;
        let candidates = warm_partitions_for_shard(
            Arc::clone(&catalog.catalog),
            Some(shard1.shard.id),
            100,
            10,
            1,
        )
        .await
        .unwrap();
        assert!(candidates.is_empty());
    }

    #[tokio::test]
    async fn l0_not_returned() {
        let TestSetup {
            catalog,
            shard1,
            table1,
            ..
        } = test_setup().await;
        let partition1 = table1.with_shard(&shard1).create_partition("one").await;
        let builder = TestParquetFileBuilder::default()
            .with_compaction_level(CompactionLevel::Initial)
            .with_file_size_bytes(100);
        partition1.create_parquet_file_catalog_record(builder).await;
        assert_eq!(catalog.count_level_0_files(shard1.shard.id).await, 1);
        let candidates = warm_partitions_for_shard(
            Arc::clone(&catalog.catalog),
            Some(shard1.shard.id),
            200, // anything bigger than our file will work
            1,
            1,
        )
        .await
        .unwrap();
        assert!(candidates.is_empty());
    }

    #[tokio::test]
    async fn l2_not_returned() {
        let TestSetup {
            catalog,
            shard1,
            table1,
            ..
        } = test_setup().await;
        let partition1 = table1.with_shard(&shard1).create_partition("one").await;
        let builder = TestParquetFileBuilder::default()
            .with_compaction_level(CompactionLevel::Final)
            .with_file_size_bytes(100);
        partition1.create_parquet_file_catalog_record(builder).await;
        let candidates = warm_partitions_for_shard(
            Arc::clone(&catalog.catalog),
            Some(shard1.shard.id),
            200, // anything bigger than our file will work
            1,
            1,
        )
        .await
        .unwrap();
        assert!(candidates.is_empty());
    }

    #[tokio::test]
    async fn only_l1_returned() {
        let TestSetup {
            catalog,
            shard1,
            table1,
            ..
        } = test_setup().await;
        let partition1 = table1.with_shard(&shard1).create_partition("one").await;
        let builder = TestParquetFileBuilder::default()
            .with_compaction_level(CompactionLevel::FileNonOverlapped)
            .with_file_size_bytes(100);
        partition1.create_parquet_file_catalog_record(builder).await;
        let candidates = warm_partitions_for_shard(
            Arc::clone(&catalog.catalog),
            Some(shard1.shard.id),
            200, // anything bigger than our file will work
            1,
            1,
        )
        .await
        .unwrap();
        assert!(!candidates.is_empty());
    }

    #[tokio::test]
    async fn below_min_count_not_returned() {
        let TestSetup {
            catalog,
            shard1,
            table1,
            ..
        } = test_setup().await;
        let partition1 = table1.with_shard(&shard1).create_partition("one").await;
        let builder = TestParquetFileBuilder::default()
            .with_compaction_level(CompactionLevel::FileNonOverlapped)
            .with_file_size_bytes(100);
        partition1.create_parquet_file_catalog_record(builder).await;
        let candidates = warm_partitions_for_shard(
            Arc::clone(&catalog.catalog),
            Some(shard1.shard.id),
            200,
            2, // min limit is more than we have
            1,
        )
        .await
        .unwrap();
        assert!(candidates.is_empty());
    }

    #[tokio::test]
    async fn over_size_threshold_not_returned() {
        let TestSetup {
            catalog,
            shard1,
            table1,
            ..
        } = test_setup().await;
        let partition1 = table1.with_shard(&shard1).create_partition("one").await;
        let builder = TestParquetFileBuilder::default()
            .with_compaction_level(CompactionLevel::FileNonOverlapped)
            .with_file_size_bytes(200);
        partition1.create_parquet_file_catalog_record(builder).await;
        let candidates = warm_partitions_for_shard(
            Arc::clone(&catalog.catalog),
            Some(shard1.shard.id),
            100, // note, smaller than our file
            1,
            1,
        )
        .await
        .unwrap();
        assert!(candidates.is_empty());
    }

    #[tokio::test]
    async fn mixed_partition_but_warm_and_returned() {
        let TestSetup {
            catalog,
            shard1,
            table1,
            ..
        } = test_setup().await;

        // create a partition that meets the criteria for being warm
        let partition1 = table1.with_shard(&shard1).create_partition("one").await;
        // create three small files
        let builder = TestParquetFileBuilder::default()
            .with_compaction_level(CompactionLevel::FileNonOverlapped)
            .with_file_size_bytes(100);
        partition1
            .create_parquet_file_catalog_record(builder.clone())
            .await;
        partition1
            .create_parquet_file_catalog_record(builder.clone())
            .await;
        partition1.create_parquet_file_catalog_record(builder).await;
        // create one file that's too big
        let builder = TestParquetFileBuilder::default()
            .with_compaction_level(CompactionLevel::FileNonOverlapped)
            .with_file_size_bytes(1000);
        partition1.create_parquet_file_catalog_record(builder).await;
        // create two files that aren't L1
        let builder = TestParquetFileBuilder::default()
            .with_compaction_level(CompactionLevel::Initial)
            .with_file_size_bytes(200);
        partition1.create_parquet_file_catalog_record(builder).await;
        let builder = TestParquetFileBuilder::default()
            .with_compaction_level(CompactionLevel::Final)
            .with_file_size_bytes(200);
        partition1.create_parquet_file_catalog_record(builder).await;
        // create two more small files, for a total of five now
        let builder = TestParquetFileBuilder::default()
            .with_compaction_level(CompactionLevel::FileNonOverlapped)
            .with_file_size_bytes(100);
        partition1
            .create_parquet_file_catalog_record(builder.clone())
            .await;
        partition1.create_parquet_file_catalog_record(builder).await;
        let candidates = warm_partitions_for_shard(
            Arc::clone(&catalog.catalog),
            Some(shard1.shard.id),
            500,
            // recall that we created 5 small files above
            5,
            10,
        )
        .await
        .unwrap();
        assert_eq!(candidates.len(), 1);
    }

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
            max_number_partitions_per_shard: 1,
            min_number_recent_ingested_files_per_partition: 1,
            hot_multiple: 4,
            warm_multiple: 1,
            memory_budget_bytes: 10 * 1024 * 1024,
            min_num_rows_allocated_per_record_batch_to_datafusion_plan: 100,
            max_num_compacting_files: 20,
            max_num_compacting_files_first_in_partition: 40,
            minutes_without_new_writes_to_be_cold: 10,
            hot_compaction_hours_threshold_1: 4,
            hot_compaction_hours_threshold_2: 24,
            max_parallel_partitions: 20,
            warm_compaction_small_size_threshold_bytes,
            warm_compaction_min_small_file_count,
        }
    }

    #[tokio::test]
    async fn test_warm_partitions_to_compact() {
        let TestSetup {
            catalog,
            shard1,
            table1,
            shard2,
        } = test_setup().await;

        // Shard 1: 7 initally empty partitions
        let partition1 = table1.with_shard(&shard1).create_partition("waḥid").await;
        let partition2 = table1.with_shard(&shard1).create_partition("ʾiṯnān").await;
        let partition3 = table1.with_shard(&shard1).create_partition("ṯalāṯah").await;
        let partition4 = table1
            .with_shard(&shard1)
            .create_partition("ʾarbaʿah")
            .await;
        let partition5 = table1.with_shard(&shard1).create_partition("ḫamsah").await;
        let partition6 = table1.with_shard(&shard1).create_partition("sittah").await;
        let partition7 = table1
            .with_shard(&shard2)
            .create_partition_with_sort_key("sabʿah", &["tag1", "time"])
            .await;

        // Create a compactor
        let time_provider = Arc::clone(&catalog.time_provider);
        let config = make_compactor_config(10_000, 5_000, 10, 30);
        let compactor = Arc::new(Compactor::new(
            ShardAssignment::Only(vec![shard1.shard.id, shard2.shard.id]),
            Arc::clone(&catalog.catalog),
            ParquetStorage::new(Arc::clone(&catalog.object_store), StorageId::from("iox")),
            catalog.exec(),
            time_provider,
            BackoffConfig::default(),
            config,
            Arc::new(metric::Registry::new()),
        ));

        // This test is an integration test that covers the priority of the candidate selection
        // algorithm when there are many files of different kinds across many partitions.
        // Warm compaction candidate selection doesn't care about creation time.

        // partition1 has a deleted L1, isn't returned
        let builder = TestParquetFileBuilder::default()
            .with_to_delete(true)
            .with_compaction_level(CompactionLevel::FileNonOverlapped)
            .with_file_size_bytes(100);
        let _pf1 = partition1.create_parquet_file_catalog_record(builder).await;

        // partition2 has a non-L1 file, isn't returned
        let builder = TestParquetFileBuilder::default()
            .with_compaction_level(CompactionLevel::Initial)
            .with_file_size_bytes(100);
        let _pf2 = partition2.create_parquet_file_catalog_record(builder).await;

        // partition3 has too few small L1 files (min. is 10), isn't returned
        let builder = TestParquetFileBuilder::default()
            .with_compaction_level(CompactionLevel::FileNonOverlapped)
            .with_file_size_bytes(100);
        for _n in 1..=9 {
            let _pf3 = partition3
                .create_parquet_file_catalog_record(builder.clone())
                .await;
        }

        // partition4 has plenty of files but not enough small ones, isn't returned
        let builder = TestParquetFileBuilder::default()
            .with_compaction_level(CompactionLevel::FileNonOverlapped)
            .with_file_size_bytes(100);
        for _n in 1..=9 {
            let _pf4 = partition4
                .create_parquet_file_catalog_record(builder.clone())
                .await;
        }
        let builder = TestParquetFileBuilder::default()
            .with_compaction_level(CompactionLevel::FileNonOverlapped)
            .with_file_size_bytes(10000); // over the threshold of 5000
        for _n in 1..=2 {
            let _pf4 = partition4
                .create_parquet_file_catalog_record(builder.clone())
                .await;
        }

        // partition5 has plenty of files but not enough small L1s, isn't returned
        let builder = TestParquetFileBuilder::default()
            .with_compaction_level(CompactionLevel::FileNonOverlapped)
            .with_file_size_bytes(100);
        for _n in 1..=9 {
            let _pf5 = partition5
                .create_parquet_file_catalog_record(builder.clone())
                .await;
        }
        let builder = TestParquetFileBuilder::default()
            .with_compaction_level(CompactionLevel::Initial)
            .with_file_size_bytes(100);
        for _n in 1..=2 {
            let _pf5 = partition5
                .create_parquet_file_catalog_record(builder.clone())
                .await;
        }

        // partition6 has many small L1 files, is returned
        let builder = TestParquetFileBuilder::default()
            .with_compaction_level(CompactionLevel::FileNonOverlapped)
            .with_file_size_bytes(100);
        for _n in 1..=10 {
            let _pf6 = partition6
                .create_parquet_file_catalog_record(builder.clone())
                .await;
        }

        // partition7 has many small L1 files, plus some other things, is returned
        let builder = TestParquetFileBuilder::default()
            .with_compaction_level(CompactionLevel::FileNonOverlapped)
            .with_file_size_bytes(100);
        for _n in 1..=10 {
            let _pf7 = partition7
                .create_parquet_file_catalog_record(builder.clone())
                .await;
        }
        let builder = TestParquetFileBuilder::default()
            .with_compaction_level(CompactionLevel::Initial)
            .with_file_size_bytes(100);
        for _n in 1..=2 {
            let _pf7 = partition7
                .create_parquet_file_catalog_record(builder.clone())
                .await;
        }
        let builder = TestParquetFileBuilder::default()
            .with_compaction_level(CompactionLevel::Final)
            .with_file_size_bytes(100);
        for _n in 1..=2 {
            let _pf7 = partition7
                .create_parquet_file_catalog_record(builder.clone())
                .await;
        }
        let builder = TestParquetFileBuilder::default()
            .with_compaction_level(CompactionLevel::Final)
            .with_file_size_bytes(10000);
        for _n in 1..=2 {
            let _pf7 = partition7
                .create_parquet_file_catalog_record(builder.clone())
                .await;
        }

        // Will have 2 candidates, one for each shard
        let mut candidates = warm_partitions_to_compact(Arc::clone(&compactor))
            .await
            .unwrap();
        candidates.sort_by_key(|c| c.candidate);
        assert_eq!(candidates.len(), 2);

        assert_eq!(candidates[0].id(), partition6.partition.id);
        // this sort key is None
        assert_eq!(candidates[0].sort_key, partition6.partition.sort_key());

        assert_eq!(candidates[1].id(), partition7.partition.id);
        // this sort key is Some(tag1, time)
        assert_eq!(candidates[1].sort_key, partition7.partition.sort_key());
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
        let partition_candidates = warm_partitions_to_compact(Arc::clone(&compactor))
            .await
            .unwrap();
        assert_eq!(partition_candidates.len(), 1);

        compact_candidates_with_memory_budget(
            Arc::clone(&compactor),
            "warm",
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
    async fn test_compact_small_l1_files_becomes_two_l1s() {
        test_helpers::maybe_start_logging();
        let catalog = TestCatalog::new();
        let time = Arc::new(SystemProvider::new());

        let files = vec![
            // two contiguous, non-overlapping small files
            TestFileToCreate {
                // size 1782
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
                // size 1782
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
            // one non-overlapping file that is too big to join its predecessors
            TestFileToCreate {
                // size 1886
                lp: vec![
                    "dog_speed,breed=greyhound speed=62i 50",
                    "dog_speed,breed=saluki speed=60i 51",
                    "dog_speed,breed=vizsla speed=58i 52",
                    "dog_speed,breed=whippet speed=54i 53",
                    "dog_speed,breed=dalmation speed=53i 54",
                    "dog_speed,breed=borzoi speed=52i 55",
                    "dog_speed,breed=dachshund speed=30i 56",
                    "dog_speed,breed=chihuahua speed=24i 57",
                    "dog_speed,breed=pug speed=15i 60",
                ]
                .join("\n"),
                max_seq: 100,
                min_time: 50,
                max_time: 60,
                creation_time: time.minutes_ago(20),
                compaction_level: CompactionLevel::FileNonOverlapped,
            },
            // two more contiguous, non-overlapping small files
            TestFileToCreate {
                // size 1782
                lp: vec![
                    "dog_speed,breed=greyhound speed=61i 70",
                    "dog_speed,breed=whippet speed=53i 80",
                ]
                .join("\n"),
                max_seq: 120,
                min_time: 70,
                max_time: 80,
                creation_time: time.minutes_ago(15),
                compaction_level: CompactionLevel::FileNonOverlapped,
            },
            TestFileToCreate {
                // size 1782
                lp: vec![
                    "dog_speed,breed=greyhound speed=60i 90",
                    "dog_speed,breed=whippet speed=52i 100",
                ]
                .join("\n"),
                max_seq: 125,
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
        // max file size is small enough for the third, larger file to not qualify.
        // (note that the small file threshold is only used for filtering candidate partitions, not
        // for the actual compaction)
        // since we have such a small max file size, to make the tests easier to write, bump up the
        // percentage value so it doesn't try to split everything really small
        let config = make_compactor_config(4_000, 10_000, 4, 90);
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

        // should have 5 level-1 files before compacting
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
        assert_eq!(count, 5);

        // there will only be 1 because all files in the same partition
        let compactor = Arc::new(compactor);
        let partition_candidates = warm_partitions_to_compact(Arc::clone(&compactor))
            .await
            .unwrap();
        assert_eq!(partition_candidates.len(), 1);

        compact_candidates_with_memory_budget(
            Arc::clone(&compactor),
            "warm",
            CompactionLevel::FileNonOverlapped,
            // we are compacting L1 files into other L1 files
            CompactionLevel::FileNonOverlapped,
            compact_in_parallel,
            true, // split
            partition_candidates.into(),
        )
        .await;

        // the compactor will take the first two files only, because the third would push it over
        // max_desired_file_size_bytes. it will then compact those together.
        //   before:      after:
        //   -------      ------
        //   file 1
        //   file 2
        //   file 3       file 3
        //   file 4       file 4
        //   file 5       file 5
        //                file 6 (combined 1+2)
        // another warm compaction pass would combine files 3&4
        let mut files = catalog.list_by_table_not_to_delete(table.table.id).await;
        assert_eq!(files.len(), 4);
        // there should be one new file
        assert!(files.iter().any(|f| f.id.get() == 6));
        // check that the new file is the combination of files 1 & 2
        let file = files.pop().unwrap();
        let batches = table.read_parquet_file(file).await;
        assert_batches_sorted_eq!(
            &[
                "+-------+-----------+--------------------------------+",
                "| speed | breed     | time                           |",
                "+-------+-----------+--------------------------------+",
                "| 55    | whippet   | 1970-01-01T00:00:00.000000040Z |",
                "| 56    | whippet   | 1970-01-01T00:00:00.000000020Z |",
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
        let partition_candidates = warm_partitions_to_compact(Arc::clone(&compactor))
            .await
            .unwrap();
        assert_eq!(partition_candidates.len(), 1);

        compact_candidates_with_memory_budget(
            Arc::clone(&compactor),
            "warm",
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
        let partition_candidates = warm_partitions_to_compact(Arc::clone(&compactor))
            .await
            .unwrap();
        assert_eq!(partition_candidates.len(), 1);

        compact_candidates_with_memory_budget(
            Arc::clone(&compactor),
            "warm",
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
