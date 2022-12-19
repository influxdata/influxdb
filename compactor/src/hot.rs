//! Collect highest hot candidates and compact them

use crate::{
    compact::{self, Compactor, ShardAssignment},
    compact_candidates_with_memory_budget, compact_in_parallel,
    utils::get_candidates_with_retry,
    PartitionCompactionCandidateWithInfo,
};
use data_types::{CompactionLevel, PartitionParam, ShardId, Timestamp};
use iox_catalog::interface::Catalog;
use iox_time::TimeProvider;
use metric::Attributes;
use observability_deps::tracing::*;
use std::sync::Arc;

/// Hot compaction. Returns the number of compacted partitions.
pub async fn compact(compactor: Arc<Compactor>) -> usize {
    let compaction_type = "hot";

    let candidates = get_candidates_with_retry(
        Arc::clone(&compactor),
        compaction_type,
        |compactor_for_retry| async move { hot_partitions_to_compact(compactor_for_retry).await },
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
        CompactionLevel::Initial,
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

/// Return a list of the most recent highest ingested throughput partitions.
/// The highest throughput partitions are prioritized as follows:
///  1. If there are partitions with new ingested files within the last 4 hours (the default, but
///     configurable), pick them.
///  2. If no new ingested files in the last 4 hours, will look for partitions with new writes
///     within the last 24 hours (the default, but configurable).
///  3. If there are no ingested files within the last 24 hours, will look for partitions
///     with any new ingested files in the past.
///
/// * New ingested files means non-deleted L0 files
/// * In all cases above, for each shard, N partitions with the most new ingested files
///   will be selected and the return list will include at most, P = N * S, partitions where S
///   is the number of shards this compactor handles.
pub(crate) async fn hot_partitions_to_compact(
    compactor: Arc<Compactor>,
) -> Result<Vec<Arc<PartitionCompactionCandidateWithInfo>>, compact::Error> {
    let compaction_type = "hot";

    let min_number_recent_ingested_files_per_partition = compactor
        .config
        .min_number_recent_ingested_files_per_partition;
    let max_number_partitions_per_shard = compactor.config.max_number_partitions_per_shard;
    let mut candidates =
        Vec::with_capacity(compactor.shards.len() * max_number_partitions_per_shard);

    // Get the most recent highest ingested throughput partitions within the last 4 hours. If not,
    // increase to 24 hours.
    let query_times = query_times(
        compactor.time_provider(),
        compactor.config.hot_compaction_hours_threshold_1,
        compactor.config.hot_compaction_hours_threshold_2,
    );

    match &compactor.shards {
        ShardAssignment::All => {
            let mut partitions = hot_partitions_for_shard(
                Arc::clone(&compactor.catalog),
                None,
                &query_times,
                min_number_recent_ingested_files_per_partition,
                max_number_partitions_per_shard,
            )
            .await?;

            // Record metric for candidates
            let num_partitions = partitions.len();
            debug!(n = num_partitions, compaction_type, "compaction candidates",);
            let attributes = Attributes::from([("partition_type", compaction_type.into())]);
            let number_gauge = compactor.compaction_candidate_gauge.recorder(attributes);
            number_gauge.set(num_partitions as u64);

            candidates.append(&mut partitions);
        }
        ShardAssignment::Only(shards) => {
            for &shard_id in shards {
                let mut partitions = hot_partitions_for_shard(
                    Arc::clone(&compactor.catalog),
                    Some(shard_id),
                    &query_times,
                    min_number_recent_ingested_files_per_partition,
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

async fn hot_partitions_for_shard(
    catalog: Arc<dyn Catalog>,
    shard_id: Option<ShardId>,
    query_times: &[(u64, Timestamp)],
    // Minimum number of the most recent writes per partition we want to count
    // to prioritize partitions
    min_number_recent_ingested_files_per_partition: usize,
    // Max number of the most recent highest ingested throughput partitions
    // per shard we want to read
    max_number_partitions_per_shard: usize,
) -> Result<Vec<PartitionParam>, compact::Error> {
    let mut repos = catalog.repositories().await;

    for &(hours_ago, hours_ago_in_ns) in query_times {
        let partitions = repos
            .parquet_files()
            .recent_highest_throughput_partitions(
                shard_id,
                hours_ago_in_ns,
                min_number_recent_ingested_files_per_partition,
                max_number_partitions_per_shard,
            )
            .await
            .map_err(|e| compact::Error::HighestThroughputPartitions {
                shard_id,
                source: e,
            })?;
        if !partitions.is_empty() {
            debug!(
                ?shard_id,
                hours_ago,
                n = partitions.len(),
                "found high-throughput partitions"
            );
            return Ok(partitions);
        }
    }

    Ok(Vec::new())
}

fn query_times(
    time_provider: Arc<dyn TimeProvider>,
    hours_threshold_1: u64,
    hours_threshold_2: u64,
) -> Vec<(u64, Timestamp)> {
    [hours_threshold_1, hours_threshold_2]
        .iter()
        .map(|&num_hours| {
            (
                num_hours,
                Timestamp::from(time_provider.hours_ago(num_hours)),
            )
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{compact::Compactor, handler::CompactorConfig};
    use backoff::BackoffConfig;
    use data_types::CompactionLevel;
    use iox_tests::util::{TestCatalog, TestParquetFileBuilder, TestShard, TestTable};
    use parquet_file::storage::{ParquetStorage, StorageId};
    use std::sync::Arc;

    const DEFAULT_HOT_COMPACTION_HOURS_THRESHOLD_1: u64 = 4;
    const DEFAULT_HOT_COMPACTION_HOURS_THRESHOLD_2: u64 = 24;
    const DEFAULT_MAX_PARALLEL_PARTITIONS: u64 = 20;

    struct TestSetup {
        catalog: Arc<TestCatalog>,
        shard1: Arc<TestShard>,
        table1: Arc<TestTable>,
        shard2: Arc<TestShard>,
        table2: Arc<TestTable>,
    }

    async fn test_setup() -> TestSetup {
        let catalog = TestCatalog::new();
        let namespace = catalog
            .create_namespace_1hr_retention("namespace_hot_compaction")
            .await;
        let shard1 = namespace.create_shard(1).await;
        let table1 = namespace.create_table("test_table1").await;
        let shard2 = namespace.create_shard(2).await;
        let table2 = namespace.create_table("test_table2").await;

        TestSetup {
            catalog,
            shard1,
            table1,
            shard2,
            table2,
        }
    }

    #[tokio::test]
    async fn no_partitions_no_candidates() {
        let TestSetup {
            catalog, shard1, ..
        } = test_setup().await;

        let candidates = hot_partitions_for_shard(
            Arc::clone(&catalog.catalog),
            Some(shard1.shard.id),
            &query_times(
                catalog.time_provider(),
                DEFAULT_HOT_COMPACTION_HOURS_THRESHOLD_1,
                DEFAULT_HOT_COMPACTION_HOURS_THRESHOLD_2,
            ),
            1,
            1,
        )
        .await
        .unwrap();

        assert!(candidates.is_empty());

        // Across all shards
        let candidates = hot_partitions_for_shard(
            Arc::clone(&catalog.catalog),
            None,
            &query_times(
                catalog.time_provider(),
                DEFAULT_HOT_COMPACTION_HOURS_THRESHOLD_1,
                DEFAULT_HOT_COMPACTION_HOURS_THRESHOLD_2,
            ),
            1,
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

        let candidates = hot_partitions_for_shard(
            Arc::clone(&catalog.catalog),
            Some(shard1.shard.id),
            &query_times(
                catalog.time_provider(),
                DEFAULT_HOT_COMPACTION_HOURS_THRESHOLD_1,
                DEFAULT_HOT_COMPACTION_HOURS_THRESHOLD_2,
            ),
            1,
            1,
        )
        .await
        .unwrap();

        assert!(candidates.is_empty());

        // Across all shards
        let candidates = hot_partitions_for_shard(
            Arc::clone(&catalog.catalog),
            None,
            &query_times(
                catalog.time_provider(),
                DEFAULT_HOT_COMPACTION_HOURS_THRESHOLD_1,
                DEFAULT_HOT_COMPACTION_HOURS_THRESHOLD_2,
            ),
            1,
            1,
        )
        .await
        .unwrap();

        assert!(candidates.is_empty());
    }

    #[tokio::test]
    async fn deleted_l0_not_returned() {
        let TestSetup {
            catalog,
            shard1,
            table1,
            ..
        } = test_setup().await;

        let partition1 = table1.with_shard(&shard1).create_partition("one").await;

        let builder = TestParquetFileBuilder::default().with_to_delete(true);
        partition1.create_parquet_file_catalog_record(builder).await;

        let candidates = hot_partitions_for_shard(
            Arc::clone(&catalog.catalog),
            Some(shard1.shard.id),
            &query_times(
                catalog.time_provider(),
                DEFAULT_HOT_COMPACTION_HOURS_THRESHOLD_1,
                DEFAULT_HOT_COMPACTION_HOURS_THRESHOLD_2,
            ),
            1,
            1,
        )
        .await
        .unwrap();

        assert!(candidates.is_empty());

        // Across all shards
        let candidates = hot_partitions_for_shard(
            Arc::clone(&catalog.catalog),
            None,
            &query_times(
                catalog.time_provider(),
                DEFAULT_HOT_COMPACTION_HOURS_THRESHOLD_1,
                DEFAULT_HOT_COMPACTION_HOURS_THRESHOLD_2,
            ),
            1,
            1,
        )
        .await
        .unwrap();

        assert!(candidates.is_empty());
    }

    #[tokio::test]
    async fn l1_not_returned() {
        let TestSetup {
            catalog,
            shard1,
            table1,
            ..
        } = test_setup().await;

        let partition1 = table1.with_shard(&shard1).create_partition("one").await;

        let builder = TestParquetFileBuilder::default()
            .with_compaction_level(CompactionLevel::FileNonOverlapped);
        partition1.create_parquet_file_catalog_record(builder).await;

        let candidates = hot_partitions_for_shard(
            Arc::clone(&catalog.catalog),
            Some(shard1.shard.id),
            &query_times(
                catalog.time_provider(),
                DEFAULT_HOT_COMPACTION_HOURS_THRESHOLD_1,
                DEFAULT_HOT_COMPACTION_HOURS_THRESHOLD_2,
            ),
            1,
            1,
        )
        .await
        .unwrap();

        assert!(candidates.is_empty());

        // Across all shards
        let candidates = hot_partitions_for_shard(
            Arc::clone(&catalog.catalog),
            None,
            &query_times(
                catalog.time_provider(),
                DEFAULT_HOT_COMPACTION_HOURS_THRESHOLD_1,
                DEFAULT_HOT_COMPACTION_HOURS_THRESHOLD_2,
            ),
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

        let builder =
            TestParquetFileBuilder::default().with_compaction_level(CompactionLevel::Final);
        partition1.create_parquet_file_catalog_record(builder).await;

        let candidates = hot_partitions_for_shard(
            Arc::clone(&catalog.catalog),
            Some(shard1.shard.id),
            &query_times(
                catalog.time_provider(),
                DEFAULT_HOT_COMPACTION_HOURS_THRESHOLD_1,
                DEFAULT_HOT_COMPACTION_HOURS_THRESHOLD_2,
            ),
            1,
            1,
        )
        .await
        .unwrap();

        assert!(candidates.is_empty());

        // Across all shards
        let candidates = hot_partitions_for_shard(
            Arc::clone(&catalog.catalog),
            None,
            &query_times(
                catalog.time_provider(),
                DEFAULT_HOT_COMPACTION_HOURS_THRESHOLD_1,
                DEFAULT_HOT_COMPACTION_HOURS_THRESHOLD_2,
            ),
            1,
            1,
        )
        .await
        .unwrap();

        assert!(candidates.is_empty());
    }

    #[tokio::test]
    async fn cold_not_returned() {
        let TestSetup {
            catalog,
            shard1,
            table1,
            ..
        } = test_setup().await;

        let partition1 = table1.with_shard(&shard1).create_partition("one").await;

        let builder = TestParquetFileBuilder::default()
            .with_creation_time(catalog.time_provider().hours_ago(38));
        partition1.create_parquet_file_catalog_record(builder).await;

        let candidates = hot_partitions_for_shard(
            Arc::clone(&catalog.catalog),
            Some(shard1.shard.id),
            &query_times(
                catalog.time_provider(),
                DEFAULT_HOT_COMPACTION_HOURS_THRESHOLD_1,
                DEFAULT_HOT_COMPACTION_HOURS_THRESHOLD_2,
            ),
            1,
            1,
        )
        .await
        .unwrap();

        assert!(candidates.is_empty());

        // Across all shards
        let candidates = hot_partitions_for_shard(
            Arc::clone(&catalog.catalog),
            None,
            &query_times(
                catalog.time_provider(),
                DEFAULT_HOT_COMPACTION_HOURS_THRESHOLD_1,
                DEFAULT_HOT_COMPACTION_HOURS_THRESHOLD_2,
            ),
            1,
            1,
        )
        .await
        .unwrap();

        assert!(candidates.is_empty());
    }

    #[tokio::test]
    async fn hot_returned() {
        let TestSetup {
            catalog,
            shard1,
            table1,
            ..
        } = test_setup().await;

        let partition1 = table1.with_shard(&shard1).create_partition("one").await;

        let builder = TestParquetFileBuilder::default()
            .with_creation_time(catalog.time_provider().hours_ago(5));
        partition1.create_parquet_file_catalog_record(builder).await;

        let candidates = hot_partitions_for_shard(
            Arc::clone(&catalog.catalog),
            Some(shard1.shard.id),
            &query_times(
                catalog.time_provider(),
                DEFAULT_HOT_COMPACTION_HOURS_THRESHOLD_1,
                DEFAULT_HOT_COMPACTION_HOURS_THRESHOLD_2,
            ),
            1,
            1,
        )
        .await
        .unwrap();

        assert_eq!(candidates.len(), 1);
        assert_eq!(candidates[0].partition_id, partition1.partition.id);

        // Across all shards
        let candidates = hot_partitions_for_shard(
            Arc::clone(&catalog.catalog),
            None,
            &query_times(
                catalog.time_provider(),
                DEFAULT_HOT_COMPACTION_HOURS_THRESHOLD_1,
                DEFAULT_HOT_COMPACTION_HOURS_THRESHOLD_2,
            ),
            1,
            1,
        )
        .await
        .unwrap();

        assert_eq!(candidates.len(), 1);
        assert_eq!(candidates[0].partition_id, partition1.partition.id);
    }

    #[tokio::test]
    async fn hot_prefers_within_4_hrs() {
        let TestSetup {
            catalog,
            shard1,
            table1,
            ..
        } = test_setup().await;

        let partition_5_hours = table1.with_shard(&shard1).create_partition("one").await;
        let builder = TestParquetFileBuilder::default()
            .with_creation_time(catalog.time_provider().hours_ago(5));
        partition_5_hours
            .create_parquet_file_catalog_record(builder)
            .await;

        let partition_3_min = table1.with_shard(&shard1).create_partition("two").await;
        let builder = TestParquetFileBuilder::default()
            .with_creation_time(catalog.time_provider().minutes_ago(3));
        partition_3_min
            .create_parquet_file_catalog_record(builder)
            .await;

        let candidates = hot_partitions_for_shard(
            Arc::clone(&catalog.catalog),
            Some(shard1.shard.id),
            &query_times(
                catalog.time_provider(),
                DEFAULT_HOT_COMPACTION_HOURS_THRESHOLD_1,
                DEFAULT_HOT_COMPACTION_HOURS_THRESHOLD_2,
            ),
            1,
            // Even if we ask for 2 partitions per shard, we'll only get the one partition with
            // writes within 4 hours
            2,
        )
        .await
        .unwrap();

        assert_eq!(candidates.len(), 1);
        assert_eq!(candidates[0].partition_id, partition_3_min.partition.id);

        // Across all shards
        let candidates = hot_partitions_for_shard(
            Arc::clone(&catalog.catalog),
            None,
            &query_times(
                catalog.time_provider(),
                DEFAULT_HOT_COMPACTION_HOURS_THRESHOLD_1,
                DEFAULT_HOT_COMPACTION_HOURS_THRESHOLD_2,
            ),
            1,
            // Even if we ask for 2 partitions per shard, we'll only get the one partition with
            // writes within 4 hours
            2,
        )
        .await
        .unwrap();

        assert_eq!(candidates.len(), 1);
        assert_eq!(candidates[0].partition_id, partition_3_min.partition.id);
    }

    #[tokio::test]
    async fn test_hot_partitions_to_compact() {
        let TestSetup {
            catalog,
            shard1,
            table1,
            shard2,
            table2,
        } = test_setup().await;

        // Shard 1: 4 empty partitions
        let partition1 = table1.with_shard(&shard1).create_partition("one").await;
        let partition2 = table1.with_shard(&shard1).create_partition("two").await;
        let partition3 = table1.with_shard(&shard1).create_partition("three").await;
        let partition4 = table1.with_shard(&shard1).create_partition("four").await;

        // Shard 2: 1 empty partition, with a sort key
        let another_partition = table2
            .with_shard(&shard2)
            .create_partition_with_sort_key("another", &["tag1", "time"])
            .await;

        // Create a compactor
        let time_provider = Arc::clone(&catalog.time_provider);
        let config = CompactorConfig {
            max_desired_file_size_bytes: 10_000,
            percentage_max_file_size: 30,
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
            hot_compaction_hours_threshold_1: DEFAULT_HOT_COMPACTION_HOURS_THRESHOLD_1,
            hot_compaction_hours_threshold_2: DEFAULT_HOT_COMPACTION_HOURS_THRESHOLD_2,
            max_parallel_partitions: DEFAULT_MAX_PARALLEL_PARTITIONS,
            warm_compaction_small_size_threshold_bytes: 5_000,
            warm_compaction_min_small_file_count: 10,
        };
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

        // Some times in the past to set to created_at of the files
        let time_three_minutes_ago = compactor.time_provider.minutes_ago(3);
        let time_five_hour_ago = compactor.time_provider.hours_ago(5);
        let time_38_hour_ago = compactor.time_provider.hours_ago(38);

        // This test is an integration test that covers the priority of the candidate selection
        // algorithm when there are many files of different kinds across many partitions.

        // partition1 has a deleted L0, isn't returned
        let builder = TestParquetFileBuilder::default().with_to_delete(true);
        let _pf1 = partition1.create_parquet_file_catalog_record(builder).await;

        // partition2 has a non-L0 file, isn't returned
        let builder = TestParquetFileBuilder::default()
            .with_compaction_level(CompactionLevel::FileNonOverlapped);
        let _pf2 = partition2.create_parquet_file_catalog_record(builder).await;

        // partition2 has an old (more than 8 hours ago) non-deleted level 0 file, isn't returned
        let builder = TestParquetFileBuilder::default().with_creation_time(time_38_hour_ago);
        let _pf3 = partition2.create_parquet_file_catalog_record(builder).await;

        // partition4 has a new write 5 hours ago, isn't returned
        let builder = TestParquetFileBuilder::default().with_creation_time(time_five_hour_ago);
        let _pf4 = partition4.create_parquet_file_catalog_record(builder).await;

        // partition3 has a new write 3 minutes ago, is returned
        let builder = TestParquetFileBuilder::default().with_creation_time(time_three_minutes_ago);
        let _pf5 = partition3.create_parquet_file_catalog_record(builder).await;

        // The another_shard now has non-deleted level-0 file ingested 5 hours ago, is returned
        let builder = TestParquetFileBuilder::default().with_creation_time(time_five_hour_ago);
        let _pf6 = another_partition
            .create_parquet_file_catalog_record(builder)
            .await;

        // Will have 2 candidates, one for each shard
        let mut candidates = hot_partitions_to_compact(Arc::clone(&compactor))
            .await
            .unwrap();
        candidates.sort_by_key(|c| c.candidate);
        assert_eq!(candidates.len(), 2);

        assert_eq!(candidates[0].id(), partition3.partition.id);
        // this sort key is None
        assert_eq!(candidates[0].sort_key, partition3.partition.sort_key());

        assert_eq!(candidates[1].id(), another_partition.partition.id);
        // this sort key is Some(tag1, time)
        assert_eq!(
            candidates[1].sort_key,
            another_partition.partition.sort_key()
        );
    }
}
