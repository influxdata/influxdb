//! Collect highest hot candidates and compact them

use crate::{
    compact::{self, Compactor},
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
///  1. If there are partitions with new ingested files within the last 4 hours, pick them.
///  2. If no new ingested files in the last 4 hours, will look for partitions with new writes
///     within the last 24 hours.
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
    let query_times = query_times(compactor.time_provider());

    for &shard_id in &compactor.shards {
        let mut partitions = hot_partitions_for_shard(
            Arc::clone(&compactor.catalog),
            shard_id,
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
    shard_id: ShardId,
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
                shard_id = shard_id.get(),
                hours_ago,
                n = partitions.len(),
                "found high-throughput partitions"
            );
            return Ok(partitions);
        }
    }

    Ok(Vec::new())
}

fn query_times(time_provider: Arc<dyn TimeProvider>) -> Vec<(u64, Timestamp)> {
    [4, 24]
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
    use data_types::{
        ColumnId, ColumnSet, CompactionLevel, ParquetFileParams, SequenceNumber, ShardIndex,
    };
    use iox_query::exec::Executor;
    use iox_tests::util::TestCatalog;
    use iox_time::SystemProvider;
    use parquet_file::storage::ParquetStorage;
    use std::sync::Arc;
    use uuid::Uuid;

    fn make_compactor_config() -> CompactorConfig {
        CompactorConfig {
            max_desired_file_size_bytes: 10_000,
            percentage_max_file_size: 30,
            split_percentage: 80,
            max_number_partitions_per_shard: 1,
            min_number_recent_ingested_files_per_partition: 1,
            hot_multiple: 4,
            memory_budget_bytes: 10 * 1024 * 1024,
        }
    }

    #[tokio::test]
    async fn test_hot_partitions_to_compact() {
        let catalog = TestCatalog::new();

        // Create a db with 2 shards, one with 4 empty partitions and the other one with one
        // empty partition
        let mut txn = catalog.catalog.start_transaction().await.unwrap();

        let topic = txn.topics().create_or_get("foo").await.unwrap();
        let pool = txn.query_pools().create_or_get("foo").await.unwrap();
        let namespace = txn
            .namespaces()
            .create(
                "namespace_hot_partitions_to_compact",
                "inf",
                topic.id,
                pool.id,
            )
            .await
            .unwrap();
        let table = txn
            .tables()
            .create_or_get("test_table", namespace.id)
            .await
            .unwrap();
        let shard = txn
            .shards()
            .create_or_get(&topic, ShardIndex::new(1))
            .await
            .unwrap();
        let partition1 = txn
            .partitions()
            .create_or_get("one".into(), shard.id, table.id)
            .await
            .unwrap();
        let partition2 = txn
            .partitions()
            .create_or_get("two".into(), shard.id, table.id)
            .await
            .unwrap();
        let partition3 = txn
            .partitions()
            .create_or_get("three".into(), shard.id, table.id)
            .await
            .unwrap();
        let partition4 = txn
            .partitions()
            .create_or_get("four".into(), shard.id, table.id)
            .await
            .unwrap();
        // other shard
        let another_table = txn
            .tables()
            .create_or_get("another_test_table", namespace.id)
            .await
            .unwrap();
        let another_shard = txn
            .shards()
            .create_or_get(&topic, ShardIndex::new(2))
            .await
            .unwrap();
        let another_partition = txn
            .partitions()
            .create_or_get(
                "another_partition".into(),
                another_shard.id,
                another_table.id,
            )
            .await
            .unwrap();
        // update sort key for this another_partition
        let another_partition = txn
            .partitions()
            .update_sort_key(another_partition.id, &["tag1", "time"])
            .await
            .unwrap();
        txn.commit().await.unwrap();

        // Create a compactor
        let time_provider = Arc::new(SystemProvider::new());
        let config = make_compactor_config();
        let compactor = Arc::new(Compactor::new(
            vec![shard.id, another_shard.id],
            Arc::clone(&catalog.catalog),
            ParquetStorage::new(Arc::clone(&catalog.object_store)),
            Arc::new(Executor::new(1)),
            time_provider,
            BackoffConfig::default(),
            config,
            Arc::new(metric::Registry::new()),
        ));

        // Some times in the past to set to created_at of the files
        let time_now = Timestamp::from(compactor.time_provider.now());
        let time_three_minutes_ago = Timestamp::from(compactor.time_provider.minutes_ago(3));
        let time_five_hour_ago = Timestamp::from(compactor.time_provider.hours_ago(5));
        let time_38_hour_ago = Timestamp::from(compactor.time_provider.hours_ago(38));

        // Basic parquet info
        let p1 = ParquetFileParams {
            shard_id: shard.id,
            namespace_id: namespace.id,
            table_id: table.id,
            partition_id: partition1.id,
            object_store_id: Uuid::new_v4(),
            max_sequence_number: SequenceNumber::new(100),
            min_time: Timestamp::new(1),
            max_time: Timestamp::new(5),
            file_size_bytes: 1337,
            row_count: 0,
            compaction_level: CompactionLevel::Initial, // level of file of new writes
            created_at: time_now,
            column_set: ColumnSet::new([ColumnId::new(1), ColumnId::new(2)]),
        };

        // Note: The order of the test cases below is important and should not be changed
        // because they depend on the order of the writes and their content. For example,
        // in order to test `Case 3`, we do not need to add asserts for `Case 1` and `Case 2`,
        // but all the writes, deletes and updates in Cases 1 and 2 are a must for testing Case 3.
        // In order words, the last Case needs all content of previous tests.
        // This shows the priority of selecting compaction candidates

        // --------------------------------------
        // Case 1: no files yet --> no partition candidates
        //
        let candidates = hot_partitions_to_compact(Arc::clone(&compactor))
            .await
            .unwrap();
        assert!(candidates.is_empty());

        // --------------------------------------
        // Case 2: no non-deleleted L0 files -->  no partition candidates
        //
        // partition1 has a deleted L0
        let mut txn = catalog.catalog.start_transaction().await.unwrap();
        let pf1 = txn.parquet_files().create(p1.clone()).await.unwrap();
        txn.parquet_files().flag_for_delete(pf1.id).await.unwrap();
        //
        // partition2 has a non-L0 file
        let p2 = ParquetFileParams {
            object_store_id: Uuid::new_v4(),
            partition_id: partition2.id,
            ..p1.clone()
        };
        let pf2 = txn.parquet_files().create(p2).await.unwrap();
        txn.parquet_files()
            .update_compaction_level(&[pf2.id], CompactionLevel::FileNonOverlapped)
            .await
            .unwrap();
        txn.commit().await.unwrap();
        // No non-deleted level 0 files yet --> no candidates
        let candidates = hot_partitions_to_compact(Arc::clone(&compactor))
            .await
            .unwrap();
        assert!(candidates.is_empty());

        // --------------------------------------
        // Case 3: no new recent writes (within the last 24 hours) --> no partition candidates
        // (the cold case will pick them up)
        //
        // partition2 has an old (more than 8 hours ago) non-deleted level 0 file
        let mut txn = catalog.catalog.start_transaction().await.unwrap();
        let p3 = ParquetFileParams {
            object_store_id: Uuid::new_v4(),
            partition_id: partition2.id,
            created_at: time_38_hour_ago,
            ..p1.clone()
        };
        let _pf3 = txn.parquet_files().create(p3).await.unwrap();
        txn.commit().await.unwrap();

        // No hot candidates
        let candidates = hot_partitions_to_compact(Arc::clone(&compactor))
            .await
            .unwrap();
        assert!(candidates.is_empty());

        // --------------------------------------
        // Case 4: has one partition with recent writes (5 hours ago) --> return that partition
        //
        // partition4 has a new write 5 hours ago
        let mut txn = catalog.catalog.start_transaction().await.unwrap();
        let p4 = ParquetFileParams {
            object_store_id: Uuid::new_v4(),
            partition_id: partition4.id,
            created_at: time_five_hour_ago,
            ..p1.clone()
        };
        let _pf4 = txn.parquet_files().create(p4).await.unwrap();
        txn.commit().await.unwrap();
        //
        // Has at least one partition with a recent write --> make it a candidate
        let candidates = hot_partitions_to_compact(Arc::clone(&compactor))
            .await
            .unwrap();
        assert_eq!(candidates.len(), 1);
        assert_eq!(candidates[0].id(), partition4.id);

        // --------------------------------------
        // Case 5: has 2 partitions with 2 different groups of recent writes:
        //  1. Within the last 4 hours
        //  2. Within the last 24 hours but older than 4 hours ago
        // When we have group 1, we will ignore partitions in group 2
        //
        // partition3 has a new write 3 hours ago
        let mut txn = catalog.catalog.start_transaction().await.unwrap();
        let p5 = ParquetFileParams {
            object_store_id: Uuid::new_v4(),
            partition_id: partition3.id,
            created_at: time_three_minutes_ago,
            ..p1.clone()
        };
        let _pf5 = txn.parquet_files().create(p5).await.unwrap();
        txn.commit().await.unwrap();
        //
        // make partitions in the most recent group candidates
        let candidates = hot_partitions_to_compact(Arc::clone(&compactor))
            .await
            .unwrap();
        assert_eq!(candidates.len(), 1);
        assert_eq!(candidates[0].id(), partition3.id);

        // --------------------------------------
        // Case 6: has partition candidates for 2 shards
        //
        // The another_shard now has non-deleted level-0 file ingested 5 hours ago
        let mut txn = catalog.catalog.start_transaction().await.unwrap();
        let p6 = ParquetFileParams {
            object_store_id: Uuid::new_v4(),
            shard_id: another_shard.id,
            table_id: another_table.id,
            partition_id: another_partition.id,
            created_at: time_five_hour_ago,
            ..p1.clone()
        };
        let _pf6 = txn.parquet_files().create(p6).await.unwrap();
        txn.commit().await.unwrap();
        //
        // Will have 2 candidates, one for each shard
        let mut candidates = hot_partitions_to_compact(Arc::clone(&compactor))
            .await
            .unwrap();
        candidates.sort_by_key(|c| c.candidate);
        assert_eq!(candidates.len(), 2);

        assert_eq!(candidates[0].id(), partition3.id);
        assert_eq!(candidates[0].shard_id(), shard.id);
        assert_eq!(*candidates[0].namespace, namespace);
        assert_eq!(*candidates[0].table, table);
        assert_eq!(candidates[0].partition_key, partition3.partition_key);
        assert_eq!(candidates[0].sort_key, partition3.sort_key()); // this sort key is None

        assert_eq!(candidates[1].id(), another_partition.id);
        assert_eq!(candidates[1].shard_id(), another_shard.id);
        assert_eq!(*candidates[1].namespace, namespace);
        assert_eq!(*candidates[1].table, another_table);
        assert_eq!(candidates[1].partition_key, another_partition.partition_key);
        assert_eq!(candidates[1].sort_key, another_partition.sort_key()); // this sort key is Some(tag1, time)
    }
}
