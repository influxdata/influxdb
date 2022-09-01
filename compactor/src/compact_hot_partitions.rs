//! Collect highest hot candidates and compact them

use crate::{
    compact::{Compactor, PartitionCompactionCandidateWithInfo},
    parquet_file_combining,
    parquet_file_filtering::{filter_hot_parquet_files, FilterResult, FilteredFiles},
    parquet_file_lookup::{self, ParquetFilesForCompaction},
};
use backoff::Backoff;
use data_types::{ColumnTypeCount, TableId};
use metric::Attributes;
use observability_deps::tracing::*;
use snafu::{ResultExt, Snafu};
use std::{
    collections::{HashMap, VecDeque},
    sync::Arc,
};
use thiserror::Error;

#[derive(Debug, Error)]
#[allow(missing_copy_implementations, missing_docs)]
pub enum Error {}

/// Return number of compacted partitions
pub async fn compact_hot_partitions(compactor: Arc<Compactor>) -> usize {
    // Select hot partition candidates
    debug!("start collecting hot partitions to compact");
    let hot_attributes = Attributes::from(&[("partition_type", "hot")]);
    let start_time = compactor.time_provider.now();
    let candidates = Backoff::new(&compactor.backoff_config)
        .retry_all_errors("hot_partitions_to_compact", || async {
            compactor
                .hot_partitions_to_compact(
                    compactor.config.max_number_partitions_per_shard(),
                    compactor
                        .config
                        .min_number_recent_ingested_files_per_partition(),
                )
                .await
        })
        .await
        .expect("retry forever");
    if let Some(delta) = compactor
        .time_provider
        .now()
        .checked_duration_since(start_time)
    {
        let duration = compactor
            .candidate_selection_duration
            .recorder(hot_attributes.clone());
        duration.record(delta);
    }

    // Get extra needed information for selected partitions
    let start_time = compactor.time_provider.now();

    // Column types and their counts of the tables of the partition candidates
    debug!(num_candidates=?candidates.len(), "start getting column types for the partition candidates");
    let table_columns = Backoff::new(&compactor.backoff_config)
        .retry_all_errors("table_columns", || async {
            compactor.table_columns(&candidates).await
        })
        .await
        .expect("retry forever");

    // Add other compaction-needed info into selected partitions
    debug!(num_candidates=?candidates.len(), "start getting column types for the partition candidates");
    let candidates = Backoff::new(&compactor.backoff_config)
        .retry_all_errors("add_info_to_partitions", || async {
            compactor.add_info_to_partitions(&candidates).await
        })
        .await
        .expect("retry forever");

    if let Some(delta) = compactor
        .time_provider
        .now()
        .checked_duration_since(start_time)
    {
        let duration = compactor
            .partitions_extra_info_reading_duration
            .recorder(hot_attributes.clone());
        duration.record(delta);
    }

    let n_candidates = candidates.len();
    if n_candidates == 0 {
        debug!("no hot compaction candidates found");
        return 0;
    } else {
        debug!(n_candidates, "found hot compaction candidates");
    }

    let start_time = compactor.time_provider.now();

    compact_candidates_with_memory_budget(
        Arc::clone(&compactor),
        "hot",
        |compactor: Arc<Compactor>,
         partition: PartitionCompactionCandidateWithInfo,
         parquet_files_for_compaction: ParquetFilesForCompaction,
         remaining_budget_bytes: u64,
         columns: &[ColumnTypeCount]| {
            filter_hot_parquet_files(
                partition,
                parquet_files_for_compaction,
                remaining_budget_bytes,
                columns,
                &compactor.parquet_file_candidate_gauge,
                &compactor.parquet_file_candidate_bytes,
            )
        },
        compact_hot_partitions_in_parallel,
        candidates,
        table_columns,
    )
    .await;

    // Done compacting all candidates in the cycle, record its time
    if let Some(delta) = compactor
        .time_provider
        .now()
        .checked_duration_since(start_time)
    {
        let duration = compactor.compaction_cycle_duration.recorder(hot_attributes);
        duration.record(delta);
    }

    n_candidates
}

// For a given list of partition candidates and a memory budget, estimate memory needed to compact
// each partition candidate and compact as many of them in parallel as possible until all
// candidates are compacted.
//
// The way this function works is the estimated memory to compact each partition will be
// accumulated until the running total hits over 90% of the compactor's memory_budget_bytes. Then
// those partitions are compacted in parallel. The process repeats until all partitions are
// compacted.
//
// If the partial remaining budget isn't enough to compact the current partition but the full
// budget is enough, the current partition will be pushed back as the last item of the list to be
// considered later with a full memory budget.
async fn compact_candidates_with_memory_budget<C, Fut, F>(
    compactor: Arc<Compactor>,
    compaction_type: &str,
    filter_function: F,
    compact_function: C,
    mut candidates: VecDeque<PartitionCompactionCandidateWithInfo>,
    table_columns: HashMap<TableId, Vec<ColumnTypeCount>>,
) where
    C: Fn(Arc<Compactor>, Vec<FilteredFiles>) -> Fut + Send + Sync + 'static,
    Fut: futures::Future<Output = ()> + Send,
    F: Fn(
            Arc<Compactor>,
            PartitionCompactionCandidateWithInfo,
            ParquetFilesForCompaction,
            u64,
            &[ColumnTypeCount],
        ) -> FilteredFiles
        + Send
        + Sync
        + 'static,
{
    let mut remaining_budget_bytes = compactor.config.memory_budget_bytes();
    let mut parallel_compacting_candidates = Vec::with_capacity(candidates.len());
    let mut num_remaining_candidates = candidates.len();
    let mut count = 0;
    while !candidates.is_empty() {
        // Algorithm:
        // 1. Remove the first candidate from the list
        // 2. Check if the candidate can be compacted fully (all files) or partially (some files)
        //    under the remaining memory budget
        // 3. If yes, add the candidate and its full or partial list of files into the
        //    `parallel_compacting_candidates` list.
        //    Otherwise:
        //      - if the remaining budget is not the full budget and this partition's estimate is
        //        less than the full budget, push the candidate back into the `candidates`
        //        to consider compacting with larger budget.
        //      - otherwise, log that the partition is too large to compact and skip it.
        // 4. If the budget is hit, compact all candidates in the compacting_list in parallel
        // 5. Repeat

        // --------------------------------------------------------------------
        // 1. Pop first candidate from the list. Since it is not empty, there must be at least one
        let partition = candidates.pop_front().unwrap();
        count += 1;
        let partition_id = partition.candidate.partition_id;
        let table_id = partition.candidate.table_id;

        // Get column types and their counts for the table of the partition
        let columns = table_columns.get(&table_id);
        let to_compact = match columns {
            None => {
                warn!(
                    ?partition_id,
                    ?table_id,
                    compaction_type,
                    "skipped due to missing column types of its table"
                );
                // todo: add this partition and its info into a new catalog table
                // https://github.com/influxdata/influxdb_iox/issues/5458
                None
            }
            Some(columns) => {
                // --------------------------------------------------------------------
                // 2. Check if the candidate can be compacted fully or partially under the
                //    remaining_budget_bytes
                // Get parquet_file info for this partition
                let parquet_files_for_compaction =
                    parquet_file_lookup::ParquetFilesForCompaction::for_partition(
                        Arc::clone(&compactor.catalog),
                        partition_id,
                        &Default::default(),
                    )
                    .await;
                match parquet_files_for_compaction {
                    Err(e) => {
                        // This may just be a hiccup reading object store, skip commpacting it in
                        // this cycle
                        warn!(
                            ?e,
                            ?partition_id,
                            compaction_type,
                            "failed due to error in reading parquet files"
                        );
                        None
                    }
                    Ok(parquet_files_for_compaction) => {
                        // Return only files under the remaining_budget_bytes that should be
                        // compacted
                        let to_compact = filter_function(
                            Arc::clone(&compactor),
                            partition.clone(),
                            parquet_files_for_compaction,
                            remaining_budget_bytes,
                            columns,
                        );
                        Some(to_compact)
                    }
                }
            }
        };

        // --------------------------------------------------------------------
        // 3. Check the compactable status and perform the action
        if let Some(to_compact) = to_compact {
            match to_compact.filter_result() {
                FilterResult::NothingToCompact => {
                    debug!(?partition_id, compaction_type, "nothing to compact");
                }
                FilterResult::ErrorEstimatingBudget => {
                    warn!(
                        ?partition_id,
                        ?table_id,
                        compaction_type,
                        "skipped; error in estimating compacting memory"
                    );
                    // todo: add this partition and its info into a new catalog table
                    // https://github.com/influxdata/influxdb_iox/issues/5458
                }
                FilterResult::OverBudget => {
                    if to_compact.budget_bytes() <= compactor.config.memory_budget_bytes() {
                        // Required budget is larger than the remaining budget but smaller than
                        // full budget, add this partition back to the end of the list to compact
                        // with full budget later
                        candidates.push_back(partition);
                    } else {
                        // Even with max budget, we cannot compact this partition, log it
                        warn!(
                            ?partition_id,
                            ?table_id,
                            compaction_type,
                            "skipped; over memory budget"
                        );
                        // todo: add this partition and its info into a new catalog table
                        // https://github.com/influxdata/influxdb_iox/issues/5458
                    }
                }
                FilterResult::Proceeed => {
                    remaining_budget_bytes -= to_compact.budget_bytes();
                    parallel_compacting_candidates.push(to_compact);
                }
            }
        }

        // --------------------------------------------------------------------
        // 4. Almost hitting max budget (only 10% left)
        //    OR no more candidates
        //    OR already considered all remaining candidates.
        if (!parallel_compacting_candidates.is_empty())
            && ((remaining_budget_bytes <= (compactor.config.memory_budget_bytes() / 10) as u64)
                || (candidates.is_empty())
                || (count == num_remaining_candidates))
        {
            debug!(
                num_parallel_compacting_candidates = parallel_compacting_candidates.len(),
                total_needed_memory_budget_bytes =
                    compactor.config.memory_budget_bytes() - remaining_budget_bytes,
                compaction_type,
                "parallel compacting candidate"
            );
            compact_function(Arc::clone(&compactor), parallel_compacting_candidates).await;

            // Reset to start adding new set of parallel candidates
            parallel_compacting_candidates = Vec::with_capacity(candidates.len());
            remaining_budget_bytes = compactor.config.memory_budget_bytes();
            num_remaining_candidates = candidates.len();
            count = 0;
        }
    }
}

// Compact given partitions in parallel
// This function assumes its caller knows there are enough resources to run all partitions
// concurrently
async fn compact_hot_partitions_in_parallel(
    compactor: Arc<Compactor>,
    partitions: Vec<FilteredFiles>,
) {
    let mut handles = Vec::with_capacity(partitions.len());
    for p in partitions {
        let comp = Arc::clone(&compactor);
        let handle = tokio::task::spawn(async move {
            let partition_id = p.partition.candidate.partition_id;
            debug!(?partition_id, "hot compaction starting");
            let compaction_result = compact_one_hot_partition(&comp, p).await;
            match compaction_result {
                Err(e) => {
                    warn!(?e, ?partition_id, "hot compaction failed");
                }
                Ok(_) => {
                    debug!(?partition_id, "hot compaction complete");
                }
            };
        });
        handles.push(handle);
    }

    let compactions_run = handles.len();
    debug!(
        ?compactions_run,
        "Number of hot concurrent partitions are being compacted"
    );

    let _ = futures::future::join_all(handles).await;
}

#[derive(Debug, Snafu)]
#[allow(missing_copy_implementations, missing_docs)]
pub(crate) enum CompactOneHotPartitionError {
    #[snafu(display("{}", source))]
    Combining {
        source: parquet_file_combining::Error,
    },
}

/// One compaction operation of one hot partition
pub(crate) async fn compact_one_hot_partition(
    compactor: &Compactor,
    to_compact: FilteredFiles,
) -> Result<(), CompactOneHotPartitionError> {
    let start_time = compactor.time_provider.now();

    let partition = to_compact.partition;
    let shard_id = partition.shard_id();

    let compact_result = parquet_file_combining::compact_parquet_files(
        to_compact.files,
        partition,
        Arc::clone(&compactor.catalog),
        compactor.store.clone(),
        Arc::clone(&compactor.exec),
        Arc::clone(&compactor.time_provider),
        &compactor.compaction_input_file_bytes,
        compactor.config.max_desired_file_size_bytes(),
        compactor.config.percentage_max_file_size(),
        compactor.config.split_percentage(),
    )
    .await
    .context(CombiningSnafu);

    let attributes = Attributes::from([
        ("shard_id", format!("{}", shard_id).into()),
        ("partition_type", "hot".into()),
    ]);
    if let Some(delta) = compactor
        .time_provider
        .now()
        .checked_duration_since(start_time)
    {
        let duration = compactor.compaction_duration.recorder(attributes);
        duration.record(delta);
    }

    compact_result
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{compact::Compactor, handler::CompactorConfig, parquet_file_filtering};
    use arrow_util::assert_batches_sorted_eq;
    use backoff::BackoffConfig;
    use data_types::{ColumnType, ColumnTypeCount, CompactionLevel, ParquetFileId};
    use iox_query::exec::Executor;
    use iox_tests::util::{
        TestCatalog, TestNamespace, TestParquetFileBuilder, TestShard, TestTable,
    };
    use iox_time::{SystemProvider, TimeProvider};
    use parquet_file::storage::ParquetStorage;
    use std::{
        collections::VecDeque,
        pin::Pin,
        sync::{Arc, Mutex},
        time::Duration,
    };

    #[tokio::test]
    async fn empty_candidates_compacts_nothing() {
        test_helpers::maybe_start_logging();

        let TestSetup {
            compactor,
            mock_compactor,
            ..
        } = test_setup().await;

        let sorted_candidates = VecDeque::new();
        let table_columns = HashMap::new();

        compact_candidates_with_memory_budget(
            Arc::clone(&compactor),
            "hot",
            |compactor: Arc<Compactor>,
             partition: PartitionCompactionCandidateWithInfo,
             parquet_files_for_compaction: ParquetFilesForCompaction,
             remaining_budget_bytes: u64,
             columns: &[ColumnTypeCount]| {
                filter_hot_parquet_files(
                    partition,
                    parquet_files_for_compaction,
                    remaining_budget_bytes,
                    columns,
                    &compactor.parquet_file_candidate_gauge,
                    &compactor.parquet_file_candidate_bytes,
                )
            },
            mock_compactor.compaction_function(),
            sorted_candidates,
            table_columns,
        )
        .await;

        let compaction_groups = mock_compactor.results();
        assert!(compaction_groups.is_empty());
    }

    #[tokio::test]
    async fn compacting_table_without_columns_does_nothing() {
        test_helpers::maybe_start_logging();

        let TestSetup {
            compactor,
            mock_compactor,
            namespace,
            shard,
            ..
        } = test_setup().await;

        let table_without_columns = namespace.create_table("test_table").await;

        let partition1 = table_without_columns
            .with_shard(&shard)
            .create_partition("one")
            .await;

        let hot_time_one_hour_ago =
            (compactor.time_provider.now() - Duration::from_secs(60 * 60)).timestamp_nanos();

        let pf1_1 = TestParquetFileBuilder::default()
            .with_min_time(1)
            .with_max_time(5)
            .with_row_count(2)
            .with_compaction_level(CompactionLevel::Initial)
            .with_creation_time(hot_time_one_hour_ago);
        partition1.create_parquet_file_catalog_record(pf1_1).await;

        let candidates = compactor
            .hot_partitions_to_compact(
                compactor.config.max_number_partitions_per_shard(),
                compactor
                    .config
                    .min_number_recent_ingested_files_per_partition(),
            )
            .await
            .unwrap();
        assert_eq!(candidates.len(), 1);

        let candidates = compactor.add_info_to_partitions(&candidates).await.unwrap();
        let mut sorted_candidates = candidates.into_iter().collect::<Vec<_>>();
        sorted_candidates.sort_by_key(|c| c.candidate.partition_id);
        let sorted_candidates = sorted_candidates.into_iter().collect::<VecDeque<_>>();
        let table_columns = HashMap::new();

        compact_candidates_with_memory_budget(
            Arc::clone(&compactor),
            "hot",
            |compactor: Arc<Compactor>,
             partition: PartitionCompactionCandidateWithInfo,
             parquet_files_for_compaction: ParquetFilesForCompaction,
             remaining_budget_bytes: u64,
             columns: &[ColumnTypeCount]| {
                filter_hot_parquet_files(
                    partition,
                    parquet_files_for_compaction,
                    remaining_budget_bytes,
                    columns,
                    &compactor.parquet_file_candidate_gauge,
                    &compactor.parquet_file_candidate_bytes,
                )
            },
            mock_compactor.compaction_function(),
            sorted_candidates,
            table_columns,
        )
        .await;

        let compaction_groups = mock_compactor.results();
        assert!(compaction_groups.is_empty());
    }

    #[tokio::test]
    async fn test_compact_hot_partition_candidates() {
        test_helpers::maybe_start_logging();

        let TestSetup {
            compactor,
            mock_compactor,
            shard,
            table,
            ..
        } = test_setup().await;

        // Some times in the past to set to created_at of the files
        let hot_time_one_hour_ago =
            (compactor.time_provider.now() - Duration::from_secs(60 * 60)).timestamp_nanos();

        // P1:
        //   L0 2 rows. bytes: 1125 * 2 = 2,250
        //   L1 2 rows. bytes: 1125 * 2 = 2,250
        // total = 2,250 + 2,250 = 4,500
        let partition1 = table.with_shard(&shard).create_partition("one").await;

        let pf1_1 = TestParquetFileBuilder::default()
            .with_min_time(1)
            .with_max_time(5)
            .with_row_count(2)
            .with_compaction_level(CompactionLevel::Initial)
            .with_creation_time(hot_time_one_hour_ago);
        partition1.create_parquet_file_catalog_record(pf1_1).await;

        let pf1_2 = TestParquetFileBuilder::default()
            .with_min_time(4) // overlapped with pf1_1
            .with_max_time(6)
            .with_row_count(2)
            .with_compaction_level(CompactionLevel::FileNonOverlapped)
            .with_creation_time(hot_time_one_hour_ago);
        partition1.create_parquet_file_catalog_record(pf1_2).await;

        // P2:
        //   L0 2 rows. bytes: 1125 * 2 = 2,250
        //   L1 2 rows. bytes: 1125 * 2 = 2,250
        // total = 2,250 + 2,250 = 4,500
        let partition2 = table.with_shard(&shard).create_partition("two").await;

        let pf2_1 = TestParquetFileBuilder::default()
            .with_min_time(1)
            .with_max_time(5)
            .with_row_count(2)
            .with_compaction_level(CompactionLevel::Initial)
            .with_creation_time(hot_time_one_hour_ago);
        partition2.create_parquet_file_catalog_record(pf2_1).await;

        let pf2_2 = TestParquetFileBuilder::default()
            .with_min_time(4) // overlapped with pf2_1
            .with_max_time(6)
            .with_row_count(2)
            .with_compaction_level(CompactionLevel::FileNonOverlapped)
            .with_creation_time(hot_time_one_hour_ago);
        partition2.create_parquet_file_catalog_record(pf2_2).await;

        // P3: bytes >= 90% of full budget = 90% * 13,500 = 12,150
        //   L0 6 rows. bytes: 1125 * 6 = 6,750
        //   L1 4 rows. bytes: 1125 * 4 = 4,500
        // total = 6,700 + 4,500 = 12,150
        let partition3 = table.with_shard(&shard).create_partition("three").await;
        let pf3_1 = TestParquetFileBuilder::default()
            .with_min_time(1)
            .with_max_time(5)
            .with_row_count(6)
            .with_compaction_level(CompactionLevel::Initial)
            .with_creation_time(hot_time_one_hour_ago);
        partition3.create_parquet_file_catalog_record(pf3_1).await;

        let pf3_2 = TestParquetFileBuilder::default()
            .with_min_time(4) // overlapped with pf3_1
            .with_max_time(6)
            .with_row_count(4)
            .with_compaction_level(CompactionLevel::FileNonOverlapped)
            .with_creation_time(hot_time_one_hour_ago);
        partition3.create_parquet_file_catalog_record(pf3_2).await;

        // P4: Over the full budget
        // L0 with 8 rows.bytes =  1125 * 8 = 9,000
        // L1 with 6 rows.bytes =  1125 * 6 = 6,750
        // total = 15,750
        let partition4 = table.with_shard(&shard).create_partition("four").await;
        let pf4_1 = TestParquetFileBuilder::default()
            .with_min_time(1)
            .with_max_time(5)
            .with_row_count(8)
            .with_compaction_level(CompactionLevel::Initial)
            .with_creation_time(hot_time_one_hour_ago);
        partition4.create_parquet_file_catalog_record(pf4_1).await;

        let pf4_2 = TestParquetFileBuilder::default()
            .with_min_time(4) // overlapped with pf4_1
            .with_max_time(6)
            .with_row_count(6)
            .with_compaction_level(CompactionLevel::FileNonOverlapped)
            .with_creation_time(hot_time_one_hour_ago);
        partition4.create_parquet_file_catalog_record(pf4_2).await;

        // P5:
        // L0 with 2 rows.bytes =  1125 * 2 = 2,250
        // L1 with 2 rows.bytes =  1125 * 2 = 2,250
        // total = 4,500
        let partition5 = table.with_shard(&shard).create_partition("five").await;
        let pf5_1 = TestParquetFileBuilder::default()
            .with_min_time(1)
            .with_max_time(5)
            .with_row_count(2)
            .with_compaction_level(CompactionLevel::Initial)
            .with_creation_time(hot_time_one_hour_ago);
        partition5.create_parquet_file_catalog_record(pf5_1).await;

        let pf5_2 = TestParquetFileBuilder::default()
            .with_min_time(4) // overlapped with pf5_1
            .with_max_time(6)
            .with_row_count(2)
            .with_compaction_level(CompactionLevel::FileNonOverlapped)
            .with_creation_time(hot_time_one_hour_ago);
        partition5.create_parquet_file_catalog_record(pf5_2).await;

        // P6:
        // L0 with 2 rows.bytes =  1125 * 2 = 2,250
        // L1 with 2 rows.bytes =  1125 * 2 = 2,250
        // total = 4,500
        let partition6 = table.with_shard(&shard).create_partition("six").await;
        let pf6_1 = TestParquetFileBuilder::default()
            .with_min_time(1)
            .with_max_time(5)
            .with_row_count(2)
            .with_compaction_level(CompactionLevel::Initial)
            .with_creation_time(hot_time_one_hour_ago);
        partition6.create_parquet_file_catalog_record(pf6_1).await;

        let pf6_2 = TestParquetFileBuilder::default()
            .with_min_time(4) // overlapped with pf6_1
            .with_max_time(6)
            .with_row_count(2)
            .with_compaction_level(CompactionLevel::FileNonOverlapped)
            .with_creation_time(hot_time_one_hour_ago);
        partition6.create_parquet_file_catalog_record(pf6_2).await;

        // partition candidates: partitions with L0 and overlapped L1
        let candidates = compactor
            .hot_partitions_to_compact(
                compactor.config.max_number_partitions_per_shard(),
                compactor
                    .config
                    .min_number_recent_ingested_files_per_partition(),
            )
            .await
            .unwrap();
        assert_eq!(candidates.len(), 6);

        // column types of the partitions
        let table_columns = compactor.table_columns(&candidates).await.unwrap();
        assert_eq!(table_columns.len(), 1);
        let mut cols = table_columns.get(&table.table.id).unwrap().clone();
        assert_eq!(cols.len(), 5);
        cols.sort_by_key(|c| c.col_type);
        let mut expected_cols = vec![
            ColumnTypeCount::new(ColumnType::Time, 1),
            ColumnTypeCount::new(ColumnType::I64, 1),
            ColumnTypeCount::new(ColumnType::Tag, 1),
            ColumnTypeCount::new(ColumnType::String, 1),
            ColumnTypeCount::new(ColumnType::Bool, 1),
        ];
        expected_cols.sort_by_key(|c| c.col_type);
        assert_eq!(cols, expected_cols);

        // Add other compaction-needed info into selected partitions
        let candidates = compactor.add_info_to_partitions(&candidates).await.unwrap();
        let mut sorted_candidates = candidates.into_iter().collect::<Vec<_>>();
        sorted_candidates.sort_by_key(|c| c.candidate.partition_id);
        let sorted_candidates = sorted_candidates.into_iter().collect::<VecDeque<_>>();

        // There are 3 rounds of parallel compaction
        //   . Round 1: 3 candidates [P1, P2, P5] and total needed budget 13,500
        //   . Round 2: 1 candidate [P6] and total needed budget 4,500
        //   . Round 3: 1 candidate [P3] and total needed budget 11,250
        //   P4 is not compacted due to overbudget
        //  Debug info shows all 3 rounds

        // Todo next: So conveniently, debug log shows this is also a reproducer of
        // https://github.com/influxdata/conductor/issues/1130
        // "hot compaction failed: 1, "Could not serialize and persist record batches failed to peek record stream schema"
        compact_candidates_with_memory_budget(
            Arc::clone(&compactor),
            "hot",
            |compactor: Arc<Compactor>,
             partition: PartitionCompactionCandidateWithInfo,
             parquet_files_for_compaction: ParquetFilesForCompaction,
             remaining_budget_bytes: u64,
             columns: &[ColumnTypeCount]| {
                filter_hot_parquet_files(
                    partition,
                    parquet_files_for_compaction,
                    remaining_budget_bytes,
                    columns,
                    &compactor.parquet_file_candidate_gauge,
                    &compactor.parquet_file_candidate_bytes,
                )
            },
            mock_compactor.compaction_function(),
            sorted_candidates,
            table_columns,
        )
        .await;

        let compaction_groups = mock_compactor.results();

        // 3 rounds of parallel compaction
        assert_eq!(compaction_groups.len(), 3);

        // Round 1
        let group1 = &compaction_groups[0];
        assert_eq!(group1.len(), 3);

        let g1_candidate1 = &group1[0];
        assert_eq!(g1_candidate1.budget_bytes(), 4500);
        assert_eq!(g1_candidate1.partition.id(), partition1.partition.id);
        let g1_candidate1_pf_ids: Vec<_> =
            g1_candidate1.files.iter().map(|pf| pf.id().get()).collect();
        assert_eq!(g1_candidate1_pf_ids, vec![2, 1]);

        let g1_candidate2 = &group1[1];
        assert_eq!(g1_candidate2.budget_bytes(), 4500);
        assert_eq!(g1_candidate2.partition.id(), partition2.partition.id);
        let g1_candidate2_pf_ids: Vec<_> =
            g1_candidate2.files.iter().map(|pf| pf.id().get()).collect();
        assert_eq!(g1_candidate2_pf_ids, vec![4, 3]);

        let g1_candidate3 = &group1[2];
        assert_eq!(g1_candidate3.budget_bytes(), 4500);
        assert_eq!(g1_candidate3.partition.id(), partition5.partition.id);
        let g1_candidate3_pf_ids: Vec<_> =
            g1_candidate3.files.iter().map(|pf| pf.id().get()).collect();
        assert_eq!(g1_candidate3_pf_ids, vec![10, 9]);

        // Round 2
        let group2 = &compaction_groups[1];
        assert_eq!(group2.len(), 1);

        let g2_candidate1 = &group2[0];
        assert_eq!(g2_candidate1.budget_bytes(), 4500);
        assert_eq!(g2_candidate1.partition.id(), partition6.partition.id);
        let g2_candidate1_pf_ids: Vec<_> =
            g2_candidate1.files.iter().map(|pf| pf.id().get()).collect();
        assert_eq!(g2_candidate1_pf_ids, vec![12, 11]);

        // Round 3
        let group3 = &compaction_groups[2];
        assert_eq!(group3.len(), 1);

        let g3_candidate1 = &group3[0];
        assert_eq!(g3_candidate1.budget_bytes(), 11250);
        assert_eq!(g3_candidate1.partition.id(), partition3.partition.id);
        let g3_candidate1_pf_ids: Vec<_> =
            g3_candidate1.files.iter().map(|pf| pf.id().get()).collect();
        assert_eq!(g3_candidate1_pf_ids, vec![6, 5]);
    }

    // A quite sophisticated integration test
    // Beside lp data, every value min/max sequence numbers and min/max time are important
    // to have a combination of needed tests in this test function
    #[tokio::test]
    async fn test_compact_hot_partition_many_files() {
        test_helpers::maybe_start_logging();
        let catalog = TestCatalog::new();

        // lp1 does not overlap with any other level 0
        let lp1 = vec![
            "table,tag1=WA field_int=1000i 10",
            "table,tag1=VT field_int=10i 20",
        ]
        .join("\n");

        // lp2 overlaps with lp3
        let lp2 = vec![
            "table,tag1=WA field_int=1000i 8000", // will be eliminated due to duplicate
            "table,tag1=VT field_int=10i 10000",
            "table,tag1=UT field_int=70i 20000",
        ]
        .join("\n");

        // lp3 overlaps with lp2
        let lp3 = vec![
            "table,tag1=WA field_int=1500i 8000", // latest duplicate and kept
            "table,tag1=VT field_int=10i 6000",
            "table,tag1=UT field_int=270i 25000",
        ]
        .join("\n");

        // lp4 does not overlap with any
        let lp4 = vec![
            "table,tag2=WA,tag3=10 field_int=1600i 28000",
            "table,tag2=VT,tag3=20 field_int=20i 26000",
        ]
        .join("\n");

        // lp5 overlaps with lp1
        let lp5 = vec![
            "table,tag2=PA,tag3=15 field_int=1601i 9",
            "table,tag2=OH,tag3=21 field_int=21i 25",
        ]
        .join("\n");

        // lp6 does not overlap with any
        let lp6 = vec![
            "table,tag2=PA,tag3=15 field_int=81601i 90000",
            "table,tag2=OH,tag3=21 field_int=421i 91000",
        ]
        .join("\n");

        let ns = catalog.create_namespace("ns").await;
        let shard = ns.create_shard(1).await;
        let table = ns.create_table("table").await;
        table.create_column("field_int", ColumnType::I64).await;
        table.create_column("tag1", ColumnType::Tag).await;
        table.create_column("tag2", ColumnType::Tag).await;
        table.create_column("tag3", ColumnType::Tag).await;
        table.create_column("time", ColumnType::Time).await;
        let table_column_types = vec![
            ColumnTypeCount {
                col_type: ColumnType::Tag as i16,
                count: 3,
            },
            ColumnTypeCount {
                col_type: ColumnType::I64 as i16,
                count: 1,
            },
            ColumnTypeCount {
                col_type: ColumnType::Time as i16,
                count: 1,
            },
        ];
        let partition = table.with_shard(&shard).create_partition("part").await;
        let time = Arc::new(SystemProvider::new());
        let config = make_compactor_config();
        let metrics = Arc::new(metric::Registry::new());
        let compactor = Compactor::new(
            vec![shard.shard.id],
            Arc::clone(&catalog.catalog),
            ParquetStorage::new(Arc::clone(&catalog.object_store)),
            Arc::new(Executor::new(1)),
            Arc::new(SystemProvider::new()),
            BackoffConfig::default(),
            config,
            Arc::clone(&metrics),
        );

        // parquet files that are all in the same partition
        let mut size_overrides = HashMap::<ParquetFileId, i64>::default();

        // pf1 does not overlap with any other level 0
        let builder = TestParquetFileBuilder::default()
            .with_line_protocol(&lp1)
            .with_max_seq(3)
            .with_min_time(10)
            .with_max_time(20)
            .with_creation_time(20);
        let f = partition.create_parquet_file(builder).await;
        size_overrides.insert(
            f.parquet_file.id,
            compactor.config.max_desired_file_size_bytes() as i64 + 10,
        );

        // pf2 overlaps with pf3
        let builder = TestParquetFileBuilder::default()
            .with_line_protocol(&lp2)
            .with_max_seq(5)
            .with_min_time(8_000)
            .with_max_time(20_000)
            .with_creation_time(time.now().timestamp_nanos());
        let f = partition.create_parquet_file(builder).await;
        size_overrides.insert(f.parquet_file.id, 100); // small file

        // pf3 overlaps with pf2
        let builder = TestParquetFileBuilder::default()
            .with_line_protocol(&lp3)
            .with_max_seq(10)
            .with_min_time(6_000)
            .with_max_time(25_000)
            .with_creation_time(time.now().timestamp_nanos());
        let f = partition.create_parquet_file(builder).await;
        size_overrides.insert(f.parquet_file.id, 100); // small file

        // pf4 does not overlap with any but is small
        let builder = TestParquetFileBuilder::default()
            .with_line_protocol(&lp4)
            .with_max_seq(18)
            .with_min_time(26_000)
            .with_max_time(28_000)
            .with_creation_time(time.now().timestamp_nanos());
        let f = partition.create_parquet_file(builder).await;
        size_overrides.insert(f.parquet_file.id, 100); // small file

        // pf5 was created in a previous compaction cycle; overlaps with pf1
        let builder = TestParquetFileBuilder::default()
            .with_line_protocol(&lp5)
            .with_max_seq(1)
            .with_min_time(9)
            .with_max_time(25)
            .with_creation_time(time.now().timestamp_nanos())
            .with_compaction_level(CompactionLevel::FileNonOverlapped);
        let f = partition.create_parquet_file(builder).await;
        size_overrides.insert(f.parquet_file.id, 100); // small file

        // pf6 was created in a previous compaction cycle; does not overlap with any
        let builder = TestParquetFileBuilder::default()
            .with_line_protocol(&lp6)
            .with_max_seq(20)
            .with_min_time(90000)
            .with_max_time(91000)
            .with_creation_time(time.now().timestamp_nanos())
            .with_compaction_level(CompactionLevel::FileNonOverlapped);
        let f = partition.create_parquet_file(builder).await;
        size_overrides.insert(f.parquet_file.id, 100); // small file

        // should have 4 level-0 files before compacting
        let count = catalog.count_level_0_files(shard.shard.id).await;
        assert_eq!(count, 4);

        // ------------------------------------------------
        // Compact
        let candidates = compactor
            .hot_partitions_to_compact(
                compactor.config.max_number_partitions_per_shard(),
                compactor
                    .config
                    .min_number_recent_ingested_files_per_partition(),
            )
            .await
            .unwrap();
        let mut candidates = compactor.add_info_to_partitions(&candidates).await.unwrap();

        assert_eq!(candidates.len(), 1);
        let c = candidates.pop_front().unwrap();

        let parquet_files_for_compaction =
            parquet_file_lookup::ParquetFilesForCompaction::for_partition(
                Arc::clone(&compactor.catalog),
                c.id(),
                &size_overrides,
            )
            .await
            .unwrap();

        let to_compact = parquet_file_filtering::filter_hot_parquet_files(
            c,
            parquet_files_for_compaction,
            compactor.config.memory_budget_bytes(),
            &table_column_types,
            &compactor.parquet_file_candidate_gauge,
            &compactor.parquet_file_candidate_bytes,
        );

        compact_one_hot_partition(&compactor, to_compact)
            .await
            .unwrap();

        // Should have 3 non-soft-deleted files:
        //
        // - the level 1 file that didn't overlap with anything
        // - the two newly created after compacting and splitting pf1, pf2, pf3, pf4, pf5
        let mut files = catalog.list_by_table_not_to_delete(table.table.id).await;
        assert_eq!(files.len(), 3);
        let files_and_levels: Vec<_> = files
            .iter()
            .map(|f| (f.id.get(), f.compaction_level))
            .collect();
        assert_eq!(
            files_and_levels,
            vec![
                (6, CompactionLevel::FileNonOverlapped),
                (7, CompactionLevel::FileNonOverlapped),
                (8, CompactionLevel::FileNonOverlapped),
            ]
        );

        // ------------------------------------------------
        // Verify the parquet file content

        // Later compacted file
        let file1 = files.pop().unwrap();
        let batches = table.read_parquet_file(file1).await;
        assert_batches_sorted_eq!(
            &[
                "+-----------+------+------+------+-----------------------------+",
                "| field_int | tag1 | tag2 | tag3 | time                        |",
                "+-----------+------+------+------+-----------------------------+",
                "| 1600      |      | WA   | 10   | 1970-01-01T00:00:00.000028Z |",
                "| 20        |      | VT   | 20   | 1970-01-01T00:00:00.000026Z |",
                "| 270       | UT   |      |      | 1970-01-01T00:00:00.000025Z |",
                "+-----------+------+------+------+-----------------------------+",
            ],
            &batches
        );

        // Earlier compacted file
        let file0 = files.pop().unwrap();
        let batches = table.read_parquet_file(file0).await;
        assert_batches_sorted_eq!(
            &[
                "+-----------+------+------+------+--------------------------------+",
                "| field_int | tag1 | tag2 | tag3 | time                           |",
                "+-----------+------+------+------+--------------------------------+",
                "| 10        | VT   |      |      | 1970-01-01T00:00:00.000000020Z |",
                "| 10        | VT   |      |      | 1970-01-01T00:00:00.000006Z    |",
                "| 10        | VT   |      |      | 1970-01-01T00:00:00.000010Z    |",
                "| 1000      | WA   |      |      | 1970-01-01T00:00:00.000000010Z |",
                "| 1500      | WA   |      |      | 1970-01-01T00:00:00.000008Z    |",
                "| 1601      |      | PA   | 15   | 1970-01-01T00:00:00.000000009Z |",
                "| 21        |      | OH   | 21   | 1970-01-01T00:00:00.000000025Z |",
                "| 70        | UT   |      |      | 1970-01-01T00:00:00.000020Z    |",
                "+-----------+------+------+------+--------------------------------+",
            ],
            &batches
        );
    }

    #[derive(Default)]
    struct MockCompactor {
        compaction_groups: Arc<Mutex<Vec<Vec<FilteredFiles>>>>,
    }

    type CompactorFunctionFactory = Box<
        dyn Fn(
                Arc<Compactor>,
                Vec<FilteredFiles>,
            ) -> Pin<Box<dyn futures::Future<Output = ()> + Send>>
            + Send
            + Sync
            + 'static,
    >;

    impl MockCompactor {
        fn compaction_function(&self) -> CompactorFunctionFactory {
            let compaction_groups_for_closure = Arc::clone(&self.compaction_groups);
            Box::new(
                move |_compactor: Arc<Compactor>,
                      parallel_compacting_candidates: Vec<FilteredFiles>| {
                    let compaction_groups_for_async = Arc::clone(&compaction_groups_for_closure);
                    Box::pin(async move {
                        compaction_groups_for_async
                            .lock()
                            .unwrap()
                            .push(parallel_compacting_candidates);
                    })
                },
            )
        }

        fn results(self) -> Vec<Vec<FilteredFiles>> {
            let Self { compaction_groups } = self;
            Arc::try_unwrap(compaction_groups)
                .unwrap()
                .into_inner()
                .unwrap()
        }
    }

    fn make_compactor_config() -> CompactorConfig {
        let max_desired_file_size_bytes = 100_000_000;
        let percentage_max_file_size = 90;
        let split_percentage = 100;
        let max_cold_concurrent_size_bytes = 90_000;
        let max_number_partitions_per_shard = 100;
        let min_number_recent_ingested_per_partition = 1;
        let cold_input_size_threshold_bytes = 600 * 1024 * 1024;
        let cold_input_file_count_threshold = 100;
        let hot_multiple = 4;
        let memory_budget_bytes = 12 * 1125; // 13,500 bytes
        CompactorConfig::new(
            max_desired_file_size_bytes,
            percentage_max_file_size,
            split_percentage,
            max_cold_concurrent_size_bytes,
            max_number_partitions_per_shard,
            min_number_recent_ingested_per_partition,
            cold_input_size_threshold_bytes,
            cold_input_file_count_threshold,
            hot_multiple,
            memory_budget_bytes,
        )
    }

    struct TestSetup {
        compactor: Arc<Compactor>,
        mock_compactor: MockCompactor,
        namespace: Arc<TestNamespace>,
        shard: Arc<TestShard>,
        table: Arc<TestTable>,
    }

    async fn test_setup() -> TestSetup {
        let catalog = TestCatalog::new();
        let namespace = catalog
            .create_namespace("namespace_hot_partitions_to_compact")
            .await;
        let shard = namespace.create_shard(1).await;

        // Create a scenario of a table of 5 columns: tag, time, field int, field string, field
        // bool. Thus, each file will have estimated memory bytes = 1125 * row_count (for even
        // row_counts).
        let table = namespace.create_table("test_table").await;

        table.create_column("tag", ColumnType::Tag).await;
        table.create_column("time", ColumnType::Time).await;
        table.create_column("field_int", ColumnType::I64).await;
        table
            .create_column("field_string", ColumnType::String)
            .await;
        table.create_column("field_bool", ColumnType::Bool).await;

        // Create a compactor
        // Compactor budget : 13,500
        let time_provider = Arc::new(SystemProvider::new());
        let config = make_compactor_config();
        let compactor = Arc::new(Compactor::new(
            vec![shard.shard.id],
            Arc::clone(&catalog.catalog),
            ParquetStorage::new(Arc::clone(&catalog.object_store)),
            Arc::new(Executor::new(1)),
            time_provider,
            BackoffConfig::default(),
            config,
            Arc::new(metric::Registry::new()),
        ));

        let mock_compactor = MockCompactor::default();

        TestSetup {
            compactor,
            mock_compactor,
            namespace,
            shard,
            table,
        }
    }
}
