//! IOx compactor implementation.

#![deny(rustdoc::broken_intra_doc_links, rust_2018_idioms)]
#![warn(
    missing_copy_implementations,
    missing_docs,
    clippy::explicit_iter_loop,
    clippy::future_not_send,
    clippy::use_self,
    clippy::clone_on_ref_ptr,
    clippy::todo,
    clippy::dbg_macro
)]

pub(crate) mod cold;
pub mod compact;
pub mod garbage_collector;
pub mod handler;
pub(crate) mod hot;
mod parquet_file;
pub(crate) mod parquet_file_combining;
pub(crate) mod parquet_file_filtering;
pub(crate) mod parquet_file_lookup;
pub mod query;
pub mod server;
pub mod utils;
pub(crate) mod warm;

use crate::{
    compact::{Compactor, PartitionCompactionCandidateWithInfo},
    parquet_file::CompactorParquetFile,
    parquet_file_filtering::{FilterResult, FilteredFiles},
    parquet_file_lookup::ParquetFilesForCompaction,
};
use data_types::{CompactionLevel, PartitionId};
use metric::{Attributes, Metric, U64Gauge};
use observability_deps::tracing::*;
use parquet_file_lookup::CompactionType;
use snafu::{ResultExt, Snafu};
use std::{collections::VecDeque, sync::Arc};

// For a given list of partition candidates and a memory budget, estimate memory needed to compact
// each partition candidate and compact as many of them in parallel as possible until all
// candidates are compacted.
//
// The way this function works is the estimated memory to compact each partition will be
// accumulated until the running total hits over 90% of the compactor's `memory_budget_bytes`. Then
// those partitions are compacted in parallel. The process repeats until all partitions are
// compacted.
//
// If the partial remaining budget isn't enough to compact the current partition but the full
// budget is enough, the current partition will be pushed back as the last item of the list to be
// considered later with a full memory budget.
async fn compact_candidates_with_memory_budget<C, Fut>(
    compactor: Arc<Compactor>,
    compaction_type: CompactionType,
    initial_level: CompactionLevel,
    target_level: CompactionLevel,
    compact_function: C,
    split: bool,
    mut candidates: VecDeque<Arc<PartitionCompactionCandidateWithInfo>>,
) where
    C: Fn(Arc<Compactor>, Vec<ReadyToCompact>, CompactionType, bool) -> Fut + Send + Sync,
    Fut: futures::Future<Output = ()> + Send,
{
    let mut remaining_budget_bytes = compactor.config.memory_budget_bytes;
    let mut parallel_compacting_candidates = Vec::with_capacity(candidates.len());
    let mut num_remaining_candidates = candidates.len();
    let mut count = 0;

    let mut candidate_counts = CandidateCounts::new();
    candidate_counts.count_total_candidates = candidates.len() as u64;

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
        // 4. If the budget is hit, compact all candidates in the `parallel_compacting_candidates`
        //    list in parallel.
        // 5. Repeat

        // --------------------------------------------------------------------
        // 1. Pop first candidate from the list
        let partition = candidates.pop_front().unwrap();
        let partition_id = partition.candidate.partition_id;
        let table_id = partition.candidate.table_id;
        count += 1;

        // --------------------------------------------------------------------
        // 2. Check if the candidate can be compacted fully or partially under the
        //    remaining_budget_bytes
        // Get parquet_file info for this partition
        let parquet_files_for_compaction =
            parquet_file_lookup::ParquetFilesForCompaction::for_partition(
                Arc::clone(&compactor),
                Arc::clone(&partition),
                compaction_type,
            )
            .await;
        let to_compact = match parquet_files_for_compaction {
            Err(e) => {
                // This may just be a hiccup reading object store, skip compacting it in
                // this cycle
                warn!(
                    %e,
                    ?partition_id,
                    %compaction_type,
                    "failed due to error in reading parquet files"
                );
                candidate_counts.count_read_hiccup_candidates += 1;
                None
            }
            Ok(Some(parquet_files_for_compaction)) => {
                // Return only files under the `remaining_budget_bytes` that should be
                // compacted
                let ParquetFilesForCompaction {
                    level_0,
                    level_1,
                    level_2,
                } = parquet_files_for_compaction;

                let (level_n, level_n_plus_1) = match (initial_level, target_level) {
                    (CompactionLevel::Initial, CompactionLevel::FileNonOverlapped) => {
                        (level_0, level_1)
                    }
                    (CompactionLevel::FileNonOverlapped, CompactionLevel::FileNonOverlapped) => {
                        // warm compaction; don't try to compact L2 files into L1
                        (level_1, vec![])
                    }
                    (CompactionLevel::FileNonOverlapped, CompactionLevel::Final) => {
                        (level_1, level_2)
                    }
                    _ => {
                        // Focusing on compacting any other level is a bug
                        panic!("Unsupported initial compaction level: {initial_level:?}");
                    }
                };

                let to_compact = parquet_file_filtering::filter_parquet_files(
                    Arc::clone(&partition),
                    level_n,
                    level_n_plus_1,
                    remaining_budget_bytes,
                    compactor.config.max_num_compacting_files,
                    compactor.config.max_num_compacting_files_first_in_partition,
                    compactor.config.max_desired_file_size_bytes,
                    &compactor.parquet_file_candidate_gauge,
                    &compactor.parquet_file_candidate_bytes,
                );
                candidate_counts.count_qualified_canddiates += 1;
                Some(to_compact)
            }
            Ok(None) => {
                // This partition candidate is not qualified to get compacted, skip it
                debug!(
                    ?partition_id,
                    %compaction_type, "not qualified for compaction"
                );
                None
            }
        };

        // --------------------------------------------------------------------
        // 3. Check the compactable status and perform the action
        if let Some(to_compact) = to_compact {
            let FilteredFiles {
                filter_result,
                partition,
            } = to_compact;

            match filter_result {
                FilterResult::NothingToCompact => {
                    debug!(?partition_id, %compaction_type, "nothing to compact");
                }
                FilterResult::OverLimitFileNum {
                    num_files,
                    budget_bytes,
                } => {
                    // We cannot compact this partition because its first set of overlapped files
                    // are over the limit of file num
                    warn!(
                        ?partition_id,
                        ?table_id,
                        %compaction_type,
                        num_files,
                        budget_bytes,
                        file_num_limit = compactor.config.max_num_compacting_files,
                        file_num_limit_first_file =
                            compactor.config.max_num_compacting_files_first_in_partition,
                        memory_budget_bytes = compactor.config.memory_budget_bytes,
                        "skipped; over limit of number of files"
                    );
                    record_skipped_compaction(
                        partition_id,
                        Arc::clone(&compactor),
                        "over limit of num_files",
                        num_files,
                        compactor.config.max_num_compacting_files,
                        compactor.config.max_num_compacting_files_first_in_partition,
                        budget_bytes,
                        compactor.config.memory_budget_bytes,
                    )
                    .await;
                    candidate_counts.count_over_file_limit_candidates += 1;
                }
                FilterResult::OverBudget {
                    budget_bytes: needed_bytes,
                    num_files,
                } => {
                    if needed_bytes <= compactor.config.memory_budget_bytes {
                        // Required budget is larger than the remaining budget but smaller than
                        // full budget, add this partition back to the end of the list to compact
                        // with full budget later
                        candidates.push_back(partition);
                    } else {
                        // Even with max budget, we cannot compact this partition, log it
                        warn!(
                            ?partition_id,
                            ?table_id,
                            %compaction_type,
                            ?needed_bytes,
                            memory_budget_bytes = compactor.config.memory_budget_bytes,
                            ?num_files,
                            limit_num_files = compactor.config.max_num_compacting_files,
                            limit_num_files_first_in_partition =
                                compactor.config.max_num_compacting_files_first_in_partition,
                            "skipped; over memory budget"
                        );
                        record_skipped_compaction(
                            partition_id,
                            Arc::clone(&compactor),
                            "over memory budget",
                            num_files,
                            compactor.config.max_num_compacting_files,
                            compactor.config.max_num_compacting_files_first_in_partition,
                            needed_bytes,
                            compactor.config.memory_budget_bytes,
                        )
                        .await;
                        candidate_counts.count_over_budget_candidates += 1;
                    }
                }
                FilterResult::Proceed {
                    files,
                    budget_bytes,
                } => {
                    remaining_budget_bytes -= budget_bytes;
                    parallel_compacting_candidates.push(ReadyToCompact {
                        files,
                        partition,
                        target_level,
                    });
                    candidate_counts.count_compacted_candidates += 1;
                }
            }
        }

        // --------------------------------------------------------------------
        // 4. Let compact the candidates that are in parallel_compacting_candidates if one of this condition hits:
        //     . candidates in parallel_compacting_candidates consume almost all the budget
        //     . no more candidates
        //     . already considered all remaining candidates.
        //     . hit the max number of partitions to compact in parallel
        if (!parallel_compacting_candidates.is_empty())
            && ((remaining_budget_bytes <= compactor.config.memory_budget_bytes / 10)
                || (candidates.is_empty())
                || (count == num_remaining_candidates)
                || (count as u64 == compactor.config.max_parallel_partitions))
        {
            debug!(
                num_parallel_compacting_candidates = parallel_compacting_candidates.len(),
                total_needed_memory_budget_bytes =
                    compactor.config.memory_budget_bytes - remaining_budget_bytes,
                config_max_parallel_partitions = compactor.config.max_parallel_partitions,
                %compaction_type,
                "parallel compacting candidates"
            );
            compact_function(
                Arc::clone(&compactor),
                parallel_compacting_candidates,
                compaction_type,
                split,
            )
            .await;

            // Reset to start adding new set of parallel candidates
            parallel_compacting_candidates = Vec::with_capacity(candidates.len());
            remaining_budget_bytes = compactor.config.memory_budget_bytes;
            num_remaining_candidates = candidates.len();
            count = 0;
        }
    }

    record_partition_metrics(
        &compactor.compaction_candidate_gauge,
        compaction_type,
        candidate_counts,
    );
}

struct CandidateCounts {
    pub count_total_candidates: u64,
    pub count_read_hiccup_candidates: u64,
    pub count_qualified_canddiates: u64,
    pub count_compacted_candidates: u64,
    pub count_over_budget_candidates: u64,
    pub count_over_file_limit_candidates: u64,
}

impl CandidateCounts {
    fn new() -> Self {
        Self {
            count_total_candidates: 0,
            count_read_hiccup_candidates: 0,
            count_qualified_canddiates: 0,
            count_compacted_candidates: 0,
            count_over_budget_candidates: 0,
            count_over_file_limit_candidates: 0,
        }
    }
}

fn record_partition_metrics(
    gauge: &Metric<U64Gauge>,
    compaction_type: CompactionType,
    candidate_counts: CandidateCounts,
) {
    let mut attributes =
        Attributes::from([("compaction_type", compaction_type.to_string().into())]);

    attributes.insert("status", "total_candidates");
    let recorder = gauge.recorder(attributes.clone());
    recorder.set(candidate_counts.count_total_candidates);

    attributes.insert("status", "read_hiccup_candidates");
    let recorder = gauge.recorder(attributes.clone());
    recorder.set(candidate_counts.count_read_hiccup_candidates);

    attributes.insert("status", "qualified_candidates");
    let recorder = gauge.recorder(attributes.clone());
    recorder.set(candidate_counts.count_qualified_canddiates);

    attributes.insert("status", "compacted_candidates");
    let recorder = gauge.recorder(attributes.clone());
    recorder.set(candidate_counts.count_compacted_candidates);

    attributes.insert("status", "over_budget_candidates");
    let recorder = gauge.recorder(attributes.clone());
    recorder.set(candidate_counts.count_over_budget_candidates);

    attributes.insert("status", "over_file_limit_candidates");
    let recorder = gauge.recorder(attributes);
    recorder.set(candidate_counts.count_over_file_limit_candidates);
}

#[allow(clippy::too_many_arguments)]
async fn record_skipped_compaction(
    partition_id: PartitionId,
    compactor: Arc<Compactor>,
    reason: &str,
    num_files: usize,
    limit_num_files: usize,
    limit_num_files_first_in_partition: usize,
    estimated_bytes: u64,
    limit_bytes: u64,
) {
    let mut repos = compactor.catalog.repositories().await;
    let record_skip = repos
        .partitions()
        .record_skipped_compaction(
            partition_id,
            reason,
            num_files,
            limit_num_files,
            limit_num_files_first_in_partition,
            estimated_bytes,
            limit_bytes,
        )
        .await;
    if let Err(e) = record_skip {
        warn!(?partition_id, %e, "could not log skipped compaction");
    }
}

/// After filtering based on the memory budget, this is a group of files that should be compacted
/// into the target level specified.
#[derive(Debug)]
pub(crate) struct ReadyToCompact {
    pub(crate) files: Vec<CompactorParquetFile>,
    pub(crate) partition: Arc<PartitionCompactionCandidateWithInfo>,
    pub(crate) target_level: CompactionLevel,
}

// Compact given groups of files in parallel.
//
// This function assumes its caller knows there are enough resources to compact all groups
// concurrently
async fn compact_in_parallel(
    compactor: Arc<Compactor>,
    groups: Vec<ReadyToCompact>,
    compaction_type: CompactionType,
    split: bool,
) {
    let mut handles = Vec::with_capacity(groups.len());
    for group in groups {
        let comp = Arc::clone(&compactor);
        let handle = tokio::task::spawn(async move {
            let partition_id = group.partition.id();
            debug!(?partition_id, %compaction_type, "compaction starting");
            let compaction_result =
                compact_one_partition(&comp, group, compaction_type, split).await;
            match compaction_result {
                Err(e) => {
                    warn!(%e, ?partition_id, %compaction_type, "compaction failed");
                }
                Ok(_) => {
                    debug!(?partition_id, %compaction_type, "compaction complete");
                }
            };
        });
        handles.push(handle);
    }

    let compactions_run = handles.len();
    debug!(
        ?compactions_run,
        %compaction_type, "Number of concurrent partitions are being compacted"
    );

    let _ = futures::future::join_all(handles).await;
}

#[derive(Debug, Snafu)]
#[allow(missing_copy_implementations, missing_docs)]
pub(crate) enum CompactOnePartitionError {
    #[snafu(display("{}", source))]
    Combining {
        source: Box<parquet_file_combining::Error>,
    },

    #[snafu(display("{}", source))]
    Upgrading {
        source: iox_catalog::interface::Error,
    },
}

impl From<parquet_file_combining::Error> for CompactOnePartitionError {
    fn from(source: parquet_file_combining::Error) -> Self {
        Self::Combining {
            source: Box::new(source),
        }
    }
}

/// One compaction operation of one group of files.
pub(crate) async fn compact_one_partition(
    compactor: &Compactor,
    to_compact: ReadyToCompact,
    compaction_type: CompactionType,
    split: bool,
) -> Result<(), CompactOnePartitionError> {
    let start_time = compactor.time_provider.now();

    let ReadyToCompact {
        files,
        partition,
        target_level,
    } = to_compact;

    let shard_id = partition.shard_id();

    if files.len() == 1 {
        // upgrade the one file, don't run compaction
        let mut repos = compactor.catalog.repositories().await;

        repos
            .parquet_files()
            .update_compaction_level(&[files[0].id()], target_level)
            .await
            .context(UpgradingSnafu)?;
    } else if split {
        parquet_file_combining::CompactPlanBuilder::new(partition)
            .with_files(files)
            .with_catalog(Arc::clone(&compactor.catalog))
            .with_store(compactor.store.clone())
            .with_exec(Arc::clone(&compactor.exec))
            .with_time_provider(Arc::clone(&compactor.time_provider))
            .with_compaction_input_file_bytes(compactor.compaction_input_file_bytes.clone())
            .with_max_desired_file_size_bytes(compactor.config.max_desired_file_size_bytes)
            .with_percentage_max_file_size(compactor.config.percentage_max_file_size)
            .with_split_percentage(compactor.config.split_percentage)
            .with_target_level(target_level)
            .build_with_splits()?
            .compact_and_update_catalog()
            .await?;
    } else {
        parquet_file_combining::CompactPlanBuilder::new(partition)
            .with_files(files)
            .with_catalog(Arc::clone(&compactor.catalog))
            .with_store(compactor.store.clone())
            .with_exec(Arc::clone(&compactor.exec))
            .with_time_provider(Arc::clone(&compactor.time_provider))
            .with_compaction_input_file_bytes(compactor.compaction_input_file_bytes.clone())
            .with_target_level(target_level)
            .build_no_splits()?
            .compact_and_update_catalog()
            .await?;
    }

    let attributes = Attributes::from([
        ("shard_id", format!("{shard_id}").into()),
        ("partition_type", compaction_type.to_string().into()),
        ("target_level", format!("{}", target_level as i16).into()),
    ]);
    if let Some(delta) = compactor
        .time_provider
        .now()
        .checked_duration_since(start_time)
    {
        let duration = compactor.compaction_duration.recorder(attributes);
        duration.record(delta);
    }

    Ok(())
}

#[cfg(test)]
pub mod tests {
    use super::*;
    use crate::{
        compact::{Compactor, ShardAssignment},
        compact_one_partition,
        handler::CompactorConfig,
        parquet_file_filtering, parquet_file_lookup, ParquetFilesForCompaction,
    };
    use ::parquet_file::storage::{ParquetStorage, StorageId};
    use arrow_util::assert_batches_sorted_eq;
    use backoff::BackoffConfig;
    use data_types::{ColumnType, CompactionLevel, ParquetFileId};
    use iox_tests::util::{
        TestCatalog, TestParquetFileBuilder, TestPartition, TestShard, TestTable,
    };
    use iox_time::{SystemProvider, TimeProvider};
    use std::{
        collections::{HashMap, VecDeque},
        pin::Pin,
        sync::{Arc, Mutex},
    };

    const DEFAULT_MAX_NUM_PARTITION_CANDIDATES: usize = 100;
    const DEFAULT_HOT_COMPACTION_HOURS_THRESHOLD_1: u64 = 4;
    const DEFAULT_HOT_COMPACTION_HOURS_THRESHOLD_2: u64 = 24;
    const DEFAULT_WARM_PARTITION_CANDIDATES_HOURS_THRESHOLD: u64 = 24;
    const DEFAULT_COLD_PARTITION_CANDIDATES_HOURS_THRESHOLD: u64 = 24;
    const DEFAULT_MAX_PARALLEL_PARTITIONS: u64 = 20;

    // In tests that are verifying successful compaction not affected by the memory budget, this
    // converts a `parquet_file_filtering::FilteredFiles` that has a `filter_result` of
    // `parquet_file_filtering::FilterResult::Proceed` into a `ReadyToCompact` and panics if it
    // gets any other variant.
    impl From<parquet_file_filtering::FilteredFiles> for ReadyToCompact {
        fn from(filtered_files: parquet_file_filtering::FilteredFiles) -> Self {
            let parquet_file_filtering::FilteredFiles {
                filter_result,
                partition,
            } = filtered_files;

            let files = if let parquet_file_filtering::FilterResult::Proceed { files, .. } =
                filter_result
            {
                files
            } else {
                panic!("Expected to get FilterResult::Proceed, got {filter_result:?}");
            };

            let target_level = files.last().unwrap().compaction_level().next();

            Self {
                files,
                partition,
                target_level,
            }
        }
    }

    #[tokio::test]
    async fn empty_candidates_compacts_nothing() {
        test_helpers::maybe_start_logging();

        let TestSetup {
            compactor,
            mock_compactor,
            ..
        } = test_setup(14350, 20).await;

        let sorted_candidates = VecDeque::new();

        compact_candidates_with_memory_budget(
            Arc::clone(&compactor),
            CompactionType::Hot,
            CompactionLevel::Initial,
            CompactionLevel::FileNonOverlapped,
            mock_compactor.compaction_function(),
            true,
            sorted_candidates,
        )
        .await;

        let compaction_groups = mock_compactor.results();
        assert!(compaction_groups.is_empty());
    }

    #[derive(Default)]
    pub(crate) struct MockCompactor {
        compaction_groups: Arc<Mutex<Vec<Vec<ReadyToCompact>>>>,
    }

    type CompactorFunctionFactory = Box<
        dyn Fn(
                Arc<Compactor>,
                Vec<ReadyToCompact>,
                CompactionType,
                bool,
            ) -> Pin<Box<dyn futures::Future<Output = ()> + Send>>
            + Send
            + Sync,
    >;

    impl MockCompactor {
        pub(crate) fn compaction_function(&self) -> CompactorFunctionFactory {
            let compaction_groups_for_closure = Arc::clone(&self.compaction_groups);
            Box::new(
                move |_compactor: Arc<Compactor>,
                      parallel_compacting_candidates: Vec<ReadyToCompact>,
                      _compaction_type: CompactionType,
                      _split: bool| {
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

        pub(crate) fn results(self) -> Vec<Vec<ReadyToCompact>> {
            let Self { compaction_groups } = self;
            Arc::try_unwrap(compaction_groups)
                .unwrap()
                .into_inner()
                .unwrap()
        }
    }

    fn make_compactor_config(budget: u64, max_parallel_jobs: u64) -> CompactorConfig {
        // All numbers in here are chosen carefully for many tests.
        // Change them will break the tests
        CompactorConfig {
            max_desired_file_size_bytes: 100_000_000,
            percentage_max_file_size: 90,
            split_percentage: 100,
            max_number_partitions_per_shard: DEFAULT_MAX_NUM_PARTITION_CANDIDATES,
            min_number_recent_ingested_files_per_partition: 1,
            hot_multiple: 4,
            warm_multiple: 1,
            memory_budget_bytes: budget,
            min_num_rows_allocated_per_record_batch_to_datafusion_plan: 2,
            max_num_compacting_files: 20,
            max_num_compacting_files_first_in_partition: 40,
            minutes_without_new_writes_to_be_cold: 10,
            cold_partition_candidates_hours_threshold:
                DEFAULT_COLD_PARTITION_CANDIDATES_HOURS_THRESHOLD,
            hot_compaction_hours_threshold_1: DEFAULT_HOT_COMPACTION_HOURS_THRESHOLD_1,
            hot_compaction_hours_threshold_2: DEFAULT_HOT_COMPACTION_HOURS_THRESHOLD_2,
            max_parallel_partitions: max_parallel_jobs,
            warm_partition_candidates_hours_threshold:
                DEFAULT_WARM_PARTITION_CANDIDATES_HOURS_THRESHOLD,
            warm_compaction_small_size_threshold_bytes: 50_000_000,
            warm_compaction_min_small_file_count: 10,
        }
    }

    pub(crate) struct TestSetup {
        pub(crate) compactor: Arc<Compactor>,
        pub(crate) mock_compactor: MockCompactor,
        pub(crate) shard: Arc<TestShard>,
        pub(crate) table: Arc<TestTable>,
    }

    pub(crate) async fn test_setup_with_default_budget() -> TestSetup {
        test_setup(14350, 20).await
    }

    pub(crate) async fn test_setup(budget: u64, max_parallel_jobs: u64) -> TestSetup {
        let catalog = TestCatalog::new();
        let namespace = catalog
            .create_namespace_1hr_retention("namespace_hot_partitions_to_compact")
            .await;
        let shard = namespace.create_shard(1).await;

        // Create a scenario of a table of 5 columns: tag, time, field int, field string, field
        // bool. Thus, given min_num_rows_allocated_per_record_batch_to_datafusion_plan = 2,
        //// todo  each file will have estimated memory bytes = 2050  + 100 * row_count (for even row_counts)
        // each file will have estimated memory bytes = 2,250
        let table = namespace.create_table("test_table").await;

        table.create_column("tag", ColumnType::Tag).await;
        table.create_column("time", ColumnType::Time).await;
        table.create_column("field_int", ColumnType::I64).await;
        table
            .create_column("field_string", ColumnType::String)
            .await;
        table.create_column("field_bool", ColumnType::Bool).await;

        // Create a compactor
        let time_provider = Arc::new(SystemProvider::new());
        let config = make_compactor_config(budget, max_parallel_jobs);
        let compactor = Arc::new(Compactor::new(
            ShardAssignment::Only(vec![shard.shard.id]),
            Arc::clone(&catalog.catalog),
            ParquetStorage::new(Arc::clone(&catalog.object_store), StorageId::from("iox")),
            catalog.exec(),
            time_provider,
            BackoffConfig::default(),
            config,
            Arc::new(metric::Registry::new()),
        ));

        let mock_compactor = MockCompactor::default();

        TestSetup {
            compactor,
            mock_compactor,
            shard,
            table,
        }
    }

    #[tokio::test]
    async fn test_hot_compact_candidates_with_limit_memory_budget() {
        test_helpers::maybe_start_logging();

        // test setup with limit memory budget, 14350, and very large (aka unlimited in this test) max_parallel_jobs, 200
        let (compactor, mock_compactor, partitions) = make_6_partitions(14350, 200).await;

        // partition candidates: partitions with L0 and overlapped L1
        let compaction_type = CompactionType::Hot;
        let hour_thresholds = vec![
            compactor.config.hot_compaction_hours_threshold_1,
            compactor.config.hot_compaction_hours_threshold_2,
        ];
        let max_num_partitions = compactor.config.max_number_partitions_per_shard;
        let mut candidates = compactor
            .partitions_to_compact(compaction_type, hour_thresholds, max_num_partitions)
            .await
            .unwrap();
        assert_eq!(candidates.len(), 6);
        candidates.sort_by_key(|c| c.candidate.partition_id);
        {
            let mut repos = compactor.catalog.repositories().await;
            let skipped_compactions = repos.partitions().list_skipped_compactions().await.unwrap();
            assert!(
                skipped_compactions.is_empty(),
                "Expected no skipped compactions, got: {skipped_compactions:?}"
            );
        }

        // There are 3 rounds of parallel compaction:
        //
        // * Round 1: 3 candidates [P1, P2, P5] and total needed budget 13,500
        // * Round 2: 1 candidate [P6] and total needed budget 4,500
        // * Round 3: 1 candidate [P3] and total needed budget 13,100
        //
        // P4 is not compacted due to overbudget.
        compact_candidates_with_memory_budget(
            Arc::clone(&compactor),
            CompactionType::Hot,
            CompactionLevel::Initial,
            CompactionLevel::FileNonOverlapped,
            mock_compactor.compaction_function(),
            true,
            candidates.into(),
        )
        .await;

        let compaction_groups = mock_compactor.results();

        // 3 rounds of parallel compaction
        assert_eq!(compaction_groups.len(), 3);

        // Round 1
        let group1 = &compaction_groups[0];
        assert_eq!(group1.len(), 3);

        let g1_candidate1 = &group1[0];
        assert_eq!(g1_candidate1.partition.id(), partitions[0].partition.id);
        let g1_candidate1_pf_ids: Vec<_> =
            g1_candidate1.files.iter().map(|pf| pf.id().get()).collect();
        assert_eq!(g1_candidate1_pf_ids, vec![2, 1]);

        let g1_candidate2 = &group1[1];
        assert_eq!(g1_candidate2.partition.id(), partitions[1].partition.id);
        let g1_candidate2_pf_ids: Vec<_> =
            g1_candidate2.files.iter().map(|pf| pf.id().get()).collect();
        assert_eq!(g1_candidate2_pf_ids, vec![4, 3]);

        let g1_candidate3 = &group1[2];
        assert_eq!(g1_candidate3.partition.id(), partitions[4].partition.id);
        let g1_candidate3_pf_ids: Vec<_> =
            g1_candidate3.files.iter().map(|pf| pf.id().get()).collect();
        assert_eq!(g1_candidate3_pf_ids, vec![19, 18]);

        // Round 2
        let group2 = &compaction_groups[1];
        assert_eq!(group2.len(), 1);

        let g2_candidate1 = &group2[0];
        assert_eq!(g2_candidate1.partition.id(), partitions[5].partition.id);
        let g2_candidate1_pf_ids: Vec<_> =
            g2_candidate1.files.iter().map(|pf| pf.id().get()).collect();
        assert_eq!(g2_candidate1_pf_ids, vec![21, 20]);

        // Round 3
        let group3 = &compaction_groups[2];
        assert_eq!(group3.len(), 1);

        let g3_candidate1 = &group3[0];
        assert_eq!(g3_candidate1.partition.id(), partitions[2].partition.id);
        let g3_candidate1_pf_ids: Vec<_> =
            g3_candidate1.files.iter().map(|pf| pf.id().get()).collect();
        // all IDs of level-1 first then level-0
        assert_eq!(g3_candidate1_pf_ids, vec![6, 7, 8, 9, 10, 5]);

        {
            let mut repos = compactor.catalog.repositories().await;
            let skipped_compactions = repos.partitions().list_skipped_compactions().await.unwrap();
            assert_eq!(skipped_compactions.len(), 1);
            assert_eq!(
                skipped_compactions[0].partition_id,
                partitions[3].partition.id
            );
            assert_eq!(skipped_compactions[0].reason, "over memory budget");
        }
    }

    #[tokio::test]
    async fn test_hot_compact_candidates_with_limit_parallel_jobs() {
        test_helpers::maybe_start_logging();

        // tes setup with plenty of memory budget 1GB (aka unlimited) but limit to 2 parallel jobs
        let (compactor, mock_compactor, partitions) =
            make_6_partitions(1024 * 1024 * 1024, 2).await;

        // partition candidates: partitions with L0 and overlapped L1
        let compaction_type = CompactionType::Hot;
        let hour_thresholds = vec![
            compactor.config.hot_compaction_hours_threshold_1,
            compactor.config.hot_compaction_hours_threshold_2,
        ];
        let max_num_partitions = compactor.config.max_number_partitions_per_shard;
        let mut candidates = compactor
            .partitions_to_compact(compaction_type, hour_thresholds, max_num_partitions)
            .await
            .unwrap();
        assert_eq!(candidates.len(), 6);
        candidates.sort_by_key(|c| c.candidate.partition_id);
        {
            let mut repos = compactor.catalog.repositories().await;
            let skipped_compactions = repos.partitions().list_skipped_compactions().await.unwrap();
            assert!(
                skipped_compactions.is_empty(),
                "Expected no skipped compactions, got: {skipped_compactions:?}"
            );
        }

        // There are 3 rounds of parallel compaction:
        //
        // * Round 1: 2 candidates [P1, P2]
        // * Round 2: 2 candidate [P3, P4]
        // * Round 3: 1 candidate [P5, P6]

        compact_candidates_with_memory_budget(
            Arc::clone(&compactor),
            CompactionType::Hot,
            CompactionLevel::Initial,
            CompactionLevel::FileNonOverlapped,
            mock_compactor.compaction_function(),
            true,
            candidates.into(),
        )
        .await;

        let compaction_groups = mock_compactor.results();

        // 3 rounds of parallel compaction
        assert_eq!(compaction_groups.len(), 3);

        // Round 1
        let group1 = &compaction_groups[0];
        assert_eq!(group1.len(), 2);

        let g1_candidate1 = &group1[0];
        assert_eq!(g1_candidate1.partition.id(), partitions[0].partition.id);

        let g1_candidate2 = &group1[1];
        assert_eq!(g1_candidate2.partition.id(), partitions[1].partition.id);

        // Round 2
        let group2 = &compaction_groups[1];
        assert_eq!(group2.len(), 2);

        let g2_candidate1 = &group2[0];
        assert_eq!(g2_candidate1.partition.id(), partitions[2].partition.id);

        let g2_candidate2 = &group2[1];
        assert_eq!(g2_candidate2.partition.id(), partitions[3].partition.id);

        // Round 3
        let group3 = &compaction_groups[2];
        assert_eq!(group3.len(), 2);

        let g3_candidate1 = &group3[0];
        assert_eq!(g3_candidate1.partition.id(), partitions[4].partition.id);

        let g3_candidate2 = &group3[1];
        assert_eq!(g3_candidate2.partition.id(), partitions[5].partition.id);
    }

    // A quite sophisticated integration test of compacting one hot partition
    // Beside lp data, every value min/max sequence numbers and min/max time are important
    // to have a combination of needed tests in this test function
    #[tokio::test]
    async fn test_many_files_hot_compact_one_partition() {
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

        let ns = catalog.create_namespace_1hr_retention("ns").await;
        let shard = ns.create_shard(1).await;
        let table = ns.create_table("table").await;
        table.create_column("field_int", ColumnType::I64).await;
        table.create_column("tag1", ColumnType::Tag).await;
        table.create_column("tag2", ColumnType::Tag).await;
        table.create_column("tag3", ColumnType::Tag).await;
        table.create_column("time", ColumnType::Time).await;

        let partition = table.with_shard(&shard).create_partition("part").await;
        let time = Arc::new(SystemProvider::new());
        let config = CompactorConfig {
            max_desired_file_size_bytes: 10_000,
            percentage_max_file_size: 30,
            split_percentage: 80,
            max_number_partitions_per_shard: 1,
            min_number_recent_ingested_files_per_partition: 1,
            hot_multiple: 4,
            warm_multiple: 1,
            memory_budget_bytes: 100_000_000,
            min_num_rows_allocated_per_record_batch_to_datafusion_plan: 100,
            max_num_compacting_files: 20,
            max_num_compacting_files_first_in_partition: 40,
            minutes_without_new_writes_to_be_cold: 10,
            cold_partition_candidates_hours_threshold:
                DEFAULT_COLD_PARTITION_CANDIDATES_HOURS_THRESHOLD,
            hot_compaction_hours_threshold_1: DEFAULT_HOT_COMPACTION_HOURS_THRESHOLD_1,
            hot_compaction_hours_threshold_2: DEFAULT_HOT_COMPACTION_HOURS_THRESHOLD_2,
            max_parallel_partitions: DEFAULT_MAX_PARALLEL_PARTITIONS,
            warm_partition_candidates_hours_threshold:
                DEFAULT_WARM_PARTITION_CANDIDATES_HOURS_THRESHOLD,
            warm_compaction_small_size_threshold_bytes: 5_000,
            warm_compaction_min_small_file_count: 10,
        };

        let metrics = Arc::new(metric::Registry::new());
        let compactor = Arc::new(Compactor::new(
            ShardAssignment::Only(vec![shard.shard.id]),
            Arc::clone(&catalog.catalog),
            ParquetStorage::new(Arc::clone(&catalog.object_store), StorageId::from("iox")),
            catalog.exec(),
            Arc::new(SystemProvider::new()),
            BackoffConfig::default(),
            config,
            Arc::clone(&metrics),
        ));

        // parquet files that are all in the same partition
        let mut size_overrides = HashMap::<ParquetFileId, i64>::default();

        // pf1 does not overlap with any other level 0
        let builder = TestParquetFileBuilder::default()
            .with_line_protocol(&lp1)
            .with_max_seq(3)
            .with_min_time(10)
            .with_max_time(20)
            .with_creation_time(time.now());
        let f = partition.create_parquet_file(builder).await;
        size_overrides.insert(
            f.parquet_file.id,
            compactor.config.max_desired_file_size_bytes as i64 + 10,
        );

        // pf2 overlaps with pf3
        let builder = TestParquetFileBuilder::default()
            .with_line_protocol(&lp2)
            .with_max_seq(5)
            .with_min_time(8_000)
            .with_max_time(20_000)
            .with_creation_time(time.now());
        let f = partition.create_parquet_file(builder).await;
        size_overrides.insert(f.parquet_file.id, 100); // small file

        // pf3 overlaps with pf2
        let builder = TestParquetFileBuilder::default()
            .with_line_protocol(&lp3)
            .with_max_seq(10)
            .with_min_time(6_000)
            .with_max_time(25_000)
            .with_creation_time(time.now());
        let f = partition.create_parquet_file(builder).await;
        size_overrides.insert(f.parquet_file.id, 100); // small file

        // pf4 does not overlap with any but is small
        let builder = TestParquetFileBuilder::default()
            .with_line_protocol(&lp4)
            .with_max_seq(18)
            .with_min_time(26_000)
            .with_max_time(28_000)
            .with_creation_time(time.now());
        let f = partition.create_parquet_file(builder).await;
        size_overrides.insert(f.parquet_file.id, 100); // small file

        // pf5 was created in a previous compaction cycle; overlaps with pf1
        let builder = TestParquetFileBuilder::default()
            .with_line_protocol(&lp5)
            .with_max_seq(1)
            .with_min_time(9)
            .with_max_time(25)
            .with_creation_time(time.now())
            .with_compaction_level(CompactionLevel::FileNonOverlapped);
        let f = partition.create_parquet_file(builder).await;
        size_overrides.insert(f.parquet_file.id, 100); // small file

        // pf6 was created in a previous compaction cycle; does not overlap with any
        let builder = TestParquetFileBuilder::default()
            .with_line_protocol(&lp6)
            .with_max_seq(20)
            .with_min_time(90000)
            .with_max_time(91000)
            .with_creation_time(time.now())
            .with_compaction_level(CompactionLevel::FileNonOverlapped);
        let f = partition.create_parquet_file(builder).await;
        size_overrides.insert(f.parquet_file.id, 100); // small file

        // should have 4 level-0 files before compacting
        let count = catalog.count_level_0_files(shard.shard.id).await;
        assert_eq!(count, 4);

        // ------------------------------------------------
        // Compact
        let compaction_type = CompactionType::Hot;
        let hour_thresholds = vec![
            compactor.config.hot_compaction_hours_threshold_1,
            compactor.config.hot_compaction_hours_threshold_2,
        ];
        let max_num_partitions = compactor.config.max_number_partitions_per_shard;
        let mut partition_candidates = compactor
            .partitions_to_compact(compaction_type, hour_thresholds, max_num_partitions)
            .await
            .unwrap();
        // let mut partition_candidates = hot::hot_partitions_to_compact(Arc::clone(&compactor))
        //     .await
        //     .unwrap();

        assert_eq!(partition_candidates.len(), 1);

        let partition = partition_candidates.pop().unwrap();

        let parquet_files_for_compaction =
            parquet_file_lookup::ParquetFilesForCompaction::for_partition_with_size_overrides(
                Arc::clone(&compactor),
                Arc::clone(&partition),
                CompactionType::Hot,
                &size_overrides,
            )
            .await
            .unwrap()
            .unwrap();

        let ParquetFilesForCompaction {
            level_0,
            level_1,
            .. // Ignore other levels
        } = parquet_files_for_compaction;

        let to_compact = parquet_file_filtering::filter_parquet_files(
            partition,
            level_0,
            level_1,
            compactor.config.memory_budget_bytes,
            compactor.config.max_num_compacting_files,
            compactor.config.max_num_compacting_files_first_in_partition,
            compactor.config.max_desired_file_size_bytes,
            &compactor.parquet_file_candidate_gauge,
            &compactor.parquet_file_candidate_bytes,
        );

        let to_compact = to_compact.into();

        compact_one_partition(&compactor, to_compact, CompactionType::Hot, true)
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

    async fn make_6_partitions(
        budget: u64,
        max_parallel_jobs: u64,
    ) -> (Arc<Compactor>, MockCompactor, Vec<Arc<TestPartition>>) {
        let TestSetup {
            compactor,
            mock_compactor,
            shard,
            table,
            ..
        } = test_setup(budget, max_parallel_jobs).await;

        // Some times in the past to set to created_at of the files
        let hot_time_one_hour_ago = compactor.time_provider.hours_ago(1);

        let mut partitions = Vec::with_capacity(6);

        // P1:
        //   L0 2 rows. bytes: 2,250
        //   L1 2 rows. bytes: 2,250
        // total = 2,250 + 2,250 = 4,500
        let partition1 = table.with_shard(&shard).create_partition("one").await;

        // 2 files with IDs 1 and 2
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
        partitions.push(partition1);

        // P2:
        //   L0 2 rows. bytes: 2,250
        //   L1 2 rows. bytes: 2,250
        // total = 2,250 + 2,250 = 4,500
        let partition2 = table.with_shard(&shard).create_partition("two").await;

        // 2 files with IDs 3 and 4
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
        partitions.push(partition2);

        // P3: bytes >= 90% of full budget = 90% * 14,350 = 12,915
        //   L0 40 rows. bytes: 2,250
        //   Five L1s. bytes: 2,250 each
        // total = 2,250 * 6 = 13,500
        let partition3 = table.with_shard(&shard).create_partition("three").await;

        // 6 files with IDs 5, 6, 7, 8, 9, 10
        let pf3_1 = TestParquetFileBuilder::default()
            .with_min_time(1)
            .with_max_time(6)
            .with_row_count(40)
            .with_compaction_level(CompactionLevel::Initial)
            .with_creation_time(hot_time_one_hour_ago);
        partition3.create_parquet_file_catalog_record(pf3_1).await;

        // Five overlapped L1 files
        for i in 1..6 {
            let pf3_i = TestParquetFileBuilder::default()
                .with_min_time(i) // overlapped with pf3_1
                .with_max_time(i)
                .with_row_count(24)
                .with_compaction_level(CompactionLevel::FileNonOverlapped)
                .with_creation_time(hot_time_one_hour_ago);
            partition3.create_parquet_file_catalog_record(pf3_i).await;
        }
        partitions.push(partition3);

        // P4: Over the full budget
        //   L0 40 rows. bytes: 2,250
        //   Six L1s. bytes: 2,250 each
        // total = 2,250 * 7 = 15,750  > 14350
        let partition4 = table.with_shard(&shard).create_partition("four").await;

        // 7 files with IDs 11, 12, 13, 14, 15, 16, 17
        let pf4_1 = TestParquetFileBuilder::default()
            .with_min_time(1)
            .with_max_time(7)
            .with_row_count(70)
            .with_compaction_level(CompactionLevel::Initial)
            .with_creation_time(hot_time_one_hour_ago);
        partition4.create_parquet_file_catalog_record(pf4_1).await;

        // Six overlapped L1 files
        for i in 1..7 {
            let pf4_i = TestParquetFileBuilder::default()
                .with_min_time(i) // overlapped with pf4_1
                .with_max_time(i)
                .with_row_count(40)
                .with_compaction_level(CompactionLevel::FileNonOverlapped)
                .with_creation_time(hot_time_one_hour_ago);
            partition4.create_parquet_file_catalog_record(pf4_i).await;
        }
        partitions.push(partition4);

        // P5:
        //   L0 2 rows. bytes: 2,250
        //   L1 2 rows. bytes: 2,250
        // total = 2,250 + 2,250 = 4,500
        let partition5 = table.with_shard(&shard).create_partition("five").await;
        // 2 files with IDs 18, 19
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
        partitions.push(partition5);

        // P6:
        //   L0 2 rows. bytes: 2,250
        //   L1 2 rows. bytes: 2,250
        // total = 2,250 + 2,250 = 4,500
        let partition6 = table.with_shard(&shard).create_partition("six").await;
        // 2 files with IDs 20, 21
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
        partitions.push(partition6);

        (compactor, mock_compactor, partitions)
    }
}
