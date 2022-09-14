//! IOx compactor implementation.

#![deny(rustdoc::broken_intra_doc_links, rust_2018_idioms)]
#![warn(
    missing_copy_implementations,
    missing_docs,
    clippy::explicit_iter_loop,
    clippy::future_not_send,
    clippy::use_self,
    clippy::clone_on_ref_ptr
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

use crate::{
    compact::{Compactor, PartitionCompactionCandidateWithInfo},
    parquet_file_filtering::{FilterResult, FilteredFiles},
};
use data_types::CompactionLevel;
use metric::Attributes;
use observability_deps::tracing::*;
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
    compaction_type: &'static str,
    compact_function: C,
    mut candidates: VecDeque<Arc<PartitionCompactionCandidateWithInfo>>,
) where
    C: Fn(Arc<Compactor>, Vec<FilteredFiles>, &'static str) -> Fut + Send + Sync + 'static,
    Fut: futures::Future<Output = ()> + Send,
{
    let mut remaining_budget_bytes = compactor.config.memory_budget_bytes;
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
        // 4. If the budget is hit, compact all candidates in the `parallel_compacting_candidates`
        //    list in parallel.
        // 5. Repeat

        // --------------------------------------------------------------------
        // 1. Pop first candidate from the list. Since it is not empty, there must be at least one
        let partition = candidates.pop_front().unwrap();
        count += 1;
        let partition_id = partition.candidate.partition_id;
        let table_id = partition.candidate.table_id;

        // --------------------------------------------------------------------
        // 2. Check if the candidate can be compacted fully or partially under the
        //    remaining_budget_bytes
        // Get parquet_file info for this partition
        let parquet_files_for_compaction =
            parquet_file_lookup::ParquetFilesForCompaction::for_partition(
                Arc::clone(&compactor.catalog),
                Arc::clone(&partition),
            )
            .await;
        let to_compact = match parquet_files_for_compaction {
            Err(e) => {
                // This may just be a hiccup reading object store, skip compacting it in
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
                // Return only files under the `remaining_budget_bytes` that should be
                // compacted
                let to_compact = parquet_file_filtering::filter_parquet_files(
                    Arc::clone(&partition),
                    parquet_files_for_compaction,
                    remaining_budget_bytes,
                    &compactor.parquet_file_candidate_gauge,
                    &compactor.parquet_file_candidate_bytes,
                );
                Some(to_compact)
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

                    let mut repos = compactor.catalog.repositories().await;
                    let record_skip = repos
                        .partitions()
                        .record_skipped_compaction(
                            partition_id,
                            "error in estimating compacting memory",
                        )
                        .await;
                    if let Err(e) = record_skip {
                        warn!(?partition_id, %e, "could not log skipped compaction");
                    }
                }
                FilterResult::OverBudget => {
                    let needed_bytes = to_compact.budget_bytes();
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
                            compaction_type,
                            ?needed_bytes,
                            memory_budget_bytes = compactor.config.memory_budget_bytes,
                            "skipped; over memory budget"
                        );
                        let mut repos = compactor.catalog.repositories().await;
                        let reason = format!(
                            "over memory budget. Needed budget = {}, memory budget = {}",
                            needed_bytes, compactor.config.memory_budget_bytes
                        );
                        let record_skip = repos
                            .partitions()
                            .record_skipped_compaction(partition_id, &reason)
                            .await;
                        if let Err(e) = record_skip {
                            warn!(?partition_id, %e, "could not log skipped compaction");
                        }
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
            && ((remaining_budget_bytes <= (compactor.config.memory_budget_bytes / 10) as u64)
                || (candidates.is_empty())
                || (count == num_remaining_candidates))
        {
            debug!(
                num_parallel_compacting_candidates = parallel_compacting_candidates.len(),
                total_needed_memory_budget_bytes =
                    compactor.config.memory_budget_bytes - remaining_budget_bytes,
                compaction_type,
                "parallel compacting candidate"
            );
            compact_function(
                Arc::clone(&compactor),
                parallel_compacting_candidates,
                compaction_type,
            )
            .await;

            // Reset to start adding new set of parallel candidates
            parallel_compacting_candidates = Vec::with_capacity(candidates.len());
            remaining_budget_bytes = compactor.config.memory_budget_bytes;
            num_remaining_candidates = candidates.len();
            count = 0;
        }
    }
}

// Compact given partitions in parallel.
//
// This function assumes its caller knows there are enough resources to run all partitions
// concurrently
async fn compact_in_parallel(
    compactor: Arc<Compactor>,
    partitions: Vec<FilteredFiles>,
    compaction_type: &'static str,
) {
    let mut handles = Vec::with_capacity(partitions.len());
    for p in partitions {
        let comp = Arc::clone(&compactor);
        let handle = tokio::task::spawn(async move {
            let partition_id = p.partition.candidate.partition_id;
            debug!(?partition_id, compaction_type, "compaction starting");
            let compaction_result = compact_one_partition(&comp, p, compaction_type).await;
            match compaction_result {
                Err(e) => {
                    warn!(?e, ?partition_id, compaction_type, "compaction failed");
                }
                Ok(_) => {
                    debug!(?partition_id, compaction_type, "compaction complete");
                }
            };
        });
        handles.push(handle);
    }

    let compactions_run = handles.len();
    debug!(
        ?compactions_run,
        compaction_type, "Number of concurrent partitions are being compacted"
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

/// One compaction operation of one partition.
///
/// Takes a hash-map `size_overrides` that mocks the size of the detected
/// [`CompactorParquetFile`]s. This will influence the size calculation of the compactor (i.e.
/// when/how to compact files), but leave the actual physical size of the file as it is (i.e. file
/// deserialization can still rely on the original value).
///
/// [`CompactorParquetFile`]: crate::parquet_file::CompactorParquetFile
pub(crate) async fn compact_one_partition(
    compactor: &Compactor,
    to_compact: FilteredFiles,
    compaction_type: &'static str,
) -> Result<(), CompactOnePartitionError> {
    let start_time = compactor.time_provider.now();

    let partition = to_compact.partition;
    let shard_id = partition.shard_id();

    if to_compact.files.len() == 1
        && to_compact.files[0].compaction_level() == CompactionLevel::Initial
    {
        // upgrade the one l0 file to l1, don't run compaction
        let mut repos = compactor.catalog.repositories().await;

        repos
            .parquet_files()
            .update_compaction_level(
                &[to_compact.files[0].id()],
                CompactionLevel::FileNonOverlapped,
            )
            .await
            .context(UpgradingSnafu)?;
    } else {
        parquet_file_combining::compact_parquet_files(
            to_compact.files,
            partition,
            Arc::clone(&compactor.catalog),
            compactor.store.clone(),
            Arc::clone(&compactor.exec),
            Arc::clone(&compactor.time_provider),
            &compactor.compaction_input_file_bytes,
            compactor.config.max_desired_file_size_bytes,
            compactor.config.percentage_max_file_size,
            compactor.config.split_percentage,
        )
        .await
        .map_err(|e| CompactOnePartitionError::Combining {
            source: Box::new(e),
        })?;
    }

    let attributes = Attributes::from([
        ("shard_id", format!("{}", shard_id).into()),
        ("partition_type", compaction_type.into()),
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
    use crate::{compact::Compactor, handler::CompactorConfig};
    use ::parquet_file::storage::ParquetStorage;
    use backoff::BackoffConfig;
    use data_types::ColumnType;
    use iox_query::exec::Executor;
    use iox_tests::util::{TestCatalog, TestShard, TestTable};
    use iox_time::SystemProvider;
    use std::{
        collections::VecDeque,
        pin::Pin,
        sync::{Arc, Mutex},
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

        compact_candidates_with_memory_budget(
            Arc::clone(&compactor),
            "hot",
            mock_compactor.compaction_function(),
            sorted_candidates,
        )
        .await;

        let compaction_groups = mock_compactor.results();
        assert!(compaction_groups.is_empty());
    }

    #[derive(Default)]
    pub(crate) struct MockCompactor {
        compaction_groups: Arc<Mutex<Vec<Vec<FilteredFiles>>>>,
    }

    type CompactorFunctionFactory = Box<
        dyn Fn(
                Arc<Compactor>,
                Vec<FilteredFiles>,
                &'static str,
            ) -> Pin<Box<dyn futures::Future<Output = ()> + Send>>
            + Send
            + Sync
            + 'static,
    >;

    impl MockCompactor {
        pub(crate) fn compaction_function(&self) -> CompactorFunctionFactory {
            let compaction_groups_for_closure = Arc::clone(&self.compaction_groups);
            Box::new(
                move |_compactor: Arc<Compactor>,
                      parallel_compacting_candidates: Vec<FilteredFiles>,
                      _compaction_type: &'static str| {
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

        pub(crate) fn results(self) -> Vec<Vec<FilteredFiles>> {
            let Self { compaction_groups } = self;
            Arc::try_unwrap(compaction_groups)
                .unwrap()
                .into_inner()
                .unwrap()
        }
    }

    fn make_compactor_config() -> CompactorConfig {
        CompactorConfig {
            max_desired_file_size_bytes: 100_000_000,
            percentage_max_file_size: 90,
            split_percentage: 100,
            max_number_partitions_per_shard: 100,
            min_number_recent_ingested_files_per_partition: 1,
            hot_multiple: 4,
            memory_budget_bytes: 12 * 1125, // 13,500 bytes
        }
    }

    pub(crate) struct TestSetup {
        pub(crate) compactor: Arc<Compactor>,
        pub(crate) mock_compactor: MockCompactor,
        pub(crate) shard: Arc<TestShard>,
        pub(crate) table: Arc<TestTable>,
    }

    pub(crate) async fn test_setup() -> TestSetup {
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
            shard,
            table,
        }
    }
}
