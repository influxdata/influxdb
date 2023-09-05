//! Current hardcoded component setup.
//!
//! TODO: Make this a runtime-config.

use std::{sync::Arc, time::Duration};

use compactor_scheduler::{create_scheduler, Scheduler};
use data_types::CompactionLevel;
use object_store::memory::InMemory;

use crate::{config::Config, error::ErrorKind, object_store::ignore_writes::IgnoreWrites};

use super::{
    changed_files_filter::logging::LoggingChangedFiles,
    commit::CommitToScheduler,
    compaction_job_done_sink::{
        error_kind::ErrorKindCompactionJobDoneSinkWrapper,
        logging::LoggingCompactionJobDoneSinkWrapper, metrics::MetricsCompactionJobDoneSinkWrapper,
        outcome::CompactionJobDoneSinkToScheduler, CompactionJobDoneSink,
    },
    compaction_job_stream::{
        endless::EndlessCompactionJobStream, once::OnceCompactionJobStream, CompactionJobStream,
    },
    compaction_jobs_source::{
        logging::LoggingCompactionJobsWrapper, metrics::MetricsCompactionJobsSourceWrapper,
        not_empty::NotEmptyCompactionJobsSourceWrapper,
        randomize_order::RandomizeOrderCompactionJobsSourcesWrapper,
        scheduled::ScheduledCompactionJobsSource, CompactionJobsSource,
    },
    df_plan_exec::{
        dedicated::DedicatedDataFusionPlanExec, noop::NoopDataFusionPlanExec, DataFusionPlanExec,
    },
    df_planner::{planner_v1::V1DataFusionPlanner, DataFusionPlanner},
    divide_initial::multiple_branches::MultipleBranchesDivideInitial,
    file_classifier::{
        logging::LoggingFileClassifierWrapper, split_based::SplitBasedFileClassifier,
        FileClassifier,
    },
    file_filter::level_range::LevelRangeFileFilter,
    files_split::{
        non_overlap_split::NonOverlapSplit, target_level_split::TargetLevelSplit,
        upgrade_split::UpgradeSplit,
    },
    ir_planner::{logging::LoggingIRPlannerWrapper, planner_v1::V1IRPlanner, IRPlanner},
    namespaces_source::catalog::CatalogNamespacesSource,
    parquet_file_sink::{
        dedicated::DedicatedExecParquetFileSinkWrapper, logging::LoggingParquetFileSinkWrapper,
        object_store::ObjectStoreParquetFileSink,
    },
    parquet_files_sink::{dispatch::DispatchParquetFilesSink, ParquetFilesSink},
    partition_files_source::{
        catalog::{CatalogPartitionFilesSource, QueryRateLimiter},
        rate_limit::RateLimit,
        PartitionFilesSource,
    },
    partition_filter::{
        and::AndPartitionFilter, greater_matching_files::GreaterMatchingFilesPartitionFilter,
        greater_size_matching_files::GreaterSizeMatchingFilesPartitionFilter,
        has_files::HasFilesPartitionFilter, has_matching_file::HasMatchingFilePartitionFilter,
        logging::LoggingPartitionFilterWrapper, max_num_columns::MaxNumColumnsPartitionFilter,
        metrics::MetricsPartitionFilterWrapper, or::OrPartitionFilter, PartitionFilter,
    },
    partition_info_source::{sub_sources::SubSourcePartitionInfoSource, PartitionInfoSource},
    partition_source::{
        catalog::CatalogPartitionSource, logging::LoggingPartitionSourceWrapper,
        metrics::MetricsPartitionSourceWrapper,
    },
    post_classification_partition_filter::{
        logging::LoggingPostClassificationFilterWrapper,
        metrics::MetricsPostClassificationFilterWrapper, possible_progress::PossibleProgressFilter,
        PostClassificationPartitionFilter,
    },
    round_info_source::{LevelBasedRoundInfo, LoggingRoundInfoWrapper, RoundInfoSource},
    round_split::many_files::ManyFilesRoundSplit,
    scratchpad::{noop::NoopScratchpadGen, prod::ProdScratchpadGen, ScratchpadGen},
    split_or_compact::{
        logging::LoggingSplitOrCompactWrapper, metrics::MetricsSplitOrCompactWrapper,
        split_compact::SplitCompact,
    },
    tables_source::catalog::CatalogTablesSource,
    Components,
};

/// Get hardcoded components.
pub fn hardcoded_components(config: &Config) -> Arc<Components> {
    let scheduler = create_scheduler(
        config.scheduler_config.clone(),
        Arc::clone(&config.catalog),
        Arc::clone(&config.time_provider),
        Arc::clone(&config.metric_registry),
        config.shadow_mode,
    );
    let (compaction_jobs_source, commit, compaction_job_done_sink) =
        make_jobs_source_commit_jobs_sink(config, Arc::clone(&scheduler));

    Arc::new(Components {
        compaction_job_stream: make_compaction_job_stream(config, compaction_jobs_source),
        partition_info_source: make_partition_info_source(config),
        partition_files_source: make_partition_files_source(config),
        round_info_source: make_round_info_source(config),
        partition_filter: make_partition_filter(config),
        compaction_job_done_sink,
        commit,
        ir_planner: make_ir_planner(config),
        df_planner: make_df_planner(config),
        df_plan_exec: make_df_plan_exec(config),
        parquet_files_sink: make_parquet_files_sink(config),
        round_split: Arc::new(ManyFilesRoundSplit::new()),
        divide_initial: Arc::new(MultipleBranchesDivideInitial::new()),
        scratchpad_gen: make_scratchpad_gen(config),
        file_classifier: make_file_classifier(config),
        post_classification_partition_filter: make_post_classification_partition_filter(config),
        changed_files_filter: Arc::new(LoggingChangedFiles::new()),
    })
}

fn make_jobs_source_commit_jobs_sink(
    config: &Config,
    scheduler: Arc<dyn Scheduler>,
) -> (
    Arc<dyn CompactionJobsSource>,
    Arc<CommitToScheduler>,
    Arc<dyn CompactionJobDoneSink>,
) {
    let compaction_jobs_source = ScheduledCompactionJobsSource::new(Arc::clone(&scheduler));

    let commit = CommitToScheduler::new(Arc::clone(&scheduler));

    let compaction_job_done_sink = CompactionJobDoneSinkToScheduler::new(Arc::clone(&scheduler));

    // compactors are responsible for error classification
    // and any future decisions regarding graceful shutdown
    let compaction_job_done_sink: Arc<dyn CompactionJobDoneSink> = if config.all_errors_are_fatal {
        Arc::new(compaction_job_done_sink)
    } else {
        Arc::new(ErrorKindCompactionJobDoneSinkWrapper::new(
            compaction_job_done_sink,
            ErrorKind::variants()
                .iter()
                .filter(|kind| {
                    // use explicit match statement so we never forget to add new variants
                    match kind {
                        ErrorKind::OutOfMemory | ErrorKind::Timeout | ErrorKind::Unknown => true,
                        ErrorKind::ObjectStore => false,
                    }
                })
                .copied()
                .collect(),
            scheduler,
        ))
    };
    let compaction_job_done_sink = Arc::new(LoggingCompactionJobDoneSinkWrapper::new(
        MetricsCompactionJobDoneSinkWrapper::new(compaction_job_done_sink, &config.metric_registry),
    ));

    // Note: Place "not empty" wrapper at the very last so that the logging and metric wrapper work
    // even when there is not data.
    let compaction_jobs_source =
        LoggingCompactionJobsWrapper::new(MetricsCompactionJobsSourceWrapper::new(
            RandomizeOrderCompactionJobsSourcesWrapper::new(compaction_jobs_source, 1234),
            &config.metric_registry,
        ));
    let compaction_jobs_source: Arc<dyn CompactionJobsSource> = if config.process_once {
        // do not wrap into the "not empty" filter because we do NOT wanna throttle in this case
        // but just exit early
        Arc::new(compaction_jobs_source)
    } else {
        Arc::new(NotEmptyCompactionJobsSourceWrapper::new(
            compaction_jobs_source,
            Duration::from_secs(5),
            Arc::clone(&config.time_provider),
        ))
    };

    (
        compaction_jobs_source,
        Arc::new(commit),
        compaction_job_done_sink,
    )
}

fn make_compaction_job_stream(
    config: &Config,
    compaction_jobs_source: Arc<dyn CompactionJobsSource>,
) -> Arc<dyn CompactionJobStream> {
    if config.process_once {
        Arc::new(OnceCompactionJobStream::new(compaction_jobs_source))
    } else {
        Arc::new(EndlessCompactionJobStream::new(compaction_jobs_source))
    }
}

fn make_partition_info_source(config: &Config) -> Arc<dyn PartitionInfoSource> {
    Arc::new(SubSourcePartitionInfoSource::new(
        LoggingPartitionSourceWrapper::new(MetricsPartitionSourceWrapper::new(
            CatalogPartitionSource::new(config.backoff_config.clone(), Arc::clone(&config.catalog)),
            &config.metric_registry,
        )),
        CatalogTablesSource::new(config.backoff_config.clone(), Arc::clone(&config.catalog)),
        CatalogNamespacesSource::new(config.backoff_config.clone(), Arc::clone(&config.catalog)),
    ))
}

fn make_partition_files_source(config: &Config) -> Arc<dyn PartitionFilesSource> {
    match config.max_partition_fetch_queries_per_second {
        Some(rps) => Arc::new(CatalogPartitionFilesSource::new(
            config.backoff_config.clone(),
            QueryRateLimiter::new(Arc::clone(&config.catalog), RateLimit::new(rps, 25)),
        )),
        None => Arc::new(CatalogPartitionFilesSource::new(
            config.backoff_config.clone(),
            Arc::clone(&config.catalog),
        )),
    }
}

fn make_round_info_source(config: &Config) -> Arc<dyn RoundInfoSource> {
    Arc::new(LoggingRoundInfoWrapper::new(Arc::new(
        LevelBasedRoundInfo::new(
            config.max_num_files_per_plan,
            config.max_compact_size_bytes(),
        ),
    )))
}

// Conditions to compact this partition
fn make_partition_filter(config: &Config) -> Arc<dyn PartitionFilter> {
    let mut partition_filters = exceptional_cases_partition_filters(config);

    partition_filters.push(continue_condition_filter(config));

    let partition_continue_conditions = "continue_conditions";
    Arc::new(LoggingPartitionFilterWrapper::new(
        MetricsPartitionFilterWrapper::new(
            AndPartitionFilter::new(partition_filters),
            &config.metric_registry,
            partition_continue_conditions,
        ),
        partition_continue_conditions,
    ))
}

fn exceptional_cases_partition_filters(config: &Config) -> Vec<Arc<dyn PartitionFilter>> {
    // Capacity is hardcoded to a somewhat arbitrary number to prevent some reallocations
    let mut partition_filters: Vec<Arc<dyn PartitionFilter>> = Vec::with_capacity(8);
    partition_filters.push(Arc::new(HasFilesPartitionFilter::new()));

    partition_filters.push(Arc::new(MaxNumColumnsPartitionFilter::new(
        config.max_num_columns_per_table,
    )));

    partition_filters
}

fn continue_condition_filter(config: &Config) -> Arc<dyn PartitionFilter> {
    // (Has-L0) OR            -- to avoid overlapped files
    // (num(L1) > N) OR       -- to avoid many files
    // (total_size(L1) > max_desired_file_size)  -- to avoid compact and than split
    Arc::new(OrPartitionFilter::new(vec![
        Arc::new(HasMatchingFilePartitionFilter::new(
            LevelRangeFileFilter::new(CompactionLevel::Initial..=CompactionLevel::Initial),
        )),
        Arc::new(GreaterMatchingFilesPartitionFilter::new(
            LevelRangeFileFilter::new(
                CompactionLevel::FileNonOverlapped..=CompactionLevel::FileNonOverlapped,
            ),
            config.min_num_l1_files_to_compact,
        )),
        Arc::new(GreaterSizeMatchingFilesPartitionFilter::new(
            LevelRangeFileFilter::new(
                CompactionLevel::FileNonOverlapped..=CompactionLevel::FileNonOverlapped,
            ),
            config.max_desired_file_size_bytes,
        )),
    ]))
}

fn make_ir_planner(config: &Config) -> Arc<dyn IRPlanner> {
    Arc::new(LoggingIRPlannerWrapper::new(V1IRPlanner::new(
        config.max_desired_file_size_bytes,
        config.percentage_max_file_size,
        config.split_percentage,
    )))
}

fn make_df_planner(config: &Config) -> Arc<dyn DataFusionPlanner> {
    Arc::new(V1DataFusionPlanner::new(
        config.parquet_store_scratchpad.clone(),
        Arc::clone(&config.exec),
    ))
}

fn make_df_plan_exec(config: &Config) -> Arc<dyn DataFusionPlanExec> {
    if config.simulate_without_object_store {
        Arc::new(NoopDataFusionPlanExec::new())
    } else {
        Arc::new(DedicatedDataFusionPlanExec::new(Arc::clone(&config.exec)))
    }
}

fn make_parquet_files_sink(config: &Config) -> Arc<dyn ParquetFilesSink> {
    if let Some(sink) = config.parquet_files_sink_override.as_ref() {
        Arc::clone(sink)
    } else {
        let parquet_file_sink = Arc::new(LoggingParquetFileSinkWrapper::new(
            DedicatedExecParquetFileSinkWrapper::new(
                ObjectStoreParquetFileSink::new(
                    config.exec.pool(),
                    config.parquet_store_scratchpad.clone(),
                    Arc::clone(&config.time_provider),
                ),
                Arc::clone(&config.exec),
            ),
        ));
        Arc::new(DispatchParquetFilesSink::new(parquet_file_sink))
    }
}

fn make_scratchpad_gen(config: &Config) -> Arc<dyn ScratchpadGen> {
    if config.simulate_without_object_store || !config.enable_scratchpad {
        Arc::new(NoopScratchpadGen::new())
    } else {
        let scratchpad_store_output = if config.shadow_mode {
            Arc::new(IgnoreWrites::new(Arc::new(InMemory::new())))
        } else {
            Arc::clone(config.parquet_store_real.object_store())
        };

        Arc::new(ProdScratchpadGen::new(
            config.shadow_mode,
            config.partition_scratchpad_concurrency,
            config.backoff_config.clone(),
            Arc::clone(config.parquet_store_real.object_store()),
            Arc::clone(config.parquet_store_scratchpad.object_store()),
            scratchpad_store_output,
        ))
    }
}

fn make_file_classifier(config: &Config) -> Arc<dyn FileClassifier> {
    Arc::new(LoggingFileClassifierWrapper::new(Arc::new(
        SplitBasedFileClassifier::new(
            TargetLevelSplit::new(),
            NonOverlapSplit::new(config.max_desired_file_size_bytes / 20), // rewrite non-overlapping files up to 5% of max
            UpgradeSplit::new(config.max_desired_file_size_bytes),
            LoggingSplitOrCompactWrapper::new(MetricsSplitOrCompactWrapper::new(
                SplitCompact::new(
                    config.max_num_files_per_plan,
                    config.max_compact_size_bytes(),
                    config.max_desired_file_size_bytes,
                ),
                &config.metric_registry,
            )),
        ),
    )))
}

fn make_post_classification_partition_filter(
    config: &Config,
) -> Arc<dyn PostClassificationPartitionFilter> {
    let partition_resource_limit_conditions = "resource_limit_conditions";

    Arc::new(LoggingPostClassificationFilterWrapper::new(
        MetricsPostClassificationFilterWrapper::new(
            PossibleProgressFilter::new(config.max_compact_size_bytes()),
            &config.metric_registry,
            partition_resource_limit_conditions,
        ),
        partition_resource_limit_conditions,
    ))
}
