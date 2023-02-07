//! Current hardcoded component setup.
//!
//! TODO: Make this a runtime-config.

use std::{sync::Arc, time::Duration};

use data_types::CompactionLevel;
use object_store::memory::InMemory;

use crate::{
    components::{
        namespaces_source::catalog::CatalogNamespacesSource,
        tables_source::catalog::CatalogTablesSource,
    },
    config::{AlgoVersion, Config},
    error::ErrorKind,
};

use super::{
    combos::{throttle_partition::throttle_partition, unique_partitions::unique_partitions},
    commit::{
        catalog::CatalogCommit, logging::LoggingCommitWrapper, metrics::MetricsCommitWrapper,
        mock::MockCommit, Commit,
    },
    df_plan_exec::dedicated::DedicatedDataFusionPlanExec,
    df_planner::{logging::LoggingDataFusionPlannerWrapper, planner_v1::V1DataFusionPlanner},
    divide_initial::single_branch::SingleBranchDivideInitial,
    file_filter::{and::AndFileFilter, level_range::LevelRangeFileFilter},
    files_filter::{chain::FilesFilterChain, per_file::PerFileFilesFilter, FilesFilter},
    files_split::{
        all_at_once_non_overlap_split::AllAtOnceNonOverlapSplit,
        all_at_once_target_level_split::AllAtOnceTargetLevelSplit,
        all_at_once_upgrade_split::AllAtOnceUpgradeSplit,
        target_level_non_overlap_split::TargetLevelNonOverlapSplit,
        target_level_target_level_split::TargetLevelTargetLevelSplit,
        target_level_upgrade_split::TargetLevelUpgradeSplit, FilesSplit,
    },
    id_only_partition_filter::{
        and::AndIdOnlyPartitionFilter, by_id::ByIdPartitionFilter, shard::ShardPartitionFilter,
        IdOnlyPartitionFilter,
    },
    level_exist::one_level::OneLevelExist,
    parquet_file_sink::{
        dedicated::DedicatedExecParquetFileSinkWrapper, logging::LoggingParquetFileSinkWrapper,
        object_store::ObjectStoreParquetFileSink,
    },
    partition_done_sink::{
        catalog::CatalogPartitionDoneSink, error_kind::ErrorKindPartitionDoneSinkWrapper,
        logging::LoggingPartitionDoneSinkWrapper, metrics::MetricsPartitionDoneSinkWrapper,
        mock::MockPartitionDoneSink, PartitionDoneSink,
    },
    partition_files_source::catalog::CatalogPartitionFilesSource,
    partition_filter::{
        and::AndPartitionFilter, greater_matching_files::GreaterMatchingFilesPartitionFilter,
        has_files::HasFilesPartitionFilter, has_matching_file::HasMatchingFilePartitionFilter,
        logging::LoggingPartitionFilterWrapper, max_files::MaxFilesPartitionFilter,
        max_parquet_bytes::MaxParquetBytesPartitionFilter, metrics::MetricsPartitionFilterWrapper,
        never_skipped::NeverSkippedPartitionFilter, or::OrPartitionFilter, PartitionFilter,
    },
    partition_source::{
        catalog::CatalogPartitionSource, logging::LoggingPartitionSourceWrapper,
        metrics::MetricsPartitionSourceWrapper,
    },
    partition_stream::{
        endless::EndlessPartititionStream, once::OncePartititionStream, PartitionStream,
    },
    partitions_source::{
        catalog::CatalogPartitionsSource, filter::FilterPartitionsSourceWrapper,
        logging::LoggingPartitionsSourceWrapper, metrics::MetricsPartitionsSourceWrapper,
        mock::MockPartitionsSource, not_empty::NotEmptyPartitionsSourceWrapper,
        randomize_order::RandomizeOrderPartitionsSourcesWrapper, PartitionsSource,
    },
    round_split::all_now::AllNowRoundSplit,
    scratchpad::{ignore_writes_object_store::IgnoreWrites, prod::ProdScratchpadGen},
    skipped_compactions_source::catalog::CatalogSkippedCompactionsSource,
    target_level_chooser::{
        all_at_once::AllAtOnceTargetLevelChooser, target_level::TargetLevelTargetLevelChooser,
        TargetLevelChooser,
    },
    Components,
};

/// Get hardcoded components.
pub fn hardcoded_components(config: &Config) -> Arc<Components> {
    // TODO: partitions source: Implementing ID-based sharding / hash-partitioning so we can run multiple compactors in
    //       parallel. This should be a wrapper around the existing partions source.

    let partitions_source: Arc<dyn PartitionsSource> = if let Some(ids) = &config.partition_filter {
        Arc::new(MockPartitionsSource::new(ids.iter().cloned().collect()))
    } else {
        Arc::new(CatalogPartitionsSource::new(
            config.backoff_config.clone(),
            Arc::clone(&config.catalog),
            config.partition_threshold,
            Arc::clone(&config.time_provider),
        ))
    };

    let mut id_only_partition_filters: Vec<Arc<dyn IdOnlyPartitionFilter>> = vec![];
    if let Some(ids) = &config.partition_filter {
        // filter as early as possible, so we don't need any catalog lookups for the filtered partitions
        id_only_partition_filters.push(Arc::new(ByIdPartitionFilter::new(ids.clone())));
    }
    if let Some(shard_config) = &config.shard_config {
        // add shard filter before performing any catalog IO
        id_only_partition_filters.push(Arc::new(ShardPartitionFilter::new(
            shard_config.n_shards,
            shard_config.shard_id,
        )));
    }
    let partitions_source = FilterPartitionsSourceWrapper::new(
        AndIdOnlyPartitionFilter::new(id_only_partition_filters),
        partitions_source,
    );

    let mut partition_filters: Vec<Arc<dyn PartitionFilter>> = vec![];
    partition_filters.push(Arc::new(HasFilesPartitionFilter::new()));
    if !config.ignore_partition_skip_marker {
        partition_filters.push(Arc::new(NeverSkippedPartitionFilter::new(
            CatalogSkippedCompactionsSource::new(
                config.backoff_config.clone(),
                Arc::clone(&config.catalog),
            ),
        )));
    }
    partition_filters.append(&mut version_specific_partition_filters(config));

    let partition_resource_limit_filters: Vec<Arc<dyn PartitionFilter>> = vec![
        Arc::new(MaxFilesPartitionFilter::new(
            config.max_input_files_per_partition,
        )),
        Arc::new(MaxParquetBytesPartitionFilter::new(
            config.max_input_parquet_bytes_per_partition,
        )),
    ];

    let partition_done_sink: Arc<dyn PartitionDoneSink> = if config.shadow_mode {
        Arc::new(MockPartitionDoneSink::new())
    } else {
        Arc::new(CatalogPartitionDoneSink::new(
            config.backoff_config.clone(),
            Arc::clone(&config.catalog),
        ))
    };

    let commit: Arc<dyn Commit> = if config.shadow_mode {
        Arc::new(MockCommit::new())
    } else {
        Arc::new(CatalogCommit::new(
            config.backoff_config.clone(),
            Arc::clone(&config.catalog),
        ))
    };

    let scratchpad_store_output = if config.shadow_mode {
        Arc::new(IgnoreWrites::new(Arc::new(InMemory::new())))
    } else {
        Arc::clone(config.parquet_store_real.object_store())
    };

    let (partitions_source, partition_done_sink) =
        unique_partitions(partitions_source, partition_done_sink, 1);
    let (partitions_source, commit, partition_done_sink) = throttle_partition(
        partitions_source,
        commit,
        partition_done_sink,
        Arc::clone(&config.time_provider),
        Duration::from_secs(60),
        1,
    );

    // Note: Place "not empty" wrapper at the very last so that the logging and metric wrapper work even when there
    //       is not data.
    let partitions_source =
        LoggingPartitionsSourceWrapper::new(MetricsPartitionsSourceWrapper::new(
            RandomizeOrderPartitionsSourcesWrapper::new(partitions_source, 1234),
            &config.metric_registry,
        ));
    let partitions_source: Arc<dyn PartitionsSource> = if config.process_once {
        // do not wrap into the "not empty" filter because we do NOT wanna throttle in this case but just exit early
        Arc::new(partitions_source)
    } else {
        Arc::new(NotEmptyPartitionsSourceWrapper::new(
            partitions_source,
            Duration::from_secs(5),
            Arc::clone(&config.time_provider),
        ))
    };

    let partition_stream: Arc<dyn PartitionStream> = if config.process_once {
        Arc::new(OncePartititionStream::new(partitions_source))
    } else {
        Arc::new(EndlessPartititionStream::new(partitions_source))
    };

    Arc::new(Components {
        partition_stream,
        partition_source: Arc::new(LoggingPartitionSourceWrapper::new(
            MetricsPartitionSourceWrapper::new(
                CatalogPartitionSource::new(
                    config.backoff_config.clone(),
                    Arc::clone(&config.catalog),
                ),
                &config.metric_registry,
            ),
        )),
        partition_files_source: Arc::new(CatalogPartitionFilesSource::new(
            config.backoff_config.clone(),
            Arc::clone(&config.catalog),
        )),
        files_filter: version_specific_files_filter(config),
        partition_filter: Arc::new(LoggingPartitionFilterWrapper::new(
            MetricsPartitionFilterWrapper::new(
                AndPartitionFilter::new(partition_filters),
                &config.metric_registry,
            ),
        )),
        partition_done_sink: Arc::new(LoggingPartitionDoneSinkWrapper::new(
            MetricsPartitionDoneSinkWrapper::new(
                ErrorKindPartitionDoneSinkWrapper::new(
                    partition_done_sink,
                    ErrorKind::variants()
                        .iter()
                        .filter(|kind| {
                            // use explicit match statement so we never forget to add new variants
                            match kind {
                                ErrorKind::OutOfMemory
                                | ErrorKind::Timeout
                                | ErrorKind::Unknown => true,
                                ErrorKind::ObjectStore => false,
                            }
                        })
                        .copied()
                        .collect(),
                ),
                &config.metric_registry,
            ),
        )),
        commit: Arc::new(LoggingCommitWrapper::new(MetricsCommitWrapper::new(
            commit,
            &config.metric_registry,
        ))),
        namespaces_source: Arc::new(CatalogNamespacesSource::new(
            config.backoff_config.clone(),
            Arc::clone(&config.catalog),
        )),
        tables_source: Arc::new(CatalogTablesSource::new(
            config.backoff_config.clone(),
            Arc::clone(&config.catalog),
        )),
        df_planner: Arc::new(LoggingDataFusionPlannerWrapper::new(
            V1DataFusionPlanner::new(
                config.parquet_store_scratchpad.clone(),
                Arc::clone(&config.exec),
                config.max_desired_file_size_bytes,
                config.percentage_max_file_size,
                config.split_percentage,
            ),
        )),
        df_plan_exec: Arc::new(DedicatedDataFusionPlanExec::new(Arc::clone(&config.exec))),
        parquet_file_sink: Arc::new(LoggingParquetFileSinkWrapper::new(
            DedicatedExecParquetFileSinkWrapper::new(
                ObjectStoreParquetFileSink::new(
                    config.shard_id,
                    config.parquet_store_scratchpad.clone(),
                    Arc::clone(&config.time_provider),
                ),
                Arc::clone(&config.exec),
            ),
        )),
        round_split: Arc::new(AllNowRoundSplit::new()),
        divide_initial: Arc::new(SingleBranchDivideInitial::new()),
        scratchpad_gen: Arc::new(ProdScratchpadGen::new(
            config.partition_scratchpad_concurrency,
            config.backoff_config.clone(),
            Arc::clone(config.parquet_store_real.object_store()),
            Arc::clone(config.parquet_store_scratchpad.object_store()),
            scratchpad_store_output,
        )),
        target_level_chooser: version_specific_target_level_chooser(config),
        target_level_split: version_specific_target_level_split(config),
        non_overlap_split: version_specific_non_ovverlapping_split(config),
        upgrade_split: version_specific_upgrade_split(config),
        partition_resource_limit_filter: Arc::new(LoggingPartitionFilterWrapper::new(
            MetricsPartitionFilterWrapper::new(
                AndPartitionFilter::new(partition_resource_limit_filters),
                &config.metric_registry,
            ),
        )),
    })
}

// Conditions to commpact this partittion
fn version_specific_partition_filters(config: &Config) -> Vec<Arc<dyn PartitionFilter>> {
    match config.compact_version {
        // Must has L0
        AlgoVersion::AllAtOnce => {
            vec![Arc::new(HasMatchingFilePartitionFilter::new(
                LevelRangeFileFilter::new(CompactionLevel::Initial..=CompactionLevel::Initial),
            ))]
        }
        // (Has-L0) OR (num(L1) > N)
        AlgoVersion::TargetLevel => {
            vec![Arc::new(OrPartitionFilter::new(vec![
                Arc::new(HasMatchingFilePartitionFilter::new(
                    LevelRangeFileFilter::new(CompactionLevel::Initial..=CompactionLevel::Initial),
                )),
                Arc::new(GreaterMatchingFilesPartitionFilter::new(
                    LevelRangeFileFilter::new(
                        CompactionLevel::FileNonOverlapped..=CompactionLevel::FileNonOverlapped,
                    ),
                    config.min_num_l1_files_to_compact,
                )),
            ]))]
        }
    }
}

// Choose the terget level to compact to
fn version_specific_target_level_chooser(config: &Config) -> Arc<dyn TargetLevelChooser> {
    match config.compact_version {
        AlgoVersion::AllAtOnce => Arc::new(AllAtOnceTargetLevelChooser::new()),
        AlgoVersion::TargetLevel => {
            Arc::new(TargetLevelTargetLevelChooser::new(OneLevelExist::new()))
        }
    }
}

// Split the files into `[<=target level]` and `[>target level]`
fn version_specific_target_level_split(config: &Config) -> Arc<dyn FilesSplit> {
    match config.compact_version {
        AlgoVersion::AllAtOnce => Arc::new(AllAtOnceTargetLevelSplit::new()),
        AlgoVersion::TargetLevel => Arc::new(TargetLevelTargetLevelSplit::new()),
    }
}

// Split the files into `[overlapping]` and `[non_overlapping]`
fn version_specific_non_ovverlapping_split(config: &Config) -> Arc<dyn FilesSplit> {
    match config.compact_version {
        AlgoVersion::AllAtOnce => Arc::new(AllAtOnceNonOverlapSplit::new()),
        AlgoVersion::TargetLevel => Arc::new(TargetLevelNonOverlapSplit::new()),
    }
}

// Split the files into `[files_to_compact]` and `[files_to_upgrade]`
fn version_specific_upgrade_split(config: &Config) -> Arc<dyn FilesSplit> {
    match config.compact_version {
        AlgoVersion::AllAtOnce => Arc::new(AllAtOnceUpgradeSplit::new()),
        AlgoVersion::TargetLevel => Arc::new(TargetLevelUpgradeSplit::new(
            config.max_desired_file_size_bytes,
        )),
    }
}

// filter files of a partition we do not compact
fn version_specific_files_filter(config: &Config) -> Arc<dyn FilesFilter> {
    match config.compact_version {
        // AllAtOnce filters out L2 files
        AlgoVersion::AllAtOnce => Arc::new(FilesFilterChain::new(vec![Arc::new(
            PerFileFilesFilter::new(AndFileFilter::new(vec![Arc::new(
                LevelRangeFileFilter::new(
                    CompactionLevel::Initial..=CompactionLevel::FileNonOverlapped,
                ),
            )])),
        )])),
        // TargetLevel does not filter any files of the partition
        AlgoVersion::TargetLevel => Arc::new(FilesFilterChain::new(vec![])),
    }
}
