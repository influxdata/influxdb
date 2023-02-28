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
    config::{Config, PartitionsSourceConfig},
    error::ErrorKind,
    object_store::ignore_writes::IgnoreWrites,
};

use super::{
    combos::{throttle_partition::throttle_partition, unique_partitions::unique_partitions},
    commit::{
        catalog::CatalogCommit, logging::LoggingCommitWrapper, metrics::MetricsCommitWrapper,
        mock::MockCommit, Commit,
    },
    df_plan_exec::{
        dedicated::DedicatedDataFusionPlanExec, noop::NoopDataFusionPlanExec, DataFusionPlanExec,
    },
    df_planner::planner_v1::V1DataFusionPlanner,
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
    id_only_partition_filter::{
        and::AndIdOnlyPartitionFilter, shard::ShardPartitionFilter, IdOnlyPartitionFilter,
    },
    ir_planner::{logging::LoggingIRPlannerWrapper, planner_v1::V1IRPlanner},
    parquet_file_sink::{
        dedicated::DedicatedExecParquetFileSinkWrapper, logging::LoggingParquetFileSinkWrapper,
        object_store::ObjectStoreParquetFileSink,
    },
    parquet_files_sink::{dispatch::DispatchParquetFilesSink, ParquetFilesSink},
    partition_done_sink::{
        catalog::CatalogPartitionDoneSink, error_kind::ErrorKindPartitionDoneSinkWrapper,
        logging::LoggingPartitionDoneSinkWrapper, metrics::MetricsPartitionDoneSinkWrapper,
        mock::MockPartitionDoneSink, PartitionDoneSink,
    },
    partition_files_source::catalog::CatalogPartitionFilesSource,
    partition_filter::{
        and::AndPartitionFilter, greater_matching_files::GreaterMatchingFilesPartitionFilter,
        greater_size_matching_files::GreaterSizeMatchingFilesPartitionFilter,
        has_files::HasFilesPartitionFilter, has_matching_file::HasMatchingFilePartitionFilter,
        logging::LoggingPartitionFilterWrapper, max_num_columns::MaxNumColumnsPartitionFilter,
        max_parquet_bytes::MaxParquetBytesPartitionFilter, metrics::MetricsPartitionFilterWrapper,
        never_skipped::NeverSkippedPartitionFilter, or::OrPartitionFilter, PartitionFilter,
    },
    partition_info_source::sub_sources::SubSourcePartitionInfoSource,
    partition_source::{
        catalog::CatalogPartitionSource, logging::LoggingPartitionSourceWrapper,
        metrics::MetricsPartitionSourceWrapper,
    },
    partition_stream::{
        endless::EndlessPartititionStream, once::OncePartititionStream, PartitionStream,
    },
    partitions_source::{
        catalog_all::CatalogAllPartitionsSource,
        catalog_to_compact::CatalogToCompactPartitionsSource,
        filter::FilterPartitionsSourceWrapper, logging::LoggingPartitionsSourceWrapper,
        metrics::MetricsPartitionsSourceWrapper, mock::MockPartitionsSource,
        not_empty::NotEmptyPartitionsSourceWrapper,
        randomize_order::RandomizeOrderPartitionsSourcesWrapper, PartitionsSource,
    },
    round_info_source::{LevelBasedRoundInfo, LoggingRoundInfoWrapper},
    round_split::many_files::ManyFilesRoundSplit,
    scratchpad::{noop::NoopScratchpadGen, prod::ProdScratchpadGen, ScratchpadGen},
    skipped_compactions_source::catalog::CatalogSkippedCompactionsSource,
    Components,
};

/// Get hardcoded components.
pub fn hardcoded_components(config: &Config) -> Arc<Components> {
    // TODO: partitions source: Implementing ID-based sharding / hash-partitioning so we can run multiple compactors in
    //       parallel. This should be a wrapper around the existing partions source.

    let partitions_source: Arc<dyn PartitionsSource> = match &config.partitions_source {
        PartitionsSourceConfig::CatalogRecentWrites => {
            Arc::new(CatalogToCompactPartitionsSource::new(
                config.backoff_config.clone(),
                Arc::clone(&config.catalog),
                config.partition_threshold,
                Arc::clone(&config.time_provider),
            ))
        }
        PartitionsSourceConfig::CatalogAll => Arc::new(CatalogAllPartitionsSource::new(
            config.backoff_config.clone(),
            Arc::clone(&config.catalog),
        )),
        PartitionsSourceConfig::Fixed(ids) => {
            Arc::new(MockPartitionsSource::new(ids.iter().cloned().collect()))
        }
    };

    let mut id_only_partition_filters: Vec<Arc<dyn IdOnlyPartitionFilter>> = vec![];
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
    partition_filters.push(Arc::new(MaxNumColumnsPartitionFilter::new(
        config.max_num_columns_per_table,
    )));
    partition_filters.append(&mut make_partition_filters(config));

    let partition_resource_limit_filters: Vec<Arc<dyn PartitionFilter>> = vec![Arc::new(
        MaxParquetBytesPartitionFilter::new(config.max_input_parquet_bytes_per_partition),
    )];

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

    let commit = if let Some(commit_wrapper) = config.commit_wrapper.as_ref() {
        commit_wrapper.wrap(commit)
    } else {
        commit
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
    let partition_done_sink: Arc<dyn PartitionDoneSink> = if config.all_errors_are_fatal {
        Arc::new(partition_done_sink)
    } else {
        Arc::new(ErrorKindPartitionDoneSinkWrapper::new(
            partition_done_sink,
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
        ))
    };

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
    let partition_continue_conditions = "continue_conditions";
    let partition_resource_limit_conditions = "resource_limit_conditions";

    let scratchpad_gen: Arc<dyn ScratchpadGen> = if config.simulate_without_object_store {
        Arc::new(NoopScratchpadGen::new())
    } else {
        Arc::new(ProdScratchpadGen::new(
            config.partition_scratchpad_concurrency,
            config.backoff_config.clone(),
            Arc::clone(config.parquet_store_real.object_store()),
            Arc::clone(config.parquet_store_scratchpad.object_store()),
            scratchpad_store_output,
        ))
    };
    let df_plan_exec: Arc<dyn DataFusionPlanExec> = if config.simulate_without_object_store {
        Arc::new(NoopDataFusionPlanExec::new())
    } else {
        Arc::new(DedicatedDataFusionPlanExec::new(Arc::clone(&config.exec)))
    };
    let parquet_files_sink: Arc<dyn ParquetFilesSink> =
        if let Some(sink) = config.parquet_files_sink_override.as_ref() {
            Arc::clone(sink)
        } else {
            let parquet_file_sink = Arc::new(LoggingParquetFileSinkWrapper::new(
                DedicatedExecParquetFileSinkWrapper::new(
                    ObjectStoreParquetFileSink::new(
                        config.shard_id,
                        config.parquet_store_scratchpad.clone(),
                        Arc::clone(&config.time_provider),
                    ),
                    Arc::clone(&config.exec),
                ),
            ));
            Arc::new(DispatchParquetFilesSink::new(parquet_file_sink))
        };

    Arc::new(Components {
        partition_stream,
        partition_info_source: Arc::new(SubSourcePartitionInfoSource::new(
            LoggingPartitionSourceWrapper::new(MetricsPartitionSourceWrapper::new(
                CatalogPartitionSource::new(
                    config.backoff_config.clone(),
                    Arc::clone(&config.catalog),
                ),
                &config.metric_registry,
            )),
            CatalogTablesSource::new(config.backoff_config.clone(), Arc::clone(&config.catalog)),
            CatalogNamespacesSource::new(
                config.backoff_config.clone(),
                Arc::clone(&config.catalog),
            ),
        )),
        partition_files_source: Arc::new(CatalogPartitionFilesSource::new(
            config.backoff_config.clone(),
            Arc::clone(&config.catalog),
        )),
        round_info_source: Arc::new(LoggingRoundInfoWrapper::new(Arc::new(
            LevelBasedRoundInfo::new(config.max_num_files_per_plan),
        ))),
        partition_filter: Arc::new(LoggingPartitionFilterWrapper::new(
            MetricsPartitionFilterWrapper::new(
                AndPartitionFilter::new(partition_filters),
                &config.metric_registry,
                partition_continue_conditions,
            ),
            partition_continue_conditions,
        )),
        partition_done_sink: Arc::new(LoggingPartitionDoneSinkWrapper::new(
            MetricsPartitionDoneSinkWrapper::new(partition_done_sink, &config.metric_registry),
        )),
        commit: Arc::new(LoggingCommitWrapper::new(MetricsCommitWrapper::new(
            commit,
            &config.metric_registry,
        ))),
        ir_planner: Arc::new(LoggingIRPlannerWrapper::new(V1IRPlanner::new(
            config.max_desired_file_size_bytes,
            config.percentage_max_file_size,
            config.split_percentage,
        ))),
        df_planner: Arc::new(V1DataFusionPlanner::new(
            config.parquet_store_scratchpad.clone(),
            Arc::clone(&config.exec),
        )),
        df_plan_exec,
        parquet_files_sink,
        round_split: Arc::new(ManyFilesRoundSplit::new()),
        divide_initial: Arc::new(MultipleBranchesDivideInitial::new()),
        scratchpad_gen,
        file_classifier: Arc::new(LoggingFileClassifierWrapper::new(make_file_classifier(
            config,
        ))),
        partition_resource_limit_filter: Arc::new(LoggingPartitionFilterWrapper::new(
            MetricsPartitionFilterWrapper::new(
                AndPartitionFilter::new(partition_resource_limit_filters),
                &config.metric_registry,
                partition_resource_limit_conditions,
            ),
            partition_resource_limit_conditions,
        )),
    })
}

// Conditions to commpact this partittion
fn make_partition_filters(config: &Config) -> Vec<Arc<dyn PartitionFilter>> {
    // (Has-L0) OR            -- to avoid overlaped files
    // (num(L1) > N) OR       -- to avoid many files
    // (total_size(L1) > max_desired_file_size)  -- to avoid compact and than split
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
        Arc::new(GreaterSizeMatchingFilesPartitionFilter::new(
            LevelRangeFileFilter::new(
                CompactionLevel::FileNonOverlapped..=CompactionLevel::FileNonOverlapped,
            ),
            config.max_desired_file_size_bytes,
        )),
    ]))]
}

fn make_file_classifier(config: &Config) -> Arc<dyn FileClassifier> {
    Arc::new(SplitBasedFileClassifier::new(
        TargetLevelSplit::new(),
        NonOverlapSplit::new(),
        UpgradeSplit::new(config.max_desired_file_size_bytes),
    ))
}
