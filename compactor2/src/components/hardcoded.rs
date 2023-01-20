//! Current hardcoded component setup.
//!
//! TODO: Make this a runtime-config.

use std::sync::Arc;

use crate::config::Config;

use super::{
    commit::{
        catalog::CatalogCommit, logging::LoggingCommitWrapper, metrics::MetricsCommitWrapper,
    },
    files_filter::chain::FilesFilterChain,
    partition_error_sink::{
        catalog::CatalogPartitionErrorSink, logging::LoggingPartitionErrorSinkWrapper,
        metrics::MetricsPartitionErrorSinkWrapper,
    },
    partition_files_source::catalog::CatalogPartitionFilesSource,
    partition_filter::{
        and::AndPartitionFilter, has_files::HasFilesPartitionFilter,
        metrics::MetricsPartitionFilterWrapper,
    },
    partitions_source::{
        catalog::CatalogPartitionsSource, logging::LoggingPartitionsSourceWrapper,
        metrics::MetricsPartitionsSourceWrapper,
        randomize_order::RandomizeOrderPartitionsSourcesWrapper,
    },
    Components,
};

/// Get hardcoded components.
pub fn hardcoded_components(config: &Config) -> Arc<Components> {
    // TODO: partitions source: Implementing ID-based sharding / hash-partitioning so we can run multiple compactors in
    //       parallel. This should be a wrapper around the existing partions source.

    Arc::new(Components {
        partitions_source: Arc::new(LoggingPartitionsSourceWrapper::new(
            MetricsPartitionsSourceWrapper::new(
                RandomizeOrderPartitionsSourcesWrapper::new(
                    CatalogPartitionsSource::new(
                        config.backoff_config.clone(),
                        Arc::clone(&config.catalog),
                        config.partition_minute_threshold,
                        Arc::clone(&config.time_provider),
                    ),
                    1234,
                ),
                &config.metric_registry,
            ),
        )),
        partition_files_source: Arc::new(CatalogPartitionFilesSource::new(
            config.backoff_config.clone(),
            Arc::clone(&config.catalog),
        )),
        files_filter: Arc::new(FilesFilterChain::new(vec![])),
        partition_filter: Arc::new(MetricsPartitionFilterWrapper::new(
            AndPartitionFilter::new(vec![Arc::new(HasFilesPartitionFilter::new())]),
            &config.metric_registry,
        )),
        partition_error_sink: Arc::new(LoggingPartitionErrorSinkWrapper::new(
            MetricsPartitionErrorSinkWrapper::new(
                CatalogPartitionErrorSink::new(
                    config.backoff_config.clone(),
                    Arc::clone(&config.catalog),
                ),
                &config.metric_registry,
            ),
        )),
        commit: Arc::new(LoggingCommitWrapper::new(MetricsCommitWrapper::new(
            CatalogCommit::new(config.backoff_config.clone(), Arc::clone(&config.catalog)),
            &config.metric_registry,
        ))),
        namespaces_source: Arc::new(
            crate::components::namespaces_source::catalog::CatalogNamespacesSource::new(
                config.backoff_config.clone(),
                Arc::clone(&config.catalog),
            ),
        ),
        tables_source: Arc::new(
            crate::components::tables_source::catalog::CatalogTablesSource::new(
                config.backoff_config.clone(),
                Arc::clone(&config.catalog),
            ),
        ),
    })
}
