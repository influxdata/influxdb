use std::sync::Arc;

use self::{
    commit::Commit, df_plan_exec::DataFusionPlanExec, df_planner::DataFusionPlanner,
    files_filter::FilesFilter, namespaces_source::NamespacesSource,
    parquet_file_sink::ParquetFileSink, partition_error_sink::PartitionErrorSink,
    partition_files_source::PartitionFilesSource, partition_filter::PartitionFilter,
    partitions_source::PartitionsSource, tables_source::TablesSource,
};

pub mod commit;
pub mod df_plan_exec;
pub mod df_planner;
pub mod file_filter;
pub mod files_filter;
pub mod hardcoded;
pub mod namespaces_source;
pub mod parquet_file_sink;
pub mod partition_error_sink;
pub mod partition_files_source;
pub mod partition_filter;
pub mod partitions_source;
pub mod report;
pub mod skipped_compactions_source;
pub mod tables_source;

#[derive(Debug)]
pub struct Components {
    pub partitions_source: Arc<dyn PartitionsSource>,
    pub partition_files_source: Arc<dyn PartitionFilesSource>,
    pub files_filter: Arc<dyn FilesFilter>,
    pub partition_filter: Arc<dyn PartitionFilter>,
    pub partition_error_sink: Arc<dyn PartitionErrorSink>,
    pub commit: Arc<dyn Commit>,
    pub namespaces_source: Arc<dyn NamespacesSource>,
    pub tables_source: Arc<dyn TablesSource>,
    pub df_planner: Arc<dyn DataFusionPlanner>,
    pub df_plan_exec: Arc<dyn DataFusionPlanExec>,
    pub parquet_file_sink: Arc<dyn ParquetFileSink>,
    pub skipped_compactions_source: Arc<dyn skipped_compactions_source::SkippedCompactionsSource>,
}
