use std::sync::Arc;

use self::{
    commit::Commit, df_plan_exec::DataFusionPlanExec, df_planner::DataFusionPlanner,
    divide_initial::DivideInitial, files_filter::FilesFilter, namespaces_source::NamespacesSource,
    parquet_file_sink::ParquetFileSink, partition_done_sink::PartitionDoneSink,
    partition_files_source::PartitionFilesSource, partition_filter::PartitionFilter,
    partition_source::PartitionSource, partitions_source::PartitionsSource,
    round_split::RoundSplit, scratchpad::ScratchpadGen, tables_source::TablesSource,
};

pub mod commit;
pub mod df_plan_exec;
pub mod df_planner;
pub mod divide_initial;
pub mod file_filter;
pub mod files_filter;
pub mod hardcoded;
pub mod id_only_partition_filter;
pub mod namespaces_source;
pub mod parquet_file_sink;
pub mod partition_done_sink;
pub mod partition_files_source;
pub mod partition_filter;
pub mod partition_source;
pub mod partitions_source;
pub mod report;
pub mod round_split;
pub mod scratchpad;
pub mod skipped_compactions_source;
pub mod tables_source;

#[derive(Debug, Clone)]
pub struct Components {
    pub partitions_source: Arc<dyn PartitionsSource>,
    pub partition_source: Arc<dyn PartitionSource>,
    pub partition_files_source: Arc<dyn PartitionFilesSource>,
    pub files_filter: Arc<dyn FilesFilter>,
    pub partition_filter: Arc<dyn PartitionFilter>,
    pub partition_done_sink: Arc<dyn PartitionDoneSink>,
    pub commit: Arc<dyn Commit>,
    pub namespaces_source: Arc<dyn NamespacesSource>,
    pub tables_source: Arc<dyn TablesSource>,
    pub df_planner: Arc<dyn DataFusionPlanner>,
    pub df_plan_exec: Arc<dyn DataFusionPlanExec>,
    pub parquet_file_sink: Arc<dyn ParquetFileSink>,
    pub round_split: Arc<dyn RoundSplit>,
    pub divide_initial: Arc<dyn DivideInitial>,
    pub scratchpad_gen: Arc<dyn ScratchpadGen>,
}
