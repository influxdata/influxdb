use std::sync::Arc;

use self::{
    commit::Commit, df_plan_exec::DataFusionPlanExec, df_planner::DataFusionPlanner,
    divide_initial::DivideInitial, file_classifier::FileClassifier, files_filter::FilesFilter,
    ir_planner::IRPlanner, parquet_files_sink::ParquetFilesSink,
    partition_done_sink::PartitionDoneSink, partition_files_source::PartitionFilesSource,
    partition_filter::PartitionFilter, partition_info_source::PartitionInfoSource,
    partition_stream::PartitionStream, round_info_source::RoundInfoSource, round_split::RoundSplit,
    scratchpad::ScratchpadGen,
};

pub mod combos;
pub mod commit;
pub mod df_plan_exec;
pub mod df_planner;
pub mod divide_initial;
pub mod file_classifier;
pub mod file_filter;
pub mod files_filter;
pub mod files_split;
pub mod hardcoded;
pub mod id_only_partition_filter;
pub mod ir_planner;
pub mod namespaces_source;
pub mod parquet_file_sink;
pub mod parquet_files_sink;
pub mod partition_done_sink;
pub mod partition_files_source;
pub mod partition_filter;
pub mod partition_info_source;
pub mod partition_source;
pub mod partition_stream;
pub mod partitions_source;
pub mod report;
pub mod round_info_source;
pub mod round_split;
pub mod scratchpad;
pub mod skipped_compactions_source;
pub mod tables_source;

/// Pluggable system to determine compactor behavior. Please see
/// [Crate Level Documentation](crate) for more details on the
/// design.
#[derive(Debug, Clone)]
pub struct Components {
    /// Source of partitions for the compactor to compact
    pub partition_stream: Arc<dyn PartitionStream>,
    /// Source of information about a partition neededed for compaction
    pub partition_info_source: Arc<dyn PartitionInfoSource>,
    /// Source of files in a partition for compaction
    pub partition_files_source: Arc<dyn PartitionFilesSource>,
    /// Determines what type of compaction round the compactor will be doing
    pub round_info_source: Arc<dyn RoundInfoSource>,
    /// filter files for each round of compaction
    pub files_filter: Arc<dyn FilesFilter>,
    /// stop condition for completing a partition compaction
    pub partition_filter: Arc<dyn PartitionFilter>,
    /// condition to avoid running out of resources during compaction
    pub partition_resource_limit_filter: Arc<dyn PartitionFilter>,
    /// Records "partition is done" status for given partition.
    pub partition_done_sink: Arc<dyn PartitionDoneSink>,
    /// Commits changes (i.e. deletion and creation) to the catalog.
    pub commit: Arc<dyn Commit>,
    /// Creates `PlanIR` that describes what files should be compacted and updated
    pub ir_planner: Arc<dyn IRPlanner>,
    /// Creates an Execution plan for a `PlanIR`
    pub df_planner: Arc<dyn DataFusionPlanner>,
    /// Executes a DataFusion plan to multiple output streams.
    pub df_plan_exec: Arc<dyn DataFusionPlanExec>,
    /// Writes the streams created by [`DataFusionPlanExec`] to the object store.
    pub parquet_files_sink: Arc<dyn ParquetFilesSink>,
    /// Split files into two buckets "now" and "later".
    pub round_split: Arc<dyn RoundSplit>,
    /// Divide files in a partition into "branches"
    pub divide_initial: Arc<dyn DivideInitial>,
    /// Create intermediate temporary storage
    pub scratchpad_gen: Arc<dyn ScratchpadGen>,
    /// Classify files for each compaction branch.
    pub file_classifier: Arc<dyn FileClassifier>,
}
