use std::sync::Arc;

use self::{
    changed_files_filter::ChangedFilesFilter, commit::CommitToScheduler,
    compaction_job_done_sink::CompactionJobDoneSink, compaction_job_stream::CompactionJobStream,
    df_plan_exec::DataFusionPlanExec, df_planner::DataFusionPlanner, divide_initial::DivideInitial,
    file_classifier::FileClassifier, ir_planner::IRPlanner, parquet_files_sink::ParquetFilesSink,
    partition_files_source::PartitionFilesSource, partition_filter::PartitionFilter,
    partition_info_source::PartitionInfoSource,
    post_classification_partition_filter::PostClassificationPartitionFilter,
    round_info_source::RoundInfoSource, round_split::RoundSplit, scratchpad::ScratchpadGen,
};

pub mod changed_files_filter;
pub mod columns_source;
pub(crate) mod commit;
pub mod compaction_job_done_sink;
pub mod compaction_job_stream;
pub mod compaction_jobs_source;
pub mod df_plan_exec;
pub mod df_planner;
pub mod divide_initial;
pub mod file_classifier;
pub mod file_filter;
pub mod files_split;
pub mod hardcoded;
pub mod ir_planner;
pub mod namespaces_source;
pub mod parquet_file_sink;
pub mod parquet_files_sink;
pub mod partition_files_source;
pub mod partition_filter;
pub mod partition_info_source;
pub mod partition_source;
pub mod post_classification_partition_filter;
pub mod report;
pub mod round_info_source;
pub mod round_split;
pub mod scratchpad;
pub mod split_or_compact;
pub mod tables_source;
pub mod timeout;

/// Pluggable system to determine compactor behavior. Please see
/// [Crate Level Documentation](crate) for more details on the
/// design.
#[derive(Debug, Clone)]
pub struct Components {
    /// Source of partitions for the compactor to compact
    pub compaction_job_stream: Arc<dyn CompactionJobStream>,
    /// Source of information about a partition neededed for compaction
    pub partition_info_source: Arc<dyn PartitionInfoSource>,
    /// Source of files in a partition for compaction
    pub partition_files_source: Arc<dyn PartitionFilesSource>,
    /// Determines what type of compaction round the compactor will be doing
    pub round_info_source: Arc<dyn RoundInfoSource>,
    /// stop condition for completing a partition compaction
    pub partition_filter: Arc<dyn PartitionFilter>,
    /// condition to avoid running out of resources during compaction
    pub post_classification_partition_filter: Arc<dyn PostClassificationPartitionFilter>,
    /// Records "compaction job is done" status for given partition.
    pub compaction_job_done_sink: Arc<dyn CompactionJobDoneSink>,
    /// Commits changes (i.e. deletion and creation).
    pub commit: Arc<CommitToScheduler>,
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
    /// Check for other processes modifying files.
    pub changed_files_filter: Arc<dyn ChangedFilesFilter>,
}
