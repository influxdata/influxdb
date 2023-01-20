use std::{
    cmp::{max, min},
    sync::Arc,
};

use data_types::{CompactionLevel, ParquetFile, TimestampMinMax};
use datafusion::logical_expr::LogicalPlan;
use iox_query::{
    exec::{Executor, ExecutorType},
    frontend::reorg::ReorgPlanner,
    QueryChunk,
};
use iox_time::TimeProvider;
use observability_deps::tracing::{debug, trace};
use parquet_file::storage::ParquetStorage;
use snafu::{ResultExt, Snafu};

use crate::{
    components::compact::query_chunk::{to_queryable_parquet_chunk, QueryableParquetChunk},
    config::Config,
};

use super::partition::PartitionInfo;

#[derive(Debug, Snafu)]
#[allow(missing_copy_implementations, missing_docs)]
pub(crate) enum Error {
    #[snafu(display("Error building compact logical plan  {}", source))]
    CompactLogicalPlan {
        source: iox_query::frontend::reorg::Error,
    },
}

/// Builder for compaction plans
pub(crate) struct CompactPlanBuilder {
    // Partition of files to compact
    partition: Arc<PartitionInfo>,
    files: Arc<Vec<ParquetFile>>,
    store: ParquetStorage,
    exec: Arc<Executor>,
    _time_provider: Arc<dyn TimeProvider>,
    max_desired_file_size_bytes: u64,
    percentage_max_file_size: u16,
    split_percentage: u16,
    target_level: CompactionLevel,
}

impl CompactPlanBuilder {
    /// Create a new compact plan builder.
    pub fn new(
        files: Arc<Vec<ParquetFile>>,
        partition: Arc<PartitionInfo>,
        config: Arc<Config>,
        compaction_level: CompactionLevel,
    ) -> Self {
        Self {
            partition,
            files,
            store: config.parquet_store.clone(),
            exec: Arc::clone(&config.exec),
            _time_provider: Arc::clone(&config.time_provider),
            // TODO: make these configurable
            max_desired_file_size_bytes: 100 * 1024 * 1024,
            percentage_max_file_size: 30,
            split_percentage: 90,
            target_level: compaction_level,
        }
    }

    /// Builds a logical compact plan respecting the specified file boundaries
    /// This functon assumes that the compaction-levels of the files are  either target_level or target_level-1
    pub fn build_logical_compact_plan(self) -> Result<LogicalPlan, Error> {
        let Self {
            partition,
            files,
            store,
            exec,
            _time_provider,
            max_desired_file_size_bytes,
            percentage_max_file_size,
            split_percentage,
            target_level,
        } = self;

        // total file size is the sum of the file sizes of the files to compact
        let file_sizes = files.iter().map(|f| f.file_size_bytes).collect::<Vec<_>>();
        let total_size: i64 = file_sizes.iter().sum();
        let total_size = total_size as u64;

        // Convert the input files into QueryableParquetChunk for making query plan
        let query_chunks: Vec<_> = files
            .iter()
            .map(|file| {
                to_queryable_parquet_chunk(
                    file.clone(),
                    store.clone(),
                    &partition.table_schema,
                    partition.sort_key.clone(),
                    target_level,
                )
            })
            .collect();

        trace!(
            n_query_chunks = query_chunks.len(),
            "gathered parquet data to compact"
        );

        // Compute min/max time
        // unwrap here will work because the len of the query_chunks already >= 1
        let (head, tail) = query_chunks.split_first().unwrap();
        let mut min_time = head.min_time();
        let mut max_time = head.max_time();
        for c in tail {
            min_time = min(min_time, c.min_time());
            max_time = max(max_time, c.max_time());
        }

        // extract the min & max chunk times for filtering potential split times.
        let chunk_times: Vec<_> = query_chunks
            .iter()
            .map(|c| TimestampMinMax::new(c.min_time(), c.max_time()))
            .collect();

        // Merge schema of the compacting chunks
        let query_chunks: Vec<_> = query_chunks
            .into_iter()
            .map(|c| Arc::new(c) as Arc<dyn QueryChunk>)
            .collect();
        let merged_schema = QueryableParquetChunk::merge_schemas(&query_chunks);
        debug!(
            num_cols = merged_schema.as_arrow().fields().len(),
            "Number of columns in the merged schema to build query plan"
        );

        // All partitions in the catalog MUST contain a sort key.
        let sort_key = partition
            .sort_key
            .as_ref()
            .expect("no partition sort key in catalog")
            .filter_to(&merged_schema.primary_key(), partition.partition_id.get());

        let (small_cutoff_bytes, large_cutoff_bytes) =
            Self::cutoff_bytes(max_desired_file_size_bytes, percentage_max_file_size);

        let ctx = exec.new_context(ExecutorType::Reorg);
        let plan = if total_size <= small_cutoff_bytes {
            // Compact everything into one file
            ReorgPlanner::new(ctx.child_ctx("ReorgPlanner"))
                .compact_plan(
                    Arc::from(partition.table.name.clone()),
                    &merged_schema,
                    query_chunks,
                    sort_key,
                )
                .context(CompactLogicalPlanSnafu)?
        } else {
            let split_times = if small_cutoff_bytes < total_size && total_size <= large_cutoff_bytes
            {
                // Split compaction into two files, the earlier of split_percentage amount of
                // max_desired_file_size_bytes, the later of the rest
                vec![min_time + ((max_time - min_time) * split_percentage as i64) / 100]
            } else {
                // Split compaction into multiple files
                Self::compute_split_time(
                    chunk_times,
                    min_time,
                    max_time,
                    total_size,
                    max_desired_file_size_bytes,
                )
            };

            if split_times.is_empty() || (split_times.len() == 1 && split_times[0] == max_time) {
                // The split times might not have actually split anything, so in this case, compact
                // everything into one file
                ReorgPlanner::new(ctx.child_ctx("ReorgPlanner"))
                    .compact_plan(
                        Arc::from(partition.table.name.clone()),
                        &merged_schema,
                        query_chunks,
                        sort_key,
                    )
                    .context(CompactLogicalPlanSnafu)?
            } else {
                // split compact query plan
                ReorgPlanner::new(ctx.child_ctx("ReorgPlanner"))
                    .split_plan(
                        Arc::from(partition.table.name.clone()),
                        &merged_schema,
                        query_chunks,
                        sort_key,
                        split_times,
                    )
                    .context(CompactLogicalPlanSnafu)?
            }
        };

        Ok(plan)
    }

    // compute cut off bytes for files
    fn cutoff_bytes(max_desired_file_size_bytes: u64, percentage_max_file_size: u16) -> (u64, u64) {
        (
            (max_desired_file_size_bytes * percentage_max_file_size as u64) / 100,
            (max_desired_file_size_bytes * (100 + percentage_max_file_size as u64)) / 100,
        )
    }

    // Compute time to split data
    // Return a list of times at which we want data to be split. The times are computed
    // based on the max_desired_file_size each file should not exceed and the total_size this input
    // time range [min_time, max_time] contains.
    // The split times assume that the data is evenly distributed in the time range and if
    // that is not the case the resulting files are not guaranteed to be below max_desired_file_size
    // Hence, the range between two contiguous returned time is percentage of
    // max_desired_file_size/total_size of the time range
    // Example:
    //  . Input
    //      min_time = 1
    //      max_time = 21
    //      total_size = 100
    //      max_desired_file_size = 30
    //
    //  . Pecentage = 70/100 = 0.3
    //  . Time range between 2 times = (21 - 1) * 0.3 = 6
    //
    //  . Output = [7, 13, 19] in which
    //     7 = 1 (min_time) + 6 (time range)
    //     13 = 7 (previous time) + 6 (time range)
    //     19 = 13 (previous time) + 6 (time range)
    fn compute_split_time(
        chunk_times: Vec<TimestampMinMax>,
        min_time: i64,
        max_time: i64,
        total_size: u64,
        max_desired_file_size: u64,
    ) -> Vec<i64> {
        // Too small to split
        if total_size <= max_desired_file_size {
            return vec![max_time];
        }

        // Same min and max time, nothing to split
        if min_time == max_time {
            return vec![max_time];
        }

        let mut split_times = vec![];
        let percentage = max_desired_file_size as f64 / total_size as f64;
        let mut min = min_time;
        loop {
            let split_time = min + ((max_time - min_time) as f64 * percentage).ceil() as i64;

            if split_time >= max_time {
                break;
            } else if Self::time_range_present(&chunk_times, min, split_time) {
                split_times.push(split_time);
            }
            min = split_time;
        }

        split_times
    }

    // time_range_present returns true if the given time range is included in any of the chunks.
    fn time_range_present(chunk_times: &[TimestampMinMax], min_time: i64, max_time: i64) -> bool {
        chunk_times
            .iter()
            .any(|&chunk| chunk.max >= min_time && chunk.min <= max_time)
    }
}
