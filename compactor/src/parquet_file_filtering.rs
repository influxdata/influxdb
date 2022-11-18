//! Logic for filtering a set of Parquet files to the desired set to be used for an optimal
//! compaction operation.

use crate::{compact::PartitionCompactionCandidateWithInfo, parquet_file::CompactorParquetFile};
use data_types::CompactionLevel;
use metric::{Attributes, Metric, U64Gauge, U64Histogram};
use observability_deps::tracing::*;
use std::{borrow::Cow, sync::Arc};

/// Groups of files, their partition, and the estimated budget for compacting this group
#[derive(Debug)]
pub(crate) struct FilteredFiles {
    /// Files and the budget in bytes neeeded to compact them
    pub filter_result: FilterResult,
    /// Partition of the files
    pub partition: Arc<PartitionCompactionCandidateWithInfo>,
}

#[derive(Debug, PartialEq)]
pub(crate) enum FilterResult {
    NothingToCompact,
    OverLimitFileNum {
        num_files: usize,
        budget_bytes: u64,
    },
    OverBudget {
        budget_bytes: u64,
        num_files: usize,
    },
    Proceed {
        files: Vec<CompactorParquetFile>,
        budget_bytes: u64,
    },
}

/// Given a list of sorted level N files and a list of level N + 1 files for a partition, select a
/// subset set of files that:
///
/// - Has a subset of the level N files selected, from the start of the sorted level N list
/// - Has a total size less than `max_bytes`
/// - Has only level N + 1 files that overlap in time with the level N files
///
/// The returned files will be ordered with the level N + 1 files first, then the level N files
/// in the same order as they were in the input.
#[allow(clippy::too_many_arguments)]
pub(crate) fn filter_parquet_files(
    // partition of the parquet files
    partition: Arc<PartitionCompactionCandidateWithInfo>,
    // Level N files, sorted
    level_n: Vec<CompactorParquetFile>,
    // Level N + 1 files
    level_n_plus_1: Vec<CompactorParquetFile>,
    // Stop considering level N files when the total estimated arrow size of all files selected for
    // compaction so far exceeds this value
    max_bytes: u64,
    // stop considering level N files when the total files selected for compaction so far exceeds
    // this value
    max_num_files: usize,
    // for the first file in a partition and its overlaps we have a separate max_num_files
    max_num_files_first_in_partition: usize,
    // max desired output file size in bytes
    max_desired_file_size_bytes: u64,
    // Gauge for the number of Parquet file candidates
    parquet_file_candidate_gauge: &Metric<U64Gauge>,
    // Histogram for the number of bytes of Parquet file candidates
    parquet_file_candidate_bytes: &Metric<U64Histogram>,
) -> FilteredFiles {
    let filter_result = filter_parquet_files_inner(
        level_n,
        level_n_plus_1,
        max_bytes,
        max_num_files,
        max_num_files_first_in_partition,
        max_desired_file_size_bytes,
        parquet_file_candidate_gauge,
        parquet_file_candidate_bytes,
    );

    FilteredFiles {
        filter_result,
        partition,
    }
}

#[allow(clippy::too_many_arguments)]
fn filter_parquet_files_inner(
    // Level N files, sorted
    level_n: Vec<CompactorParquetFile>,
    // Level N + 1 files
    mut remaining_level_n_plus_1: Vec<CompactorParquetFile>,
    // Stop considering level N files when the total estimated arrow size of all files selected for
    // compaction so far exceeds this value
    max_bytes: u64,
    // stop considering level N files when the total files selected for compaction so far exceeds
    // this value
    max_num_files: usize,
    // for the first file in a partition and its overlaps we have a separate max_num_files
    max_num_files_first_in_partition: usize,
    // max desired output file size in bytes
    max_desired_file_size_bytes: u64,
    // Gauge for the number of Parquet file candidates
    parquet_file_candidate_gauge: &Metric<U64Gauge>,
    // Histogram for the number of bytes of Parquet file candidates
    parquet_file_candidate_bytes: &Metric<U64Histogram>,
) -> FilterResult {
    if level_n.is_empty() {
        info!("No level N files to consider for compaction");
        return FilterResult::NothingToCompact;
    }

    // Guaranteed to exist because of the empty check and early return above. Also assuming all
    // files are for the same partition.
    let partition_id = level_n[0].partition_id();

    let compaction_level_n = level_n[0].compaction_level();
    let compaction_level_n_plus_1 = compaction_level_n.next();

    let num_level_n_considering = level_n.len();
    let num_level_n_plus_1_considering = remaining_level_n_plus_1.len();

    // This will start by holding the level N + 1 files that are found to overlap an included level
    // N file. At the end of this function, the level N files are added to the end.
    let mut files_to_return = Vec::with_capacity(level_n.len() + remaining_level_n_plus_1.len());
    // Estimated memory bytes needed to compact returned LN+1 files
    let mut ln_plus_1_estimated_budget =
        Vec::with_capacity(level_n.len() + remaining_level_n_plus_1.len());
    // Keep track of level N files to include in this compaction operation; maintain assumed
    // ordering by max sequence number.
    let mut level_n_to_return = Vec::with_capacity(level_n.len());
    // Estimated memory bytes needed to compact returned LN files
    let mut ln_estimated_budget = Vec::with_capacity(level_n.len());

    // Memory needed to compact the returned files
    // Estimated memory include:
    //  a. bytes needed to store the file in memory
    //  b. bytes to stream the file
    //  c. bytes needed for output streams
    // If we have n input files and m output streams, the estimated memory is:
    //    n * (a + b ) + m * c
    //  . a will be different for different file and depends on their file size
    //  . b will be different for different file and depends on the number of columns,
    //    their data types, and cardinality which is estimated using their row counts.
    //    Since files in a partition of a table would have similar schema, unless they
    //    have very different row count, their b would be similar.
    //  . c is computed similar to b, but since it is the estimate of an output stream
    //    that we do not know ahead, we will use value of b for the first input file for c.
    let mut total_estimated_budget = 0;
    for level_n_file in level_n {
        // Estimate memory needed for this LN file
        let ln_estimated_file_bytes =
            level_n_file.estimated_file_total_bytes_for_in_memory_storing_and_scanning();

        // Note: even though we can stop here if the ln_estimated_file_bytes is larger than the
        // given budget,we still continue estimated the memory needed for its overlapped LN+1 to
        // return the total memory needed to compact this LN with all of its overlapped LN+1s

        // Find all level N+1 files that overlap with this level N file.
        let (overlaps, non_overlaps): (Vec<_>, Vec<_>) = remaining_level_n_plus_1
            .into_iter()
            .partition(|level_n_plus_1_file| overlaps_in_time(level_n_plus_1_file, &level_n_file));

        // Estimate memory needed for each LN+1
        let current_ln_plus_1_estimated_file_bytes: Vec<_> = overlaps
            .iter()
            .map(|file| file.estimated_file_total_bytes_for_in_memory_storing_and_scanning())
            .collect();
        let estimated_processing_file_bytes =
            ln_estimated_file_bytes + current_ln_plus_1_estimated_file_bytes.iter().sum::<u64>();

        // Estimate memory needed for the output streams if the files are large enough to split
        // Get total size of ln files (level_n_file and level_n_to_return) and ln+1 files (overlaps
        // and files_to_return)
        let total_file_size_bytes = level_n_file.file_size_bytes()
            + overlaps.iter().map(|f| f.file_size_bytes()).sum::<i64>()
            + level_n_to_return
                .iter()
                .map(|f: &CompactorParquetFile| f.file_size_bytes())
                .sum::<i64>()
            + files_to_return
                .iter()
                .map(|f: &CompactorParquetFile| f.file_size_bytes())
                .sum::<i64>();
        let estimated_memory_for_output_streams = estimate_memory_for_output_streams(
            total_file_size_bytes as u64,
            max_desired_file_size_bytes,
            &level_n_file,
        );

        // memory needed if we only compact the current ln and its overlapped ln_plus files
        let budget_for_current_ln_and_ln_plus =
            estimated_processing_file_bytes + estimated_memory_for_output_streams;

        // Over limit of num files
        // At this stage files_to_return only includes LN+1 files. To get both returning LN+1 and
        // LN files, we need to consider both files_to_return and level_n_to_return
        // The file limit only applies after the first file, because we want to avoid the situation
        // where a file (and its overlapping files) will never get processed. In this scenario,
        // however, we do not proceed to pick up any more files other than this first one (and its
        // overlaps)
        if files_to_return.len() + level_n_to_return.len() + 1 /* LN file */ + overlaps.len()
            > max_num_files
            && !(files_to_return.is_empty() && level_n_to_return.is_empty())
            || (files_to_return.is_empty()
                && level_n_to_return.is_empty()
                && 1 /* LN file */ + overlaps.len() > max_num_files_first_in_partition)
        {
            if files_to_return.is_empty() && level_n_to_return.is_empty() {
                // Cannot compact this partition because its first set of overlapped files
                // exceed the file limit
                return FilterResult::OverLimitFileNum {
                    num_files: 1 + overlaps.len(),
                    budget_bytes: budget_for_current_ln_and_ln_plus,
                };
            } else {
                // Only compact files that are under limit number of files
                break;
            }
        } else if total_estimated_budget + budget_for_current_ln_and_ln_plus > max_bytes {
            // Over budget
            if total_estimated_budget == 0 {
                // Cannot compact this partition further with the given budget
                return FilterResult::OverBudget {
                    budget_bytes: budget_for_current_ln_and_ln_plus,
                    num_files: 1 + overlaps.len(),
                };
            } else {
                // Only compact the ones under the given budget
                break;
            }
        } else if compaction_level_n == CompactionLevel::FileNonOverlapped
            && total_file_size_bytes as u64 > max_desired_file_size_bytes
            // Don't limit to 0 files if the first L1 file is already over the limit, let it
            // be added to the group anyway
            && !level_n_to_return.is_empty()
        {
            // Only compact the level 1 files under the desired file size
            break;
        } else {
            // Still under budget and under limit of number of files, OR
            // it's the first LN file and it is under max_num_files_first_in_partition.
            //
            // We do not add memory budget for output stream yet because it will change when we add
            // more files
            total_estimated_budget += estimated_processing_file_bytes;
            ln_estimated_budget.push(ln_estimated_file_bytes);
            ln_plus_1_estimated_budget.extend(current_ln_plus_1_estimated_file_bytes);

            // Move the overlapping level N+1 files to `files_to_return` so they're not considered
            // again; a level N+1 file overlapping with one level N file is enough for its
            // inclusion. This way, we also don't include level N+1 files multiple times.
            files_to_return.extend(overlaps);

            // The remaining level N+1 files to possibly include in future iterations are the
            // remaining ones that did not overlap with this level N file.
            remaining_level_n_plus_1 = non_overlaps;

            // Move the level N file into the list of level N files to return
            level_n_to_return.push(level_n_file);
        }
    }

    let num_level_n_compacting = level_n_to_return.len();
    let num_level_n_plus_1_compacting = files_to_return.len();

    info!(
        partition_id = partition_id.get(),
        num_level_n_considering,
        num_level_n_plus_1_considering,
        num_level_n_compacting,
        num_level_n_plus_1_compacting,
        "filtered Parquet files for compaction",
    );

    record_file_metrics(
        parquet_file_candidate_gauge,
        compaction_level_n,
        compaction_level_n_plus_1,
        num_level_n_considering as u64,
        num_level_n_plus_1_considering as u64,
        num_level_n_compacting as u64,
        num_level_n_plus_1_compacting as u64,
    );

    record_byte_metrics(
        parquet_file_candidate_bytes,
        compaction_level_n,
        compaction_level_n_plus_1,
        level_n_to_return
            .iter()
            .map(|pf| pf.file_size_bytes() as u64)
            .collect(),
        files_to_return
            .iter()
            .map(|pf| pf.file_size_bytes() as u64)
            .collect(),
        ln_estimated_budget,
        ln_plus_1_estimated_budget,
    );

    // Return the level N+1 files first, followed by the level N files. The order is arbitrary;
    // ordering for deduplication happens using `QueryChunk.order`.
    files_to_return.extend(level_n_to_return);

    // Add estimated memory for output streams
    if !files_to_return.is_empty() {
        let output_size = (files_to_return
            .iter()
            .map(|f| f.file_size_bytes())
            .sum::<i64>()) as u64;
        total_estimated_budget += estimate_memory_for_output_streams(
            output_size,
            max_desired_file_size_bytes,
            &files_to_return[0],
        );
    }

    FilterResult::Proceed {
        files: files_to_return,
        budget_bytes: total_estimated_budget,
    }
}

fn estimate_memory_for_output_streams(
    total_size: u64,
    max_output_size: u64,
    file: &CompactorParquetFile,
) -> u64 {
    let estimated_num_streams = (total_size as f64 / max_output_size as f64).ceil() as u64;
    let etimated_memory_for_one_output_stream =
        file.estimated_arrow_bytes_for_streaming_query_plan();

    estimated_num_streams * etimated_memory_for_one_output_stream
}

fn overlaps_in_time(a: &CompactorParquetFile, b: &CompactorParquetFile) -> bool {
    (a.min_time() <= b.min_time() && a.max_time() >= b.min_time())
        || (a.min_time() > b.min_time() && a.min_time() <= b.max_time())
}

fn record_file_metrics(
    gauge: &Metric<U64Gauge>,
    compaction_level_n: CompactionLevel,
    compaction_level_n_plus_1: CompactionLevel,
    num_level_n_considering: u64,
    num_level_n_plus_1_considering: u64,
    num_level_n_compacting: u64,
    num_level_n_plus_1_compacting: u64,
) {
    let compaction_level_n_string = Cow::from(format!("{}", compaction_level_n as i16));
    let mut level_n_attributes =
        Attributes::from([("compaction_level", compaction_level_n_string)]);

    level_n_attributes.insert("status", "selected_for_compaction");
    let recorder = gauge.recorder(level_n_attributes.clone());
    recorder.set(num_level_n_compacting);

    level_n_attributes.insert("status", "not_selected_for_compaction");
    let recorder = gauge.recorder(level_n_attributes);
    recorder.set(num_level_n_considering - num_level_n_compacting);

    let compaction_level_n_plus_1_string =
        Cow::from(format!("{}", compaction_level_n_plus_1 as i16));
    let mut level_n_plus_1_attributes =
        Attributes::from([("compaction_level", compaction_level_n_plus_1_string)]);

    level_n_plus_1_attributes.insert("status", "selected_for_compaction");
    let recorder = gauge.recorder(level_n_plus_1_attributes.clone());
    recorder.set(num_level_n_plus_1_compacting);

    level_n_plus_1_attributes.insert("status", "not_selected_for_compaction");
    let recorder = gauge.recorder(level_n_plus_1_attributes);
    recorder.set(num_level_n_plus_1_considering - num_level_n_plus_1_compacting);
}

fn record_byte_metrics(
    histogram: &Metric<U64Histogram>,
    compaction_level_n: CompactionLevel,
    compaction_level_n_plus_1: CompactionLevel,
    level_n_sizes: Vec<u64>,
    level_n_plus_1_sizes: Vec<u64>,
    level_n_estimated_compacting_budgets: Vec<u64>,
    level_n_plus_1_estimated_compacting_budgets: Vec<u64>,
) {
    let compaction_level_n_string = Cow::from(format!("{}", compaction_level_n as i16));
    let level_n_attributes = Attributes::from([(
        "file_size_compaction_level",
        compaction_level_n_string.clone(),
    )]);

    let compaction_level_n_plus_1_string =
        Cow::from(format!("{}", compaction_level_n_plus_1 as i16));
    let level_n_plus_1_attributes = Attributes::from([(
        "file_size_compaction_level",
        compaction_level_n_plus_1_string.clone(),
    )]);

    let recorder = histogram.recorder(level_n_attributes);
    for size in level_n_sizes {
        recorder.record(size);
    }

    let recorder = histogram.recorder(level_n_plus_1_attributes);
    for size in level_n_plus_1_sizes {
        recorder.record(size);
    }

    let level_n_attributes = Attributes::from([(
        "file_estimated_compacting_budget_compaction_level",
        compaction_level_n_string,
    )]);
    let recorder = histogram.recorder(level_n_attributes);
    for size in level_n_estimated_compacting_budgets {
        recorder.record(size);
    }

    let level_n_plus_1_attributes = Attributes::from([(
        "file_estimated_compacting_budget_compaction_level",
        compaction_level_n_plus_1_string,
    )]);

    let recorder = histogram.recorder(level_n_plus_1_attributes);
    for size in level_n_plus_1_estimated_compacting_budgets {
        recorder.record(size);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use data_types::{
        ColumnSet, CompactionLevel, NamespaceId, ParquetFile, ParquetFileId, PartitionId,
        SequenceNumber, ShardId, TableId, Timestamp,
    };
    use metric::U64HistogramOptions;
    use std::sync::Arc;
    use uuid::Uuid;

    const BUCKET_500_KB: u64 = 500 * 1024;
    const BUCKET_1_MB: u64 = 1024 * 1024;

    #[test]
    fn test_overlaps_in_time() {
        assert_overlap((1, 3), (2, 4));
        assert_overlap((1, 3), (1, 3));
        assert_overlap((1, 3), (3, 4));
        assert_overlap((1, 4), (2, 3));
        assert_overlap((1, 3), (2, 3));
        assert_overlap((1, 3), (1, 2));

        assert_no_overlap((1, 2), (3, 4));
    }

    fn assert_overlap((a_min, a_max): (i64, i64), (b_min, b_max): (i64, i64)) {
        let a = ParquetFileBuilder::level_0()
            .min_time(a_min)
            .max_time(a_max)
            .build();
        let b = ParquetFileBuilder::level_1()
            .min_time(b_min)
            .max_time(b_max)
            .build();

        assert!(
            overlaps_in_time(&a, &b),
            "Expected ({a_min}, {a_max}) to overlap with ({b_min}, {b_max}) but it didn't",
        );
        assert!(
            overlaps_in_time(&b, &a),
            "Expected ({b_min}, {b_max}) to overlap with ({a_min}, {a_max}) but it didn't",
        );
    }

    fn assert_no_overlap((a_min, a_max): (i64, i64), (b_min, b_max): (i64, i64)) {
        let a = ParquetFileBuilder::level_0()
            .min_time(a_min)
            .max_time(a_max)
            .build();
        let b = ParquetFileBuilder::level_1()
            .min_time(b_min)
            .max_time(b_max)
            .build();

        assert!(
            !overlaps_in_time(&a, &b),
            "Expected ({a_min}, {a_max}) to not overlap with ({b_min}, {b_max}) but it did",
        );
        assert!(
            !overlaps_in_time(&b, &a),
            "Expected ({b_min}, {b_max}) to not overlap with ({a_min}, {a_max}) but it did",
        );
    }

    fn metrics() -> (Metric<U64Gauge>, Metric<U64Histogram>) {
        let registry = Arc::new(metric::Registry::new());

        let parquet_file_candidate_gauge = registry.register_metric(
            "parquet_file_candidates",
            "Number of Parquet file candidates",
        );

        let parquet_file_candidate_bytes = registry.register_metric_with_options(
            "parquet_file_candidate_bytes",
            "Number of bytes of Parquet file candidates",
            || {
                U64HistogramOptions::new([
                    BUCKET_500_KB,    // 500 KB
                    BUCKET_1_MB,      // 1 MB
                    3 * 1024 * 1024,  // 3 MB
                    10 * 1024 * 1024, // 10 MB
                    30 * 1024 * 1024, // 30 MB
                    u64::MAX,         // Inf
                ])
            },
        );

        (parquet_file_candidate_gauge, parquet_file_candidate_bytes)
    }

    const MEMORY_BUDGET: u64 = 1024 * 1024 * 10;
    const FILE_NUM_LIMIT: usize = 20;
    const DEFAULT_MAX_OUTPUT_SIZE: u64 = 1024 * 1024 * 1024;
    const ESTIMATED_STREAM_BYTES: u64 = 1176;
    const FILE_SIZE_BYTES: i64 = 10;

    #[test]
    fn empty_in_empty_out() {
        let level_0 = vec![];
        let level_1 = vec![];
        let (files_metric, bytes_metric) = metrics();

        let filter_result = filter_parquet_files_inner(
            level_0,
            level_1,
            MEMORY_BUDGET,
            FILE_NUM_LIMIT,
            FILE_NUM_LIMIT * 2,
            DEFAULT_MAX_OUTPUT_SIZE,
            &files_metric,
            &bytes_metric,
        );

        assert_eq!(filter_result, FilterResult::NothingToCompact);
    }

    #[test]
    fn budget_0_returns_over_budget() {
        let level_0 = vec![ParquetFileBuilder::level_0().id(1).build()];
        let level_1 = vec![];
        let (files_metric, bytes_metric) = metrics();

        let bytes_stored_the_file_in_memory = (2 * level_0[0].file_size_bytes()) as u64;
        let expected_estimated_budget =
            bytes_stored_the_file_in_memory + 2 * ESTIMATED_STREAM_BYTES; // one input stream, one output stream

        let filter_result = filter_parquet_files_inner(
            level_0,
            level_1,
            0,
            FILE_NUM_LIMIT,
            FILE_NUM_LIMIT * 2,
            DEFAULT_MAX_OUTPUT_SIZE,
            &files_metric,
            &bytes_metric,
        );

        assert_eq!(
            filter_result,
            FilterResult::OverBudget {
                budget_bytes: expected_estimated_budget,
                num_files: 1
            }
        );
    }

    #[test]
    fn budget_0_small_output_returns_over_budget() {
        let level_0 = vec![ParquetFileBuilder::level_0().id(1).build()];
        let file_size_bytes = level_0[0].file_size_bytes();
        let level_1 = vec![];
        let (files_metric, bytes_metric) = metrics();

        let bytes_stored_the_file_in_memory = (2 * level_0[0].file_size_bytes()) as u64;
        let expected_estimated_budget =
            bytes_stored_the_file_in_memory + 3 * ESTIMATED_STREAM_BYTES; // one input stream, two output streams

        let filter_result = filter_parquet_files_inner(
            level_0,
            level_1,
            0,
            FILE_NUM_LIMIT,
            FILE_NUM_LIMIT * 2,
            // output size smaller than input size to force return 2 output streams
            (file_size_bytes - 1) as u64,
            &files_metric,
            &bytes_metric,
        );

        assert_eq!(
            filter_result,
            FilterResult::OverBudget {
                budget_bytes: expected_estimated_budget,
                num_files: 1
            }
        );
    }

    #[test]
    fn budget_0_super_small_output_returns_over_budget() {
        let level_0 = vec![ParquetFileBuilder::level_0().id(1).build()];
        let file_size_bytes = level_0[0].file_size_bytes();
        let level_1 = vec![];
        let (files_metric, bytes_metric) = metrics();

        let bytes_stored_the_file_in_memory = (2 * level_0[0].file_size_bytes()) as u64;
        let expected_estimated_budget =
            bytes_stored_the_file_in_memory + 4 * ESTIMATED_STREAM_BYTES; // one input stream, three output streams

        let filter_result = filter_parquet_files_inner(
            level_0,
            level_1,
            0,
            FILE_NUM_LIMIT,
            FILE_NUM_LIMIT * 2,
            // max desired output size ~1/3 input size to force returning 3 output streams
            (file_size_bytes / 3 + 1) as u64,
            &files_metric,
            &bytes_metric,
        );

        assert_eq!(
            filter_result,
            FilterResult::OverBudget {
                budget_bytes: expected_estimated_budget,
                num_files: 1
            }
        );
    }

    #[test]
    fn budget_1000_returns_over_budget() {
        let level_0 = vec![ParquetFileBuilder::level_0().id(1).build()];
        let level_1 = vec![];
        let (files_metric, bytes_metric) = metrics();

        let bytes_stored_the_file_in_memory = (2 * level_0[0].file_size_bytes()) as u64;
        let expected_estimated_budget =
            bytes_stored_the_file_in_memory + 2 * ESTIMATED_STREAM_BYTES; // one input stream, one output stream

        let filter_result = filter_parquet_files_inner(
            level_0,
            level_1,
            1000,
            FILE_NUM_LIMIT,
            FILE_NUM_LIMIT * 2,
            DEFAULT_MAX_OUTPUT_SIZE,
            &files_metric,
            &bytes_metric,
        );

        assert_eq!(
            filter_result,
            FilterResult::OverBudget {
                budget_bytes: expected_estimated_budget,
                num_files: 1
            }
        );
    }

    #[test]
    fn file_num_0_over_file_limit_but_proceeds() {
        let level_0 = vec![ParquetFileBuilder::level_0().id(1).build()];
        let level_1 = vec![];
        let (files_metric, bytes_metric) = metrics();

        let bytes_stored_the_file_in_memory = (2 * level_0[0].file_size_bytes()) as u64;
        let expected_estimated_budget =
            bytes_stored_the_file_in_memory + 2 * ESTIMATED_STREAM_BYTES; // one input stream, one output stream

        let filter_result = filter_parquet_files_inner(
            level_0,
            level_1,
            MEMORY_BUDGET,
            0, // note max file count is zero
            1, // however we are not over the special limit for the first file in the partition
            DEFAULT_MAX_OUTPUT_SIZE,
            &files_metric,
            &bytes_metric,
        );

        // we will process this despite > max file count because it's the first
        assert!(
            matches!(
                  &filter_result,
              FilterResult::Proceed { files, budget_bytes }
                  if files.len() == 1
                      && files.clone().pop().unwrap().id().get() == 1
                      && *budget_bytes == expected_estimated_budget
            ),
            "Match failed, got: {filter_result:?}"
        );
    }

    #[test]
    fn file_num_0_over_file_limit_for_first_in_partition() {
        let level_0 = vec![
            ParquetFileBuilder::level_0()
                .id(1)
                .min_time(200)
                .max_time(300)
                .build(),
            // second file that shouldn't get picked up for processing
            ParquetFileBuilder::level_0().id(2).build(),
        ];
        let level_1 = vec![
            // all overlap the L0 file
            ParquetFileBuilder::level_1()
                .id(102)
                .min_time(150)
                .max_time(250)
                .build(),
            ParquetFileBuilder::level_1()
                .id(103)
                .min_time(250)
                .max_time(350)
                .build(),
            ParquetFileBuilder::level_1()
                .id(104)
                .min_time(270)
                .max_time(280)
                .build(),
        ];
        let (files_metric, bytes_metric) = metrics();

        let bytes_stored_4_files_in_memory = 4 * (2 * level_0[0].file_size_bytes()) as u64;
        let expected_estimated_budget = bytes_stored_4_files_in_memory + 5 * ESTIMATED_STREAM_BYTES;

        let filter_result = filter_parquet_files_inner(
            level_0,
            level_1,
            MEMORY_BUDGET,
            1, // this isn't a factor in this test
            2, // we are over this limit for the first file in partition
            DEFAULT_MAX_OUTPUT_SIZE,
            &files_metric,
            &bytes_metric,
        );

        // we will process this despite > max file count because it's the first
        assert_eq!(
            filter_result,
            FilterResult::OverLimitFileNum {
                num_files: 4,
                budget_bytes: expected_estimated_budget,
            }
        );
    }

    #[test]
    fn file_num_0_over_file_limit_but_proceeds_but_no_more() {
        let level_0 = vec![
            ParquetFileBuilder::level_0()
                .id(1)
                .min_time(200)
                .max_time(300)
                .build(),
            // second file that shouldn't get picked up for processing
            ParquetFileBuilder::level_0().id(2).build(),
        ];
        let level_1 = vec![
            // overlaps with L0 id 1
            ParquetFileBuilder::level_1()
                .id(102)
                .min_time(250)
                .max_time(350)
                .build(),
        ];
        let (files_metric, bytes_metric) = metrics();

        let bytes_stored_2_files_in_memory = 2 * (2 * level_0[0].file_size_bytes()) as u64;
        let expected_estimated_budget = bytes_stored_2_files_in_memory + 3 * ESTIMATED_STREAM_BYTES; // two input streams, one output stream

        let filter_result = filter_parquet_files_inner(
            level_0,
            level_1,
            MEMORY_BUDGET,
            0, // note max file count is zero
            2, // ...but given it's the first file in the partition we use this limit
            DEFAULT_MAX_OUTPUT_SIZE,
            &files_metric,
            &bytes_metric,
        );

        // L0 file and its overlapping files get picked despite > max file count because it's the
        // first file
        assert!(
            matches!(
                  &filter_result,
              FilterResult::Proceed { files, budget_bytes }
                  if files.len() == 2
                      // L0 file + its overlapping L1 file but not the next L0 file even though we
                      // have the memory for it
                      && files.iter().map(|f| f.id().get()).collect::<Vec<_>>() == [102, 1]
                      && *budget_bytes == expected_estimated_budget
            ),
            "Match failed, got: {filter_result:?}"
        );
    }

    #[test]
    fn file_num_limit_1_over_file_limit_but_proceeds_but_no_more() {
        // in this test it's the L1 file that pushes us over max file count, but it still proceeds
        // because it overlaps the first L0 file
        let level_0 = vec![
            ParquetFileBuilder::level_0()
                .id(1)
                .min_time(200)
                .max_time(300)
                .build(),
            // second file that shouldn't get picked up for processing
            ParquetFileBuilder::level_0().id(2).build(),
        ];
        let level_1 = vec![
            // Completely contains the level 0 times
            ParquetFileBuilder::level_1()
                .id(102)
                .min_time(150)
                .max_time(350)
                .build(),
        ];
        let (files_metric, bytes_metric) = metrics();

        let bytes_stored_2_files_in_memory = 2 * (2 * level_0[0].file_size_bytes()) as u64;
        let expected_estimated_budget = bytes_stored_2_files_in_memory + 3 * ESTIMATED_STREAM_BYTES; // two input streams, one output stream

        let filter_result = filter_parquet_files_inner(
            level_0,
            level_1,
            MEMORY_BUDGET,
            1,
            2,
            DEFAULT_MAX_OUTPUT_SIZE,
            &files_metric,
            &bytes_metric,
        );

        assert!(
            matches!(
                  &filter_result,
              FilterResult::Proceed { files, budget_bytes }
                  if files.len() == 2
                      && files.iter().map(|f| f.id().get()).collect::<Vec<_>>() == [102, 1]
                      && *budget_bytes == expected_estimated_budget
            ),
            "Match failed, got: {filter_result:?}"
        );
    }

    #[test]
    fn file_num_limit_second_l1_over_file_limit_but_proceeds_but_no_more() {
        // in this test it's a second L1 file that pushes us over max file count, but it still
        // proceeds because it overlaps the first L0 file
        let level_0 = vec![
            ParquetFileBuilder::level_0()
                .id(1)
                .min_time(200)
                .max_time(300)
                .build(),
            // second file that shouldn't get picked up for processing
            ParquetFileBuilder::level_0().id(2).build(),
        ];
        let level_1 = vec![
            // both overlap the L0 file
            ParquetFileBuilder::level_1()
                .id(102)
                .min_time(150)
                .max_time(250)
                .build(),
            ParquetFileBuilder::level_1()
                .id(103)
                .min_time(250)
                .max_time(350)
                .build(),
        ];
        let (files_metric, bytes_metric) = metrics();

        let bytes_stored_3_files_in_memory = 3 * (2 * level_0[0].file_size_bytes()) as u64;
        let expected_estimated_budget = bytes_stored_3_files_in_memory + 4 * ESTIMATED_STREAM_BYTES; // three input streams, one output stream

        let filter_result = filter_parquet_files_inner(
            level_0,
            level_1,
            MEMORY_BUDGET,
            2,
            3,
            DEFAULT_MAX_OUTPUT_SIZE,
            &files_metric,
            &bytes_metric,
        );

        assert!(
            matches!(
                  &filter_result,
              FilterResult::Proceed { files, budget_bytes }
                  if files.len() == 3
                      && files.iter().map(|f| f.id().get()).collect::<Vec<_>>() == [102, 103, 1]
                      && *budget_bytes == expected_estimated_budget
            ),
            "Match failed, got: {filter_result:?}"
        );
    }

    #[test]
    fn file_num_limit_first_over_file_limit_but_insufficient_budget() {
        // in this test the first file and its overlaps push us over the file limit, which usually
        // means they're included anyway but in this case we don't have the memory budget
        let level_0 = vec![ParquetFileBuilder::level_0()
            .id(1)
            .min_time(200)
            .max_time(300)
            .build()];
        let level_1 = vec![
            // both overlap the L0 file
            ParquetFileBuilder::level_1()
                .id(102)
                .min_time(150)
                .max_time(250)
                .build(),
            ParquetFileBuilder::level_1()
                .id(103)
                .min_time(250)
                .max_time(350)
                .build(),
        ];
        let (files_metric, bytes_metric) = metrics();

        let bytes_stored_3_files_in_memory = 3 * (2 * level_0[0].file_size_bytes()) as u64;
        let expected_estimated_budget = bytes_stored_3_files_in_memory + 4 * ESTIMATED_STREAM_BYTES; // three input streams, one output stream

        let filter_result = filter_parquet_files_inner(
            level_0,
            level_1,
            // enough for 2 files and one output stream
            (ESTIMATED_STREAM_BYTES * 3) + (FILE_SIZE_BYTES as u64 * 2 * 2) + 1,
            // max file count not a factor here
            1,
            // it's under the limit
            3,
            DEFAULT_MAX_OUTPUT_SIZE,
            &files_metric,
            &bytes_metric,
        );

        assert_eq!(
            filter_result,
            FilterResult::OverBudget {
                budget_bytes: expected_estimated_budget,
                num_files: 3
            }
        );
    }

    #[test]
    fn file_num_limit_2_return_2_files() {
        let level_0 = vec![
            // Level 0 files that overlap in time slightly.
            ParquetFileBuilder::level_0()
                .id(1)
                .min_time(200)
                .max_time(300)
                .file_size_bytes(FILE_SIZE_BYTES)
                .build(),
            ParquetFileBuilder::level_0()
                .id(2)
                .min_time(280)
                .max_time(310)
                .file_size_bytes(FILE_SIZE_BYTES)
                .build(),
            ParquetFileBuilder::level_0()
                .id(3)
                .min_time(309)
                .max_time(350)
                .file_size_bytes(FILE_SIZE_BYTES)
                .build(),
        ];

        let level_1 = vec![
            // overlaps in time with second and third L0s
            ParquetFileBuilder::level_1()
                .id(101)
                .min_time(301)
                .max_time(500)
                .file_size_bytes(FILE_SIZE_BYTES)
                .build(),
            // overlaps in time with first and second L0s
            ParquetFileBuilder::level_1()
                .id(102)
                .min_time(150)
                .max_time(250)
                .file_size_bytes(FILE_SIZE_BYTES)
                .build(),
        ];
        let (files_metric, bytes_metric) = metrics();

        // bytes stored 2 files in memory: first L0 (id=1) and second L1 (id=102)
        let bytes_stored_2_files_in_memory = 2 * (2 * level_0[0].file_size_bytes()) as u64;
        let mut expected_estimated_budget =
            bytes_stored_2_files_in_memory + 3 * ESTIMATED_STREAM_BYTES; // two input streams, one output stream

        // Test 1: allow large output size --> only one output stream
        let filter_result = filter_parquet_files_inner(
            level_0.clone(),
            level_1.clone(),
            MEMORY_BUDGET,
            2,
            4,
            DEFAULT_MAX_OUTPUT_SIZE,
            &files_metric,
            &bytes_metric,
        );

        // should include 2 files: first L0 (id=1) and second L1 (id=102)
        assert!(
            matches!(
                &filter_result,
                FilterResult::Proceed { files, budget_bytes }
                    if files.len() == 2
                        && files.iter().map(|f| f.id().get()).collect::<Vec<_>>() == [102, 1]
                        && *budget_bytes == expected_estimated_budget
            ),
            "Match failed, got: {filter_result:?}"
        );

        // Test 2: max output size small
        let filter_result = filter_parquet_files_inner(
            level_0,
            level_1,
            MEMORY_BUDGET,
            2,
            4,
            // max output size is just over 1/3 of (10 + 10) which is size of first L0 (id=1) and second L1 (id=102)
            8,
            &files_metric,
            &bytes_metric,
        );

        // add budget for 2 two more output streams
        expected_estimated_budget += 2 * ESTIMATED_STREAM_BYTES;

        // should include 2 files: first L0 (id=1) and second L1 (id=102)
        assert!(
            matches!(
                &filter_result,
                FilterResult::Proceed { files, budget_bytes }
                    if files.len() == 2
                        && files.iter().map(|f| f.id().get()).collect::<Vec<_>>() == [102, 1]
                        && *budget_bytes == expected_estimated_budget// 2 input streams & 3 output streams
            ),
            "Match failed, got: {filter_result:?}"
        );
    }

    #[test]
    fn file_num_limit_10_return_all_5_files() {
        let level_0 = vec![
            // Level 0 files that overlap in time slightly.
            ParquetFileBuilder::level_0()
                .id(1)
                .min_time(200)
                .max_time(300)
                .file_size_bytes(FILE_SIZE_BYTES)
                .build(),
            ParquetFileBuilder::level_0()
                .id(2)
                .min_time(280)
                .max_time(310)
                .file_size_bytes(FILE_SIZE_BYTES)
                .build(),
            ParquetFileBuilder::level_0()
                .id(3)
                .min_time(309)
                .max_time(350)
                .file_size_bytes(FILE_SIZE_BYTES)
                .build(),
        ];

        let level_1 = vec![
            // overlaps in time with second and third L0s
            ParquetFileBuilder::level_1()
                .id(101)
                .min_time(301)
                .max_time(500)
                .build(),
            // overlaps in time with first and second L0s
            ParquetFileBuilder::level_1()
                .id(102)
                .min_time(150)
                .max_time(250)
                .build(),
        ];
        let (files_metric, bytes_metric) = metrics();

        let bytes_stored_5_files_in_memory = 5 * (2 * level_0[0].file_size_bytes()) as u64;
        let expected_estimated_budget = bytes_stored_5_files_in_memory + 6 * ESTIMATED_STREAM_BYTES; // 5 input streams, one output stream

        let filter_result = filter_parquet_files_inner(
            level_0,
            level_1,
            MEMORY_BUDGET,
            10,
            20,
            DEFAULT_MAX_OUTPUT_SIZE,
            &files_metric,
            &bytes_metric,
        );

        // should include 2 files: first L0 (id=1) and second L1 (id=102)
        assert!(
            matches!(
                &filter_result,
                FilterResult::Proceed { files, budget_bytes }
                    if files.len() == 5
                        && files.iter().map(|f| f.id().get()).collect::<Vec<_>>()
                            == [102, 101, 1, 2, 3]
                        && *budget_bytes ==  expected_estimated_budget
            ),
            "Match failed, got: {filter_result:?}"
        );
    }

    // bug: https://github.com/influxdata/conductor/issues/1178
    #[test]
    fn file_num_limit_3_five_l0_one_l1_files() {
        let level_0 = vec![
            // Level 0 files that overlap in time slightly.
            ParquetFileBuilder::level_0()
                .id(1)
                .min_time(200)
                .max_time(300)
                .file_size_bytes(FILE_SIZE_BYTES)
                .build(),
            ParquetFileBuilder::level_0()
                .id(2)
                .min_time(280)
                .max_time(310)
                .file_size_bytes(FILE_SIZE_BYTES)
                .build(),
            ParquetFileBuilder::level_0()
                .id(3)
                .min_time(309)
                .max_time(350)
                .file_size_bytes(FILE_SIZE_BYTES)
                .build(),
            ParquetFileBuilder::level_0()
                .id(4)
                .min_time(309)
                .max_time(350)
                .file_size_bytes(FILE_SIZE_BYTES)
                .build(),
            ParquetFileBuilder::level_0()
                .id(5)
                .min_time(309)
                .max_time(350)
                .file_size_bytes(FILE_SIZE_BYTES)
                .build(),
        ];

        let level_1 = vec![
            // overlaps in time with first and second L0s
            ParquetFileBuilder::level_1()
                .id(101)
                .min_time(150)
                .max_time(250)
                .build(),
        ];
        let (files_metric, bytes_metric) = metrics();
        let bytes_stored_3_files_in_memory = 3 * (2 * level_0[0].file_size_bytes()) as u64;
        let expected_estimated_budget = bytes_stored_3_files_in_memory + 4 * ESTIMATED_STREAM_BYTES; // 3 input streams, one output stream

        let filter_result = filter_parquet_files_inner(
            level_0,
            level_1,
            MEMORY_BUDGET,
            3,
            6,
            DEFAULT_MAX_OUTPUT_SIZE,
            &files_metric,
            &bytes_metric,
        );

        // should include 3 files: first 2 L0s (id=1 and id=2) and sthe L1 (id=101)
        assert!(
            matches!(
                &filter_result,
                FilterResult::Proceed { files, budget_bytes }
                    if files.len() == 3
                        && files.iter().map(|f| f.id().get()).collect::<Vec<_>>() == [101, 1, 2]
                        && *budget_bytes == expected_estimated_budget
            ),
            "Match failed, got: {filter_result:?}"
        );
    }

    #[test]
    fn large_budget_returns_one_level_0_file_and_its_level_1_overlaps() {
        let level_0 = vec![ParquetFileBuilder::level_0()
            .id(1)
            .min_time(200)
            .max_time(300)
            .build()];
        let level_1 = vec![
            // Too early
            ParquetFileBuilder::level_1()
                .id(101)
                .min_time(1)
                .max_time(50)
                .build(),
            // Completely contains the level 0 times
            ParquetFileBuilder::level_1()
                .id(102)
                .min_time(150)
                .max_time(350)
                .build(),
            // Too late
            ParquetFileBuilder::level_1()
                .id(103)
                .min_time(400)
                .max_time(500)
                .build(),
        ];
        let (files_metric, bytes_metric) = metrics();

        let bytes_stored_2_files_in_memory = 2 * (2 * level_0[0].file_size_bytes()) as u64;
        let expected_estimated_budget = bytes_stored_2_files_in_memory + 3 * ESTIMATED_STREAM_BYTES; // 2 input streams, one output stream

        let filter_result = filter_parquet_files_inner(
            level_0,
            level_1,
            MEMORY_BUDGET,
            FILE_NUM_LIMIT,
            FILE_NUM_LIMIT * 2,
            DEFAULT_MAX_OUTPUT_SIZE,
            &files_metric,
            &bytes_metric,
        );

        assert!(
            matches!(
                &filter_result,
                FilterResult::Proceed { files, budget_bytes }
                    if files.len() == 2
                        && files.iter().map(|f| f.id().get()).collect::<Vec<_>>() == [102, 1]
                        && *budget_bytes == expected_estimated_budget
            ),
            "Match failed, got: {filter_result:?}"
        );
    }

    #[test]
    fn returns_only_overlapping_level_1_files_in_order() {
        let level_0 = vec![
            // Level 0 files that overlap in time slightly.
            ParquetFileBuilder::level_0()
                .id(1)
                .min_time(200)
                .max_time(300)
                .file_size_bytes(FILE_SIZE_BYTES)
                .build(),
            ParquetFileBuilder::level_0()
                .id(2)
                .min_time(280)
                .max_time(310)
                .file_size_bytes(FILE_SIZE_BYTES)
                .build(),
            ParquetFileBuilder::level_0()
                .id(3)
                .min_time(309)
                .max_time(350)
                .file_size_bytes(FILE_SIZE_BYTES)
                .build(),
        ];

        // Level 1 files can be assumed not to overlap each other.
        let level_1 = vec![
            // Does not overlap any level 0, times are too early
            ParquetFileBuilder::level_1()
                .id(101)
                .min_time(1)
                .max_time(50)
                .file_size_bytes(FILE_SIZE_BYTES)
                .build(),
            // Overlaps file 1
            ParquetFileBuilder::level_1()
                .id(102)
                .min_time(199)
                .max_time(201)
                .file_size_bytes(FILE_SIZE_BYTES)
                .build(),
            // Overlaps files 1 and 2
            ParquetFileBuilder::level_1()
                .id(103)
                .min_time(290)
                .max_time(300)
                .file_size_bytes(FILE_SIZE_BYTES)
                .build(),
            // Overlaps file 2
            ParquetFileBuilder::level_1()
                .id(104)
                .min_time(305)
                .max_time(305)
                .file_size_bytes(FILE_SIZE_BYTES)
                .build(),
            // Overlaps files 2 and 3
            ParquetFileBuilder::level_1()
                .id(105)
                .min_time(308)
                .max_time(311)
                .file_size_bytes(FILE_SIZE_BYTES)
                .build(),
            // Overlaps file 3
            ParquetFileBuilder::level_1()
                .id(106)
                .min_time(340)
                .max_time(360)
                .file_size_bytes(BUCKET_500_KB as i64 + 1) // exercise metrics
                .build(),
            // Does not overlap any level 0, times are too late
            ParquetFileBuilder::level_1()
                .id(107)
                .min_time(390)
                .max_time(399)
                .file_size_bytes(FILE_SIZE_BYTES)
                .build(),
        ];

        // Test 1: budget enough for 3 files only
        let (files_metric, bytes_metric) = metrics();

        let bytes_stored_3_files_in_memory = 3 * (2 * level_0[0].file_size_bytes()) as u64;
        let expected_estimated_budget = bytes_stored_3_files_in_memory + 4 * ESTIMATED_STREAM_BYTES; // 3 input streams, one output stream

        let filter_result = filter_parquet_files_inner(
            level_0.clone(),
            level_1.clone(),
            // enough for 3 files and one output stream
            (ESTIMATED_STREAM_BYTES * 4) + (FILE_SIZE_BYTES as u64 * 3 * 2) + 1,
            FILE_NUM_LIMIT,
            FILE_NUM_LIMIT * 2,
            DEFAULT_MAX_OUTPUT_SIZE,
            &files_metric,
            &bytes_metric,
        );

        assert!(
            matches!(
                &filter_result,
                FilterResult::Proceed { files, budget_bytes }
                    if files.len() == 3
                        && files
                                .iter()
                                .map(|f| f.id().get())
                                .collect::<Vec<_>>() == [102, 103, 1]
                        && *budget_bytes == expected_estimated_budget
            ),
            "Match failed, got: {filter_result:?}"
        );

        assert_eq!(
            extract_file_metrics(
                &files_metric,
                CompactionLevel::Initial,
                CompactionLevel::FileNonOverlapped
            ),
            ExtractedFileMetrics {
                level_n_selected: 1,
                level_n_not_selected: 2,
                level_n_plus_1_selected: 2,
                level_n_plus_1_not_selected: 5,
            }
        );

        // Test 2: increase budget to fit all files
        let (files_metric, bytes_metric) = metrics();

        let bytes_stored_6_files_in_memory = 6 * (2 * level_0[0].file_size_bytes()) as u64;
        let expected_estimated_budget = bytes_stored_6_files_in_memory + 7 * ESTIMATED_STREAM_BYTES; // 6 input streams, one output stream

        let filter_result = filter_parquet_files_inner(
            level_0,
            level_1,
            // Increase budget to more than 6 files; 1st two level 0 files & their overlapping
            // level 1 files get returned. Enough for 6 files/input streams and one output stream.
            (ESTIMATED_STREAM_BYTES * 7) + (FILE_SIZE_BYTES as u64 * 6 * 2) + 1,
            FILE_NUM_LIMIT,
            FILE_NUM_LIMIT * 2,
            DEFAULT_MAX_OUTPUT_SIZE,
            &files_metric,
            &bytes_metric,
        );

        assert!(
            matches!(
                &filter_result,
                FilterResult::Proceed { files, budget_bytes }
                    if files.len() == 6
                        && files
                                .iter()
                                .map(|f| f.id().get())
                                .collect::<Vec<_>>() == [102, 103, 104, 105, 1, 2]
                        && *budget_bytes == expected_estimated_budget
            ),
            "Match failed, got: {filter_result:?}"
        );

        assert_eq!(
            extract_file_metrics(
                &files_metric,
                CompactionLevel::Initial,
                CompactionLevel::FileNonOverlapped
            ),
            ExtractedFileMetrics {
                level_n_selected: 2,
                level_n_not_selected: 1,
                level_n_plus_1_selected: 4,
                level_n_plus_1_not_selected: 3,
            }
        );
    }

    #[test]
    fn returns_only_overlapping_level_2_files_in_order() {
        let level_1 = vec![
            // Level 1 files don't overlap each other.
            ParquetFileBuilder::level_1()
                .id(1)
                .min_time(200)
                .max_time(300)
                .file_size_bytes(FILE_SIZE_BYTES)
                .build(),
            ParquetFileBuilder::level_1()
                .id(2)
                .min_time(310)
                .max_time(330)
                .file_size_bytes(FILE_SIZE_BYTES)
                .build(),
            ParquetFileBuilder::level_1()
                .id(3)
                .min_time(340)
                .max_time(350)
                .file_size_bytes(FILE_SIZE_BYTES)
                .build(),
        ];

        // Level 2 files can be assumed not to overlap each other.
        let level_2 = vec![
            // Does not overlap any level 1, times are too early
            ParquetFileBuilder::level_2()
                .id(101)
                .min_time(1)
                .max_time(50)
                .file_size_bytes(FILE_SIZE_BYTES)
                .build(),
            // Overlaps file 1
            ParquetFileBuilder::level_2()
                .id(102)
                .min_time(199)
                .max_time(201)
                .file_size_bytes(FILE_SIZE_BYTES)
                .build(),
            // Overlaps files 1 and 2
            ParquetFileBuilder::level_2()
                .id(103)
                .min_time(290)
                .max_time(312)
                .file_size_bytes(FILE_SIZE_BYTES)
                .build(),
            // Overlaps file 2
            ParquetFileBuilder::level_2()
                .id(104)
                .min_time(315)
                .max_time(315)
                .file_size_bytes(FILE_SIZE_BYTES)
                .build(),
            // Overlaps files 2 and 3
            ParquetFileBuilder::level_2()
                .id(105)
                .min_time(329)
                .max_time(341)
                .file_size_bytes(FILE_SIZE_BYTES)
                .build(),
            // Overlaps file 3
            ParquetFileBuilder::level_2()
                .id(106)
                .min_time(342)
                .max_time(360)
                .file_size_bytes(BUCKET_500_KB as i64 + 1) // exercise metrics
                .build(),
            // Does not overlap any level 1, times are too late
            ParquetFileBuilder::level_2()
                .id(107)
                .min_time(390)
                .max_time(399)
                .file_size_bytes(FILE_SIZE_BYTES)
                .build(),
        ];

        let (files_metric, bytes_metric) = metrics();

        let bytes_stored_3_files_in_memory = 3 * (2 * level_1[0].file_size_bytes()) as u64;
        let expected_estimated_budget = bytes_stored_3_files_in_memory + 4 * ESTIMATED_STREAM_BYTES; // 3 input streams, one output stream

        let filter_result = filter_parquet_files_inner(
            level_1.clone(),
            level_2.clone(),
            // enough for 3 files and one output stream
            (ESTIMATED_STREAM_BYTES * 4) + (FILE_SIZE_BYTES as u64 * 3 * 2) + 1,
            FILE_NUM_LIMIT,
            FILE_NUM_LIMIT * 2,
            DEFAULT_MAX_OUTPUT_SIZE,
            &files_metric,
            &bytes_metric,
        );

        assert!(
            matches!(
                &filter_result,
                FilterResult::Proceed { files, budget_bytes }
                    if files.len() == 3
                        && files
                                .iter()
                                .map(|f| f.id().get())
                                .collect::<Vec<_>>() == [102, 103, 1]
                        && *budget_bytes == expected_estimated_budget
            ),
            "Match failed, got: {filter_result:?}"
        );

        assert_eq!(
            extract_file_metrics(
                &files_metric,
                CompactionLevel::FileNonOverlapped,
                CompactionLevel::Final
            ),
            ExtractedFileMetrics {
                level_n_selected: 1,
                level_n_not_selected: 2,
                level_n_plus_1_selected: 2,
                level_n_plus_1_not_selected: 5,
            }
        );

        let (files_metric, bytes_metric) = metrics();

        let bytes_stored_6_files_in_memory = 6 * (2 * level_1[0].file_size_bytes()) as u64;
        let expected_estimated_budget = bytes_stored_6_files_in_memory + 7 * ESTIMATED_STREAM_BYTES; // 6 input streams, one output stream

        let filter_result = filter_parquet_files_inner(
            level_1,
            level_2,
            // Increase budget to more than 6 files; 1st two level 1 files & their overlapping
            // level 2 files get returned. Enough for 3 files and one output stream.
            (ESTIMATED_STREAM_BYTES * 7) + (FILE_SIZE_BYTES as u64 * 6 * 2) + 1,
            FILE_NUM_LIMIT,
            FILE_NUM_LIMIT * 2,
            DEFAULT_MAX_OUTPUT_SIZE,
            &files_metric,
            &bytes_metric,
        );

        assert!(
            matches!(
                &filter_result,
                FilterResult::Proceed { files, budget_bytes }
                    if files.len() == 6
                        && files
                                .iter()
                                .map(|f| f.id().get())
                                .collect::<Vec<_>>() == [102, 103, 104, 105, 1, 2]
                        && *budget_bytes == expected_estimated_budget
            ),
            "Match failed, got: {filter_result:?}"
        );

        assert_eq!(
            extract_file_metrics(
                &files_metric,
                CompactionLevel::FileNonOverlapped,
                CompactionLevel::Final
            ),
            ExtractedFileMetrics {
                level_n_selected: 2,
                level_n_not_selected: 1,
                level_n_plus_1_selected: 4,
                level_n_plus_1_not_selected: 3,
            }
        );
    }

    #[test]
    fn level_1_file_already_desired_size() {
        let level_1 = vec![
            ParquetFileBuilder::level_1()
                .id(101)
                .min_time(1)
                .max_time(50)
                .file_size_bytes(FILE_SIZE_BYTES)
                .build(),
            ParquetFileBuilder::level_1()
                .id(102)
                .min_time(199)
                .max_time(201)
                .file_size_bytes(FILE_SIZE_BYTES)
                .build(),
        ];

        let (files_metric, bytes_metric) = metrics();
        let filter_result = filter_parquet_files_inner(
            level_1,
            vec![],
            // enough to not limit based on this criteria
            (ESTIMATED_STREAM_BYTES * 4) + (FILE_SIZE_BYTES as u64 * 3 * 2) + 1,
            FILE_NUM_LIMIT,
            FILE_NUM_LIMIT * 2,
            // Limit to only 1 of the 2 files based on file size
            FILE_SIZE_BYTES as u64 + 1,
            &files_metric,
            &bytes_metric,
        );

        assert!(
            matches!(
                &filter_result,
                FilterResult::Proceed { files, budget_bytes: _ }
                    if files.len() == 1
                        && files
                                .iter()
                                .map(|f| f.id().get())
                                .collect::<Vec<_>>() == [101]
            ),
            "Match failed, got: {filter_result:#?}"
        );
    }

    #[test]
    fn level_1_file_already_over_desired_size() {
        let level_1 = vec![
            ParquetFileBuilder::level_1()
                .id(101)
                .min_time(1)
                .max_time(50)
                .file_size_bytes(FILE_SIZE_BYTES)
                .build(),
            ParquetFileBuilder::level_1()
                .id(102)
                .min_time(199)
                .max_time(201)
                .file_size_bytes(FILE_SIZE_BYTES)
                .build(),
        ];

        let (files_metric, bytes_metric) = metrics();
        let filter_result = filter_parquet_files_inner(
            level_1,
            vec![],
            // enough to not limit based on this criteria
            (ESTIMATED_STREAM_BYTES * 4) + (FILE_SIZE_BYTES as u64 * 3 * 2) + 1,
            FILE_NUM_LIMIT,
            FILE_NUM_LIMIT * 2,
            // Limit such that the first file is already over the limit
            FILE_SIZE_BYTES as u64 - 1,
            &files_metric,
            &bytes_metric,
        );

        assert!(
            matches!(
                &filter_result,
                FilterResult::Proceed { files, budget_bytes: _ }
                    if files.len() == 1
                        && files
                                .iter()
                                .map(|f| f.id().get())
                                .collect::<Vec<_>>() == [101]
            ),
            "Match failed, got: {filter_result:#?}"
        );
    }

    #[test]
    fn level_1_files_only_take_enough_to_get_to_desired_size() {
        let level_1 = vec![
            ParquetFileBuilder::level_1()
                .id(101)
                .min_time(1)
                .max_time(50)
                .file_size_bytes(FILE_SIZE_BYTES)
                .build(),
            ParquetFileBuilder::level_1()
                .id(102)
                .min_time(199)
                .max_time(201)
                .file_size_bytes(FILE_SIZE_BYTES)
                .build(),
            ParquetFileBuilder::level_1()
                .id(103)
                .min_time(400)
                .max_time(500)
                .file_size_bytes(FILE_SIZE_BYTES)
                .build(),
        ];

        let (files_metric, bytes_metric) = metrics();
        let filter_result = filter_parquet_files_inner(
            level_1,
            vec![],
            // Don't limit based on this criteria
            u64::MAX,
            FILE_NUM_LIMIT,
            FILE_NUM_LIMIT * 2,
            // Limit to only 2 of the 3 files based on file size
            2 * FILE_SIZE_BYTES as u64 + 1,
            &files_metric,
            &bytes_metric,
        );

        assert!(
            matches!(
                &filter_result,
                FilterResult::Proceed { files, budget_bytes: _ }
                    if files.len() == 2
                        && files
                                .iter()
                                .map(|f| f.id().get())
                                .collect::<Vec<_>>() == [101, 102]
            ),
            "Match failed, got: {filter_result:#?}"
        );
    }

    /// Create ParquetFile instances for testing. Only sets fields relevant to the filtering; other
    /// fields are set to arbitrary and possibly invalid values. For example, by default, all
    /// ParquetFile instances created by this function will have the same ParquetFileId, which is
    /// invalid in production but irrelevant to this module.
    #[derive(Debug)]
    struct ParquetFileBuilder {
        compaction_level: CompactionLevel,
        id: i64,
        min_time: i64,
        max_time: i64,
        file_size_bytes: i64,
    }

    impl ParquetFileBuilder {
        // Start building a level 0 file
        fn level_0() -> Self {
            Self {
                compaction_level: CompactionLevel::Initial,
                id: 1,
                min_time: 8,
                max_time: 9,
                file_size_bytes: FILE_SIZE_BYTES,
            }
        }

        // Start building a level 1 file
        fn level_1() -> Self {
            Self {
                compaction_level: CompactionLevel::FileNonOverlapped,
                id: 1,
                min_time: 8,
                max_time: 9,
                file_size_bytes: 10,
            }
        }

        // Start building a level 2 file
        fn level_2() -> Self {
            Self {
                compaction_level: CompactionLevel::Final,
                id: 1,
                min_time: 8,
                max_time: 9,
                file_size_bytes: 10,
            }
        }

        fn id(mut self, id: i64) -> Self {
            self.id = id;
            self
        }

        fn min_time(mut self, min_time: i64) -> Self {
            self.min_time = min_time;
            self
        }

        fn max_time(mut self, max_time: i64) -> Self {
            self.max_time = max_time;
            self
        }

        fn file_size_bytes(mut self, file_size_bytes: i64) -> Self {
            self.file_size_bytes = file_size_bytes;
            self
        }

        fn build(self) -> CompactorParquetFile {
            let Self {
                compaction_level,
                id,
                min_time,
                max_time,
                file_size_bytes,
            } = self;

            let f = ParquetFile {
                id: ParquetFileId::new(id),
                shard_id: ShardId::new(2),
                namespace_id: NamespaceId::new(3),
                table_id: TableId::new(4),
                partition_id: PartitionId::new(5),
                object_store_id: Uuid::new_v4(),
                max_sequence_number: SequenceNumber::new(7),
                min_time: Timestamp::new(min_time),
                max_time: Timestamp::new(max_time),
                to_delete: None,
                file_size_bytes,
                row_count: 11,
                compaction_level,
                created_at: Timestamp::new(12),
                column_set: ColumnSet::new(std::iter::empty()),
            };
            // Estimated arrow bytes for one file with a tag, a time and 11 rows = 1176
            CompactorParquetFile::new(f, ESTIMATED_STREAM_BYTES, (file_size_bytes * 2) as u64)
        }
    }

    #[derive(Debug, PartialEq)]
    struct ExtractedFileMetrics {
        level_n_selected: u64,
        level_n_not_selected: u64,
        level_n_plus_1_selected: u64,
        level_n_plus_1_not_selected: u64,
    }

    fn extract_file_metrics(
        metric: &Metric<U64Gauge>,
        level_n: CompactionLevel,
        level_n_plus_1: CompactionLevel,
    ) -> ExtractedFileMetrics {
        let level_n_string = Cow::from(format!("{}", level_n as i16));
        let level_n_selected = metric
            .get_observer(&Attributes::from([
                ("compaction_level", level_n_string.clone()),
                ("status", "selected_for_compaction".into()),
            ]))
            .unwrap()
            .fetch();
        let level_n_not_selected = metric
            .get_observer(&Attributes::from([
                ("compaction_level", level_n_string),
                ("status", "not_selected_for_compaction".into()),
            ]))
            .unwrap()
            .fetch();

        let level_n_plus_1_string = Cow::from(format!("{}", level_n_plus_1 as i16));
        let level_n_plus_1_selected = metric
            .get_observer(&Attributes::from([
                ("compaction_level", level_n_plus_1_string.clone()),
                ("status", "selected_for_compaction".into()),
            ]))
            .unwrap()
            .fetch();
        let level_n_plus_1_not_selected = metric
            .get_observer(&Attributes::from([
                ("compaction_level", level_n_plus_1_string),
                ("status", "not_selected_for_compaction".into()),
            ]))
            .unwrap()
            .fetch();

        ExtractedFileMetrics {
            level_n_selected,
            level_n_not_selected,
            level_n_plus_1_selected,
            level_n_plus_1_not_selected,
        }
    }
}
