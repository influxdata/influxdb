//! Logic for filtering a set of Parquet files to the desired set to be used for an optimal
//! compaction operation.

use crate::parquet_file_lookup::ParquetFilesForCompaction;
use data_types::ParquetFile;
use metric::{Attributes, Metric, U64Gauge};
use observability_deps::tracing::*;

/// Given a list of hot level 0 files sorted by max sequence number and a list of level 1 files for
/// a partition, select a subset set of files that:
///
/// - Has a subset of the level 0 files selected, from the start of the sorted level 0 list
/// - Has a total size less than `max_bytes`
/// - Has a total number of files less than `input_file_count_threshold`
/// - Has only level 1 files that overlap in time with the level 0 files
///
/// The returned files will be ordered with the level 1 files first, then the level 0 files ordered
/// in ascending order by their max sequence number.
pub(crate) fn filter_hot_parquet_files(
    // Level 0 files sorted by max sequence number and level 1 files in arbitrary order for one
    // partition
    parquet_files_for_compaction: ParquetFilesForCompaction,
    // Stop considering level 0 files when the total size of all files selected for compaction so
    // far exceeds this value
    max_bytes: u64,
    // Stop considering level 0 files when the count of L0 + L1 files selected for compaction so
    // far exceeds this value
    input_file_count_threshold: usize,
    // Gauge for the number of Parquet file candidates
    parquet_file_candidate_gauge: &Metric<U64Gauge>,
    // Gauge for the number of bytes of Parquet file candidates
    parquet_file_candidate_bytes_gauge: &Metric<U64Gauge>,
) -> Vec<ParquetFile> {
    let ParquetFilesForCompaction {
        level_0,
        level_1: mut remaining_level_1,
    } = parquet_files_for_compaction;

    if level_0.is_empty() {
        info!("No hot level 0 files to consider for compaction");
        return Vec::new();
    }

    // Guaranteed to exist because of the empty check and early return above. Also assuming all
    // files are for the same partition.
    let partition_id = level_0[0].partition_id;

    let num_level_0_considering = level_0.len();
    let num_level_1_considering = remaining_level_1.len();

    // This will start by holding the level 1 files that are found to overlap an included level 0
    // file. At the end of this function, the level 0 files are added to the end so they are sorted
    // last.
    let mut files_to_return = Vec::with_capacity(level_0.len() + remaining_level_1.len());
    // Keep track of level 0 files to include in this compaction operation; maintain assumed
    // ordering by max sequence number.
    let mut level_0_to_return = Vec::with_capacity(level_0.len());
    // Running total of the size, in bytes, of level 0 files and level 1 files for inclusion.
    // The levels are counted up separately for metrics but added together when comparing with the
    // max_size.
    let mut total_level_0_bytes = 0;
    let mut total_level_1_bytes = 0;

    for level_0_file in level_0 {
        // Check we haven't exceeded `input_file_count_threshold`, if we have, stop considering
        // level 0 files
        if (level_0_to_return.len() + files_to_return.len()) >= input_file_count_threshold {
            break;
        }

        // Include at least one level 0 file without checking against `max_bytes`
        total_level_0_bytes += level_0_file.file_size_bytes as u64;

        // Find all level 1 files that overlap with this level 0 file.
        let (overlaps, non_overlaps): (Vec<_>, Vec<_>) = remaining_level_1
            .into_iter()
            .partition(|level_1_file| overlaps_in_time(level_1_file, &level_0_file));

        // Increase the running total by the size of all the overlapping level 1 files
        total_level_1_bytes += overlaps.iter().map(|f| f.file_size_bytes).sum::<i64>() as u64;

        // Move the overlapping level 1 files to `files_to_return` so they're not considered again;
        // a level 1 file overlapping with one level 0 file is enough for its inclusion. This way,
        // we also don't include level 1 files multiple times.
        files_to_return.extend(overlaps);

        // The remaining level 1 files to possibly include in future iterations are the remaining
        // ones that did not overlap with this level 0 file.
        remaining_level_1 = non_overlaps;

        // Move the level 0 file into the list of level 0 files to return
        level_0_to_return.push(level_0_file);

        // Stop considering level 0 files if the total size of all files is over or equal to
        // `max_bytes`
        if (total_level_0_bytes + total_level_1_bytes) >= max_bytes {
            break;
        }
    }

    let num_level_0_compacting = level_0_to_return.len();
    let num_level_1_compacting = files_to_return.len();

    info!(
        partition_id = partition_id.get(),
        num_level_0_considering,
        num_level_1_considering,
        num_level_0_compacting,
        num_level_1_compacting,
        "filtered hot Parquet files for compaction",
    );

    record_file_metrics(
        parquet_file_candidate_gauge,
        num_level_0_considering as u64,
        num_level_1_considering as u64,
        num_level_0_compacting as u64,
        num_level_1_compacting as u64,
    );
    record_byte_metrics(
        parquet_file_candidate_bytes_gauge,
        total_level_0_bytes as u64,
        total_level_1_bytes as u64,
    );

    // Return the level 1 files first, followed by the level 0 files assuming we've maintained
    // their ordering by max sequence number.
    files_to_return.extend(level_0_to_return);
    files_to_return
}

/// Given a list of cold level 0 files sorted by max sequence number and a list of level 1 files for
/// a partition, select a subset set of files that:
///
/// - Has all of the level 0 files selected
/// - Has only level 1 files that overlap in time with the level 0 files
///
/// The returned files will be ordered with the level 1 files first, then the level 0 files ordered
/// in ascending order by their max sequence number.
///
/// If only one level 0 file is returned, it can be upgraded to level 1 without running compaction.
pub(crate) fn filter_cold_parquet_files(
    // Level 0 files sorted by max sequence number and level 1 files in arbitrary order for one
    // partition
    parquet_files_for_compaction: ParquetFilesForCompaction,
    // Stop considering level 0 files when the total size of all files selected for compaction so
    // far exceeds this value
    max_bytes: u64,
    // Gauge for the number of Parquet file candidates
    parquet_file_candidate_gauge: &Metric<U64Gauge>,
    // Gauge for the number of bytes of Parquet file candidates
    parquet_file_candidate_bytes_gauge: &Metric<U64Gauge>,
) -> Vec<ParquetFile> {
    let ParquetFilesForCompaction {
        level_0,
        level_1: mut remaining_level_1,
    } = parquet_files_for_compaction;

    if level_0.is_empty() {
        info!("No cold level 0 files to consider for compaction");
        return Vec::new();
    }

    // Guaranteed to exist because of the empty check and early return above. Also assuming all
    // files are for the same partition.
    let partition_id = level_0[0].partition_id;

    let num_level_0_considering = level_0.len();
    let num_level_1_considering = remaining_level_1.len();

    // This will start by holding the level 1 files that are found to overlap an included level 0
    // file. At the end of this function, the level 0 files are added to the end so they are sorted
    // last.
    let mut files_to_return = Vec::with_capacity(level_0.len() + remaining_level_1.len());
    // Keep track of level 0 files to include in this compaction operation; maintain assumed
    // ordering by max sequence number.
    let mut level_0_to_return = Vec::with_capacity(level_0.len());
    // Running total of the size, in bytes, of level 0 files and level 1 files for inclusion.
    // The levels are counted up separately for metrics.
    let mut total_level_0_bytes = 0;
    let mut total_level_1_bytes = 0;

    for level_0_file in level_0 {
        // Include at least one level 0 file without checking against `max_bytes`
        total_level_0_bytes += level_0_file.file_size_bytes as u64;

        // Find all level 1 files that overlap with this level 0 file.
        let (overlaps, non_overlaps): (Vec<_>, Vec<_>) = remaining_level_1
            .into_iter()
            .partition(|level_1_file| overlaps_in_time(level_1_file, &level_0_file));

        // Increase the running total by the size of all the overlapping level 1 files
        total_level_1_bytes += overlaps.iter().map(|f| f.file_size_bytes).sum::<i64>() as u64;

        // Move the overlapping level 1 files to `files_to_return` so they're not considered again;
        // a level 1 file overlapping with one level 0 file is enough for its inclusion. This way,
        // we also don't include level 1 files multiple times.
        files_to_return.extend(overlaps);

        // The remaining level 1 files to possibly include in future iterations are the remaining
        // ones that did not overlap with this level 0 file.
        remaining_level_1 = non_overlaps;

        // Move the level 0 file into the list of level 0 files to return
        level_0_to_return.push(level_0_file);

        // Stop considering level 0 files if the total size of all files is over or equal to
        // `max_bytes`
        if (total_level_0_bytes + total_level_1_bytes) >= max_bytes {
            break;
        }
    }

    let num_level_0_compacting = level_0_to_return.len();
    let num_level_1_compacting = files_to_return.len();

    info!(
        partition_id = partition_id.get(),
        num_level_0_considering,
        num_level_1_considering,
        num_level_0_compacting,
        num_level_1_compacting,
        "filtered cold Parquet files for compaction",
    );

    record_file_metrics(
        parquet_file_candidate_gauge,
        num_level_0_considering as u64,
        num_level_1_considering as u64,
        num_level_0_compacting as u64,
        num_level_1_compacting as u64,
    );
    record_byte_metrics(
        parquet_file_candidate_bytes_gauge,
        total_level_0_bytes as u64,
        total_level_1_bytes as u64,
    );

    // Return the level 1 files first, followed by the level 0 files assuming we've maintained
    // their ordering by max sequence number.
    files_to_return.extend(level_0_to_return);
    files_to_return
}

fn overlaps_in_time(a: &ParquetFile, b: &ParquetFile) -> bool {
    (a.min_time <= b.min_time && a.max_time >= b.min_time)
        || (a.min_time > b.min_time && a.min_time <= b.max_time)
}

fn record_file_metrics(
    gauge: &Metric<U64Gauge>,
    num_level_0_considering: u64,
    num_level_1_considering: u64,
    num_level_0_compacting: u64,
    num_level_1_compacting: u64,
) {
    let attributes = Attributes::from(&[
        ("compaction_level", "0"),
        ("status", "selected_for_compaction"),
    ]);
    let recorder = gauge.recorder(attributes);
    recorder.set(num_level_0_compacting);

    let attributes = Attributes::from(&[
        ("compaction_level", "0"),
        ("status", "not_selected_for_compaction"),
    ]);
    let recorder = gauge.recorder(attributes);
    recorder.set(num_level_0_considering - num_level_0_compacting);

    let attributes = Attributes::from(&[
        ("compaction_level", "1"),
        ("status", "selected_for_compaction"),
    ]);
    let recorder = gauge.recorder(attributes);
    recorder.set(num_level_1_compacting);

    let attributes = Attributes::from(&[
        ("compaction_level", "1"),
        ("status", "not_selected_for_compaction"),
    ]);
    let recorder = gauge.recorder(attributes);
    recorder.set(num_level_1_considering - num_level_1_compacting);
}

fn record_byte_metrics(
    gauge: &Metric<U64Gauge>,
    total_level_0_bytes: u64,
    total_level_1_bytes: u64,
) {
    let attributes = Attributes::from(&[("compaction_level", "0")]);
    let recorder = gauge.recorder(attributes);
    recorder.set(total_level_0_bytes);

    let attributes = Attributes::from(&[("compaction_level", "1")]);
    let recorder = gauge.recorder(attributes);
    recorder.set(total_level_1_bytes);
}

#[cfg(test)]
mod tests {
    use super::*;
    use data_types::{
        ColumnSet, CompactionLevel, NamespaceId, ParquetFileId, PartitionId, SequenceNumber,
        SequencerId, TableId, Timestamp,
    };
    use std::sync::Arc;
    use uuid::Uuid;

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

    fn metrics() -> (Metric<U64Gauge>, Metric<U64Gauge>) {
        let registry = Arc::new(metric::Registry::new());

        let parquet_file_candidate_gauge = registry.register_metric(
            "parquet_file_candidates",
            "Number of Parquet file candidates",
        );

        let parquet_file_candidate_bytes_gauge = registry.register_metric(
            "parquet_file_candidate_bytes",
            "Number of bytes of Parquet file candidates",
        );

        (
            parquet_file_candidate_gauge,
            parquet_file_candidate_bytes_gauge,
        )
    }

    mod hot {
        use super::*;

        const DEFAULT_MAX_FILE_SIZE: u64 = 1024 * 1024 * 10;
        const DEFAULT_INPUT_FILE_COUNT: usize = 100;

        #[test]
        fn empty_in_empty_out() {
            let parquet_files_for_compaction = ParquetFilesForCompaction {
                level_0: vec![],
                level_1: vec![],
            };
            let (files_metric, bytes_metric) = metrics();

            let files = filter_hot_parquet_files(
                parquet_files_for_compaction,
                DEFAULT_MAX_FILE_SIZE,
                DEFAULT_INPUT_FILE_COUNT,
                &files_metric,
                &bytes_metric,
            );

            assert!(files.is_empty(), "Expected empty, got: {:#?}", files);
        }

        #[test]
        fn max_size_0_returns_one_level_0_file() {
            let parquet_files_for_compaction = ParquetFilesForCompaction {
                level_0: vec![ParquetFileBuilder::level_0().id(1).build()],
                level_1: vec![],
            };
            let (files_metric, bytes_metric) = metrics();

            let files = filter_hot_parquet_files(
                parquet_files_for_compaction,
                0,
                DEFAULT_INPUT_FILE_COUNT,
                &files_metric,
                &bytes_metric,
            );

            assert_eq!(files.len(), 1);
            assert_eq!(files[0].id.get(), 1);
        }

        #[test]
        fn max_file_count_0_returns_empty() {
            let parquet_files_for_compaction = ParquetFilesForCompaction {
                level_0: vec![ParquetFileBuilder::level_0().id(1).build()],
                level_1: vec![],
            };
            let (files_metric, bytes_metric) = metrics();

            let files = filter_hot_parquet_files(
                parquet_files_for_compaction,
                DEFAULT_MAX_FILE_SIZE,
                0,
                &files_metric,
                &bytes_metric,
            );

            assert!(files.is_empty(), "Expected empty, got: {:#?}", files);
        }

        #[test]
        fn max_size_0_returns_one_level_0_file_and_its_level_1_overlaps() {
            let parquet_files_for_compaction = ParquetFilesForCompaction {
                level_0: vec![ParquetFileBuilder::level_0()
                    .id(1)
                    .min_time(200)
                    .max_time(300)
                    .build()],
                level_1: vec![
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
                ],
            };
            let (files_metric, bytes_metric) = metrics();

            let files = filter_hot_parquet_files(
                parquet_files_for_compaction,
                0,
                DEFAULT_INPUT_FILE_COUNT,
                &files_metric,
                &bytes_metric,
            );

            assert_eq!(files.len(), 2);
            assert_eq!(files[0].id.get(), 102);
            assert_eq!(files[1].id.get(), 1);
        }

        #[test]
        fn returns_only_overlapping_level_1_files_in_order() {
            let parquet_files_for_compaction = ParquetFilesForCompaction {
                level_0: vec![
                    // Level 0 files that overlap in time slightly.
                    ParquetFileBuilder::level_0()
                        .id(1)
                        .min_time(200)
                        .max_time(300)
                        .file_size_bytes(10)
                        .build(),
                    ParquetFileBuilder::level_0()
                        .id(2)
                        .min_time(280)
                        .max_time(310)
                        .file_size_bytes(10)
                        .build(),
                    ParquetFileBuilder::level_0()
                        .id(3)
                        .min_time(309)
                        .max_time(350)
                        .file_size_bytes(10)
                        .build(),
                ],
                // Level 1 files can be assumed not to overlap each other.
                level_1: vec![
                    // Does not overlap any level 0, times are too early
                    ParquetFileBuilder::level_1()
                        .id(101)
                        .min_time(1)
                        .max_time(50)
                        .file_size_bytes(10)
                        .build(),
                    // Overlaps file 1
                    ParquetFileBuilder::level_1()
                        .id(102)
                        .min_time(199)
                        .max_time(201)
                        .file_size_bytes(10)
                        .build(),
                    // Overlaps files 1 and 2
                    ParquetFileBuilder::level_1()
                        .id(103)
                        .min_time(290)
                        .max_time(300)
                        .file_size_bytes(10)
                        .build(),
                    // Overlaps file 2
                    ParquetFileBuilder::level_1()
                        .id(104)
                        .min_time(305)
                        .max_time(305)
                        .file_size_bytes(10)
                        .build(),
                    // Overlaps files 2 and 3
                    ParquetFileBuilder::level_1()
                        .id(105)
                        .min_time(308)
                        .max_time(311)
                        .file_size_bytes(10)
                        .build(),
                    // Overlaps file 3
                    ParquetFileBuilder::level_1()
                        .id(106)
                        .min_time(340)
                        .max_time(360)
                        .file_size_bytes(10)
                        .build(),
                    // Does not overlap any level 0, times are too late
                    ParquetFileBuilder::level_1()
                        .id(107)
                        .min_time(390)
                        .max_time(399)
                        .file_size_bytes(10)
                        .build(),
                ],
            };

            // Max size 0; only the first level 0 file and its overlapping level 1 files get
            // returned
            let max_size = 0;
            let (files_metric, bytes_metric) = metrics();
            let files = filter_hot_parquet_files(
                parquet_files_for_compaction.clone(),
                max_size,
                DEFAULT_INPUT_FILE_COUNT,
                &files_metric,
                &bytes_metric,
            );
            let ids: Vec<_> = files.iter().map(|f| f.id.get()).collect();
            assert_eq!(ids, [102, 103, 1]);
            assert_eq!(
                extract_file_metrics(&files_metric),
                ExtractedFileMetrics {
                    level_0_selected: 1,
                    level_0_not_selected: 2,
                    level_1_selected: 2,
                    level_1_not_selected: 5,
                }
            );
            assert_eq!(
                extract_byte_metrics(&bytes_metric),
                ExtractedByteMetrics {
                    level_0: 10,
                    level_1: 20,
                }
            );

            // Increase max size; 1st two level 0 files & their overlapping level 1 files get
            // returned
            let max_size = 40;
            let (files_metric, bytes_metric) = metrics();
            let files = filter_hot_parquet_files(
                parquet_files_for_compaction.clone(),
                max_size,
                DEFAULT_INPUT_FILE_COUNT,
                &files_metric,
                &bytes_metric,
            );
            let ids: Vec<_> = files.iter().map(|f| f.id.get()).collect();
            assert_eq!(ids, [102, 103, 104, 105, 1, 2]);
            assert_eq!(
                extract_file_metrics(&files_metric),
                ExtractedFileMetrics {
                    level_0_selected: 2,
                    level_0_not_selected: 1,
                    level_1_selected: 4,
                    level_1_not_selected: 3,
                }
            );
            assert_eq!(
                extract_byte_metrics(&bytes_metric),
                ExtractedByteMetrics {
                    level_0: 20,
                    level_1: 40,
                }
            );

            // Increase max size to be exactly equal to the size of the 1st two level 0 files &
            // their overlapping level 1 files, which is all that should get returned
            let max_size = 60;
            let (files_metric, bytes_metric) = metrics();
            let files = filter_hot_parquet_files(
                parquet_files_for_compaction.clone(),
                max_size,
                DEFAULT_INPUT_FILE_COUNT,
                &files_metric,
                &bytes_metric,
            );
            let ids: Vec<_> = files.iter().map(|f| f.id.get()).collect();
            assert_eq!(ids, [102, 103, 104, 105, 1, 2]);
            assert_eq!(
                extract_file_metrics(&files_metric),
                ExtractedFileMetrics {
                    level_0_selected: 2,
                    level_0_not_selected: 1,
                    level_1_selected: 4,
                    level_1_not_selected: 3,
                }
            );
            assert_eq!(
                extract_byte_metrics(&bytes_metric),
                ExtractedByteMetrics {
                    level_0: 20,
                    level_1: 40,
                }
            );

            // Increase max size; all level 0 files & their overlapping level 1 files get returned
            let max_size = 70;
            let (files_metric, bytes_metric) = metrics();
            let files = filter_hot_parquet_files(
                parquet_files_for_compaction.clone(),
                max_size,
                DEFAULT_INPUT_FILE_COUNT,
                &files_metric,
                &bytes_metric,
            );
            let ids: Vec<_> = files.iter().map(|f| f.id.get()).collect();
            assert_eq!(ids, [102, 103, 104, 105, 106, 1, 2, 3]);
            assert_eq!(
                extract_file_metrics(&files_metric),
                ExtractedFileMetrics {
                    level_0_selected: 3,
                    level_0_not_selected: 0,
                    level_1_selected: 5,
                    level_1_not_selected: 2,
                }
            );
            assert_eq!(
                extract_byte_metrics(&bytes_metric),
                ExtractedByteMetrics {
                    level_0: 30,
                    level_1: 50,
                }
            );

            // Set input_file_count_threshold to 1; the first level 0 file and its overlapping
            // level 1 files get returned
            let input_file_count_threshold = 1;
            let (files_metric, bytes_metric) = metrics();
            let files = filter_hot_parquet_files(
                parquet_files_for_compaction.clone(),
                DEFAULT_MAX_FILE_SIZE,
                input_file_count_threshold,
                &files_metric,
                &bytes_metric,
            );
            let ids: Vec<_> = files.iter().map(|f| f.id.get()).collect();
            assert_eq!(ids, [102, 103, 1]);
            assert_eq!(
                extract_file_metrics(&files_metric),
                ExtractedFileMetrics {
                    level_0_selected: 1,
                    level_0_not_selected: 2,
                    level_1_selected: 2,
                    level_1_not_selected: 5,
                }
            );
            assert_eq!(
                extract_byte_metrics(&bytes_metric),
                ExtractedByteMetrics {
                    level_0: 10,
                    level_1: 20,
                }
            );

            // Set input_file_count_threshold to 3; still only the first level 0 file and its
            // overlapping level 1 files get returned
            let input_file_count_threshold = 3;
            let (files_metric, bytes_metric) = metrics();
            let files = filter_hot_parquet_files(
                parquet_files_for_compaction.clone(),
                DEFAULT_MAX_FILE_SIZE,
                input_file_count_threshold,
                &files_metric,
                &bytes_metric,
            );
            let ids: Vec<_> = files.iter().map(|f| f.id.get()).collect();
            assert_eq!(ids, [102, 103, 1]);
            assert_eq!(
                extract_file_metrics(&files_metric),
                ExtractedFileMetrics {
                    level_0_selected: 1,
                    level_0_not_selected: 2,
                    level_1_selected: 2,
                    level_1_not_selected: 5,
                }
            );
            assert_eq!(
                extract_byte_metrics(&bytes_metric),
                ExtractedByteMetrics {
                    level_0: 10,
                    level_1: 20,
                }
            );

            // Set input_file_count_threshold to 4; the first two level 0 files and their
            // overlapping level 1 files get returned
            let input_file_count_threshold = 4;
            let (files_metric, bytes_metric) = metrics();
            let files = filter_hot_parquet_files(
                parquet_files_for_compaction,
                DEFAULT_MAX_FILE_SIZE,
                input_file_count_threshold,
                &files_metric,
                &bytes_metric,
            );
            let ids: Vec<_> = files.iter().map(|f| f.id.get()).collect();
            assert_eq!(ids, [102, 103, 104, 105, 1, 2]);
            assert_eq!(
                extract_file_metrics(&files_metric),
                ExtractedFileMetrics {
                    level_0_selected: 2,
                    level_0_not_selected: 1,
                    level_1_selected: 4,
                    level_1_not_selected: 3,
                }
            );
            assert_eq!(
                extract_byte_metrics(&bytes_metric),
                ExtractedByteMetrics {
                    level_0: 20,
                    level_1: 40,
                }
            );
        }
    }

    mod cold {
        use super::*;

        const DEFAULT_MAX_FILE_SIZE: u64 = 1024 * 1024 * 10;

        #[test]
        fn empty_in_empty_out() {
            let parquet_files_for_compaction = ParquetFilesForCompaction {
                level_0: vec![],
                level_1: vec![],
            };
            let (files_metric, bytes_metric) = metrics();

            let files = filter_cold_parquet_files(
                parquet_files_for_compaction,
                DEFAULT_MAX_FILE_SIZE,
                &files_metric,
                &bytes_metric,
            );

            assert!(files.is_empty(), "Expected empty, got: {:#?}", files);
        }

        #[test]
        fn max_size_0_returns_one_level_0_file() {
            let parquet_files_for_compaction = ParquetFilesForCompaction {
                level_0: vec![ParquetFileBuilder::level_0().id(1).build()],
                level_1: vec![],
            };
            let (files_metric, bytes_metric) = metrics();

            let files = filter_cold_parquet_files(
                parquet_files_for_compaction,
                0,
                &files_metric,
                &bytes_metric,
            );

            assert_eq!(files.len(), 1);
            assert_eq!(files[0].id.get(), 1);
        }

        #[test]
        fn one_level_0_file_no_level_1_overlaps() {
            let parquet_files_for_compaction = ParquetFilesForCompaction {
                level_0: vec![ParquetFileBuilder::level_0()
                    .id(1)
                    .min_time(200)
                    .max_time(300)
                    .build()],
                level_1: vec![
                    // Too early
                    ParquetFileBuilder::level_1()
                        .id(101)
                        .min_time(1)
                        .max_time(50)
                        .build(),
                    // Too late
                    ParquetFileBuilder::level_1()
                        .id(103)
                        .min_time(400)
                        .max_time(500)
                        .build(),
                ],
            };
            let (files_metric, bytes_metric) = metrics();

            let files = filter_cold_parquet_files(
                parquet_files_for_compaction,
                DEFAULT_MAX_FILE_SIZE,
                &files_metric,
                &bytes_metric,
            );

            assert_eq!(files.len(), 1);
            assert_eq!(files[0].id.get(), 1);
        }

        #[test]
        fn one_level_0_file_with_level_1_overlaps() {
            let parquet_files_for_compaction = ParquetFilesForCompaction {
                level_0: vec![ParquetFileBuilder::level_0()
                    .id(1)
                    .min_time(200)
                    .max_time(300)
                    .build()],
                level_1: vec![
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
                ],
            };
            let (files_metric, bytes_metric) = metrics();

            let files = filter_cold_parquet_files(
                parquet_files_for_compaction,
                DEFAULT_MAX_FILE_SIZE,
                &files_metric,
                &bytes_metric,
            );

            assert_eq!(files.len(), 2);
            assert_eq!(files[0].id.get(), 102);
            assert_eq!(files[1].id.get(), 1);
        }

        #[test]
        fn multiple_level_0_files_no_level_1_overlaps() {
            let parquet_files_for_compaction = ParquetFilesForCompaction {
                level_0: vec![
                    // Level 0 files, some of which overlap in time slightly.
                    ParquetFileBuilder::level_0()
                        .id(1)
                        .min_time(200)
                        .max_time(300)
                        .file_size_bytes(10)
                        .build(),
                    ParquetFileBuilder::level_0()
                        .id(2)
                        .min_time(280)
                        .max_time(310)
                        .file_size_bytes(10)
                        .build(),
                    ParquetFileBuilder::level_0()
                        .id(3)
                        .min_time(320)
                        .max_time(350)
                        .file_size_bytes(10)
                        .build(),
                ],
                // Level 1 files can be assumed not to overlap each other.
                level_1: vec![
                    // too early
                    ParquetFileBuilder::level_1()
                        .id(101)
                        .min_time(1)
                        .max_time(50)
                        .file_size_bytes(10)
                        .build(),
                    // between 2 and 3 (there can't be one between 1 and 2 because they overlap)
                    ParquetFileBuilder::level_1()
                        .id(103)
                        .min_time(315)
                        .max_time(316)
                        .file_size_bytes(10)
                        .build(),
                    // too late
                    ParquetFileBuilder::level_1()
                        .id(107)
                        .min_time(390)
                        .max_time(399)
                        .file_size_bytes(10)
                        .build(),
                ],
            };

            // all level 0 files & no level 1 files get returned
            let (files_metric, bytes_metric) = metrics();
            let files = filter_cold_parquet_files(
                parquet_files_for_compaction,
                DEFAULT_MAX_FILE_SIZE,
                &files_metric,
                &bytes_metric,
            );
            let ids: Vec<_> = files.iter().map(|f| f.id.get()).collect();
            assert_eq!(ids, [1, 2, 3]);
            assert_eq!(
                extract_file_metrics(&files_metric),
                ExtractedFileMetrics {
                    level_0_selected: 3,
                    level_0_not_selected: 0,
                    level_1_selected: 0,
                    level_1_not_selected: 3,
                }
            );
            assert_eq!(
                extract_byte_metrics(&bytes_metric),
                ExtractedByteMetrics {
                    level_0: 30,
                    level_1: 0,
                }
            );
        }

        #[test]
        fn multiple_level_0_files_with_level_1_overlaps() {
            let parquet_files_for_compaction = ParquetFilesForCompaction {
                level_0: vec![
                    // Level 0 files that overlap in time slightly.
                    ParquetFileBuilder::level_0()
                        .id(1)
                        .min_time(200)
                        .max_time(300)
                        .file_size_bytes(10)
                        .build(),
                    ParquetFileBuilder::level_0()
                        .id(2)
                        .min_time(280)
                        .max_time(310)
                        .file_size_bytes(10)
                        .build(),
                    ParquetFileBuilder::level_0()
                        .id(3)
                        .min_time(309)
                        .max_time(350)
                        .file_size_bytes(10)
                        .build(),
                ],
                // Level 1 files can be assumed not to overlap each other.
                level_1: vec![
                    // Does not overlap any level 0, times are too early
                    ParquetFileBuilder::level_1()
                        .id(101)
                        .min_time(1)
                        .max_time(50)
                        .file_size_bytes(10)
                        .build(),
                    // Overlaps file 1
                    ParquetFileBuilder::level_1()
                        .id(102)
                        .min_time(199)
                        .max_time(201)
                        .file_size_bytes(10)
                        .build(),
                    // Overlaps files 1 and 2
                    ParquetFileBuilder::level_1()
                        .id(103)
                        .min_time(290)
                        .max_time(300)
                        .file_size_bytes(10)
                        .build(),
                    // Overlaps file 2
                    ParquetFileBuilder::level_1()
                        .id(104)
                        .min_time(305)
                        .max_time(305)
                        .file_size_bytes(10)
                        .build(),
                    // Overlaps files 2 and 3
                    ParquetFileBuilder::level_1()
                        .id(105)
                        .min_time(308)
                        .max_time(311)
                        .file_size_bytes(10)
                        .build(),
                    // Overlaps file 3
                    ParquetFileBuilder::level_1()
                        .id(106)
                        .min_time(340)
                        .max_time(360)
                        .file_size_bytes(10)
                        .build(),
                    // Does not overlap any level 0, times are too late
                    ParquetFileBuilder::level_1()
                        .id(107)
                        .min_time(390)
                        .max_time(399)
                        .file_size_bytes(10)
                        .build(),
                ],
            };

            // Max size 0; only the first level 0 file and its overlapping level 1 files get
            // returned
            let max_size = 0;
            let (files_metric, bytes_metric) = metrics();
            let files = filter_cold_parquet_files(
                parquet_files_for_compaction.clone(),
                max_size,
                &files_metric,
                &bytes_metric,
            );
            let ids: Vec<_> = files.iter().map(|f| f.id.get()).collect();
            assert_eq!(ids, [102, 103, 1]);
            assert_eq!(
                extract_file_metrics(&files_metric),
                ExtractedFileMetrics {
                    level_0_selected: 1,
                    level_0_not_selected: 2,
                    level_1_selected: 2,
                    level_1_not_selected: 5,
                }
            );
            assert_eq!(
                extract_byte_metrics(&bytes_metric),
                ExtractedByteMetrics {
                    level_0: 10,
                    level_1: 20,
                }
            );

            // Increase max size; 1st two level 0 files & their overlapping level 1 files get
            // returned
            let max_size = 40;
            let (files_metric, bytes_metric) = metrics();
            let files = filter_cold_parquet_files(
                parquet_files_for_compaction.clone(),
                max_size,
                &files_metric,
                &bytes_metric,
            );
            let ids: Vec<_> = files.iter().map(|f| f.id.get()).collect();
            assert_eq!(ids, [102, 103, 104, 105, 1, 2]);
            assert_eq!(
                extract_file_metrics(&files_metric),
                ExtractedFileMetrics {
                    level_0_selected: 2,
                    level_0_not_selected: 1,
                    level_1_selected: 4,
                    level_1_not_selected: 3,
                }
            );
            assert_eq!(
                extract_byte_metrics(&bytes_metric),
                ExtractedByteMetrics {
                    level_0: 20,
                    level_1: 40,
                }
            );

            // Increase max size to be exactly equal to the size of the 1st two level 0 files &
            // their overlapping level 1 files, which is all that should get returned
            let max_size = 60;
            let (files_metric, bytes_metric) = metrics();
            let files = filter_cold_parquet_files(
                parquet_files_for_compaction.clone(),
                max_size,
                &files_metric,
                &bytes_metric,
            );
            let ids: Vec<_> = files.iter().map(|f| f.id.get()).collect();
            assert_eq!(ids, [102, 103, 104, 105, 1, 2]);
            assert_eq!(
                extract_file_metrics(&files_metric),
                ExtractedFileMetrics {
                    level_0_selected: 2,
                    level_0_not_selected: 1,
                    level_1_selected: 4,
                    level_1_not_selected: 3,
                }
            );
            assert_eq!(
                extract_byte_metrics(&bytes_metric),
                ExtractedByteMetrics {
                    level_0: 20,
                    level_1: 40,
                }
            );

            // Increase max size; all level 0 files & their overlapping level 1 files get returned
            let (files_metric, bytes_metric) = metrics();
            let files = filter_cold_parquet_files(
                parquet_files_for_compaction,
                DEFAULT_MAX_FILE_SIZE,
                &files_metric,
                &bytes_metric,
            );
            let ids: Vec<_> = files.iter().map(|f| f.id.get()).collect();
            assert_eq!(ids, [102, 103, 104, 105, 106, 1, 2, 3]);
            assert_eq!(
                extract_file_metrics(&files_metric),
                ExtractedFileMetrics {
                    level_0_selected: 3,
                    level_0_not_selected: 0,
                    level_1_selected: 5,
                    level_1_not_selected: 2,
                }
            );
            assert_eq!(
                extract_byte_metrics(&bytes_metric),
                ExtractedByteMetrics {
                    level_0: 30,
                    level_1: 50,
                }
            );
        }
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
                file_size_bytes: 10,
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

        fn build(self) -> ParquetFile {
            let Self {
                compaction_level,
                id,
                min_time,
                max_time,
                file_size_bytes,
            } = self;

            ParquetFile {
                id: ParquetFileId::new(id),
                sequencer_id: SequencerId::new(2),
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
            }
        }
    }

    #[derive(Debug, PartialEq)]
    struct ExtractedFileMetrics {
        level_0_selected: u64,
        level_0_not_selected: u64,
        level_1_selected: u64,
        level_1_not_selected: u64,
    }

    fn extract_file_metrics(metric: &Metric<U64Gauge>) -> ExtractedFileMetrics {
        let level_0_selected = metric
            .get_observer(&Attributes::from(&[
                ("compaction_level", "0"),
                ("status", "selected_for_compaction"),
            ]))
            .unwrap()
            .fetch();

        let level_0_not_selected = metric
            .get_observer(&Attributes::from(&[
                ("compaction_level", "0"),
                ("status", "not_selected_for_compaction"),
            ]))
            .unwrap()
            .fetch();

        let level_1_selected = metric
            .get_observer(&Attributes::from(&[
                ("compaction_level", "1"),
                ("status", "selected_for_compaction"),
            ]))
            .unwrap()
            .fetch();

        let level_1_not_selected = metric
            .get_observer(&Attributes::from(&[
                ("compaction_level", "1"),
                ("status", "not_selected_for_compaction"),
            ]))
            .unwrap()
            .fetch();

        ExtractedFileMetrics {
            level_0_selected,
            level_0_not_selected,
            level_1_selected,
            level_1_not_selected,
        }
    }

    #[derive(Debug, PartialEq)]
    struct ExtractedByteMetrics {
        level_0: u64,
        level_1: u64,
    }

    fn extract_byte_metrics(metric: &Metric<U64Gauge>) -> ExtractedByteMetrics {
        let level_0 = metric
            .get_observer(&Attributes::from(&[("compaction_level", "0")]))
            .unwrap()
            .fetch();

        let level_1 = metric
            .get_observer(&Attributes::from(&[("compaction_level", "1")]))
            .unwrap()
            .fetch();

        ExtractedByteMetrics { level_0, level_1 }
    }
}
