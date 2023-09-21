use std::fmt::Display;

use data_types::{CompactionLevel, ParquetFile};
use metric::{Registry, U64Counter, U64Histogram, U64HistogramOptions};

use super::SplitOrCompact;
use crate::{file_classification::FilesToSplitOrCompact, partition_info::PartitionInfo};

const METRIC_NAME_FILES_TO_SPLIT: &str = "iox_compactor_files_to_split";
const METRIC_NAME_SPLIT_DECISION_COUNT: &str = "iox_compactor_split_decision";
const METRIC_NAME_COMPACT_DECISION_COUNT: &str = "iox_compactor_compact_decision";

#[derive(Debug)]
pub struct MetricsSplitOrCompactWrapper<T>
where
    T: SplitOrCompact,
{
    files_to_split: U64Histogram,
    split_decision_count: U64Counter,
    compact_decision_count: U64Counter,
    inner: T,
}

impl<T> MetricsSplitOrCompactWrapper<T>
where
    T: SplitOrCompact,
{
    pub fn new(inner: T, registry: &Registry) -> Self {
        let files_to_split = registry
            .register_metric_with_options::<U64Histogram, _>(
                METRIC_NAME_FILES_TO_SPLIT,
                "Number of files needing to be split to minimize overlap",
                || U64HistogramOptions::new([1, 10, 100, 1_000, 10_000, u64::MAX]),
            )
            .recorder(&[]);

        let split_decision_count = registry
            .register_metric::<U64Counter>(
                METRIC_NAME_SPLIT_DECISION_COUNT,
                "Number of times the compactor decided to split files",
            )
            .recorder(&[]);

        let compact_decision_count = registry
            .register_metric::<U64Counter>(
                METRIC_NAME_COMPACT_DECISION_COUNT,
                "Number of times the compactor decided to compact files",
            )
            .recorder(&[]);

        Self {
            files_to_split,
            split_decision_count,
            compact_decision_count,
            inner,
        }
    }
}

impl<T> SplitOrCompact for MetricsSplitOrCompactWrapper<T>
where
    T: SplitOrCompact,
{
    fn apply(
        &self,
        partition_info: &PartitionInfo,
        files: Vec<ParquetFile>,
        target_level: CompactionLevel,
    ) -> (FilesToSplitOrCompact, Vec<ParquetFile>) {
        let (files_to_split_or_compact, files_to_keep) =
            self.inner.apply(partition_info, files, target_level);

        match &files_to_split_or_compact {
            FilesToSplitOrCompact::Split(..) => {
                self.files_to_split
                    .record(files_to_split_or_compact.num_files_to_split() as u64);
                self.split_decision_count.inc(1);
            }
            FilesToSplitOrCompact::Compact(..) => self.compact_decision_count.inc(1),
            _ => {} // Nothing to do
        }

        (files_to_split_or_compact, files_to_keep)
    }
}

impl<T> Display for MetricsSplitOrCompactWrapper<T>
where
    T: SplitOrCompact,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "metrics({})", self.inner)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::sync::Arc;

    use compactor_test_utils::{create_overlapped_l0_l1_files_2, create_overlapped_l1_l2_files_2};
    use data_types::CompactionLevel;
    use metric::{assert_counter, assert_histogram};

    use crate::{
        components::split_or_compact::{split_compact::SplitCompact, SplitOrCompact},
        test_utils::PartitionInfoBuilder,
    };

    const MAX_FILE: usize = 100;

    #[test]
    fn empty_records_nothing() {
        let registry = Registry::new();

        let files = vec![];
        let p_info = Arc::new(PartitionInfoBuilder::new().build());
        let split_compact = MetricsSplitOrCompactWrapper::new(
            SplitCompact::new(MAX_FILE, MAX_FILE, MAX_FILE as u64),
            &registry,
        );
        let (_files_to_split_or_compact, _files_to_keep) =
            split_compact.apply(&p_info, files, CompactionLevel::Initial);

        assert_histogram!(
            registry,
            U64Histogram,
            METRIC_NAME_FILES_TO_SPLIT,
            samples = 0,
        );
        assert_counter!(
            registry,
            U64Counter,
            METRIC_NAME_SPLIT_DECISION_COUNT,
            value = 0,
        );
        assert_counter!(
            registry,
            U64Counter,
            METRIC_NAME_COMPACT_DECISION_COUNT,
            value = 0,
        );
    }

    #[test]
    fn files_to_split_get_recorded() {
        let registry = Registry::new();

        let files = create_overlapped_l0_l1_files_2(MAX_FILE as i64);
        let p_info = Arc::new(PartitionInfoBuilder::new().build());
        let split_compact = MetricsSplitOrCompactWrapper::new(
            SplitCompact::new(MAX_FILE, MAX_FILE, MAX_FILE as u64),
            &registry,
        );
        let (_files_to_split_or_compact, _files_to_keep) =
            split_compact.apply(&p_info, files, CompactionLevel::FileNonOverlapped);

        assert_histogram!(
            registry,
            U64Histogram,
            METRIC_NAME_FILES_TO_SPLIT,
            samples = 1,
            sum = 1,
        );
        assert_counter!(
            registry,
            U64Counter,
            METRIC_NAME_SPLIT_DECISION_COUNT,
            value = 1,
        );
        assert_counter!(
            registry,
            U64Counter,
            METRIC_NAME_COMPACT_DECISION_COUNT,
            value = 0,
        );
    }

    #[test]
    fn files_to_compact_get_recorded() {
        let registry = Registry::new();

        let files = create_overlapped_l1_l2_files_2(MAX_FILE as i64);
        let p_info = Arc::new(PartitionInfoBuilder::new().build());
        let split_compact = MetricsSplitOrCompactWrapper::new(
            SplitCompact::new(MAX_FILE, MAX_FILE * 3, MAX_FILE as u64),
            &registry,
        );
        let (_files_to_split_or_compact, _files_to_keep) =
            split_compact.apply(&p_info, files, CompactionLevel::Final);

        assert_histogram!(
            registry,
            U64Histogram,
            METRIC_NAME_FILES_TO_SPLIT,
            samples = 0,
        );
        assert_counter!(
            registry,
            U64Counter,
            METRIC_NAME_SPLIT_DECISION_COUNT,
            value = 0,
        );
        assert_counter!(
            registry,
            U64Counter,
            METRIC_NAME_COMPACT_DECISION_COUNT,
            value = 1,
        );
    }
}
