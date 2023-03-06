use std::fmt::Display;

use data_types::{CompactionLevel, ParquetFile};
use metric::{Registry, U64Histogram, U64HistogramOptions};

use super::SplitOrCompact;
use crate::{file_classification::FilesToCompactOrSplit, partition_info::PartitionInfo};

const METRIC_NAME_FILES_TO_SPLIT: &str = "iox_compactor_files_to_split";

#[derive(Debug)]
pub struct MetricsSplitOrCompactWrapper<T>
where
    T: SplitOrCompact,
{
    files_to_split: U64Histogram,
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

        Self {
            files_to_split,
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
    ) -> (FilesToCompactOrSplit, Vec<ParquetFile>) {
        let (files_to_split, files_not_to_split) =
            self.inner.apply(partition_info, files, target_level);

        if let FilesToCompactOrSplit::FilesToSplit(inner_files_to_split) = &files_to_split {
            if !inner_files_to_split.is_empty() {
                self.files_to_split
                    .record(inner_files_to_split.len() as u64);
            }
        }

        (files_to_split, files_not_to_split)
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

    use compactor2_test_utils::create_overlapped_l0_l1_files_2;
    use data_types::CompactionLevel;
    use metric::{Attributes, Metric};

    use crate::{
        components::split_or_compact::{split_compact::SplitCompact, SplitOrCompact},
        test_utils::PartitionInfoBuilder,
    };

    const MAX_SIZE: usize = 100;

    #[test]
    fn empty_records_nothing() {
        let registry = Registry::new();

        let files = vec![];
        let p_info = Arc::new(PartitionInfoBuilder::new().build());
        let split_compact =
            MetricsSplitOrCompactWrapper::new(SplitCompact::new(MAX_SIZE), &registry);
        let (files_to_compact_or_split, _files_to_keep) =
            split_compact.apply(&p_info, files, CompactionLevel::Initial);

        assert!(files_to_compact_or_split.is_empty());

        let hist = registry
            .get_instrument::<Metric<U64Histogram>>(METRIC_NAME_FILES_TO_SPLIT)
            .expect("failed to find metric with provided name")
            .get_observer(&Attributes::from(&[]))
            .expect("failed to find metric with provided attributes")
            .fetch();

        assert_eq!(hist.sample_count(), 0);
    }

    #[test]
    fn files_to_split_get_recorded() {
        let registry = Registry::new();

        let files = create_overlapped_l0_l1_files_2(MAX_SIZE as i64);
        let p_info = Arc::new(PartitionInfoBuilder::new().build());
        let split_compact =
            MetricsSplitOrCompactWrapper::new(SplitCompact::new(MAX_SIZE), &registry);
        let (files_to_compact_or_split, _files_to_keep) =
            split_compact.apply(&p_info, files, CompactionLevel::FileNonOverlapped);

        assert_eq!(files_to_compact_or_split.files_to_split_len(), 1);

        let hist = registry
            .get_instrument::<Metric<U64Histogram>>(METRIC_NAME_FILES_TO_SPLIT)
            .expect("failed to find metric with provided name")
            .get_observer(&Attributes::from(&[]))
            .expect("failed to find metric with provided attributes")
            .fetch();

        assert_eq!(hist.sample_count(), 1);
        assert_eq!(hist.total, 1);
    }
}
