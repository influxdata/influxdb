use std::fmt::Display;

use data_types::ParquetFile;
use observability_deps::tracing::info;

use crate::{file_classification::FileClassification, partition_info::PartitionInfo, RoundInfo};

use super::FileClassifier;

#[derive(Debug)]
pub struct LoggingFileClassifierWrapper<T>
where
    T: FileClassifier,
{
    inner: T,
}

impl<T> LoggingFileClassifierWrapper<T>
where
    T: FileClassifier,
{
    pub fn new(inner: T) -> Self {
        Self { inner }
    }
}

impl<T> Display for LoggingFileClassifierWrapper<T>
where
    T: FileClassifier,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "display({})", self.inner)
    }
}

impl<T> FileClassifier for LoggingFileClassifierWrapper<T>
where
    T: FileClassifier,
{
    fn classify(
        &self,
        partition_info: &PartitionInfo,
        round_info: &RoundInfo,
        files: Vec<ParquetFile>,
    ) -> FileClassification {
        let classification = self.inner.classify(partition_info, round_info, files);

        info!(
            partition_id = partition_info.partition_id.get(),
            target_level = %classification.target_level,
            round_info = %round_info,
            files_to_compacts = classification.files_to_compact.len(),
            files_to_upgrade = classification.files_to_upgrade.len(),
            files_to_keep = classification.files_to_keep.len(),
            "file classification"
        );

        classification
    }
}
