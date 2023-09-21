use std::fmt::Display;

use data_types::ParquetFile;
use observability_deps::tracing::info;

use crate::{
    file_classification::FileClassification, partition_info::PartitionInfo, round_info::CompactType,
};

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
        op: &CompactType,
        files: Vec<ParquetFile>,
    ) -> FileClassification {
        let classification = self.inner.classify(partition_info, op, files);

        info!(
            partition_id = partition_info.partition_id.get(),
            target_level = %classification.target_level,
            op = %op,
            files_to_compact = classification.num_files_to_compact(),
            files_to_split = classification.num_files_to_split(),
            files_to_upgrade = classification.num_files_to_upgrade(),
            files_to_keep = classification.num_files_to_keep(),
            "file classification"
        );

        classification
    }
}
