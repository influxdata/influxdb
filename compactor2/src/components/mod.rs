use std::sync::Arc;

use self::{
    file_filter::FileFilter, partition_files_source::PartitionFilesSource,
    partition_filter::PartitionFilter, partitions_source::PartitionsSource,
};

pub mod file_filter;
pub mod hardcoded;
pub mod partition_files_source;
pub mod partition_filter;
pub mod partitions_source;

#[derive(Debug)]
pub struct Components {
    pub partitions_source: Arc<dyn PartitionsSource>,
    pub partition_files_source: Arc<dyn PartitionFilesSource>,
    pub file_filters: Vec<Arc<dyn FileFilter>>,
    pub partition_filters: Vec<Arc<dyn PartitionFilter>>,
}
