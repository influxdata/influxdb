use std::fmt::{Debug, Display};

use async_trait::async_trait;
use data_types::ParquetFile;

use crate::{error::DynError, PartitionInfo};

pub mod and;
pub mod greater_matching_files;
pub mod greater_size_matching_files;
pub mod has_files;
pub mod has_matching_file;
pub mod logging;
pub mod max_num_columns;
pub mod metrics;
pub mod or;

/// Filters partition based on ID and Parquet files.
///
/// May return an error. In this case, the partition will be marked as "skipped".
#[async_trait]
pub trait PartitionFilter: Debug + Display + Send + Sync {
    /// Return `true` if the compactor should run a compaction on this partition. Return `false`
    /// if this partition does not need any more compaction.
    async fn apply(
        &self,
        partition_info: &PartitionInfo,
        files: &[ParquetFile],
    ) -> Result<bool, DynError>;
}

// Simple Partitions filters for testing purposes

/// True partition filter.
#[derive(Debug)]
pub struct TruePartitionFilter;

impl Display for TruePartitionFilter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "true")
    }
}

#[async_trait]
impl PartitionFilter for TruePartitionFilter {
    async fn apply(
        &self,
        _partition_info: &PartitionInfo,
        _files: &[ParquetFile],
    ) -> Result<bool, DynError> {
        Ok(true)
    }
}

impl TruePartitionFilter {
    #[allow(dead_code)]
    pub fn new() -> Self {
        Self
    }
}

/// False partition filter.
#[derive(Debug)]
pub struct FalsePartitionFilter;

impl Display for FalsePartitionFilter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "false")
    }
}

#[async_trait]
impl PartitionFilter for FalsePartitionFilter {
    async fn apply(
        &self,
        _partition_info: &PartitionInfo,
        _files: &[ParquetFile],
    ) -> Result<bool, DynError> {
        Ok(false)
    }
}

impl FalsePartitionFilter {
    #[allow(dead_code)]
    pub fn new() -> Self {
        Self
    }
}
