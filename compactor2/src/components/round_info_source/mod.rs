use std::{
    fmt::{Debug, Display},
    sync::Arc,
};

use async_trait::async_trait;
use data_types::ParquetFile;
use observability_deps::tracing::debug;

use crate::{error::DynError, PartitionInfo, RoundInfo};

/// Calculates information about what this compaction round does
#[async_trait]
pub trait RoundInfoSource: Debug + Display + Send + Sync {
    async fn calculate(
        &self,
        partition_info: &PartitionInfo,
        files: &[ParquetFile],
    ) -> Result<Arc<RoundInfo>, DynError>;
}

#[derive(Debug)]
pub struct LoggingRoundInfoWrapper {
    inner: Arc<dyn RoundInfoSource>,
}

impl LoggingRoundInfoWrapper {
    pub fn new(inner: Arc<dyn RoundInfoSource>) -> Self {
        Self { inner }
    }
}

impl Display for LoggingRoundInfoWrapper {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "LoggingRoundInfoWrapper({})", self.inner)
    }
}

#[async_trait]
impl RoundInfoSource for LoggingRoundInfoWrapper {
    async fn calculate(
        &self,
        partition_info: &PartitionInfo,
        files: &[ParquetFile],
    ) -> Result<Arc<RoundInfo>, DynError> {
        let res = self.inner.calculate(partition_info, files).await;
        if let Ok(round_info) = &res {
            debug!(round_info_source=%self.inner, %round_info, "running round");
        }
        res
    }
}

/// Computes the type of round based on the levels of the input files
#[derive(Debug)]
pub struct LevelBasedRoundInfo {}

impl Display for LevelBasedRoundInfo {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "LevelBasedRoundInfo")
    }
}
impl LevelBasedRoundInfo {
    pub fn new() -> Self {
        Self {}
    }
}

#[async_trait]
impl RoundInfoSource for LevelBasedRoundInfo {
    async fn calculate(
        &self,
        _partition_info: &PartitionInfo,
        _files: &[ParquetFile],
    ) -> Result<Arc<RoundInfo>, DynError> {
        // TODO: use this to calculate splits
        Ok(Arc::new(RoundInfo::TargetLevel {
            target_level: data_types::CompactionLevel::Initial,
        }))
    }
}
