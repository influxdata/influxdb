use std::{fmt::Display, sync::Arc};

use async_trait::async_trait;
use data_types::{CompactionLevel, ParquetFileParams};
use datafusion::{error::DataFusionError, physical_plan::SendableRecordBatchStream};
use iox_time::Time;
use observability_deps::tracing::{info, warn};

use crate::partition_info::PartitionInfo;

use super::ParquetFileSink;

#[derive(Debug)]
pub struct LoggingParquetFileSinkWrapper<T>
where
    T: ParquetFileSink,
{
    inner: T,
}

impl<T> LoggingParquetFileSinkWrapper<T>
where
    T: ParquetFileSink,
{
    pub fn new(inner: T) -> Self {
        Self { inner }
    }
}

impl<T> Display for LoggingParquetFileSinkWrapper<T>
where
    T: ParquetFileSink,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "logging({})", self.inner)
    }
}

#[async_trait]
impl<T> ParquetFileSink for LoggingParquetFileSinkWrapper<T>
where
    T: ParquetFileSink,
{
    async fn store(
        &self,
        stream: SendableRecordBatchStream,
        partition: Arc<PartitionInfo>,
        level: CompactionLevel,
        max_l0_created_at: Time,
    ) -> Result<Option<ParquetFileParams>, DataFusionError> {
        let res = self
            .inner
            .store(stream, Arc::clone(&partition), level, max_l0_created_at)
            .await;
        match &res {
            Ok(Some(f)) => {
                info!(
                    partition_id = partition.partition_id.get(),
                    object_store_id=%f.object_store_id,
                    file_size_bytes=f.file_size_bytes,
                    "Stored file",
                )
            }
            Ok(None) => {
                warn!(
                    partition_id = partition.partition_id.get(),
                    "SplitExec produced an empty result stream"
                );
            }
            Err(e) => {
                warn!(
                    %e,
                    partition_id=partition.partition_id.get(),
                    "Error while uploading file",
                );
            }
        }
        res
    }
}
