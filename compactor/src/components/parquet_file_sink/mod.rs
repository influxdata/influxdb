use std::{
    fmt::{Debug, Display},
    sync::Arc,
};

use async_trait::async_trait;
use data_types::{CompactionLevel, ParquetFileParams};
use datafusion::{error::DataFusionError, physical_plan::SendableRecordBatchStream};
use iox_time::Time;

use crate::partition_info::PartitionInfo;

pub mod dedicated;
pub mod logging;
pub mod mock;
pub mod object_store;

/// Writes streams if data to the object store as one or more parquet files
#[async_trait]
pub trait ParquetFileSink: Debug + Display + Send + Sync {
    async fn store(
        &self,
        stream: SendableRecordBatchStream,
        partition: Arc<PartitionInfo>,
        level: CompactionLevel,
        max_l0_created_at: Time,
    ) -> Result<Option<ParquetFileParams>, DataFusionError>;
}

#[async_trait]
impl<T> ParquetFileSink for Arc<T>
where
    T: ParquetFileSink + ?Sized,
{
    async fn store(
        &self,
        stream: SendableRecordBatchStream,
        partition: Arc<PartitionInfo>,
        level: CompactionLevel,
        max_l0_created_at: Time,
    ) -> Result<Option<ParquetFileParams>, DataFusionError> {
        self.as_ref()
            .store(stream, partition, level, max_l0_created_at)
            .await
    }
}
