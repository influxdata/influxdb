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
