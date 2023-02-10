use std::{
    fmt::{Debug, Display},
    sync::Arc,
};

use async_trait::async_trait;
use data_types::{CompactionLevel, ParquetFileParams};
use datafusion::physical_plan::SendableRecordBatchStream;

use crate::{error::DynError, partition_info::PartitionInfo, plan_ir::PlanIR};

pub mod dispatch;
pub mod simulator;

#[async_trait]
pub trait ParquetFilesSink: Debug + Display + Send + Sync {
    /// Writes streams, which corresponds to the `plan_ir.files()` to
    /// parquet files on object store, returning information about the
    /// files that were created.
    async fn stream_into_file_sink(
        &self,
        streams: Vec<SendableRecordBatchStream>,
        partition_info: Arc<PartitionInfo>,
        target_level: CompactionLevel,
        plan_ir: &PlanIR,
    ) -> Result<Vec<ParquetFileParams>, DynError>;

    /// return this files sync as an Any dynamic object
    fn as_any(&self) -> &dyn std::any::Any;
}
