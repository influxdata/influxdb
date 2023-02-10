use std::{fmt::Display, sync::Arc};

use async_trait::async_trait;
use data_types::{CompactionLevel, ParquetFileParams};
use datafusion::physical_plan::SendableRecordBatchStream;
use futures::{stream::FuturesOrdered, TryFutureExt, TryStreamExt};
use iox_time::Time;

use crate::{
    components::parquet_file_sink::ParquetFileSink, error::DynError, partition_info::PartitionInfo,
    plan_ir::PlanIR,
};

use super::ParquetFilesSink;

#[derive(Debug)]
/// Writes parquet files to an inner [`ParquetFileSink`] (note the
/// lack of "s").
pub struct DispatchParquetFilesSink<T>
where
    T: ParquetFileSink,
{
    inner: Arc<T>,
}

impl<T> DispatchParquetFilesSink<T>
where
    T: ParquetFileSink,
{
    pub fn new(inner: T) -> Self {
        Self {
            inner: Arc::new(inner),
        }
    }
}

impl<T> Display for DispatchParquetFilesSink<T>
where
    T: ParquetFileSink,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "dispatch({})", self.inner)
    }
}

#[async_trait]
impl<T> ParquetFilesSink for DispatchParquetFilesSink<T>
where
    T: ParquetFileSink + 'static,
{
    async fn stream_into_file_sink(
        &self,
        streams: Vec<SendableRecordBatchStream>,
        partition_info: Arc<PartitionInfo>,
        target_level: CompactionLevel,
        plan_ir: &PlanIR,
    ) -> Result<Vec<ParquetFileParams>, DynError> {
        // compute max_l0_created_at
        let max_l0_created_at: Time = plan_ir
            .input_files()
            .iter()
            .map(|f| f.file.max_l0_created_at)
            .max()
            .expect("max_l0_created_at should have value")
            .into();

        let inner = Arc::clone(&self.inner);
        streams
            .into_iter()
            .map(move |stream| {
                let inner = Arc::clone(&inner);
                let partition_info = Arc::clone(&partition_info);
                async move {
                    inner
                        .store(stream, partition_info, target_level, max_l0_created_at)
                        .await
                }
            })
            // NB: FuturesOrdered allows the futures to run in parallel
            .collect::<FuturesOrdered<_>>()
            // Discard the streams that resulted in empty output / no file uploaded
            // to the object store.
            .try_filter_map(|v| futures::future::ready(Ok(v)))
            // Collect all the persisted parquet files together.
            .try_collect::<Vec<_>>()
            .map_err(|e| Box::new(e) as _)
            .await
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}
