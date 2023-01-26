use std::{fmt::Display, sync::Arc};

use async_trait::async_trait;
use data_types::{CompactionLevel, ParquetFileParams};
use datafusion::{error::DataFusionError, physical_plan::SendableRecordBatchStream};
use iox_query::exec::{Executor, ExecutorType};
use iox_time::Time;

use crate::partition_info::PartitionInfo;

use super::ParquetFileSink;

#[derive(Debug)]
pub struct DedicatedExecParquetFileSinkWrapper<T>
where
    T: ParquetFileSink + 'static,
{
    exec: Arc<Executor>,
    inner: Arc<T>,
}

impl<T> DedicatedExecParquetFileSinkWrapper<T>
where
    T: ParquetFileSink + 'static,
{
    pub fn new(inner: T, exec: Arc<Executor>) -> Self {
        Self {
            inner: Arc::new(inner),
            exec,
        }
    }
}

impl<T> Display for DedicatedExecParquetFileSinkWrapper<T>
where
    T: ParquetFileSink + 'static,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "dedicated_exec({})", self.inner)
    }
}

#[async_trait]
impl<T> ParquetFileSink for DedicatedExecParquetFileSinkWrapper<T>
where
    T: ParquetFileSink + 'static,
{
    async fn store(
        &self,
        stream: SendableRecordBatchStream,
        partition: Arc<PartitionInfo>,
        level: CompactionLevel,
        max_l0_created_at: Time,
    ) -> Result<Option<ParquetFileParams>, DataFusionError> {
        let inner = Arc::clone(&self.inner);
        self.exec
            .executor(ExecutorType::Reorg)
            .spawn(async move {
                inner
                    .store(stream, partition, level, max_l0_created_at)
                    .await
            })
            .await
            .map_err(|e| DataFusionError::External(e.into()))?
    }
}

#[cfg(test)]
mod tests {
    use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
    use schema::SchemaBuilder;

    use crate::{
        components::parquet_file_sink::mock::MockParquetFileSink, test_util::partition_info,
    };

    use super::*;

    #[test]
    fn test_display() {
        let sink = DedicatedExecParquetFileSinkWrapper::new(
            MockParquetFileSink::new(),
            Arc::new(Executor::new_testing()),
        );
        assert_eq!(sink.to_string(), "dedicated_exec(mock)",)
    }

    #[tokio::test]
    async fn test_panic() {
        let sink = DedicatedExecParquetFileSinkWrapper::new(
            MockParquetFileSink::new(),
            Arc::new(Executor::new_testing()),
        );
        let schema = SchemaBuilder::new().build().unwrap().as_arrow();
        let stream = Box::pin(RecordBatchStreamAdapter::new(
            Arc::clone(&schema),
            futures::stream::once(async move { panic!("foo") }),
        ));
        let partition = partition_info();
        let level = CompactionLevel::FileNonOverlapped;
        let max_l0_created_at = Time::from_timestamp_nanos(0);
        let err = sink
            .store(stream, partition, level, max_l0_created_at)
            .await
            .unwrap_err();
        assert_eq!(err.to_string(), "External error: foo",);
    }
}
