use std::{fmt::Display, sync::Arc};

use async_trait::async_trait;
use data_types::{CompactionLevel, ParquetFileParams};
use datafusion::{error::DataFusionError, physical_plan::SendableRecordBatchStream};
use iox_query::exec::{Executor, ExecutorType};

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
    ) -> Result<Option<ParquetFileParams>, DataFusionError> {
        let inner = Arc::clone(&self.inner);
        self.exec
            .executor(ExecutorType::Reorg)
            .spawn(async move { inner.store(stream, partition, level).await })
            .await
            .map_err(|e| DataFusionError::External(e.into()))?
    }
}
