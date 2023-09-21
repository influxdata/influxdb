use std::{fmt::Display, sync::Arc};

use async_trait::async_trait;
use data_types::{CompactionLevel, ParquetFileParams};
use datafusion::{
    error::DataFusionError, execution::memory_pool::MemoryPool,
    physical_plan::SendableRecordBatchStream,
};
use iox_time::{Time, TimeProvider};
use parquet_file::{
    metadata::IoxMetadata,
    serialize::CodecError,
    storage::{ParquetStorage, UploadError},
};
use uuid::Uuid;

use crate::partition_info::PartitionInfo;

use super::ParquetFileSink;

#[derive(Debug)]
pub struct ObjectStoreParquetFileSink {
    // pool on which to register parquet buffering
    pool: Arc<dyn MemoryPool>,
    store: ParquetStorage,
    time_provider: Arc<dyn TimeProvider>,
}

impl ObjectStoreParquetFileSink {
    pub fn new(
        pool: Arc<dyn MemoryPool>,
        store: ParquetStorage,
        time_provider: Arc<dyn TimeProvider>,
    ) -> Self {
        Self {
            pool,
            store,
            time_provider,
        }
    }
}

impl Display for ObjectStoreParquetFileSink {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "object_store")
    }
}

#[async_trait]
impl ParquetFileSink for ObjectStoreParquetFileSink {
    async fn store(
        &self,
        stream: SendableRecordBatchStream,
        partition: Arc<PartitionInfo>,
        level: CompactionLevel,
        max_l0_created_at: Time,
    ) -> Result<Option<ParquetFileParams>, DataFusionError> {
        let meta = IoxMetadata {
            object_store_id: Uuid::new_v4(),
            creation_timestamp: self.time_provider.now(),
            namespace_id: partition.namespace_id,
            namespace_name: partition.namespace_name.clone().into(),
            table_id: partition.table.id,
            table_name: partition.table.name.clone().into(),
            partition_key: partition.partition_key.clone(),
            compaction_level: level,
            sort_key: partition.sort_key.clone(),
            max_l0_created_at,
        };

        // Stream the record batches from the compaction exec, serialize
        // them, and directly upload the resulting Parquet files to
        // object storage.
        let pool = Arc::clone(&self.pool);
        let (parquet_meta, file_size) = match self
            .store
            .upload(stream, &partition.partition_id(), &meta, pool)
            .await
        {
            Ok(v) => v,
            Err(UploadError::Serialise(CodecError::NoRows | CodecError::NoRecordBatches)) => {
                // This MAY be a bug.
                //
                // This also may happen legitimately, though very, very
                // rarely. See test_empty_parquet_file_panic for an
                // explanation.
                return Ok(None);
            }
            Err(e) => {
                return Err(e.into());
            }
        };

        let parquet_file =
            meta.to_parquet_file(partition.partition_id(), file_size, &parquet_meta, |name| {
                partition
                    .table_schema
                    .columns
                    .get(name)
                    .expect("unknown column")
                    .id
            });

        Ok(Some(parquet_file))
    }
}
