use std::{
    fmt::Display,
    sync::{Arc, Mutex},
};

use async_trait::async_trait;
use data_types::{
    ColumnSet, CompactionLevel, ParquetFileParams, SequenceNumber, ShardId, Timestamp,
};
use datafusion::{
    arrow::{datatypes::SchemaRef, record_batch::RecordBatch},
    error::DataFusionError,
    physical_plan::SendableRecordBatchStream,
};
use futures::TryStreamExt;
use iox_time::Time;
use uuid::Uuid;

use crate::partition_info::PartitionInfo;

use super::ParquetFileSink;

#[derive(Debug, Clone)]
pub struct StoredFile {
    pub batches: Vec<RecordBatch>,
    pub level: CompactionLevel,
    pub partition: Arc<PartitionInfo>,
    pub schema: SchemaRef,
}

#[derive(Debug, Default)]
pub struct MockParquetFileSink {
    records: Mutex<Vec<StoredFile>>,
}

impl MockParquetFileSink {
    #[allow(dead_code)] // not used anywhere
    pub fn new() -> Self {
        Self::default()
    }

    #[allow(dead_code)] // not used anywhere
    pub fn records(&self) -> Vec<StoredFile> {
        self.records.lock().expect("not poisoned").clone()
    }
}

impl Display for MockParquetFileSink {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "mock")
    }
}

#[async_trait]
impl ParquetFileSink for MockParquetFileSink {
    async fn store(
        &self,
        stream: SendableRecordBatchStream,
        partition: Arc<PartitionInfo>,
        level: CompactionLevel,
        max_l0_created_at: Time,
    ) -> Result<Option<ParquetFileParams>, DataFusionError> {
        let schema = stream.schema();
        let batches: Vec<_> = stream.try_collect().await?;
        let row_count = batches.iter().map(|b| b.num_rows()).sum::<usize>();
        let mut guard = self.records.lock().expect("not poisoned");
        let out = (row_count > 0).then(|| ParquetFileParams {
            shard_id: ShardId::new(1),
            namespace_id: partition.namespace_id,
            table_id: partition.table.id,
            partition_id: partition.partition_id,
            object_store_id: Uuid::from_u128(guard.len() as u128),
            max_sequence_number: SequenceNumber::new(0),
            min_time: Timestamp::new(0),
            max_time: Timestamp::new(0),
            file_size_bytes: 1,
            row_count: 1,
            compaction_level: level,
            created_at: Timestamp::new(1),
            column_set: ColumnSet::new(vec![]),
            max_l0_created_at: max_l0_created_at.into(),
        });
        guard.push(StoredFile {
            batches,
            level,
            partition,
            schema,
        });
        Ok(out)
    }
}

#[cfg(test)]
mod tests {
    use arrow_util::assert_batches_eq;
    use data_types::{NamespaceId, PartitionId, TableId};
    use datafusion::{
        arrow::{array::new_null_array, datatypes::DataType},
        physical_plan::stream::RecordBatchStreamAdapter,
    };
    use schema::SchemaBuilder;

    use crate::test_util::partition_info;

    use super::*;

    #[test]
    fn test_display() {
        assert_eq!(MockParquetFileSink::new().to_string(), "mock");
    }

    #[tokio::test]
    async fn test_store() {
        let sink = MockParquetFileSink::new();

        let schema = SchemaBuilder::new()
            .field("f", DataType::Int64)
            .unwrap()
            .build()
            .unwrap()
            .as_arrow();
        let partition = partition_info();
        let level = CompactionLevel::FileNonOverlapped;
        let max_l0_created_at = Time::from_timestamp_nanos(1);

        let stream = Box::pin(RecordBatchStreamAdapter::new(
            Arc::clone(&schema),
            futures::stream::empty(),
        ));
        assert_eq!(
            sink.store(stream, Arc::clone(&partition), level, max_l0_created_at)
                .await
                .unwrap(),
            None,
        );

        let record_batch = RecordBatch::new_empty(Arc::clone(&schema));
        let record_batch_captured = record_batch.clone();
        let stream = Box::pin(RecordBatchStreamAdapter::new(
            Arc::clone(&schema),
            futures::stream::once(async move { Ok(record_batch_captured) }),
        ));
        assert_eq!(
            sink.store(stream, Arc::clone(&partition), level, max_l0_created_at)
                .await
                .unwrap(),
            None,
        );

        let record_batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![new_null_array(&DataType::Int64, 1)],
        )
        .unwrap();
        let record_batch_captured = record_batch.clone();
        let stream = Box::pin(RecordBatchStreamAdapter::new(
            Arc::clone(&schema),
            futures::stream::once(async move { Ok(record_batch_captured) }),
        ));
        assert_eq!(
            sink.store(stream, Arc::clone(&partition), level, max_l0_created_at)
                .await
                .unwrap(),
            Some(ParquetFileParams {
                shard_id: ShardId::new(1),
                namespace_id: NamespaceId::new(2),
                table_id: TableId::new(3),
                partition_id: PartitionId::new(1),
                object_store_id: Uuid::from_u128(2),
                max_sequence_number: SequenceNumber::new(0),
                min_time: Timestamp::new(0),
                max_time: Timestamp::new(0),
                file_size_bytes: 1,
                row_count: 1,
                compaction_level: CompactionLevel::FileNonOverlapped,
                created_at: Timestamp::new(1),
                column_set: ColumnSet::new([]),
                max_l0_created_at: max_l0_created_at.into(),
            }),
        );

        let records = sink.records();
        assert_eq!(records.len(), 3);

        assert_eq!(records[0].batches.len(), 0);
        assert_eq!(records[0].schema, schema);
        assert_eq!(records[0].level, level);
        assert_eq!(records[0].partition, partition);

        assert_batches_eq!(["+---+", "| f |", "+---+", "+---+",], &records[1].batches);
        assert_eq!(records[1].batches.len(), 1);
        assert_eq!(records[1].schema, schema);
        assert_eq!(records[1].level, level);
        assert_eq!(records[1].partition, partition);

        assert_batches_eq!(
            ["+---+", "| f |", "+---+", "|   |", "+---+",],
            &records[2].batches
        );
        assert_eq!(records[2].batches.len(), 1);
        assert_eq!(records[2].schema, schema);
        assert_eq!(records[2].level, level);
        assert_eq!(records[2].partition, partition);
    }
}
