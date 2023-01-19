use data_types::{
    ColumnSet, CompactionLevel, NamespaceId, ParquetFile, ParquetFileId, PartitionId,
    SequenceNumber, ShardId, TableId, Timestamp,
};
use uuid::Uuid;

#[derive(Debug)]
pub struct ParquetFileBuilder {
    file: ParquetFile,
}

impl ParquetFileBuilder {
    pub fn new(id: i64) -> Self {
        Self {
            file: ParquetFile {
                id: ParquetFileId::new(id),
                shard_id: ShardId::new(0),
                namespace_id: NamespaceId::new(0),
                table_id: TableId::new(0),
                partition_id: PartitionId::new(0),
                object_store_id: Uuid::from_u128(id.try_into().expect("invalid id")),
                max_sequence_number: SequenceNumber::new(0),
                min_time: Timestamp::new(0),
                max_time: Timestamp::new(0),
                to_delete: None,
                file_size_bytes: 1,
                row_count: 1,
                compaction_level: CompactionLevel::FileNonOverlapped,
                created_at: Timestamp::new(0),
                column_set: ColumnSet::new(vec![]),
            },
        }
    }

    pub fn with_partition(self, id: i64) -> Self {
        Self {
            file: ParquetFile {
                partition_id: PartitionId::new(id),
                ..self.file
            },
        }
    }

    pub fn build(self) -> ParquetFile {
        self.file
    }
}
