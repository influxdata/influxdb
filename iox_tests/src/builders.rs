use data_types::{
    ColumnSet, CompactionLevel, NamespaceId, ParquetFile, ParquetFileId, Partition, PartitionId,
    PartitionKey, SequenceNumber, ShardId, SkippedCompaction, Table, TableId, Timestamp,
};
use uuid::Uuid;

#[derive(Debug, Clone)]
/// Build up [`ParquetFile`]s for testing
pub struct ParquetFileBuilder {
    file: ParquetFile,
}

impl ParquetFileBuilder {
    /// Create a builder that will create a parquet file with
    /// `parquet_id` of `id`
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
                max_l0_created_at: Timestamp::new(0),
            },
        }
    }

    /// Set the partition id
    pub fn with_partition(self, id: i64) -> Self {
        Self {
            file: ParquetFile {
                partition_id: PartitionId::new(id),
                ..self.file
            },
        }
    }

    /// Set the compaction level
    pub fn with_compaction_level(self, level: CompactionLevel) -> Self {
        Self {
            file: ParquetFile {
                compaction_level: level,
                ..self.file
            },
        }
    }

    /// Set the file size
    pub fn with_file_size_bytes(self, file_size_bytes: i64) -> Self {
        Self {
            file: ParquetFile {
                file_size_bytes,
                ..self.file
            },
        }
    }

    /// Set the min/max time range
    pub fn with_time_range(self, min_time: i64, max_time: i64) -> Self {
        Self {
            file: ParquetFile {
                min_time: Timestamp::new(min_time),
                max_time: Timestamp::new(max_time),
                ..self.file
            },
        }
    }

    /// Set the row_count
    pub fn with_row_count(self, row_count: i64) -> Self {
        Self {
            file: ParquetFile {
                row_count,
                ..self.file
            },
        }
    }

    /// Set max_l0_created_at
    pub fn with_max_l0_created_at(self, max_l0_created_at: i64) -> Self {
        Self {
            file: ParquetFile {
                max_l0_created_at: Timestamp::new(max_l0_created_at),
                ..self.file
            },
        }
    }

    /// Create the [`ParquetFile`]
    pub fn build(self) -> ParquetFile {
        self.file
    }
}

impl From<ParquetFile> for ParquetFileBuilder {
    fn from(file: ParquetFile) -> Self {
        Self { file }
    }
}

#[derive(Debug)]
/// Build  [`Table`]s for testing
pub struct TableBuilder {
    table: Table,
}

impl TableBuilder {
    /// Create a builder to create a table with `table_id` `id`
    pub fn new(id: i64) -> Self {
        Self {
            table: Table {
                id: TableId::new(id),
                namespace_id: NamespaceId::new(0),
                name: "table".to_string(),
            },
        }
    }

    /// Set the table name
    pub fn with_name(self, name: &str) -> Self {
        Self {
            table: Table {
                name: name.to_string(),
                ..self.table
            },
        }
    }

    /// Create the table
    pub fn build(self) -> Table {
        self.table
    }
}

#[derive(Debug)]
/// Builds  [`Partition`]s for testing
pub struct PartitionBuilder {
    partition: Partition,
}

impl PartitionBuilder {
    /// Create a builder to create a partition with `partition_id` `id`
    pub fn new(id: i64) -> Self {
        Self {
            partition: Partition {
                id: PartitionId::new(id),
                shard_id: ShardId::new(0),
                table_id: TableId::new(0),
                partition_key: PartitionKey::from("key"),
                sort_key: vec![],
                persisted_sequence_number: None,
                new_file_at: None,
            },
        }
    }

    /// Create the partition
    pub fn build(self) -> Partition {
        self.partition
    }
}

#[derive(Debug)]
/// A builder to create a skipped compaction record
pub struct SkippedCompactionBuilder {
    skipped_compaction: SkippedCompaction,
}

impl SkippedCompactionBuilder {
    /// Create the builder for skipped_compaction_id = id
    pub fn new(id: i64) -> Self {
        Self {
            skipped_compaction: SkippedCompaction {
                partition_id: PartitionId::new(id),
                reason: "test skipped compaction".to_string(),
                skipped_at: Timestamp::new(0),
                num_files: 0,
                limit_num_files: 0,
                estimated_bytes: 0,
                limit_bytes: 0,
                limit_num_files_first_in_partition: 0,
            },
        }
    }

    /// Add a reason for the skipped compaction
    pub fn with_reason(self, reason: &str) -> Self {
        Self {
            skipped_compaction: SkippedCompaction {
                reason: reason.to_string(),
                ..self.skipped_compaction
            },
        }
    }

    /// Build the skipped compaction
    pub fn build(self) -> SkippedCompaction {
        self.skipped_compaction
    }
}
