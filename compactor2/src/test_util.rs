use std::collections::BTreeMap;

use data_types::{
    ColumnId, ColumnSchema, ColumnSet, ColumnType, CompactionLevel, NamespaceId, NamespaceSchema,
    ParquetFile, ParquetFileId, Partition, PartitionId, PartitionKey, QueryPoolId, SequenceNumber,
    ShardId, Table, TableId, TableSchema, Timestamp, TopicId,
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

#[derive(Debug)]
pub struct TableBuilder {
    table: Table,
}

impl TableBuilder {
    pub fn new(id: i64) -> Self {
        Self {
            table: Table {
                id: TableId::new(id),
                namespace_id: NamespaceId::new(0),
                name: "table".to_string(),
            },
        }
    }

    pub fn with_name(self, name: &str) -> Self {
        Self {
            table: Table {
                name: name.to_string(),
                ..self.table
            },
        }
    }

    pub fn build(self) -> Table {
        self.table
    }
}

#[derive(Debug)]
pub struct NamespaceBuilder {
    ns: NamespaceSchema,
}

impl NamespaceBuilder {
    pub fn new(id: i64) -> Self {
        let tables = BTreeMap::from([
            (
                "table1".to_string(),
                TableSchema {
                    id: TableId::new(1),
                    columns: BTreeMap::from([
                        (
                            "col1".to_string(),
                            ColumnSchema {
                                id: ColumnId::new(1),
                                column_type: ColumnType::I64,
                            },
                        ),
                        (
                            "col2".to_string(),
                            ColumnSchema {
                                id: ColumnId::new(2),
                                column_type: ColumnType::String,
                            },
                        ),
                    ]),
                },
            ),
            (
                "table2".to_string(),
                TableSchema {
                    id: TableId::new(2),
                    columns: BTreeMap::from([
                        (
                            "col1".to_string(),
                            ColumnSchema {
                                id: ColumnId::new(3),
                                column_type: ColumnType::I64,
                            },
                        ),
                        (
                            "col2".to_string(),
                            ColumnSchema {
                                id: ColumnId::new(4),
                                column_type: ColumnType::String,
                            },
                        ),
                        (
                            "col3".to_string(),
                            ColumnSchema {
                                id: ColumnId::new(5),
                                column_type: ColumnType::F64,
                            },
                        ),
                    ]),
                },
            ),
        ]);

        Self {
            ns: NamespaceSchema {
                id: NamespaceId::new(id),
                topic_id: TopicId::new(0),
                query_pool_id: QueryPoolId::new(0),
                tables,
                max_columns_per_table: 10,
                retention_period_ns: None,
            },
        }
    }

    pub fn build(self) -> NamespaceSchema {
        self.ns
    }
}

#[derive(Debug)]
pub struct PartitionBuilder {
    partition: Partition,
}

impl PartitionBuilder {
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

    pub fn build(self) -> Partition {
        self.partition
    }
}
