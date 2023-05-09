use std::{collections::BTreeMap, sync::Arc};

use data_types::{
    ColumnId, ColumnSchema, ColumnType, NamespaceId, PartitionId, PartitionKey, Table, TableId,
    TableSchema,
};

use crate::PartitionInfo;

pub struct PartitionInfoBuilder {
    inner: PartitionInfo,
}

impl PartitionInfoBuilder {
    pub fn new() -> Self {
        let partition_id = PartitionId::new(1);
        let namespace_id = NamespaceId::new(2);
        let table_id = TableId::new(3);

        Self {
            inner: PartitionInfo {
                partition_id,
                namespace_id,
                namespace_name: String::from("ns"),
                table: Arc::new(Table {
                    id: TableId::new(3),
                    namespace_id,
                    name: String::from("table"),
                }),
                table_schema: Arc::new(TableSchema {
                    id: table_id,
                    columns: BTreeMap::new(),
                }),
                sort_key: None,
                partition_key: PartitionKey::from("key"),
            },
        }
    }

    pub fn with_partition_id(mut self, id: i64) -> Self {
        self.inner.partition_id = PartitionId::new(id);
        self
    }

    pub fn with_num_columns(mut self, num_cols: usize) -> Self {
        let mut columns = BTreeMap::new();
        for i in 0..num_cols {
            let col = ColumnSchema {
                id: ColumnId::new(i as i64),
                column_type: ColumnType::I64,
            };
            columns.insert(i.to_string(), col);
        }

        let table_schema = Arc::new(TableSchema {
            id: self.inner.table.id,
            columns,
        });
        self.inner.table_schema = table_schema;

        self
    }

    pub fn build(self) -> PartitionInfo {
        self.inner
    }
}
