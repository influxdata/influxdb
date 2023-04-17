use std::sync::Arc;

use data_types::{
    Column, ColumnId, ColumnType, ColumnsByName, NamespaceId, PartitionHashId, PartitionId,
    PartitionKey, Table, TableId, TableSchema,
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
        let partition_key = PartitionKey::from("key");
        let partition_hash_id = Some(PartitionHashId::new(table_id, &partition_key));
        let table = Arc::new(Table {
            id: table_id,
            namespace_id,
            name: String::from("table"),
            partition_template: Default::default(),
        });
        let table_schema = Arc::new(TableSchema::new_empty_from(&table));

        Self {
            inner: PartitionInfo {
                partition_id,
                partition_hash_id,
                namespace_id,
                namespace_name: String::from("ns"),
                table,
                table_schema,
                sort_key: None,
                partition_key,
            },
        }
    }

    pub fn with_partition_id(mut self, id: i64) -> Self {
        self.inner.partition_id = PartitionId::new(id);
        self
    }

    pub fn with_num_columns(mut self, num_cols: usize) -> Self {
        let columns: Vec<_> = (0..num_cols)
            .map(|i| Column {
                id: ColumnId::new(i as i64),
                name: i.to_string(),
                column_type: ColumnType::I64,
                table_id: self.inner.table.id,
            })
            .collect();

        let table_schema = Arc::new(TableSchema {
            id: self.inner.table.id,
            partition_template: Default::default(),
            columns: ColumnsByName::new(columns),
        });
        self.inner.table_schema = table_schema;

        self
    }

    pub fn build(self) -> PartitionInfo {
        self.inner
    }
}
