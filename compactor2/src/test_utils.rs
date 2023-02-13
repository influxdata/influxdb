use std::{collections::BTreeMap, sync::Arc};

use data_types::{NamespaceId, PartitionId, PartitionKey, Table, TableId, TableSchema};

use crate::PartitionInfo;

pub fn partition_info() -> Arc<PartitionInfo> {
    let namespace_id = NamespaceId::new(2);
    let table_id = TableId::new(3);

    Arc::new(PartitionInfo {
        partition_id: PartitionId::new(1),
        namespace_id,
        namespace_name: String::from("ns"),
        table: Arc::new(Table {
            id: table_id,
            namespace_id,
            name: String::from("table"),
        }),
        table_schema: Arc::new(TableSchema {
            id: table_id,
            columns: BTreeMap::from([]),
        }),
        sort_key: None,
        partition_key: PartitionKey::from("pk"),
    })
}
