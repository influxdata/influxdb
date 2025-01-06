//! A set of helper methods for creating WAL operations in tests.

use std::sync::Arc;

use influxdb3_id::{ColumnId, DbId};

use crate::*;

/// Create a new [`WalContents`] with the provided arguments that will have a `persisted_timestamp_ns`
/// of `0`.
pub fn wal_contents(
    (min_timestamp_ns, max_timestamp_ns, wal_file_number): (i64, i64, u64),
    ops: impl IntoIterator<Item = WalOp>,
) -> WalContents {
    WalContents {
        persist_timestamp_ms: 0,
        min_timestamp_ns,
        max_timestamp_ns,
        wal_file_number: WalFileSequenceNumber::new(wal_file_number),
        ops: ops.into_iter().collect(),
    }
}

/// Create a new [`WalContents`] with the provided arguments that will have a `persisted_timestamp_ns`
/// of `0`.
pub fn wal_contents_with_snapshot(
    (min_timestamp_ns, max_timestamp_ns, wal_file_number): (i64, i64, u64),
    ops: impl IntoIterator<Item = WalOp>,
    snapshot: SnapshotDetails,
) -> WalContents {
    let mut wal_ops: Vec<WalOp> = ops.into_iter().collect();
    wal_ops.push(WalOp::Snapshot(snapshot));
    WalContents {
        persist_timestamp_ms: 0,
        min_timestamp_ns,
        max_timestamp_ns,
        wal_file_number: WalFileSequenceNumber::new(wal_file_number),
        ops: wal_ops,
    }
}

pub fn write_batch_op(write_batch: WriteBatch) -> WalOp {
    WalOp::Write(write_batch)
}

pub fn catalog_batch(
    db_id: DbId,
    db_name: impl Into<Arc<str>>,
    time_ns: i64,
    ops: impl IntoIterator<Item = CatalogOp>,
) -> CatalogBatch {
    CatalogBatch {
        database_id: db_id,
        database_name: db_name.into(),
        time_ns,
        ops: ops.into_iter().collect(),
    }
}

pub fn add_fields_op(
    database_id: DbId,
    db_name: impl Into<Arc<str>>,
    table_id: TableId,
    table_name: impl Into<Arc<str>>,
    fields: impl IntoIterator<Item = FieldDefinition>,
) -> CatalogOp {
    CatalogOp::AddFields(FieldAdditions {
        database_name: db_name.into(),
        database_id,
        table_name: table_name.into(),
        table_id,
        field_definitions: fields.into_iter().collect(),
    })
}

pub fn create_table_op(
    db_id: DbId,
    db_name: impl Into<Arc<str>>,
    table_id: TableId,
    table_name: impl Into<Arc<str>>,
    fields: impl IntoIterator<Item = FieldDefinition>,
    key: impl IntoIterator<Item = ColumnId>,
) -> CatalogOp {
    CatalogOp::CreateTable(TableDefinition {
        database_id: db_id,
        database_name: db_name.into(),
        table_name: table_name.into(),
        table_id,
        field_definitions: fields.into_iter().collect(),
        key: key.into_iter().collect(),
    })
}

pub fn field_def(
    id: ColumnId,
    name: impl Into<Arc<str>>,
    data_type: FieldDataType,
) -> FieldDefinition {
    FieldDefinition {
        name: name.into(),
        data_type,
        id,
    }
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct CreateLastCacheOpBuilder {
    table_id: TableId,
    table_name: Arc<str>,
    name: Arc<str>,
    key_columns: Vec<ColumnId>,
    value_columns: Option<LastCacheValueColumnsDef>,
    count: Option<LastCacheSize>,
    ttl: Option<u64>,
}

impl CreateLastCacheOpBuilder {
    pub fn build(self) -> CatalogOp {
        CatalogOp::CreateLastCache(LastCacheDefinition {
            table_id: self.table_id,
            table: self.table_name,
            name: self.name,
            key_columns: self.key_columns,
            value_columns: self
                .value_columns
                .unwrap_or(LastCacheValueColumnsDef::AllNonKeyColumns),
            count: self.count.unwrap_or_else(|| LastCacheSize::new(1).unwrap()),
            ttl: self.ttl.unwrap_or(3600),
        })
    }
}

pub fn create_last_cache_op_builder(
    table_id: TableId,
    table_name: impl Into<Arc<str>>,
    cache_name: impl Into<Arc<str>>,
    key_columns: impl IntoIterator<Item = ColumnId>,
) -> CreateLastCacheOpBuilder {
    CreateLastCacheOpBuilder {
        table_id,
        table_name: table_name.into(),
        name: cache_name.into(),
        key_columns: key_columns.into_iter().collect(),
        value_columns: None,
        count: None,
        ttl: None,
    }
}

pub fn delete_last_cache_op(
    table_id: TableId,
    table_name: impl Into<Arc<str>>,
    cache_name: impl Into<Arc<str>>,
) -> CatalogOp {
    CatalogOp::DeleteLastCache(LastCacheDelete {
        table_name: table_name.into(),
        table_id,
        name: cache_name.into(),
    })
}
