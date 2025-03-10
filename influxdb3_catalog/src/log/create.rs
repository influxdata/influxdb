use std::sync::Arc;

use influxdb3_id::{ColumnId, DbId, TableId};

use super::*;

pub fn catalog_database_batch_op(
    db_id: DbId,
    db_name: impl Into<Arc<str>>,
    time_ns: i64,
    ops: impl IntoIterator<Item = DatabaseCatalogOp>,
    sequence_number: u64,
) -> OrderedCatalogBatch {
    OrderedCatalogBatch::new(
        catalog_batch(db_id, db_name, time_ns, ops),
        CatalogSequenceNumber::new(sequence_number),
    )
}

pub fn catalog_batch(
    db_id: DbId,
    db_name: impl Into<Arc<str>>,
    time_ns: i64,
    ops: impl IntoIterator<Item = DatabaseCatalogOp>,
) -> CatalogBatch {
    CatalogBatch::Database(DatabaseBatch {
        database_id: db_id,
        database_name: db_name.into(),
        time_ns,
        ops: ops.into_iter().collect(),
    })
}

pub fn add_fields_op(
    database_id: DbId,
    db_name: impl Into<Arc<str>>,
    table_id: TableId,
    table_name: impl Into<Arc<str>>,
    fields: impl IntoIterator<Item = FieldDefinition>,
) -> DatabaseCatalogOp {
    DatabaseCatalogOp::AddFields(AddFieldsLog {
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
) -> DatabaseCatalogOp {
    DatabaseCatalogOp::CreateTable(CreateTableLog {
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
    ttl: Option<LastCacheTtl>,
}

impl CreateLastCacheOpBuilder {
    pub fn build(self) -> DatabaseCatalogOp {
        DatabaseCatalogOp::CreateLastCache(CreateLastCacheLog {
            table_id: self.table_id,
            table: self.table_name,
            name: self.name,
            key_columns: self.key_columns,
            value_columns: self
                .value_columns
                .unwrap_or(LastCacheValueColumnsDef::AllNonKeyColumns),
            count: self.count.unwrap_or_default(),
            ttl: self.ttl.unwrap_or_default(),
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
) -> DatabaseCatalogOp {
    DatabaseCatalogOp::DeleteLastCache(DeleteLastCacheLog {
        table_name: table_name.into(),
        table_id,
        name: cache_name.into(),
    })
}
