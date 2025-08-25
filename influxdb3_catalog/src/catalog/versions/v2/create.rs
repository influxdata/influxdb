use influxdb3_id::{ColumnId, DbId, TableId, TagId};
use std::sync::Arc;

use super::log::{
    AddColumnsLog, CatalogBatch, ColumnDefinitionLog, DatabaseBatch, DatabaseCatalogOp,
    FieldFamilyDefinitionLog, TagColumnLog,
};

pub(crate) fn catalog_batch(
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

pub(crate) fn add_columns_op(
    database_id: impl Into<DbId>,
    db_name: impl Into<Arc<str>>,
    table_id: impl Into<TableId>,
    table_name: impl Into<Arc<str>>,
    columns: impl IntoIterator<Item = ColumnDefinitionLog>,
    field_families: impl IntoIterator<Item = FieldFamilyDefinitionLog>,
) -> DatabaseCatalogOp {
    DatabaseCatalogOp::AddColumns(AddColumnsLog {
        database_name: db_name.into(),
        database_id: database_id.into(),
        table_name: table_name.into(),
        table_id: table_id.into(),
        column_definitions: columns.into_iter().collect(),
        field_family_definitions: field_families.into_iter().collect(),
    })
}

pub(crate) fn tag_def(
    id: impl Into<TagId>,
    column_id: impl Into<ColumnId>,
    name: impl Into<Arc<str>>,
) -> ColumnDefinitionLog {
    ColumnDefinitionLog::Tag(TagColumnLog {
        id: id.into(),
        column_id: column_id.into(),
        name: name.into(),
    })
}
