//! Table operations (record_ids 5-7, 22).

use std::sync::Arc;

use iox_time::Time;

use super::conversions::soft_deleted_name;
use super::impl_bitcode_encoding;
use super::types::{
    ColumnDefinition as WireColumnDef, DeletionScope as WireDeletionScope,
    FieldDataType as WireFieldDataType, FieldFamilyDefinition as WireFieldFamilyDef,
    FieldFamilyMode as WireFieldFamilyMode, FieldFamilyName as WireFieldFamilyName,
    RetentionPeriod as WireRetentionPeriod,
};
use crate::catalog::versions::v3::deletes::DeletionScope;
use crate::catalog::versions::v3::events::CatalogEvent;
use crate::catalog::versions::v3::inner::InnerCatalog;
use crate::catalog::versions::v3::schema::column::{
    ColumnDefinition, FieldColumn, FieldDataType, FieldFamilyDefinition, FieldFamilyMode,
    FieldFamilyName, TagColumn, TimestampColumn,
};
use crate::catalog::versions::v3::schema::retention::RetentionPeriod as SchemaRetentionPeriod;
use crate::catalog::versions::v3::schema::table::TableDefinition;
use crate::format::apply::ApplyError;
use crate::format::{CatalogRecord, RecordFlags, RecordId, RegisteredRecord, record_ids};
use influxdb3_id::{ColumnId, DbId, FieldFamilyId, FieldId, FieldIdentifier, TableId, TagId};
use schema::InfluxFieldType;

/// Create a new table.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, bitcode::Encode, bitcode::Decode)]
pub struct CreateTable {
    /// Database catalog ID.
    pub database_id: u32,
    /// Database name.
    pub database_name: String,
    /// Table name.
    pub table_name: String,
    /// Table catalog ID.
    pub table_id: u32,
    /// Retention period for this table.
    pub retention_period: WireRetentionPeriod,
    /// Field family mode for this table.
    pub field_family_mode: WireFieldFamilyMode,
}

impl CatalogRecord for CreateTable {
    const ID: RecordId = record_ids::CREATE_TABLE;
    const FLAGS: RecordFlags = RecordFlags::none();
    const NAME: &'static str = "CreateTable";

    fn apply(&self, catalog: &mut InnerCatalog) -> Result<(), ApplyError> {
        let db_id = DbId::new(self.database_id);
        let table_id = TableId::new(self.table_id);

        let mut db = catalog.databases.require_by_id(&db_id)?;

        let field_family_mode = FieldFamilyMode::from(self.field_family_mode);
        let mut new_table = TableDefinition::new_empty(
            table_id,
            Arc::from(self.table_name.as_str()),
            field_family_mode,
        );
        new_table.retention_period = SchemaRetentionPeriod::from(&self.retention_period);

        Arc::make_mut(&mut db).tables.insert(table_id, new_table)?;
        catalog.databases.update(db_id, db).map_err(Into::into)
    }

    fn event(&self) -> CatalogEvent {
        CatalogEvent::TableCreated {
            db_id: DbId::new(self.database_id),
            table_id: TableId::new(self.table_id),
        }
    }
}

inventory::submit! {
    RegisteredRecord::new::<CreateTable>()
}

/// Soft delete a table (mark for deletion).
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, bitcode::Encode, bitcode::Decode)]
pub struct SoftDeleteTable {
    /// Database catalog ID.
    pub database_id: u32,
    /// Table catalog ID.
    pub table_id: u32,
    /// Deletion timestamp in nanoseconds.
    pub deletion_time_ns: i64,
    /// Optional hard deletion timestamp in nanoseconds.
    pub hard_deletion_time_ns: Option<i64>,
    /// Scope of the hard deletion.
    pub hard_delete_scope: Option<WireDeletionScope>,
}

impl CatalogRecord for SoftDeleteTable {
    const ID: RecordId = record_ids::SOFT_DELETE_TABLE;
    const FLAGS: RecordFlags = RecordFlags::none();
    const NAME: &'static str = "SoftDeleteTable";

    fn apply(&self, catalog: &mut InnerCatalog) -> Result<(), ApplyError> {
        let db_id = DbId::new(self.database_id);
        let table_id = TableId::new(self.table_id);

        let mut db = catalog.databases.require_by_id(&db_id)?;
        let mut table = db.tables.require_by_id(&table_id)?;

        let t = Arc::make_mut(&mut table);
        if !t.deleted {
            let deletion_time = Time::from_timestamp_nanos(self.deletion_time_ns);
            t.table_name = soft_deleted_name(&t.table_name, deletion_time, table_id.get(), |n| {
                db.tables.contains_name(n)
            });
            t.deleted = true;
        }
        t.hard_delete_time = self.hard_deletion_time_ns.map(Time::from_timestamp_nanos);
        t.hard_delete_scope = self.hard_delete_scope.as_ref().map(DeletionScope::from);

        let d = Arc::make_mut(&mut db);
        d.tables.update(table_id, table)?;
        catalog.databases.update(db_id, db).map_err(Into::into)
    }

    fn event(&self) -> CatalogEvent {
        CatalogEvent::TableSoftDeleted {
            db_id: DbId::new(self.database_id),
            table_id: TableId::new(self.table_id),
        }
    }
}

inventory::submit! {
    RegisteredRecord::new::<SoftDeleteTable>()
}

/// Permanently delete a table.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, bitcode::Encode, bitcode::Decode)]
pub struct HardDeleteTable {
    /// Database catalog ID.
    pub db_id: u32,
    /// Table catalog ID.
    pub table_id: u32,
}

impl CatalogRecord for HardDeleteTable {
    const ID: RecordId = record_ids::DELETE_TABLE;
    const FLAGS: RecordFlags = RecordFlags::none();
    const NAME: &'static str = "HardDeleteTable";

    fn apply(&self, catalog: &mut InnerCatalog) -> Result<(), ApplyError> {
        let db_id = DbId::new(self.db_id);
        let table_id = TableId::new(self.table_id);

        let mut db = catalog.databases.require_by_id(&db_id)?;
        let table = db.tables.require_by_id(&table_id)?;

        let scope = DeletionScope::from_option(table.hard_delete_scope);
        match scope {
            DeletionScope::DataAndCatalog => {
                Arc::make_mut(&mut db).tables.remove(&table_id);
                catalog.databases.update(db_id, db)?;
            }
            DeletionScope::DataOnlyKeepResources | DeletionScope::DataOnlyRemoveTables => {
                // No catalog mutation for tables in these scopes
            }
        }
        Ok(())
    }

    fn event(&self) -> CatalogEvent {
        CatalogEvent::TableHardDeleted {
            db_id: DbId::new(self.db_id),
            table_id: TableId::new(self.table_id),
        }
    }
}

inventory::submit! {
    RegisteredRecord::new::<HardDeleteTable>()
}

/// Add columns to a table. Also carries field family definitions for new columns.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, bitcode::Encode, bitcode::Decode)]
pub struct AddColumns {
    /// Database catalog ID.
    pub database_id: u32,
    /// Table catalog ID.
    pub table_id: u32,
    /// New column definitions to add.
    pub columns: Vec<WireColumnDef>,
    /// Field family definitions for new field columns.
    pub field_families: Vec<WireFieldFamilyDef>,
}

impl CatalogRecord for AddColumns {
    const ID: RecordId = record_ids::ADD_COLUMNS;
    const FLAGS: RecordFlags = RecordFlags::none();
    const NAME: &'static str = "AddColumns";

    fn apply(&self, catalog: &mut InnerCatalog) -> Result<(), ApplyError> {
        let db_id = DbId::new(self.database_id);
        let table_id = TableId::new(self.table_id);

        let mut db = catalog.databases.require_by_id(&db_id)?;
        let mut table = db.tables.require_by_id(&table_id)?;
        let t = Arc::make_mut(&mut table);

        // Insert field family definitions before adding columns — add_columns panics
        // if a field column references a family that doesn't exist yet.
        for ff_def in &self.field_families {
            let ff_id = FieldFamilyId::new(ff_def.id);
            let name = FieldFamilyName::from(&ff_def.name);
            // For auto-field families, ensure the auto field family name is set:
            if let WireFieldFamilyName::Auto(n) = ff_def.name {
                t.set_auto_field_family(ff_id, n).map_err(|e| {
                    ApplyError(format!(
                        "{}: set auto field family (db_id={}, table_id={}): {e}",
                        Self::NAME,
                        self.database_id,
                        self.table_id,
                    ))
                })?;
            }
            let ff = FieldFamilyDefinition::new(ff_id, name);
            t.field_families.insert(ff_id, ff)?;
        }

        let new_columns: Vec<ColumnDefinition> =
            self.columns.iter().map(ColumnDefinition::from).collect();
        if !new_columns.is_empty() {
            t.add_columns(new_columns).map_err(|e| {
                ApplyError(format!(
                    "{}: add columns (db_id={}, table_id={}): {e}",
                    Self::NAME,
                    self.database_id,
                    self.table_id,
                ))
            })?;
        }

        let d = Arc::make_mut(&mut db);
        d.tables.update(table_id, table)?;
        catalog.databases.update(db_id, db).map_err(Into::into)
    }

    fn event(&self) -> CatalogEvent {
        CatalogEvent::TableUpdated {
            db_id: DbId::new(self.database_id),
            table_id: TableId::new(self.table_id),
        }
    }
}

inventory::submit! {
    RegisteredRecord::new::<AddColumns>()
}

// ---------------------------------------------------------------------------
// Wire → schema conversions
// ---------------------------------------------------------------------------

impl From<WireFieldFamilyMode> for FieldFamilyMode {
    fn from(value: WireFieldFamilyMode) -> Self {
        match value {
            WireFieldFamilyMode::Aware => Self::Aware,
            WireFieldFamilyMode::Auto => Self::Auto,
        }
    }
}

impl From<&WireFieldFamilyName> for FieldFamilyName {
    fn from(value: &WireFieldFamilyName) -> Self {
        match value {
            WireFieldFamilyName::User(s) => Self::User(Arc::from(s.as_str())),
            WireFieldFamilyName::Auto(n) => Self::Auto(*n),
        }
    }
}

impl From<&WireColumnDef> for ColumnDefinition {
    fn from(value: &WireColumnDef) -> Self {
        match value {
            WireColumnDef::Timestamp(ts) => Self::Timestamp(Arc::new(TimestampColumn {
                column_id: ts.column_id.map(ColumnId::new),
                name: Arc::from(ts.name.as_str()),
            })),
            WireColumnDef::Tag(tag) => Self::Tag(Arc::new(TagColumn {
                id: TagId::new(tag.id),
                column_id: tag.column_id.map(ColumnId::new),
                name: Arc::from(tag.name.as_str()),
            })),
            WireColumnDef::Field(field) => Self::Field(Arc::new(FieldColumn {
                id: FieldIdentifier::new(
                    FieldFamilyId::new(field.id.family_id),
                    FieldId::new(field.id.field_id),
                ),
                column_id: field.column_id.map(ColumnId::new),
                name: Arc::from(field.name.as_str()),
                data_type: InfluxFieldType::from(field.data_type),
            })),
        }
    }
}

impl From<WireFieldDataType> for InfluxFieldType {
    fn from(value: WireFieldDataType) -> Self {
        match value {
            WireFieldDataType::String => Self::String,
            WireFieldDataType::Integer => Self::Integer,
            WireFieldDataType::UInteger => Self::UInteger,
            WireFieldDataType::Float => Self::Float,
            WireFieldDataType::Boolean => Self::Boolean,
            // Binary is reserved in the wire format but InfluxFieldType does not
            // yet support it. Binary field records should not be produced or
            // persisted until InfluxFieldType gains a Binary variant.
            WireFieldDataType::Binary => {
                unimplemented!("Binary field data type is not yet supported")
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Schema → wire conversions
// ---------------------------------------------------------------------------

impl From<&FieldFamilyMode> for WireFieldFamilyMode {
    fn from(value: &FieldFamilyMode) -> Self {
        match value {
            FieldFamilyMode::Aware => Self::Aware,
            FieldFamilyMode::Auto => Self::Auto,
        }
    }
}

impl From<&FieldDataType> for WireFieldDataType {
    fn from(value: &FieldDataType) -> Self {
        match value {
            FieldDataType::String => Self::String,
            FieldDataType::Integer => Self::Integer,
            FieldDataType::UInteger => Self::UInteger,
            FieldDataType::Float => Self::Float,
            FieldDataType::Boolean => Self::Boolean,
            FieldDataType::Binary => Self::Binary,
        }
    }
}

impl_bitcode_encoding!(CreateTable, SoftDeleteTable, HardDeleteTable, AddColumns);

#[cfg(test)]
mod tests;
