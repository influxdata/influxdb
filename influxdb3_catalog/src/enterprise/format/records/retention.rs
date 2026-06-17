//! Enterprise retention records (record_ids e2-e3).

use crate::catalog::versions::v3::events::CatalogEvent;
use crate::catalog::versions::v3::inner::InnerCatalog;
use crate::catalog::versions::v3::schema::retention::RetentionPeriod as SchemaRetentionPeriod;
use crate::format::apply::ApplyError;
use crate::format::records::impl_bitcode_encoding;
use crate::format::records::types::RetentionPeriod;
use crate::format::{CatalogRecord, RecordFlags, RecordId, RegisteredRecord, record_ids};
use influxdb3_id::{DbId, TableId};

/// Set retention period on a table (enterprise).
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, bitcode::Encode, bitcode::Decode)]
pub struct SetTableRetentionPeriod {
    /// Database catalog ID.
    pub database_id: u32,
    /// Table catalog ID.
    pub table_id: u32,
    /// Retention period configuration.
    pub retention_period: RetentionPeriod,
}

impl CatalogRecord for SetTableRetentionPeriod {
    const ID: RecordId = record_ids::SET_TABLE_RETENTION_PERIOD;
    const FLAGS: RecordFlags = RecordFlags::none();
    const NAME: &'static str = "SetTableRetentionPeriod";

    fn apply(&self, catalog: &mut InnerCatalog) -> Result<(), ApplyError> {
        let db_id = DbId::new(self.database_id);
        let table_id = TableId::new(self.table_id);

        catalog.databases.modify_by_id(&db_id, |db| {
            db.tables.modify_by_id(&table_id, |table| {
                table.retention_period = SchemaRetentionPeriod::from(&self.retention_period);
                Ok(())
            })
        })
    }

    fn event(&self) -> CatalogEvent {
        CatalogEvent::TableRetentionPeriodChanged {
            db_id: DbId::new(self.database_id),
            table_id: TableId::new(self.table_id),
        }
    }
}

inventory::submit! {
    RegisteredRecord::new::<SetTableRetentionPeriod>()
}

/// Clear retention period on a table (enterprise).
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, bitcode::Encode, bitcode::Decode)]
pub struct ClearTableRetentionPeriod {
    /// Database catalog ID.
    pub database_id: u32,
    /// Table catalog ID.
    pub table_id: u32,
}

impl CatalogRecord for ClearTableRetentionPeriod {
    const ID: RecordId = record_ids::CLEAR_TABLE_RETENTION_PERIOD;
    const FLAGS: RecordFlags = RecordFlags::none();
    const NAME: &'static str = "ClearTableRetentionPeriod";

    fn apply(&self, catalog: &mut InnerCatalog) -> Result<(), ApplyError> {
        let db_id = DbId::new(self.database_id);
        let table_id = TableId::new(self.table_id);

        catalog.databases.modify_by_id(&db_id, |db| {
            db.tables.modify_by_id(&table_id, |table| {
                table.retention_period = SchemaRetentionPeriod::Indefinite;
                Ok(())
            })
        })
    }

    fn event(&self) -> CatalogEvent {
        CatalogEvent::TableRetentionPeriodChanged {
            db_id: DbId::new(self.database_id),
            table_id: TableId::new(self.table_id),
        }
    }
}

inventory::submit! {
    RegisteredRecord::new::<ClearTableRetentionPeriod>()
}

impl_bitcode_encoding!(SetTableRetentionPeriod, ClearTableRetentionPeriod);

#[cfg(test)]
mod tests;
