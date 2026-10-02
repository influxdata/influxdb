//! Enterprise retention records (record_ids e2-e3).

use influxdb3_catalog_macros::catalog_record;

use crate::catalog::versions::v3::events::CatalogEvent;
use crate::catalog::versions::v3::inner::InnerCatalog;
use crate::catalog::versions::v3::schema::retention::RetentionPeriod as SchemaRetentionPeriod;
use crate::format::apply::ApplyError;
use crate::format::records::types::RetentionPeriod;
use crate::format::{RecordApply, record_ids};
use influxdb3_id::{DbId, TableId};

/// Set retention period on a table (enterprise).
#[catalog_record(id = record_ids::SET_TABLE_RETENTION_PERIOD, shape = 0x05e39eb5)]
#[derive(Copy)]
pub struct SetTableRetentionPeriod {
    /// Database catalog ID.
    pub database_id: u32,
    /// Table catalog ID.
    pub table_id: u32,
    /// Retention period configuration.
    pub retention_period: RetentionPeriod,
}

impl RecordApply for SetTableRetentionPeriod {
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

/// Clear retention period on a table (enterprise).
#[catalog_record(id = record_ids::CLEAR_TABLE_RETENTION_PERIOD, shape = 0x4fa490e5)]
#[derive(Copy)]
pub struct ClearTableRetentionPeriod {
    /// Database catalog ID.
    pub database_id: u32,
    /// Table catalog ID.
    pub table_id: u32,
}

impl RecordApply for ClearTableRetentionPeriod {
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

#[cfg(test)]
mod tests;
