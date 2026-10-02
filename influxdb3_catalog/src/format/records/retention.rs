//! Retention period records (record_ids 16-17).

use super::types::RetentionPeriod;
use influxdb3_catalog_macros::catalog_record;

use crate::catalog::versions::v3::events::CatalogEvent;
use crate::catalog::versions::v3::inner::InnerCatalog;
use crate::catalog::versions::v3::schema::retention::RetentionPeriod as SchemaRetentionPeriod;
use crate::format::apply::ApplyError;
use crate::format::{RecordApply, record_ids};
use influxdb3_id::DbId;

/// Set retention period on a database.
#[catalog_record(id = record_ids::SET_DB_RETENTION_PERIOD, shape = 0x6e095d4a)]
#[derive(Copy)]
pub struct SetDbRetentionPeriod {
    /// Database catalog ID.
    pub database_id: u32,
    /// Retention period configuration.
    pub retention_period: RetentionPeriod,
}

impl RecordApply for SetDbRetentionPeriod {
    fn apply(&self, catalog: &mut InnerCatalog) -> Result<(), ApplyError> {
        let db_id = DbId::new(self.database_id);
        catalog.databases.modify_by_id_in_place(&db_id, |db| {
            db.retention_period = SchemaRetentionPeriod::from(&self.retention_period);
            Ok(())
        })
    }

    fn event(&self) -> CatalogEvent {
        CatalogEvent::DatabaseRetentionPeriodChanged {
            db_id: DbId::new(self.database_id),
        }
    }
}

/// Clear retention period on a database (set to indefinite).
#[catalog_record(id = record_ids::CLEAR_DB_RETENTION_PERIOD, shape = 0x0c40adea)]
#[derive(Copy)]
pub struct ClearDbRetentionPeriod {
    /// Database catalog ID.
    pub database_id: u32,
}

impl RecordApply for ClearDbRetentionPeriod {
    fn apply(&self, catalog: &mut InnerCatalog) -> Result<(), ApplyError> {
        let db_id = DbId::new(self.database_id);
        catalog.databases.modify_by_id_in_place(&db_id, |db| {
            db.retention_period = SchemaRetentionPeriod::Indefinite;
            Ok(())
        })
    }

    fn event(&self) -> CatalogEvent {
        CatalogEvent::DatabaseRetentionPeriodChanged {
            db_id: DbId::new(self.database_id),
        }
    }
}

#[cfg(test)]
mod tests;
