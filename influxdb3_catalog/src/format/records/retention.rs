//! Retention period records (record_ids 16-17).

use std::sync::Arc;

use super::impl_bitcode_encoding;
use super::types::RetentionPeriod;
use crate::catalog::versions::v3::events::CatalogEvent;
use crate::catalog::versions::v3::inner::InnerCatalog;
use crate::catalog::versions::v3::schema::retention::RetentionPeriod as SchemaRetentionPeriod;
use crate::format::apply::ApplyError;
use crate::format::{CatalogRecord, RecordFlags, RecordId, RegisteredRecord, record_ids};
use crate::resource::CatalogResource;
use influxdb3_id::DbId;

/// Set retention period on a database.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, bitcode::Encode, bitcode::Decode)]
pub struct SetDbRetentionPeriod {
    /// Database catalog ID.
    pub database_id: u32,
    /// Retention period configuration.
    pub retention_period: RetentionPeriod,
}

impl CatalogRecord for SetDbRetentionPeriod {
    const ID: RecordId = record_ids::SET_DB_RETENTION_PERIOD;
    const FLAGS: RecordFlags = RecordFlags::none();
    const NAME: &'static str = "SetDbRetentionPeriod";

    fn apply(&self, catalog: &mut InnerCatalog) -> Result<(), ApplyError> {
        let db_id = DbId::new(self.database_id);
        let mut db = catalog.databases.require_by_id(&db_id)?;
        Arc::make_mut(&mut db).retention_period =
            SchemaRetentionPeriod::from(&self.retention_period);
        catalog.databases.update(db.id(), db).map_err(Into::into)
    }

    fn event(&self) -> CatalogEvent {
        CatalogEvent::DatabaseRetentionPeriodChanged {
            db_id: DbId::new(self.database_id),
        }
    }
}

inventory::submit! {
    RegisteredRecord::new::<SetDbRetentionPeriod>()
}

/// Clear retention period on a database (set to indefinite).
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, bitcode::Encode, bitcode::Decode)]
pub struct ClearDbRetentionPeriod {
    /// Database catalog ID.
    pub database_id: u32,
}

impl CatalogRecord for ClearDbRetentionPeriod {
    const ID: RecordId = record_ids::CLEAR_DB_RETENTION_PERIOD;
    const FLAGS: RecordFlags = RecordFlags::none();
    const NAME: &'static str = "ClearDbRetentionPeriod";

    fn apply(&self, catalog: &mut InnerCatalog) -> Result<(), ApplyError> {
        let db_id = DbId::new(self.database_id);
        let mut db = catalog.databases.require_by_id(&db_id)?;
        Arc::make_mut(&mut db).retention_period = SchemaRetentionPeriod::Indefinite;
        catalog.databases.update(db.id(), db).map_err(Into::into)
    }

    fn event(&self) -> CatalogEvent {
        CatalogEvent::DatabaseRetentionPeriodChanged {
            db_id: DbId::new(self.database_id),
        }
    }
}

inventory::submit! {
    RegisteredRecord::new::<ClearDbRetentionPeriod>()
}

impl_bitcode_encoding!(SetDbRetentionPeriod, ClearDbRetentionPeriod);

#[cfg(test)]
mod tests;
