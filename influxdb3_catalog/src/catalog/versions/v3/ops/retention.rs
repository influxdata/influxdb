//! Retention operations: Set/Clear for databases and tables.

use std::sync::Arc;
use std::time::Duration;

use super::CatalogOp;
use crate::CatalogError;
use crate::catalog::versions::v3::inner::InnerCatalog;
use crate::catalog::versions::v3::schema::database::DatabaseSchema;
use crate::catalog::versions::v3::schema::table::TableDefinition;
use crate::format::RecordBatch;
use crate::format::records::types::RetentionPeriod as WireRetentionPeriod;
use crate::format::records::{ClearDbRetentionPeriod, SetDbRetentionPeriod};
use crate::resource::CatalogResource;
use influxdb3_id::DbId;

// ---------------------------------------------------------------------------
// SetDbRetentionPeriod
// ---------------------------------------------------------------------------

#[derive(Debug)]
pub(crate) struct SetDbRetentionPeriodArgs {
    pub db_name: String,
    pub retention_period: Duration,
}

pub(crate) struct SetDbRetentionPeriodOp {
    db_id: DbId,
}

impl CatalogOp for SetDbRetentionPeriodOp {
    type Input = SetDbRetentionPeriodArgs;
    type Output = Arc<DatabaseSchema>;

    fn prepare(
        args: &Self::Input,
        catalog: &InnerCatalog,
        records: &mut RecordBatch,
    ) -> Result<Self, CatalogError> {
        let db = super::active_database(catalog, &args.db_name)?;
        let db_id = db.id();

        records.push(&SetDbRetentionPeriod {
            database_id: db_id.get(),
            retention_period: WireRetentionPeriod::Duration {
                duration_secs: args.retention_period.as_secs(),
            },
        });

        Ok(Self { db_id })
    }

    fn output(&self, catalog: &InnerCatalog) -> Self::Output {
        catalog
            .databases
            .get_by_id(&self.db_id)
            .expect("database should exist after setting retention")
    }
}

// ---------------------------------------------------------------------------
// ClearDbRetentionPeriod
// ---------------------------------------------------------------------------

#[derive(Debug)]
pub(crate) struct ClearDbRetentionPeriodArgs {
    pub db_name: String,
}

pub(crate) struct ClearDbRetentionPeriodOp {
    db_id: DbId,
}

impl CatalogOp for ClearDbRetentionPeriodOp {
    type Input = ClearDbRetentionPeriodArgs;
    type Output = Arc<DatabaseSchema>;

    fn prepare(
        args: &Self::Input,
        catalog: &InnerCatalog,
        records: &mut RecordBatch,
    ) -> Result<Self, CatalogError> {
        let db = super::active_database(catalog, &args.db_name)?;
        let db_id = db.id();

        records.push(&ClearDbRetentionPeriod {
            database_id: db_id.get(),
        });

        Ok(Self { db_id })
    }

    fn output(&self, catalog: &InnerCatalog) -> Self::Output {
        catalog
            .databases
            .get_by_id(&self.db_id)
            .expect("database should exist after clearing retention")
    }
}

// ---------------------------------------------------------------------------
// SetTableRetentionPeriod
// ---------------------------------------------------------------------------

#[derive(Debug)]
pub(crate) struct SetTableRetentionPeriodArgs {
    pub db_name: String,
    pub table_name: String,
    pub retention_period: Duration,
}

pub(crate) struct SetTableRetentionPeriodOp {
    db_id: DbId,
    table_id: influxdb3_id::TableId,
}

impl CatalogOp for SetTableRetentionPeriodOp {
    type Input = SetTableRetentionPeriodArgs;
    type Output = Arc<TableDefinition>;

    fn prepare(
        args: &Self::Input,
        catalog: &InnerCatalog,
        records: &mut RecordBatch,
    ) -> Result<Self, CatalogError> {
        let (db, table) = super::active_table(catalog, &args.db_name, &args.table_name)?;
        let db_id = db.id();
        let table_id = table.id();

        records.push(
            &crate::enterprise::format::records::SetTableRetentionPeriod {
                database_id: db_id.get(),
                table_id: table_id.get(),
                retention_period: WireRetentionPeriod::Duration {
                    duration_secs: args.retention_period.as_secs(),
                },
            },
        );

        Ok(Self { db_id, table_id })
    }

    fn output(&self, catalog: &InnerCatalog) -> Self::Output {
        let db = catalog
            .databases
            .get_by_id(&self.db_id)
            .expect("database should exist after setting table retention");
        db.tables
            .get_by_id(&self.table_id)
            .expect("table should exist after setting retention")
    }
}

// ---------------------------------------------------------------------------
// ClearTableRetentionPeriod
// ---------------------------------------------------------------------------

#[derive(Debug)]
pub(crate) struct ClearTableRetentionPeriodArgs {
    pub db_name: String,
    pub table_name: String,
}

pub(crate) struct ClearTableRetentionPeriodOp {
    db_id: DbId,
    table_id: influxdb3_id::TableId,
}

impl CatalogOp for ClearTableRetentionPeriodOp {
    type Input = ClearTableRetentionPeriodArgs;
    type Output = Arc<TableDefinition>;

    fn prepare(
        args: &Self::Input,
        catalog: &InnerCatalog,
        records: &mut RecordBatch,
    ) -> Result<Self, CatalogError> {
        let (db, table) = super::active_table(catalog, &args.db_name, &args.table_name)?;
        let db_id = db.id();
        let table_id = table.id();

        records.push(
            &crate::enterprise::format::records::ClearTableRetentionPeriod {
                database_id: db_id.get(),
                table_id: table_id.get(),
            },
        );

        Ok(Self { db_id, table_id })
    }

    fn output(&self, catalog: &InnerCatalog) -> Self::Output {
        let db = catalog
            .databases
            .get_by_id(&self.db_id)
            .expect("database should exist after clearing table retention");
        db.tables
            .get_by_id(&self.table_id)
            .expect("table should exist after clearing retention")
    }
}

#[cfg(test)]
mod tests;
