//! Table operations: CreateTableOp, SoftDeleteTableOp, HardDeleteTableOp.

use std::sync::Arc;

use iox_time::Time;

use super::CatalogOp;
use crate::CatalogError;
use crate::catalog::INTERNAL_DB_NAME;
use crate::catalog::versions::v3::deletes::DeletionScope;
use crate::catalog::versions::v3::inner::InnerCatalog;
use crate::catalog::versions::v3::schema::table::TableDefinition;
use crate::format::RecordBatch;
use crate::format::records::{HardDeleteTable, SoftDeleteTable};
use crate::resource::CatalogResource;
use influxdb3_id::{DbId, TableId};

// ---------------------------------------------------------------------------
// SoftDeleteTable
// ---------------------------------------------------------------------------

#[derive(Debug)]
pub(crate) struct SoftDeleteTableArgs {
    pub db_name: String,
    pub table_name: String,
    pub deletion_time: Time,
    pub hard_delete_time: Option<Time>,
    pub hard_delete_scope: Option<DeletionScope>,
}

pub(crate) struct SoftDeleteTableOp {
    db_id: DbId,
    table_id: TableId,
}

impl CatalogOp for SoftDeleteTableOp {
    type Input = SoftDeleteTableArgs;
    type Output = Arc<TableDefinition>;

    fn prepare(
        args: &Self::Input,
        catalog: &InnerCatalog,
        records: &mut RecordBatch,
    ) -> Result<Self, CatalogError> {
        let (db, tbl) = super::active_table(catalog, &args.db_name, &args.table_name)?;
        let db_id = db.id();
        let table_id = tbl.id();

        // Allow re-soft-delete to update the scheduled hard_delete_time
        if tbl.deleted && tbl.hard_delete_time == args.hard_delete_time {
            return Err(CatalogError::AlreadyDeleted(args.table_name.clone()));
        }

        records.push(&SoftDeleteTable {
            database_id: db_id.get(),
            table_id: table_id.get(),
            deletion_time_ns: args.deletion_time.timestamp_nanos(),
            hard_deletion_time_ns: args.hard_delete_time.map(|t| t.timestamp_nanos()),
            hard_delete_scope: args.hard_delete_scope.as_ref().map(Into::into),
        });

        Ok(Self { db_id, table_id })
    }

    fn output(&self, catalog: &InnerCatalog) -> Self::Output {
        let db = catalog
            .databases
            .get_by_id(&self.db_id)
            .expect("database should exist after soft delete");
        db.tables
            .get_by_id(&self.table_id)
            .expect("table should exist after soft delete")
    }
}

// ---------------------------------------------------------------------------
// HardDeleteTable
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Copy)]
pub(crate) struct HardDeleteTableArgs {
    pub db_id: DbId,
    pub table_id: TableId,
}

pub(crate) struct HardDeleteTableOp {
    /// Captured before deletion — the table may be removed from the catalog after apply.
    table_def: Arc<TableDefinition>,
}

impl CatalogOp for HardDeleteTableOp {
    type Input = HardDeleteTableArgs;
    type Output = Arc<TableDefinition>;

    fn prepare(
        args: &Self::Input,
        catalog: &InnerCatalog,
        records: &mut RecordBatch,
    ) -> Result<Self, CatalogError> {
        let db = catalog.databases.get_by_id(&args.db_id).ok_or_else(|| {
            CatalogError::DatabaseNotFound {
                db_name: Arc::from(format!("db_id={}", args.db_id).as_str()),
            }
        })?;
        if db.name().as_ref() == INTERNAL_DB_NAME {
            return Err(CatalogError::CannotModifyInternalDatabase);
        }
        let table_def =
            db.tables
                .get_by_id(&args.table_id)
                .ok_or_else(|| CatalogError::TableNotFound {
                    db_name: db.name(),
                    table_name: Arc::from(format!("table_id={}", args.table_id).as_str()),
                })?;

        // We still need to push the HardDelete here because that's how the catalog is told that it
        // needs to remove this database from its internal state. But this `HardDelete` record should
        // be deleted down the line - specifically, within InnerCatalog::create_snapshot
        records.push(&HardDeleteTable {
            db_id: args.db_id.get(),
            table_id: args.table_id.get(),
        });

        Ok(Self { table_def })
    }

    fn output(&self, _catalog: &InnerCatalog) -> Self::Output {
        Arc::clone(&self.table_def)
    }
}

#[cfg(test)]
mod tests;
