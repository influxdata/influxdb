//! Operation layer for the v3 catalog write path.

use std::sync::Arc;

use crate::CatalogError;
use crate::catalog::INTERNAL_DB_NAME;
use crate::catalog::versions::v3::inner::InnerCatalog;
use crate::catalog::versions::v3::schema::database::DatabaseSchema;
use crate::catalog::versions::v3::schema::table::TableDefinition;
use crate::catalog::versions::v3::usage::{CatalogLimiter, CurrentCatalogUsage};
use crate::format::RecordBatch;
pub mod cache;
pub mod config;
pub mod database;
pub mod feature_level;
pub mod node;
pub mod query_group;
pub mod restore;
pub mod retention;
pub mod role;
pub mod table;
pub mod token;
pub mod trigger;
pub mod user;

/// Operation layer trait for the v3 catalog write path.
pub(crate) trait CatalogOp: Sized {
    type Input;
    type Output;

    /// Resource-limit hook for the configured [`CatalogLimiter`]. Override
    /// for ops that introduce databases, tables, or columns; default is a
    /// no-op.
    fn limits_check(
        _args: &Self::Input,
        _catalog: &InnerCatalog,
        _usage: &CurrentCatalogUsage,
        _limiter: &dyn CatalogLimiter,
    ) -> Result<(), CatalogError> {
        Ok(())
    }

    fn prepare(
        args: &Self::Input,
        catalog: &InnerCatalog,
        records: &mut RecordBatch,
    ) -> Result<Self, CatalogError>;

    fn output(&self, catalog: &InnerCatalog) -> Self::Output;
}

/// Look up an active (not deleted, not internal) database by name.
pub(super) fn active_database(
    catalog: &InnerCatalog,
    db_name: &str,
) -> Result<Arc<DatabaseSchema>, CatalogError> {
    if db_name == INTERNAL_DB_NAME {
        return Err(CatalogError::CannotModifyInternalDatabase);
    }
    let db =
        catalog
            .databases
            .get_by_name(db_name)
            .ok_or_else(|| CatalogError::DatabaseNotFound {
                db_name: Arc::from(db_name),
            })?;
    if db.deleted {
        return Err(CatalogError::AlreadyDeleted(db_name.to_string()));
    }
    Ok(db)
}

/// Look up an active (not deleted, not internal) database and active table by name.
pub(super) fn active_table(
    catalog: &InnerCatalog,
    db_name: &str,
    table_name: &str,
) -> Result<(Arc<DatabaseSchema>, Arc<TableDefinition>), CatalogError> {
    let db = active_database(catalog, db_name)?;
    let table = db
        .tables
        .get_by_name(table_name)
        .ok_or_else(|| CatalogError::TableNotFound {
            db_name: Arc::from(db_name),
            table_name: Arc::from(table_name),
        })?;
    if table.deleted {
        return Err(CatalogError::AlreadyDeleted(table_name.to_string()));
    }
    Ok((db, table))
}

#[cfg(test)]
pub(super) mod test_util;
