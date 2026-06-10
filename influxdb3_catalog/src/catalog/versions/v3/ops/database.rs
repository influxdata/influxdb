//! Database operations: CreateDatabaseOp, SoftDeleteDatabaseOp, HardDeleteDatabaseOp.

use std::sync::Arc;
use std::time::Duration;

use iox_time::Time;

use super::CatalogOp;
use crate::CatalogError;
use crate::catalog::INTERNAL_DB_NAME;
use crate::catalog::versions::v3::deletes::DeletionScope;
use crate::catalog::versions::v3::inner::InnerCatalog;
use crate::catalog::versions::v3::schema::database::DatabaseSchema;
use crate::catalog::versions::v3::usage::{CatalogLimiter, CurrentCatalogUsage};
use crate::format::RecordBatch;
use crate::format::records::{CreateDatabase, HardDeleteDatabase, SoftDeleteDatabase};
use crate::resource::CatalogResource;
use influxdb3_id::DbId;

// ---------------------------------------------------------------------------
// CreateDatabase
// ---------------------------------------------------------------------------

#[derive(Debug)]
pub(crate) struct CreateDatabaseArgs {
    pub name: String,
    pub retention_period: Option<Duration>,
}

pub(crate) struct CreateDatabaseOp {
    db_id: DbId,
}

impl CatalogOp for CreateDatabaseOp {
    type Input = CreateDatabaseArgs;
    type Output = Arc<DatabaseSchema>;

    fn limits_check(
        _args: &Self::Input,
        _catalog: &InnerCatalog,
        usage: &CurrentCatalogUsage,
        limiter: &dyn CatalogLimiter,
    ) -> Result<(), CatalogError> {
        let limit = limiter.database_count_limit(usage);
        if usage.total_db_count() >= limit {
            return Err(CatalogError::TooManyDbs(limit));
        }
        Ok(())
    }

    fn prepare(
        args: &Self::Input,
        catalog: &InnerCatalog,
        records: &mut RecordBatch,
    ) -> Result<Self, CatalogError> {
        if catalog.databases.contains_name(&args.name) {
            return Err(CatalogError::AlreadyExists);
        }
        let db_id = catalog.databases.next_id();
        records.push(&CreateDatabase {
            database_id: db_id.get(),
            database_name: args.name.clone(),
            retention_period: args.retention_period.into(),
        });
        Ok(Self { db_id })
    }

    fn output(&self, catalog: &InnerCatalog) -> Self::Output {
        catalog
            .databases
            .get_by_id(&self.db_id)
            .expect("database should exist after apply")
    }
}

// ---------------------------------------------------------------------------
// SoftDeleteDatabase
// ---------------------------------------------------------------------------

#[derive(Debug)]
pub(crate) struct SoftDeleteDatabaseArgs {
    pub database_name: String,
    pub deletion_time: Time,
    pub hard_delete_time: Option<Time>,
    pub hard_delete_scope: Option<DeletionScope>,
}

pub(crate) struct SoftDeleteDatabaseOp {
    db_id: DbId,
}

impl CatalogOp for SoftDeleteDatabaseOp {
    type Input = SoftDeleteDatabaseArgs;
    type Output = Arc<DatabaseSchema>;

    fn prepare(
        args: &Self::Input,
        catalog: &InnerCatalog,
        records: &mut RecordBatch,
    ) -> Result<Self, CatalogError> {
        if args.database_name == INTERNAL_DB_NAME {
            return Err(CatalogError::CannotDeleteInternalDatabase);
        }
        let db = catalog
            .databases
            .get_by_name(&args.database_name)
            .ok_or_else(|| CatalogError::NotFound(args.database_name.clone()))?;
        // Allow re-soft-delete to update the scheduled hard_delete_time
        if db.deleted && db.hard_delete_time == args.hard_delete_time {
            return Err(CatalogError::AlreadyDeleted(args.database_name.clone()));
        }
        let db_id = db.id();
        records.push(&SoftDeleteDatabase {
            database_id: db_id.get(),
            deletion_time_ns: args.deletion_time.timestamp_nanos(),
            hard_deletion_time_ns: args.hard_delete_time.map(|t| t.timestamp_nanos()),
            hard_delete_scope: args.hard_delete_scope.as_ref().map(Into::into),
        });
        Ok(Self { db_id })
    }

    fn output(&self, catalog: &InnerCatalog) -> Self::Output {
        catalog
            .databases
            .get_by_id(&self.db_id)
            .expect("database should exist after soft delete")
    }
}

// ---------------------------------------------------------------------------
// HardDeleteDatabase
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Copy)]
pub(crate) struct HardDeleteDatabaseArgs {
    pub db_id: DbId,
}

pub(crate) struct HardDeleteDatabaseOp {
    /// Captured before deletion — the db may be removed from the catalog after apply.
    db_schema: Arc<DatabaseSchema>,
}

impl CatalogOp for HardDeleteDatabaseOp {
    type Input = HardDeleteDatabaseArgs;
    type Output = Arc<DatabaseSchema>;

    fn prepare(
        args: &Self::Input,
        catalog: &InnerCatalog,
        records: &mut RecordBatch,
    ) -> Result<Self, CatalogError> {
        let db_schema = catalog.databases.get_by_id(&args.db_id).ok_or_else(|| {
            CatalogError::DatabaseNotFound {
                db_name: Arc::from(format!("db_id={}", args.db_id).as_str()),
            }
        })?;
        if db_schema.name().as_ref() == INTERNAL_DB_NAME {
            return Err(CatalogError::CannotDeleteInternalDatabase);
        }
        records.push(&HardDeleteDatabase {
            db_id: args.db_id.get(),
        });
        Ok(Self { db_schema })
    }

    fn output(&self, _catalog: &InnerCatalog) -> Self::Output {
        Arc::clone(&self.db_schema)
    }
}

#[cfg(test)]
mod tests;
