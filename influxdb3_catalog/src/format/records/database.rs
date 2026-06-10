//! Database operations (record_ids 3, 4, 21).

use std::sync::Arc;

use iox_time::Time;

use super::conversions::soft_deleted_name;
use super::impl_bitcode_encoding;
use super::types::{DeletionScope, RetentionPeriod};
use crate::catalog::versions::v3::deletes::DeletionScope as SchemaDeletionScope;
use crate::catalog::versions::v3::events::CatalogEvent;
use crate::catalog::versions::v3::inner::InnerCatalog;
use crate::catalog::versions::v3::schema::database::DatabaseSchema;
use crate::catalog::versions::v3::schema::retention::RetentionPeriod as SchemaRetentionPeriod;
use crate::format::apply::ApplyError;
use crate::format::{CatalogRecord, RecordFlags, RecordId, RegisteredRecord, record_ids};
use indexmap::IndexMap;
use influxdb3_authz::{ResourceIdentifier, ResourceMetadata};
use influxdb3_id::DbId;

/// Capture the current name of database `db_id` into any token permissions that
/// reference it, so `show tokens` can still display the original name after the
/// database is renamed (soft delete) or removed (hard delete). Uses `or_insert`
/// so the first (pre-rename) capture wins. Mirrors v2's `apply_database_batch`.
fn capture_db_name_in_tokens(catalog: &mut InnerCatalog, db_id: DbId, db_name: &str) {
    let token_ids: Vec<_> = catalog.tokens.repo().id_iter().copied().collect();
    for token_id in token_ids {
        let Some(token_info) = catalog.tokens.repo().get_by_id(&token_id) else {
            continue;
        };
        let mut updated_token = Arc::clone(&token_info);
        let mut needs_update = false;
        let token_mut = Arc::make_mut(&mut updated_token);
        for permission in &mut token_mut.permissions {
            if let ResourceIdentifier::Database(db_ids) = &permission.resource_identifier
                && db_ids.contains(&db_id)
            {
                permission
                    .resource_names
                    .get_or_insert_with(IndexMap::new)
                    .entry(db_id.to_string())
                    .or_insert_with(|| ResourceMetadata {
                        name: db_name.to_string(),
                        deleted: true,
                    });
                needs_update = true;
            }
        }
        if needs_update {
            catalog
                .tokens
                .update_token(token_id, (*updated_token).clone())
                .expect("token to be updated");
        }
    }
}

/// Create a new database.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, bitcode::Encode, bitcode::Decode)]
pub struct CreateDatabase {
    /// Database catalog ID.
    pub database_id: u32,
    /// Database name.
    pub database_name: String,
    /// Retention period for this database.
    pub retention_period: RetentionPeriod,
}

impl CatalogRecord for CreateDatabase {
    const ID: RecordId = record_ids::CREATE_DATABASE;
    const FLAGS: RecordFlags = RecordFlags::none();
    const NAME: &'static str = "CreateDatabase";

    fn apply(&self, catalog: &mut InnerCatalog) -> Result<(), ApplyError> {
        let db_id = DbId::new(self.database_id);
        let mut db = DatabaseSchema::new(db_id, Arc::from(self.database_name.as_str()));
        db.retention_period = SchemaRetentionPeriod::from(&self.retention_period);
        catalog.databases.insert(db_id, db).map_err(Into::into)
    }

    fn event(&self) -> CatalogEvent {
        CatalogEvent::DatabaseCreated {
            db_id: DbId::new(self.database_id),
        }
    }
}

inventory::submit! {
    RegisteredRecord::new::<CreateDatabase>()
}

/// Soft delete a database (mark for deletion).
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, bitcode::Encode, bitcode::Decode)]
pub struct SoftDeleteDatabase {
    /// Database catalog ID.
    pub database_id: u32,
    /// Deletion timestamp in nanoseconds.
    pub deletion_time_ns: i64,
    /// Optional hard deletion timestamp in nanoseconds.
    pub hard_deletion_time_ns: Option<i64>,
    /// Scope of the hard deletion.
    pub hard_delete_scope: Option<DeletionScope>,
}

impl CatalogRecord for SoftDeleteDatabase {
    const ID: RecordId = record_ids::SOFT_DELETE_DATABASE;
    const FLAGS: RecordFlags = RecordFlags::none();
    const NAME: &'static str = "SoftDeleteDatabase";

    fn apply(&self, catalog: &mut InnerCatalog) -> Result<(), ApplyError> {
        let db_id = DbId::new(self.database_id);
        let mut db = catalog.databases.require_by_id(&db_id)?;

        // Capture the original database name into referencing tokens before the
        // rename below, so `show tokens` still shows the original name.
        let original_name = Arc::clone(&db.name);
        capture_db_name_in_tokens(catalog, db_id, &original_name);

        let owned = Arc::make_mut(&mut db);
        let deletion_time = Time::from_timestamp_nanos(self.deletion_time_ns);
        let hard_delete_time = self.hard_deletion_time_ns.map(Time::from_timestamp_nanos);
        let hard_delete_scope = self
            .hard_delete_scope
            .as_ref()
            .map(SchemaDeletionScope::from);
        if !owned.deleted {
            // Rename the database so the name can be reused for a new database
            owned.name = soft_deleted_name(&owned.name, deletion_time, db_id.get(), |n| {
                catalog.databases.contains_name(n)
            });
            owned.deleted = true;
        }
        owned.hard_delete_time = hard_delete_time;
        owned.hard_delete_scope = hard_delete_scope;

        // Cascade soft-delete to all tables in the database so they no longer
        // count against the global table limit. Mirrors v2's behavior.
        let table_ids: Vec<_> = owned
            .tables
            .iter()
            .filter(|(_, t)| !t.deleted)
            .map(|(id, _)| *id)
            .collect();
        for table_id in table_ids {
            if let Some(mut table) = owned.tables.get_by_id(&table_id) {
                let table_owned = Arc::make_mut(&mut table);
                if !table_owned.deleted {
                    table_owned.table_name = soft_deleted_name(
                        &table_owned.table_name,
                        deletion_time,
                        table_id.get(),
                        |n| owned.tables.contains_name(n),
                    );
                    table_owned.deleted = true;
                }
                table_owned.hard_delete_time = hard_delete_time;
                table_owned.hard_delete_scope = hard_delete_scope;
                owned.tables.update(table_id, table)?;
            }
        }

        // Disable all triggers so they're marked disabled in the catalog.
        // The processing engine watches for `DatabaseSoftDeleted` to do its
        // own runtime cleanup, but the persisted state needs to reflect
        // that triggers shouldn't restart on the next load.
        let trigger_ids: Vec<_> = owned
            .processing_engine_triggers
            .iter()
            .filter(|(_, t)| !t.disabled)
            .map(|(id, _)| *id)
            .collect();
        for id in trigger_ids {
            if let Some(mut trigger) = owned.processing_engine_triggers.get_by_id(&id) {
                Arc::make_mut(&mut trigger).disabled = true;
                owned.processing_engine_triggers.update(id, trigger)?;
            }
        }

        catalog.databases.update(db_id, db).map_err(Into::into)
    }

    fn event(&self) -> CatalogEvent {
        CatalogEvent::DatabaseSoftDeleted {
            db_id: DbId::new(self.database_id),
        }
    }
}

inventory::submit! {
    RegisteredRecord::new::<SoftDeleteDatabase>()
}

/// Permanently delete a database.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, bitcode::Encode, bitcode::Decode)]
pub struct HardDeleteDatabase {
    /// Database catalog ID.
    pub db_id: u32,
}

impl CatalogRecord for HardDeleteDatabase {
    const ID: RecordId = record_ids::DELETE_DATABASE;
    const FLAGS: RecordFlags = RecordFlags::none();
    const NAME: &'static str = "HardDeleteDatabase";

    fn apply(&self, catalog: &mut InnerCatalog) -> Result<(), ApplyError> {
        let db_id = DbId::new(self.db_id);
        let mut db = catalog.databases.require_by_id(&db_id)?;
        let scope = SchemaDeletionScope::from_option(db.hard_delete_scope);
        match scope {
            SchemaDeletionScope::DataAndCatalog => {
                // Capture the name into referencing tokens before removal so
                // `show tokens` keeps it. A direct hard delete may never have
                // soft-deleted (which would have captured already); `or_insert`
                // keeps the first capture. Mirrors v2's `apply_delete_batch`.
                let db_name = Arc::clone(&db.name);
                capture_db_name_in_tokens(catalog, db_id, &db_name);
                catalog.databases.remove(&db_id);
            }
            SchemaDeletionScope::DataOnlyRemoveTables => {
                Arc::make_mut(&mut db).tables.clear();
                catalog.databases.update(db_id, db)?;
            }
            SchemaDeletionScope::DataOnlyKeepResources => {
                // No catalog mutation; downstream reacts to event
            }
        }
        Ok(())
    }

    fn event(&self) -> CatalogEvent {
        CatalogEvent::DatabaseHardDeleted {
            db_id: DbId::new(self.db_id),
        }
    }
}

inventory::submit! {
    RegisteredRecord::new::<HardDeleteDatabase>()
}

impl_bitcode_encoding!(CreateDatabase, SoftDeleteDatabase, HardDeleteDatabase);

#[cfg(test)]
mod tests;
