//! Migration from the v2 catalog to the v3 catalog
//!
//! The v2 catalog persists state as a [`CatalogSnapshot`][v4::CatalogSnapshot].
//! The v3 catalog persists state as a sequence of [`Record`][crate::format::Record]s
//! in the binary framing format. This module provides the data conversion that
//! synthesizes the equivalent v3 records from a v2 snapshot.

// TODO(tjh): remove this when the migration and v3 catalog are wired in. Note, #![expect(dead_code)]
// would be more appropriate, but doesnt work here, see:
// - https://github.com/rust-lang/rust/issues/152370
// - https://github.com/rust-lang/rust/pull/154377
// It will work in 1.97, but hopefully the catalog will be wired in by then :)
#![allow(dead_code)]

mod conversions;

use std::sync::Arc;

use influxdb3_id::ColumnId;
use object_store::ObjectStore;
use observability_deps::tracing::{debug, info, warn};

use crate::catalog::TIME_COLUMN_NAME;
use crate::catalog::migrations::MigrationError;
use crate::catalog::versions::v2::Snapshot as V2Snapshot;
use crate::catalog::versions::v3::inner::InnerCatalog as V3InnerCatalog;
use crate::catalog::versions::v3::schema::storage::StorageMode as V3StorageMode;
use crate::enterprise::format::records::CreateResourceScopedToken;
use crate::format::FeatureLevel;
use crate::format::RecordBatch;
use crate::format::apply::{RestorePreload, apply_records};
use crate::format::records::conversions::SOFT_DELETION_TIME_FORMAT;
use crate::format::records::types::ColumnDefinition as WireColumnDef;
use crate::format::records::types::TimestampColumn as WireTimestampColumn;
use crate::format::records::{
    AddColumns, CreateAdminToken, CreateDatabase, CreateDistinctCache, CreateLastCache,
    CreateTable, CreateTrigger, NextIdScope, RegisterNode, SetGenerationDuration, SetNextId,
    SetStorageMode, SoftDeleteDatabase, SoftDeleteTable, StopNode,
};
use crate::log::versions::v4::StorageMode;
use crate::object_store::PersistCatalogResult;
use crate::object_store::versions as ostore;
use crate::serialize::versions as ser;
use crate::snapshot::versions::v4::{
    self, ActionsSnapshot, ColumnDefinitionSnapshot, DatabaseSnapshot, DistinctCacheSnapshot,
    GenerationConfigSnapshot, LastCacheSnapshot, NodeSnapshot, NodeStateSnapshot,
    PermissionSnapshot, ProcessingEngineTriggerSnapshot, ResourceIdentifierSnapshot,
    ResourceTypeSnapshot, TableSnapshot, TokenInfoSnapshot,
};

/// Outcome of a v2 → v3 catalog migration.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum MigrationResult {
    /// This call performed the migration: the v3 snapshot was written
    /// and the v2 log stream was terminated with an `UpgradedLog`.
    Migrated,
    /// A v3 snapshot was already present at the prefix — another node
    /// migrated, or this binary previously completed the migration.
    AlreadyMigrated,
    /// No v2 catalog was present at the prefix, so there is nothing to
    /// migrate.
    NothingToMigrate,
}

/// Check for a v2 catalog at `prefix` and migrate it to v3 if present.
///
/// Idempotent: re-running after success returns
/// [`MigrationResult::AlreadyMigrated`]; re-running after a partial
/// failure (UpgradedLog written but v3 snapshot missing) completes the
/// write the next time around. Concurrent migration attempts converge
/// via the create-only semantics of the v3 snapshot write — the loser
/// observes its peer's snapshot already on object store and treats
/// `AlreadyExists` as success.
pub(crate) async fn check_and_migrate_v2_to_v3(
    prefix: Arc<str>,
    store: Arc<dyn ObjectStore>,
) -> Result<MigrationResult, MigrationError> {
    let v3_store = ostore::v3::ObjectStoreCatalog::new(
        Arc::clone(&prefix),
        Arc::clone(&store),
        V3StorageMode::default(),
    );

    if v3_store.load_snapshot().await?.is_some() {
        return Ok(MigrationResult::AlreadyMigrated);
    }

    let v2_store = ostore::v2::ObjectStoreCatalog::new(
        Arc::clone(&prefix),
        u64::MAX,
        Arc::clone(&store),
        StorageMode::default(),
    );

    if !v2_store.checkpoint_exists().await? {
        return Ok(MigrationResult::NothingToMigrate);
    }

    let v2_catalog = ser::v2::load_catalog(Arc::clone(&prefix), &v2_store)
        .await?
        .expect("v2 catalog should exist since checkpoint was found");

    let catalog_uuid = v2_catalog.catalog_uuid;
    let catalog_sequence = v2_catalog.sequence_number();
    let v2_snapshot = v2_catalog.snapshot();
    let batch = synthesize_records(&v2_snapshot);

    let mut v3_inner = V3InnerCatalog::new(Arc::clone(&prefix), catalog_uuid);
    apply_records(
        batch.as_slice(),
        &mut v3_inner,
        catalog_sequence,
        &mut RestorePreload::empty(),
    )
    .map_err(|e| MigrationError::Unexpected(anyhow::anyhow!(e)))?;
    let v3_snapshot_bytes = v3_inner.create_snapshot();

    if !v2_catalog.has_upgraded {
        let next_sequence = catalog_sequence.next();
        if let PersistCatalogResult::AlreadyExists =
            v2_store.persist_upgrade_log(next_sequence).await?
        {
            return Err(MigrationError::UpgradeLogAlreadyExists);
        }
    } else {
        info!("skipping write of UpgradedLog, as it is already present");
    }

    info!("writing the v3 catalog snapshot");
    match v3_store.initialize_snapshot(v3_snapshot_bytes).await? {
        PersistCatalogResult::Success | PersistCatalogResult::AlreadyExists => {
            Ok(MigrationResult::Migrated)
        }
    }
}

/// Append the v3 `CatalogRecord`s that reconstruct this v2 snapshot's state to
/// `batch`.
///
/// Implementors are part of the v2 → v3 catalog migration. Each implementor
/// converts one snapshot type into the corresponding v3 records.
pub(crate) trait FromV2 {
    // clippy doesn't like the from_ name.
    #[allow(clippy::wrong_self_convention)]
    fn from_v2(&self, batch: &mut RecordBatch);
}

/// Synthesize the v3 records that reconstruct `snapshot`'s state.
///
/// Returns a [`RecordBatch`] stamped with the snapshot's sequence number.
/// Applying the batch to a fresh
/// [`InnerCatalog`][crate::catalog::versions::v3::inner::InnerCatalog] produces
/// equivalent in-memory state.
pub(crate) fn synthesize_records(snapshot: &v4::CatalogSnapshot) -> RecordBatch {
    let mut batch = RecordBatch::new(snapshot.sequence_number().get());
    snapshot.from_v2(&mut batch);
    batch
}

impl FromV2 for v4::CatalogSnapshot {
    fn from_v2(&self, batch: &mut RecordBatch) {
        self.generation_config.from_v2(batch);

        if self.storage_mode != StorageMode::default() {
            batch.push(&SetStorageMode {
                mode: self.storage_mode.into(),
            });
        }

        for (_, node) in &self.nodes.repo {
            node.from_v2(batch);
        }

        for (_, db) in &self.databases.repo {
            db.from_v2(batch);
        }
        emit_set_next_id_if_gap_due_to_deletion(
            NextIdScope::Databases,
            u64::from(self.databases.next_id.get()),
            self.databases
                .repo
                .keys()
                .map(|id| u64::from(id.get()) + 1)
                .max()
                .unwrap_or(0),
            batch,
        );

        for (_, token) in &self.tokens.repo {
            token.from_v2(batch);
        }
        emit_set_next_id_if_gap_due_to_deletion(
            NextIdScope::Tokens,
            self.tokens.next_id.get(),
            self.tokens
                .repo
                .keys()
                .map(|id| id.get() + 1)
                .max()
                .unwrap_or(0),
            batch,
        );
    }
}

/// When a `next_id` in the v2 snapshot exceeds the derived `next_id`
/// from the migration, emit a `SetNextId` record into the catalog to
/// fill the gap. Such a gap may exist due to the _Nth_ record being
/// deleted for a resource. Note, not all resources can be deleted in
/// v2, so this is only used where necessary.
///
/// v2 enum variants are frozen, so the set of v2-deletable resources
/// is permanent. v2-deletable (callers below):
/// - databases
/// - tokens
/// - per-database: tables, triggers
/// - per-table: last-caches, distinct-caches
///
/// v2-non-deletable (no caller below): nodes, columns, field-families.
fn emit_set_next_id_if_gap_due_to_deletion(
    scope: NextIdScope,
    snapshot_next: u64,
    derived_next: u64,
    batch: &mut RecordBatch,
) {
    if snapshot_next > derived_next {
        batch.push(&SetNextId {
            scope,
            id: snapshot_next,
        });
    }
}

impl FromV2 for GenerationConfigSnapshot {
    fn from_v2(&self, batch: &mut RecordBatch) {
        for (level, duration) in &self.generation_durations {
            batch.push(&SetGenerationDuration {
                level: *level,
                duration_ns: u64::try_from(duration.as_nanos())
                    .expect("generation duration overflows u64 nanoseconds"),
            });
        }
    }
}

impl FromV2 for NodeSnapshot {
    /// `RegisterNode` is always emitted. For nodes whose state is Stopped, v2
    /// doesn't preserve the original registration timestamp — use 0 as the
    /// documented placeholder, then emit `StopNode` to drive the node into
    /// the Stopped state on apply.
    fn from_v2(&self, batch: &mut RecordBatch) {
        let registered_time_ns = match &self.state {
            NodeStateSnapshot::Running { registered_time_ns } => *registered_time_ns,
            NodeStateSnapshot::Stopped { .. } => 0,
        };

        batch.push(&RegisterNode {
            node_catalog_id: self.node_catalog_id.get(),
            node_id: self.node_id.to_string(),
            instance_id: self.instance_id.to_string(),
            registered_time_ns,
            core_count: self.core_count,
            mode: self.mode.iter().copied().map(Into::into).collect(),
            process_uuid: [0u8; 16], // placeholder -- not preserved in v2
            conn_info: self.conn_info.as_ref().map(|s| s.to_string()),
            cli_params: self.cli_params.as_ref().map(|s| s.to_string()),
            row_delete_predicate_version: u64::try_from(self.row_delete_predicate_version)
                .expect("row_delete_predicate_version exceeds u64"),
            // v2 has no concept of feature level:
            feature_level: FeatureLevel::ZERO,
        });

        if let NodeStateSnapshot::Stopped { stopped_time_ns } = &self.state {
            batch.push(&StopNode {
                node_catalog_id: self.node_catalog_id.get(),
                node_id: self.node_id.to_string(),
                stopped_time_ns: *stopped_time_ns,
                process_uuid: [0u8; 16], // placeholder -- not preserved in v2
            });
        }
    }
}

impl FromV2 for DatabaseSnapshot {
    fn from_v2(&self, batch: &mut RecordBatch) {
        let db_id = self.id.get();
        let (database_name, deletion_time_ns) = if self.deleted {
            let (name, ts) = split_soft_deleted_name(&self.name);
            (name, Some(ts))
        } else {
            (self.name.to_string(), None)
        };

        batch.push(&CreateDatabase {
            database_id: db_id,
            database_name: database_name.clone(),
            retention_period: (&self.retention_period).into(),
        });

        for (_, table) in &self.tables.repo {
            synthesize_table(table, db_id, &database_name, batch);
        }
        emit_set_next_id_if_gap_due_to_deletion(
            NextIdScope::Tables { database_id: db_id },
            u64::from(self.tables.next_id.get()),
            self.tables
                .repo
                .keys()
                .map(|id| u64::from(id.get()) + 1)
                .max()
                .unwrap_or(0),
            batch,
        );

        for (_, trigger) in &self.processing_engine_triggers.repo {
            synthesize_trigger(trigger, db_id, batch);
        }
        emit_set_next_id_if_gap_due_to_deletion(
            NextIdScope::Triggers { database_id: db_id },
            u64::from(self.processing_engine_triggers.next_id.get()),
            self.processing_engine_triggers
                .repo
                .keys()
                .map(|id| u64::from(id.get()) + 1)
                .max()
                .unwrap_or(0),
            batch,
        );

        if let Some(deletion_time_ns) = deletion_time_ns {
            batch.push(&SoftDeleteDatabase {
                database_id: db_id,
                deletion_time_ns,
                hard_deletion_time_ns: self.hard_delete_time,
                hard_delete_scope: self.hard_delete_scope.as_ref().map(Into::into),
            });
        }
    }
}

// NOTE(tjh): A table references its parent database by id, but `TableSnapshot`
// doesn't store one — the id comes from the caller. `database_name` is also
// threaded through because `CreateTable` carries it as metadata.
fn synthesize_table(table: &TableSnapshot, db_id: u32, db_name: &str, batch: &mut RecordBatch) {
    let table_id = table.table_id.get();
    let (table_name, deletion_time_ns) = if table.deleted {
        let (name, ts) = split_soft_deleted_name(&table.table_name);
        (name, Some(ts))
    } else {
        (table.table_name.to_string(), None)
    };

    batch.push(&CreateTable {
        database_id: db_id,
        database_name: db_name.to_string(),
        table_name,
        table_id,
        retention_period: (&table.retention_period).into(),
        field_family_mode: table.field_family_mode.into(),
    });

    // v3's apply rebuilds the series key from the order tag columns are added
    // in `AddColumns.columns`, so tags have to come first in v2 series-key
    // order. Non-tag columns (timestamp, fields) follow in repo order.
    let mut columns: Vec<WireColumnDef> = Vec::with_capacity(table.columns.repo.len());
    for tag_id in &table.key {
        if let Some(col) = table
            .columns
            .repo
            .iter()
            .find(|c| matches!(c, ColumnDefinitionSnapshot::Tag(t) if t.id == *tag_id))
        {
            columns.push(col.into());
        } else {
            warn!(
                tag_id = tag_id.get(),
                table_snapshot = ?table,
                "table snapshot contained tag id that is not represented as a tag column"
            );
        }
    }
    for col in &table.columns.repo {
        if !matches!(col, ColumnDefinitionSnapshot::Tag(_)) {
            columns.push(col.into());
        }
    }

    // Every v3 table the catalog produces has a timestamp column. If a v2
    // snapshot is missing one, add it here; that state should not be possible,
    // but doing this ensures we don't leave this table in a corrupted state
    // because there is no API to add `time`.
    let has_timestamp = columns.iter().any(|c| {
        if let WireColumnDef::Timestamp(col) = c
            && col.name == TIME_COLUMN_NAME
        {
            true
        } else {
            false
        }
    });
    if !has_timestamp {
        debug!(
            db_id,
            table_id, table_snapshot = ?table,
            "v2 table snapshot missing time column"
        );
        let column_id = columns
            .iter()
            .map(WireColumnDef::column_id)
            .max()
            .unwrap_or(Some(ColumnId::default().get())) // if no columns (max == None), it gets id 0
            .and_then(|i| i.checked_add(1)); // otherwise, max + 1; overflow to None
        columns.push(WireColumnDef::Timestamp(WireTimestampColumn {
            column_id,
            name: TIME_COLUMN_NAME.to_string(),
        }));
    }

    let field_families = table
        .field_families
        .repo
        .iter()
        .map(|(_, ff)| ff.into())
        .collect();

    batch.push(&AddColumns {
        database_id: db_id,
        table_id,
        columns,
        field_families,
    });

    for (_, cache) in &table.last_caches.repo {
        synthesize_last_cache(cache, db_id, batch);
    }
    emit_set_next_id_if_gap_due_to_deletion(
        NextIdScope::LastCaches {
            database_id: db_id,
            table_id,
        },
        u64::from(table.last_caches.next_id.get()),
        table
            .last_caches
            .repo
            .keys()
            .map(|id| u64::from(id.get()) + 1)
            .max()
            .unwrap_or(0),
        batch,
    );

    for (_, cache) in &table.distinct_caches.repo {
        synthesize_distinct_cache(cache, db_id, batch);
    }
    emit_set_next_id_if_gap_due_to_deletion(
        NextIdScope::DistinctCaches {
            database_id: db_id,
            table_id,
        },
        u64::from(table.distinct_caches.next_id.get()),
        table
            .distinct_caches
            .repo
            .keys()
            .map(|id| u64::from(id.get()) + 1)
            .max()
            .unwrap_or(0),
        batch,
    );

    if let Some(deletion_time_ns) = deletion_time_ns {
        batch.push(&SoftDeleteTable {
            database_id: db_id,
            table_id,
            deletion_time_ns,
            hard_deletion_time_ns: table.hard_delete_time,
            hard_delete_scope: table.hard_delete_scope.as_ref().map(Into::into),
        });
    }
}

fn synthesize_last_cache(cache: &LastCacheSnapshot, db_id: u32, batch: &mut RecordBatch) {
    batch.push(&CreateLastCache {
        db_id,
        table_id: cache.table_id.get(),
        id: cache.id.get(),
        node_spec: (&cache.node_spec).into(),
        name: cache.name.to_string(),
        key_columns: cache.keys.iter().copied().map(Into::into).collect(),
        value_columns: (&cache.vals).into(),
        count: u64::try_from(cache.n).expect("last cache size overflows u64"),
        ttl_seconds: cache.ttl,
    });
}

impl FromV2 for TokenInfoSnapshot {
    /// A token with a single wildcard-everywhere permission is an admin
    /// token; emit `CreateAdminToken`. Anything else is resource-scoped and
    /// carries its permissions as-is in `CreateResourceScopedToken`.
    fn from_v2(&self, batch: &mut RecordBatch) {
        let token_id = self.id.get();
        let expiry = optional_expiry(self.expiry);
        if is_admin_permissions(&self.permissions) {
            batch.push(&CreateAdminToken {
                token_id,
                name: self.name.to_string(),
                hash: self.hash.clone(),
                created_at: self.created_at,
                updated_at: self.updated_at,
                expiry,
                description: self.description.clone(),
                created_by: self.created_by.map(|id| id.get()),
                updated_by: self.updated_by.map(|id| id.get()),
            });
        } else {
            batch.push(&CreateResourceScopedToken {
                token_id,
                name: self.name.to_string(),
                hash: self.hash.clone(),
                created_at: self.created_at,
                updated_at: self.updated_at,
                expiry,
                description: self.description.clone(),
                created_by: self.created_by.map(|id| id.get()),
                updated_by: self.updated_by.map(|id| id.get()),
                permissions: self.permissions.iter().map(Into::into).collect(),
            });
        }
    }
}

/// v2 encodes "no expiry" as `i64::MAX`; v3's records use `None` for the
/// same concept. Any other value passes through unchanged.
fn optional_expiry(expiry: i64) -> Option<i64> {
    (expiry < i64::MAX).then_some(expiry)
}

fn is_admin_permissions(perms: &[PermissionSnapshot]) -> bool {
    perms.len() == 1
        && matches!(perms[0].resource_type, ResourceTypeSnapshot::Wildcard)
        && matches!(
            perms[0].resource_identifier,
            ResourceIdentifierSnapshot::Wildcard
        )
        && matches!(perms[0].actions, ActionsSnapshot::Wildcard)
}

fn synthesize_distinct_cache(cache: &DistinctCacheSnapshot, db_id: u32, batch: &mut RecordBatch) {
    batch.push(&CreateDistinctCache {
        db_id,
        table_id: cache.table_id.get(),
        node_spec: (&cache.node_spec).into(),
        cache_id: cache.id.get(),
        cache_name: cache.name.to_string(),
        column_ids: cache.cols.iter().copied().map(Into::into).collect(),
        max_cardinality: cache.max_cardinality.to_u64(),
        max_age_seconds: cache.max_age_seconds.as_secs(),
        source: cache.source.into(),
        lookback_seconds: cache.lookback_seconds,
        refresh_interval_seconds: cache.refresh_interval.as_ref().map(|r| r.as_secs()),
    });
}

// NOTE(tjh): A processing-engine trigger references its parent database by id,
// but `ProcessingEngineTriggerSnapshot` only stores the database's name — `db_id`
// has to come from the caller. That extra parameter is why this is a free
// function rather than a `FromV2` impl.
fn synthesize_trigger(
    trigger: &ProcessingEngineTriggerSnapshot,
    db_id: u32,
    batch: &mut RecordBatch,
) {
    batch.push(&CreateTrigger {
        trigger_id: trigger.trigger_id.get(),
        trigger_name: trigger.trigger_name.to_string(),
        plugin_filename: trigger.plugin_filename.clone(),
        database_id: db_id,
        node_spec: (&trigger.node_spec).into(),
        trigger: (&trigger.trigger_specification).into(),
        trigger_settings: trigger.trigger_settings.into(),
        // HashMap iteration order is non-deterministic; sort by key so
        // re-synthesizing the same v2 snapshot produces identical output.
        trigger_arguments: trigger.trigger_arguments.as_ref().map(|args| {
            let mut entries: Vec<(String, String)> =
                args.iter().map(|(k, v)| (k.clone(), v.clone())).collect();
            entries.sort_by(|a, b| a.0.cmp(&b.0));
            entries
        }),
        disabled: trigger.disabled,
    });
}

/// Split a v2 soft-deleted resource name into its original name and the
/// deletion timestamp encoded in the suffix.
///
/// Soft-deleted database/tables are renamed in place to `{original}-{%Y%m%dT%H%M%S}`.
/// There is no separate deletion-time field. v3's `SoftDelete*` records carry
/// `deletion_time_ns` and re-derive the renamed name on apply, so when synthesizing
/// v3 records from a v2 snapshot, there needs to be a `Create*` record with the original
/// name and a `SoftDelete*` record with the timestamp parsed from the database name in the
/// snapshot. A name whose suffix doesn't parse falls back to `(name, 0)`.
fn split_soft_deleted_name(name: &str) -> (String, i64) {
    name.rsplit_once('-')
        .and_then(|(original, suffix)| {
            chrono::NaiveDateTime::parse_from_str(suffix, SOFT_DELETION_TIME_FORMAT)
                .ok()
                .and_then(|dt| dt.and_utc().timestamp_nanos_opt())
                .map(|nanos| (original.to_string(), nanos))
        })
        .unwrap_or_else(|| (name.to_string(), 0))
}

#[cfg(test)]
mod tests;
