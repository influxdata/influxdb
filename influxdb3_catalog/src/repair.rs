//! Offline repair for catalogs holding duplicate-name table records.
//!
//! A lenient `Repository::insert` let a `CreateTable` reuse a live table's
//! name: the name map entry moved to the new id while the old table stayed
//! in the repository, and replaying such a history panics the loader.
//!
//! The repair renames the earlier table to `<name>-orphaned-<table_id>`
//! inside its `CreateTable` record, leaves every other record byte
//! untouched, and writes the edited history back as the snapshot at the
//! latest observed sequence — loads then skip the poisoned logs, on older
//! binaries too. The newer table keeps the name because the map insert made
//! the newer id win on every running node; the orphan is renamed, not
//! deleted, so data under either id stays reachable.
//!
//! A create that reuses a live *id* under a free name (racing writers
//! allocating from stale views) is handled by inserting a synthetic
//! hard-delete record ahead of the re-create: the lenient insert replaced
//! the earlier definition wholesale, so delete-then-recreate replays to the
//! same converged state — and keeps the history loadable under the strict
//! insert, which refuses id reuse outright. If the reused id's name was
//! also taken, the lenient insert rejected the record and no node applied
//! it, so the repair refuses rather than invent a state no node had.

use std::io::Cursor;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use futures::StreamExt;
use object_store::path::Path as ObjPath;
use object_store::{ObjectStore, PutMode, PutOptions, UpdateVersion};
use uuid::Uuid;

use crate::catalog::CatalogSequenceNumber;
use crate::catalog::versions::v3::deletes::DeletionScope;
use crate::catalog::versions::v3::inner::InnerCatalog;
use crate::catalog::versions::v3::schema::node::{NodeDefinition, NodeState};
use crate::format::MakeRecord;
use crate::format::apply::serialize_snapshot_file;
use crate::format::records::{CreateDatabase, CreateTable, HardDeleteDatabase, HardDeleteTable};
use crate::format::{
    ApplyError, CatalogFile, Decode, Encode, FormatError, REGISTRY, Record, record_ids,
    validate_record_flags,
};
use crate::object_store::CatalogFilePath;
use influxdb3_id::{DbId, TableId};

#[derive(Debug, thiserror::Error)]
pub enum RepairError {
    #[error("catalog format error: {0}")]
    Format(#[from] FormatError),
    #[error("failed to apply record at sequence {sequence}: {message}")]
    Apply { sequence: u64, message: String },
    #[error(
        "catalog contains a RestoreCatalog record; repairing a restored catalog is not supported"
    )]
    RestoreUnsupported,
    #[error(
        "database name '{name}' is duplicated (ids {first} and {second}); \
         database repair is not supported"
    )]
    DuplicateDatabaseName {
        name: String,
        first: u32,
        second: u32,
    },
    #[error("no CreateTable record found for table id {table_id} in database {database_id}")]
    OrphanCreateNotFound { database_id: u32, table_id: u32 },
    #[error("verification of the repaired records failed: {0}")]
    Verification(String),
    #[error("object store error: {0}")]
    Store(String),
    #[error("no catalog snapshot found at prefix '{0}'")]
    NoSnapshot(String),
    #[error(
        "the catalog snapshot changed while the repair was running (most likely a \
         concurrent checkpoint from a live node); the repaired snapshot was not \
         written, though the pre-repair backup object remains — re-run the repair"
    )]
    SnapshotChanged,
    #[error(
        "cluster appears active — refusing to write. Object-store activity during \
         the {window_secs}s quiesce window: {}. Stop every node and re-run.",
        evidence.join("; ")
    )]
    ClusterActive {
        window_secs: u64,
        evidence: Vec<String>,
    },
    #[error(
        "the catalog's node registry lists nodes not confirmed stopped: {}. Verify \
         each is actually down (a crashed node never records its stop) and pass \
         --allow-unstopped-nodes to proceed on object-store evidence alone.",
        nodes.join(", ")
    )]
    NodesNotStopped { nodes: Vec<String> },
    #[error(
        "log sequence gap: expected sequence {expected} after {previous}, found {found}. \
         Repairing would permanently erase the missing log, so find it first"
    )]
    LogSequenceGap {
        previous: u64,
        expected: u64,
        found: u64,
    },
    #[error(
        "the superseded {resource} (id {id}) has a data-only hard-delete scope, so the \
         synthetic hard delete would not remove it. Clear the stored scope first: extend \
         this subcommand to do it, or rewrite the soft-delete record that set it"
    )]
    SupersededHasDataOnlyScope { resource: &'static str, id: u32 },
    /// Not repairable automatically: the lenient insert rejected the
    /// record, replaying nodes stopped mid-log at it, and the stale writer
    /// applied its whole batch, so there is no single state to reconstruct.
    /// A fix must pick a side and reconcile the record's log-siblings too.
    #[error(
        "the {resource} create at sequence {sequence} (id {id}, name '{name}') reuses a live \
         id while its name is also taken; the lenient insert rejected it, so no node applied \
         it and replaying nodes stopped at it. There is no single state to reconstruct: \
         inspect the record with 'debug catalog sequence {sequence}' and consult the \
         catalog maintainers before editing anything"
    )]
    LenientlyRejectedCreate {
        resource: &'static str,
        id: u32,
        name: String,
        sequence: u64,
    },
    #[error(
        "database id {id} is superseded but tokens hold permissions scoped to it; the \
         synthetic hard delete would capture the superseded name into those tokens' \
         metadata, which no lenient node did. Remove or rescope the affected token \
         grants before repairing, or extend this subcommand to skip the capture"
    )]
    SupersededDatabaseHasScopedTokens { id: u32 },
}

/// One planned (or applied) orphan-table rename.
#[derive(Debug, Clone, serde::Serialize)]
pub struct PlannedRename {
    pub database_id: u32,
    pub database_name: String,
    /// The contested table name; the newer table keeps it.
    pub table_name: String,
    pub kept_table_id: u32,
    pub orphaned_table_id: u32,
    /// Name the orphaned table is renamed to.
    pub orphaned_table_name: String,
    /// Sequence of the duplicate `CreateTable` record that stole the name.
    pub duplicate_create_sequence: u64,
}

/// A create that reused a live id; the earlier definition is dropped via a
/// synthetic hard delete, matching the replacement the lenient insert did.
#[derive(Debug, Clone, serde::Serialize)]
pub struct SupersededCreate {
    pub database_id: u32,
    /// `None` when a whole database was superseded.
    pub table_id: Option<u32>,
    pub old_name: String,
    pub new_name: String,
    /// Sequence of the re-creating record.
    pub recreate_sequence: u64,
}

/// A registered node and whether the catalog confirms it stopped.
#[derive(Debug, Clone, serde::Serialize)]
pub struct NodeReport {
    pub node_id: String,
    pub state: String,
    pub confirmed_stopped: bool,
}

#[derive(Debug, serde::Serialize)]
pub struct RepairPlan {
    pub renames: Vec<PlannedRename>,
    pub superseded: Vec<SupersededCreate>,
    pub record_count: usize,
    /// Sequence the repaired snapshot is (or would be) written at.
    pub snapshot_sequence: u64,
    /// Node registry as of the repaired history's last record.
    pub nodes: Vec<NodeReport>,
}

#[derive(Debug, serde::Serialize)]
pub struct RepairOutcome {
    pub plan: RepairPlan,
    /// Whether a repaired snapshot was written to the object store.
    pub executed: bool,
    /// Where the pre-repair snapshot was copied before being replaced.
    pub backup_path: Option<String>,
}

/// Inspect the catalog under `prefix` and, with `execute`, write the repaired
/// snapshot back. Without `execute` this is a read-only dry run.
///
/// An `execute` write is gated on a quiesce check: nodes the registry does
/// not confirm stopped refuse the write unless `allow_unstopped` is set
/// (necessary when a node crashed, since it never records its stop), and
/// the write is refused regardless if any catalog or WAL object appears
/// during `quiesce_window` of watching the store.
pub async fn repair_catalog(
    store: Arc<dyn ObjectStore>,
    prefix: &str,
    execute: bool,
    quiesce_window: Duration,
    allow_unstopped: bool,
    progress: &mut (dyn FnMut(String) + Send),
) -> Result<RepairOutcome, RepairError> {
    // Raw get: the bytes feed the backup, the etag guards the conditional write.
    let snapshot_path = CatalogFilePath::snapshot(prefix);
    let get = match store.get(&snapshot_path).await {
        Ok(get) => get,
        Err(object_store::Error::NotFound { .. }) => {
            return Err(RepairError::NoSnapshot(prefix.to_string()));
        }
        Err(e) => return Err(store_err(e)),
    };
    let observed_version = UpdateVersion {
        e_tag: get.meta.e_tag.clone(),
        version: get.meta.version.clone(),
    };
    let snapshot_bytes = get.bytes().await.map_err(store_err)?;
    let snapshot = CatalogFile::read_from(&mut Cursor::new(snapshot_bytes.as_ref()))?;
    let catalog_uuid = Uuid::from_u128(snapshot.header.catalog_uuid);
    let snapshot_sequence = snapshot.header.sequence_number;

    // Zero-padded log filenames make lexicographic order match sequence order.
    let offset: object_store::path::Path =
        CatalogFilePath::log(prefix, CatalogSequenceNumber::new(snapshot_sequence)).into();
    let logs_dir = CatalogFilePath::logs_dir(prefix);
    let mut metas = Vec::new();
    let mut stream = store.list_with_offset(Some(&logs_dir), &offset);
    while let Some(item) = stream.next().await {
        metas.push(item.map_err(store_err)?);
    }
    metas.sort_unstable_by(|a, b| a.location.cmp(&b.location));

    let mut records = snapshot.records;
    let mut last_sequence = snapshot_sequence;
    for meta in metas {
        let bytes = store
            .get(&meta.location)
            .await
            .map_err(store_err)?
            .bytes()
            .await
            .map_err(store_err)?;
        let file = CatalogFile::read_from(&mut Cursor::new(bytes.as_ref()))?;
        // A foreign log repaired into the snapshot would bury the uuid
        // mismatch for good.
        let file_uuid = Uuid::from_u128(file.header.catalog_uuid);
        if file_uuid != catalog_uuid {
            return Err(RepairError::Verification(format!(
                "catalog uuid mismatch at log sequence {}: snapshot is {catalog_uuid}, \
                 log is {file_uuid}",
                file.header.sequence_number
            )));
        }
        // Log files in the current, non-grouped layout are written at dense
        // sequences; a log the listing missed would be baked out of the
        // repaired snapshot permanently. (Legacy gappiness is record order
        // *inside* snapshots, which replay tolerates — not missing files.)
        if file.header.sequence_number != last_sequence + 1 {
            return Err(RepairError::LogSequenceGap {
                previous: last_sequence,
                expected: last_sequence + 1,
                found: file.header.sequence_number,
            });
        }
        last_sequence = file.header.sequence_number;
        records.extend(file.records);
    }

    let catalog_id: Arc<str> = Arc::from(prefix);
    let edits = plan_repair(Arc::clone(&catalog_id), catalog_uuid, &records)?;
    let PlannedEdits {
        renames,
        superseded,
        records: edited,
    } = edits;
    let repaired_state = verify(catalog_id, catalog_uuid, &edited, &renames)?;

    let plan = RepairPlan {
        renames,
        superseded,
        record_count: edited.len(),
        snapshot_sequence: last_sequence,
        nodes: repaired_state
            .nodes
            .resource_iter()
            .map(|n| node_report(n))
            .collect(),
    };
    let mut executed = false;
    let mut backup_path = None;
    if execute && !(plan.renames.is_empty() && plan.superseded.is_empty()) {
        quiesce_check(
            &store,
            prefix,
            &plan.nodes,
            last_sequence,
            &snapshot_path,
            &observed_version,
            quiesce_window,
            allow_unstopped,
            progress,
        )
        .await?;
        let backup = CatalogFilePath::repair_backup(prefix, snapshot_sequence);
        store
            .put(&backup, snapshot_bytes.clone().into())
            .await
            .map_err(|e| RepairError::Store(format!("writing the pre-repair backup: {e}")))?;

        let bytes = serialize_snapshot_file(catalog_uuid, last_sequence, &edited);
        let opts = PutOptions::from(PutMode::Update(observed_version));
        match store
            .put_opts(&snapshot_path, bytes.clone().into(), opts)
            .await
        {
            Ok(_) => {}
            Err(object_store::Error::Precondition { .. }) => {
                return Err(RepairError::SnapshotChanged);
            }
            // Stores without conditional writes (e.g. local files, used when
            // repairing a copied catalog) get a plain overwrite instead.
            Err(object_store::Error::NotImplemented) => {
                progress(
                    "warning: this object store does not support conditional writes; \
                     replacing the snapshot with a plain, non-atomic overwrite"
                        .to_string(),
                );
                store
                    .put(&snapshot_path, bytes.into())
                    .await
                    .map_err(store_err)?;
            }
            Err(e) => return Err(store_err(e)),
        }
        executed = true;
        backup_path = Some(backup.as_ref().to_string());
    }
    Ok(RepairOutcome {
        plan,
        executed,
        backup_path,
    })
}

/// The edits `plan_repair` computed: what it renamed and superseded, and
/// the edited record list itself.
#[derive(Debug)]
struct PlannedEdits {
    renames: Vec<PlannedRename>,
    superseded: Vec<SupersededCreate>,
    records: Vec<Record>,
}

/// Replay `records`; rename each duplicate-name `CreateTable`'s victim (in
/// the replay state and retroactively in the output record list), and front
/// each duplicate-id create with a synthetic hard delete.
fn plan_repair(
    catalog_id: Arc<str>,
    catalog_uuid: Uuid,
    records: &[Record],
) -> Result<PlannedEdits, RepairError> {
    // A restore swaps in out-of-band state that record edits can't span.
    if records
        .iter()
        .any(|r| r.id() == record_ids::RESTORE_CATALOG)
    {
        return Err(RepairError::RestoreUnsupported);
    }

    let mut catalog = InnerCatalog::new(catalog_id, catalog_uuid);
    let mut out: Vec<Record> = Vec::with_capacity(records.len());
    let mut renames = Vec::new();
    let mut superseded = Vec::new();

    for record in records {
        match record.id() {
            record_ids::CREATE_DATABASE => {
                let cd = CreateDatabase::decode(&record.data)?;
                let db_id = DbId::new(cd.database_id);
                if let Some(existing) = catalog.databases.get_by_id(&db_id) {
                    // The lenient insert rejected an id reuse whose name was
                    // also taken (under any id): no node applied it, and
                    // replaying nodes stopped at it — refuse, don't guess.
                    if catalog.databases.contains_name(&cd.database_name) {
                        return Err(RepairError::LenientlyRejectedCreate {
                            resource: "database",
                            id: cd.database_id,
                            name: cd.database_name,
                            sequence: record.sequence(),
                        });
                    }
                    if any_token_scoped_to_db(&catalog, db_id) {
                        return Err(RepairError::SupersededDatabaseHasScopedTokens {
                            id: cd.database_id,
                        });
                    }
                    if matches!(
                        existing.hard_delete_scope,
                        Some(
                            DeletionScope::DataOnlyKeepResources
                                | DeletionScope::DataOnlyRemoveTables
                        )
                    ) {
                        return Err(RepairError::SupersededHasDataOnlyScope {
                            resource: "database",
                            id: cd.database_id,
                        });
                    }
                    superseded.push(SupersededCreate {
                        database_id: cd.database_id,
                        table_id: None,
                        old_name: existing.name().to_string(),
                        new_name: cd.database_name.clone(),
                        recreate_sequence: record.sequence(),
                    });
                    let synthetic = HardDeleteDatabase {
                        db_id: cd.database_id,
                    }
                    .make_record(record.sequence());
                    apply_one(&mut catalog, &synthetic)?;
                    out.push(synthetic);
                }
                if let Some(existing) = catalog.databases.name_to_id(&cd.database_name)
                    && existing.get() != cd.database_id
                {
                    return Err(RepairError::DuplicateDatabaseName {
                        name: cd.database_name,
                        first: existing.get(),
                        second: cd.database_id,
                    });
                }
            }
            record_ids::CREATE_TABLE => {
                let ct = CreateTable::decode(&record.data)?;
                let db_id = DbId::new(ct.database_id);
                let table_id = TableId::new(ct.table_id);
                let existing_def = catalog
                    .databases
                    .get_by_id(&db_id)
                    .and_then(|db| db.tables.get_by_id(&table_id));
                if let Some(existing) = existing_def {
                    let name_taken = catalog
                        .databases
                        .get_by_id(&db_id)
                        .is_some_and(|db| db.tables.contains_name(&ct.table_name));
                    if name_taken {
                        return Err(RepairError::LenientlyRejectedCreate {
                            resource: "table",
                            id: ct.table_id,
                            name: ct.table_name,
                            sequence: record.sequence(),
                        });
                    }
                    if matches!(
                        existing.hard_delete_scope,
                        Some(
                            DeletionScope::DataOnlyKeepResources
                                | DeletionScope::DataOnlyRemoveTables
                        )
                    ) {
                        return Err(RepairError::SupersededHasDataOnlyScope {
                            resource: "table",
                            id: ct.table_id,
                        });
                    }
                    superseded.push(SupersededCreate {
                        database_id: ct.database_id,
                        table_id: Some(ct.table_id),
                        old_name: existing.table_name.to_string(),
                        new_name: ct.table_name.clone(),
                        recreate_sequence: record.sequence(),
                    });
                    let synthetic = HardDeleteTable {
                        db_id: ct.database_id,
                        table_id: ct.table_id,
                    }
                    .make_record(record.sequence());
                    apply_one(&mut catalog, &synthetic)?;
                    out.push(synthetic);
                }
                let orphan = catalog
                    .databases
                    .get_by_id(&db_id)
                    .and_then(|db| db.tables.name_to_id(&ct.table_name))
                    .filter(|id| id.get() != ct.table_id);
                if let Some(orphan_id) = orphan {
                    let rename = rename_orphan(&mut catalog, db_id, orphan_id, record, &ct)?;
                    retro_edit_create(&mut out, &rename)?;
                    renames.push(rename);
                }
            }
            _ => {}
        }
        apply_one(&mut catalog, record)?;
        out.push(record.clone());
    }

    Ok(PlannedEdits {
        renames,
        superseded,
        records: out,
    })
}

/// Rename the live table holding the contested name in the replay state.
fn rename_orphan(
    catalog: &mut InnerCatalog,
    db_id: DbId,
    orphan_id: TableId,
    record: &Record,
    ct: &CreateTable,
) -> Result<PlannedRename, RepairError> {
    let database_name = catalog
        .databases
        .get_by_id(&db_id)
        .map(|d| d.name().to_string())
        .unwrap_or_default();
    let orphaned_table_name = catalog
        .databases
        .modify_by_id::<_, ApplyError>(&db_id, |db| {
            let mut candidate = format!("{}-orphaned-{}", ct.table_name, orphan_id.get());
            let mut n = 0u32;
            while db.tables.contains_name(&candidate) {
                n += 1;
                candidate = format!("{}-orphaned-{}-{n}", ct.table_name, orphan_id.get());
            }
            let name: Arc<str> = Arc::from(candidate.as_str());
            db.tables.modify_by_id::<_, ApplyError>(&orphan_id, |t| {
                t.table_name = Arc::clone(&name);
                Ok(())
            })?;
            Ok(candidate)
        })
        .map_err(|e| RepairError::Apply {
            sequence: record.sequence(),
            message: e.0,
        })?;
    Ok(PlannedRename {
        database_id: db_id.get(),
        database_name,
        table_name: ct.table_name.clone(),
        kept_table_id: ct.table_id,
        orphaned_table_id: orphan_id.get(),
        orphaned_table_name,
        duplicate_create_sequence: record.sequence(),
    })
}

/// Rewrite the orphaned table's `CreateTable` record in the output list to
/// carry the orphan name. Header id, flags and sequence are preserved.
fn retro_edit_create(out: &mut [Record], rename: &PlannedRename) -> Result<(), RepairError> {
    let idx = out
        .iter()
        .rposition(|r| {
            r.id() == record_ids::CREATE_TABLE
                && CreateTable::decode(&r.data).is_ok_and(|c| {
                    c.table_id == rename.orphaned_table_id && c.database_id == rename.database_id
                })
        })
        .ok_or(RepairError::OrphanCreateNotFound {
            database_id: rename.database_id,
            table_id: rename.orphaned_table_id,
        })?;
    let mut ct = CreateTable::decode(&out[idx].data)?;
    ct.table_name = rename.orphaned_table_name.clone();
    let mut buf = Vec::new();
    ct.encode(&mut buf);
    out[idx] = Record::new(
        record_ids::CREATE_TABLE.raw(),
        out[idx].flags(),
        out[idx].sequence(),
        Bytes::from(buf),
    );
    Ok(())
}

/// Apply one record through the regular registry apply path.
fn apply_one(catalog: &mut InnerCatalog, record: &Record) -> Result<(), RepairError> {
    if validate_record_flags(record.id(), record.flags())? {
        let entry = REGISTRY
            .get(record.id())
            .expect("validate_record_flags confirmed the registry entry");
        (entry.decode_apply_and_event)(&record.data, catalog).map_err(|e| RepairError::Apply {
            sequence: record.sequence(),
            message: e.0,
        })?;
    }
    Ok(())
}

/// Replay the edited records from scratch, erroring on any remaining
/// duplicate-name `CreateTable`, and check the planned renames took effect.
/// Returns the final replayed state.
fn verify(
    catalog_id: Arc<str>,
    catalog_uuid: Uuid,
    records: &[Record],
    renames: &[PlannedRename],
) -> Result<InnerCatalog, RepairError> {
    let mut catalog = InnerCatalog::new(catalog_id, catalog_uuid);
    for record in records {
        match record.id() {
            record_ids::CREATE_TABLE => {
                let ct = CreateTable::decode(&record.data)?;
                let db = catalog.databases.get_by_id(&DbId::new(ct.database_id));
                let id_duplicated = db
                    .as_ref()
                    .is_some_and(|db| db.tables.get_by_id(&TableId::new(ct.table_id)).is_some());
                if id_duplicated {
                    return Err(RepairError::Verification(format!(
                        "table id {} is still duplicated at sequence {}",
                        ct.table_id,
                        record.sequence()
                    )));
                }
                // The id check above returned, so any holder of this name
                // is necessarily a different table.
                let name_duplicated = db
                    .and_then(|db| db.tables.name_to_id(&ct.table_name))
                    .is_some();
                if name_duplicated {
                    return Err(RepairError::Verification(format!(
                        "table name '{}' is still duplicated at sequence {}",
                        ct.table_name,
                        record.sequence()
                    )));
                }
            }
            record_ids::CREATE_DATABASE => {
                let cd = CreateDatabase::decode(&record.data)?;
                if catalog
                    .databases
                    .get_by_id(&DbId::new(cd.database_id))
                    .is_some()
                {
                    return Err(RepairError::Verification(format!(
                        "database id {} is still duplicated at sequence {}",
                        cd.database_id,
                        record.sequence()
                    )));
                }
            }
            _ => {}
        }
        apply_one(&mut catalog, record)?;
    }
    for (i, rename) in renames.iter().enumerate() {
        // A later collision can rename the same orphan again, re-editing its
        // create; only the final rename per orphan carries the live name.
        let renamed_again = renames[i + 1..].iter().any(|later| {
            later.database_id == rename.database_id
                && later.orphaned_table_id == rename.orphaned_table_id
        });
        if renamed_again {
            continue;
        }
        // Later history may rename the orphan again (soft delete) or drop it;
        // only when its repair name is live must it resolve to the orphan.
        let resolved = catalog
            .databases
            .get_by_id(&DbId::new(rename.database_id))
            .and_then(|db| db.tables.name_to_id(&rename.orphaned_table_name));
        if let Some(id) = resolved
            && id.get() != rename.orphaned_table_id
        {
            return Err(RepairError::Verification(format!(
                "'{}' resolves to table {} instead of the orphaned table {}",
                rename.orphaned_table_name,
                id.get(),
                rename.orphaned_table_id
            )));
        }
    }
    Ok(catalog)
}

/// Whether any token holds a permission scoped to `db_id`; a synthetic hard
/// delete would capture the superseded name into that token's metadata.
fn any_token_scoped_to_db(catalog: &InnerCatalog, db_id: DbId) -> bool {
    catalog.tokens.repo().resource_iter().any(|token| {
        token.permissions.iter().any(|permission| {
            matches!(
                &permission.resource_identifier,
                influxdb3_authz::ResourceIdentifier::Database(ids) if ids.contains(&db_id)
            )
        })
    })
}

fn node_report(def: &NodeDefinition) -> NodeReport {
    let (state, confirmed_stopped) = match def.state() {
        NodeState::Running { .. } => ("running", false),
        NodeState::Stopping { .. } => ("stopping", false),
        NodeState::Stopped { .. } => ("stopped", true),
        NodeState::Removing { .. } => ("removing", false),
    };
    NodeReport {
        node_id: def.node_id.to_string(),
        state: state.to_string(),
        confirmed_stopped,
    }
}

/// Gate an `--execute` write: refuse while the node registry lists nodes not
/// confirmed stopped (unless `allow_unstopped`), then watch the store for
/// `window` and refuse if any catalog or WAL object appears — or if the
/// snapshot itself was replaced since the repair read it.
#[allow(clippy::too_many_arguments)]
async fn quiesce_check(
    store: &Arc<dyn ObjectStore>,
    prefix: &str,
    nodes: &[NodeReport],
    last_sequence: u64,
    snapshot_path: &CatalogFilePath,
    observed_version: &UpdateVersion,
    window: Duration,
    allow_unstopped: bool,
    progress: &mut (dyn FnMut(String) + Send),
) -> Result<(), RepairError> {
    let not_stopped: Vec<&NodeReport> = nodes.iter().filter(|n| !n.confirmed_stopped).collect();
    if not_stopped.is_empty() {
        progress(format!(
            "catalog reports every node stopped; confirming with a {}s quiet window \
             on the object store",
            window.as_secs()
        ));
    } else {
        if !allow_unstopped {
            return Err(RepairError::NodesNotStopped {
                nodes: not_stopped
                    .iter()
                    .map(|n| format!("{} ({})", n.node_id, n.state))
                    .collect(),
            });
        }
        let list = not_stopped
            .iter()
            .map(|n| format!("{} ({})", n.node_id, n.state))
            .collect::<Vec<_>>()
            .join(", ");
        progress(format!(
            "warning: the catalog does not confirm these nodes as stopped: {list}. \
             Proceeding on --allow-unstopped-nodes; watching the object store for \
             {}s for evidence of activity instead",
            window.as_secs()
        ));
    }

    // Newest existing WAL object per node; only later arrivals are evidence.
    let mut wal_baseline: Vec<(String, Option<ObjPath>)> = Vec::new();
    for n in nodes {
        let dir = ObjPath::from(format!("{}/wal", n.node_id));
        let mut newest: Option<ObjPath> = None;
        let mut listing = store.list(Some(&dir));
        while let Some(item) = listing.next().await {
            let meta = item.map_err(store_err)?;
            if newest.as_ref().is_none_or(|max| meta.location > *max) {
                newest = Some(meta.location);
            }
        }
        wal_baseline.push((n.node_id.clone(), newest));
    }

    tokio::time::sleep(window).await;

    let mut evidence = Vec::new();
    // Any catalog log beyond what the repair read, pre-existing included.
    let offset: ObjPath =
        CatalogFilePath::log(prefix, CatalogSequenceNumber::new(last_sequence)).into();
    let logs_dir = CatalogFilePath::logs_dir(prefix);
    let mut listing = store.list_with_offset(Some(&logs_dir), &offset);
    while let Some(item) = listing.next().await {
        evidence.push(format!("catalog log {}", item.map_err(store_err)?.location));
    }
    for (node_id, newest) in &wal_baseline {
        let dir = ObjPath::from(format!("{node_id}/wal"));
        let mut listing = match newest {
            Some(max) => store.list_with_offset(Some(&dir), max),
            None => store.list(Some(&dir)),
        };
        while let Some(item) = listing.next().await {
            evidence.push(format!("wal object {}", item.map_err(store_err)?.location));
        }
    }
    let head = store.head(snapshot_path).await.map_err(store_err)?;
    if head.e_tag != observed_version.e_tag || head.version != observed_version.version {
        evidence.push(format!(
            "catalog snapshot {} was rewritten",
            snapshot_path.as_ref()
        ));
    }

    if !evidence.is_empty() {
        return Err(RepairError::ClusterActive {
            window_secs: window.as_secs(),
            evidence,
        });
    }
    progress("no catalog or WAL activity observed; proceeding with the write".to_string());
    Ok(())
}

fn store_err(e: impl std::fmt::Display) -> RepairError {
    RepairError::Store(e.to_string())
}

#[cfg(test)]
mod tests;
