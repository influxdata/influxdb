//! Repository id-counter records (record_id 25).

use influxdb3_id::{
    ColumnId, DbId, DistinctCacheId, FieldFamilyId, LastCacheId, NodeId, QueryGroupId, RoleId,
    TableId, TokenId, TriggerId,
};

use super::impl_bitcode_encoding;
use crate::catalog::versions::v3::events::CatalogEvent;
use crate::catalog::versions::v3::inner::InnerCatalog;
use crate::catalog::versions::v3::schema::database::DatabaseSchema;
use crate::catalog::versions::v3::schema::table::TableDefinition;
use crate::format::apply::ApplyError;
use crate::format::{CatalogRecord, RecordFlags, RecordId, RegisteredRecord, record_ids};

/// Set a repository's next-id counter.
///
/// The catalog derives a repository's `next_id` from `max(present id) + 1`, so
/// hard-deleting the highest-id member would otherwise let the counter regress
/// and reuse an id. This record carries the counter explicitly. It is emitted
/// by the v2 → v3 migration (and, in future, by snapshot compaction) for any
/// repository whose counter has advanced past `max(present id) + 1`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, bitcode::Encode, bitcode::Decode)]
pub struct SetNextId {
    /// Which repository the counter belongs to.
    pub scope: NextIdScope,
    /// The next id value. Widened to `u128` to fit the widest repo id
    /// (`TokenId`); narrowed and validated per scope on apply.
    pub id: u64,
}

/// Identifies which repository's id counter a [`SetNextId`] record targets.
/// Nested variants carry the IDs of the parent resource(s) whose
/// sub-repository is addressed (a database, or a database + table).
///
/// # Adding variants
///
/// `NextIdScope` is part of the on-disk format and follows the append-only
/// rule for bitcode enums (see [`super::types`]): a new variant goes at the
/// **end**; existing discriminants are positional and frozen; never reorder,
/// insert, or remove one — a retired entity type keeps its now-dead variant
/// so old records still decode. Add a variant when a *new* entity type becomes
/// removable (and so can leave a gap in its counter).
///
/// If new variants are added, it is assumed that they would not be serialized
/// into the log during an upgrade, because any HardDelete* records that they
/// are intended to replace would be gated, and therefore not written, until
/// the entire cluster has upgraded.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, bitcode::Encode, bitcode::Decode)]
pub enum NextIdScope {
    Nodes,
    Databases,
    Tokens,
    Tables {
        database_id: u32,
    },
    Triggers {
        database_id: u32,
    },
    Columns {
        database_id: u32,
        table_id: u32,
    },
    FieldFamilies {
        database_id: u32,
        table_id: u32,
    },
    LastCaches {
        database_id: u32,
        table_id: u32,
    },
    DistinctCaches {
        database_id: u32,
        table_id: u32,
    },
    Roles,
    /// Query groups can be deleted, so snapshot compaction may need to
    /// preserve a `next_id` that has advanced past `max(present id) + 1`.
    QueryGroups,
}

impl CatalogRecord for SetNextId {
    const ID: RecordId = record_ids::SET_NEXT_ID;
    const FLAGS: RecordFlags = RecordFlags::none();
    const NAME: &'static str = "SetNextId";

    fn apply(&self, catalog: &mut InnerCatalog) -> Result<(), ApplyError> {
        match self.scope {
            NextIdScope::Nodes => catalog.nodes.set_next_id(NodeId::new(self.narrow_u32()?)),
            NextIdScope::Databases => catalog.databases.set_next_id(DbId::new(self.narrow_u32()?)),
            NextIdScope::Tokens => catalog.tokens.set_next_id(TokenId::from(self.id)),
            NextIdScope::Tables { database_id } => {
                self.with_database(catalog, database_id, |db| {
                    db.tables.set_next_id(TableId::new(self.narrow_u32()?));
                    Ok(())
                })?
            }
            NextIdScope::Triggers { database_id } => {
                self.with_database(catalog, database_id, |db| {
                    db.processing_engine_triggers
                        .set_next_id(TriggerId::new(self.narrow_u32()?));
                    Ok(())
                })?
            }
            NextIdScope::Columns {
                database_id,
                table_id,
            } => self.with_table(catalog, database_id, table_id, |table| {
                table.columns.set_next_id(ColumnId::new(self.narrow_u16()?));
                Ok(())
            })?,
            NextIdScope::FieldFamilies {
                database_id,
                table_id,
            } => self.with_table(catalog, database_id, table_id, |table| {
                table
                    .field_families
                    .set_next_id(FieldFamilyId::new(self.narrow_u16()?));
                Ok(())
            })?,
            NextIdScope::LastCaches {
                database_id,
                table_id,
            } => self.with_table(catalog, database_id, table_id, |table| {
                table
                    .last_caches
                    .set_next_id(LastCacheId::new(self.narrow_u16()?));
                Ok(())
            })?,
            NextIdScope::DistinctCaches {
                database_id,
                table_id,
            } => self.with_table(catalog, database_id, table_id, |table| {
                table
                    .distinct_caches
                    .set_next_id(DistinctCacheId::new(self.narrow_u16()?));
                Ok(())
            })?,
            NextIdScope::Roles => catalog.roles.set_next_id(RoleId::new(self.id)),
            NextIdScope::QueryGroups => catalog
                .query_groups
                .set_next_id(QueryGroupId::new(self.narrow_u32()?)),
        }
        Ok(())
    }

    fn event(&self) -> CatalogEvent {
        CatalogEvent::NextIdSet
    }
}

impl SetNextId {
    fn narrow_u32(&self) -> Result<u32, ApplyError> {
        u32::try_from(self.id).map_err(|_| self.overflow_err("u32"))
    }

    fn narrow_u16(&self) -> Result<u16, ApplyError> {
        u16::try_from(self.id).map_err(|_| self.overflow_err("u16"))
    }

    fn overflow_err(&self, ty: &str) -> ApplyError {
        ApplyError(format!(
            "{}: scope {:?}: id {} overflows {ty}",
            Self::NAME,
            self.scope,
            self.id,
        ))
    }

    fn with_database(
        &self,
        catalog: &mut InnerCatalog,
        database_id: u32,
        f: impl FnOnce(&mut DatabaseSchema) -> Result<(), ApplyError>,
    ) -> Result<(), ApplyError> {
        let db_id = DbId::new(database_id);
        catalog.databases.modify_by_id(&db_id, f)
    }

    fn with_table(
        &self,
        catalog: &mut InnerCatalog,
        database_id: u32,
        table_id: u32,
        f: impl FnOnce(&mut TableDefinition) -> Result<(), ApplyError>,
    ) -> Result<(), ApplyError> {
        let table_id = TableId::new(table_id);
        self.with_database(catalog, database_id, |db| {
            db.tables.modify_by_id(&table_id, f)
        })
    }
}

inventory::submit! {
    RegisteredRecord::new::<SetNextId>()
}

impl_bitcode_encoding!(SetNextId);

#[cfg(test)]
mod tests;
