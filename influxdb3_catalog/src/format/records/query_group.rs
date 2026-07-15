//! Query group operations (record_ids e7-e9).
//!
//! A query group is an ordered list of query nodes set up by the operator. It
//! acts as one availability zone. The group holds a full copy of the data, so
//! it can answer any query on its own.
//!
//! The query work is split across the query nodes in the query group. Each node
//! reads only some of the ingester streams and compacted shards. No single node
//! holds everything, but together the query nodes cover all the data.
//!
//! See the
//! [Distributed Query design doc](https://github.com/influxdata/influxdb_pro/blob/main/docs/distributed-queries.md).

use std::num::NonZeroUsize;
use std::sync::Arc;

use influxdb3_id::{NodeId, QueryGroupId};

use super::impl_bitcode_encoding;
use crate::catalog::versions::v3::events::CatalogEvent;
use crate::catalog::versions::v3::inner::InnerCatalog;
use crate::catalog::versions::v3::schema::query_group::QueryGroupDefinition;
use crate::format::apply::ApplyError;
use crate::format::{CatalogRecord, RecordFlags, RecordId, RegisteredRecord, record_ids};

// ---------------------------------------------------------------------------
// Helper functions
// ---------------------------------------------------------------------------

/// Convert the frozen `replication_factor` wire value into a [`NonZeroUsize`],
/// rejecting zero (which would make data placement unrepresentable).
fn decode_replication_factor(record: &'static str, value: u64) -> Result<NonZeroUsize, ApplyError> {
    usize::try_from(value)
        .ok()
        .and_then(NonZeroUsize::new)
        .ok_or_else(|| ApplyError(format!("{record}: invalid replication factor {value}")))
}

/// Create a new query group.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, bitcode::Encode, bitcode::Decode)]
pub struct CreateQueryGroup {
    /// The query group's catalog ID.
    pub query_group_id: u32,
    /// Unique operator-facing name for the query group.
    pub query_group_name: String,
    /// Query node members with catalog IDs, in operator-supplied order.
    pub members: Vec<u32>,
    /// Number of in-memory copies the group keeps of each ingester stream and
    /// compacted shard.
    pub replication_factor: u64,
}

impl CatalogRecord for CreateQueryGroup {
    const ID: RecordId = record_ids::CREATE_QUERY_GROUP;
    const FLAGS: RecordFlags = RecordFlags::none();
    const NAME: &'static str = "CreateQueryGroup";

    fn apply(&self, catalog: &mut InnerCatalog) -> Result<(), ApplyError> {
        let id = QueryGroupId::new(self.query_group_id);
        let name: Arc<str> = Arc::from(self.query_group_name.as_str());

        // Reject duplicate ID or name explicitly before insert. `Repository::insert`
        // only rejects when the same ID and name already exist together, so a
        // bare insert could otherwise overwrite an existing group when only one
        // of the two collides.
        if catalog.query_groups.contains_id(&id) {
            return Err(ApplyError(format!(
                "{}: query group id {} already exists",
                Self::NAME,
                self.query_group_id,
            )));
        }
        if catalog.query_groups.name_to_id(&name).is_some() {
            return Err(ApplyError(format!(
                "{}: query group name '{}' already exists",
                Self::NAME,
                self.query_group_name,
            )));
        }

        let members: Vec<_> = self.members.iter().copied().map(NodeId::new).collect();
        let replication_factor = decode_replication_factor(Self::NAME, self.replication_factor)?;

        let definition = QueryGroupDefinition::new(id, name, members, replication_factor);
        catalog.query_groups.insert(id, definition)?;
        Ok(())
    }

    fn event(&self) -> CatalogEvent {
        CatalogEvent::QueryGroupCreated {
            query_group_id: QueryGroupId::new(self.query_group_id),
        }
    }
}

inventory::submit! {
    RegisteredRecord::new::<CreateQueryGroup>()
}

/// Replace an existing query group's name, members, and replication factor.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, bitcode::Encode, bitcode::Decode)]
pub struct UpdateQueryGroup {
    /// The query group's catalog ID.
    pub query_group_id: u32,
    /// Unique operator-facing name for the query group.
    pub query_group_name: String,
    /// Query node members with catalog IDs, in operator-supplied order.
    pub members: Vec<u32>,
    /// Number of in-memory copies the group keeps of each ingester stream and
    /// compacted shard.
    pub replication_factor: u64,
}

impl CatalogRecord for UpdateQueryGroup {
    const ID: RecordId = record_ids::UPDATE_QUERY_GROUP;
    const FLAGS: RecordFlags = RecordFlags::none();
    const NAME: &'static str = "UpdateQueryGroup";

    fn apply(&self, catalog: &mut InnerCatalog) -> Result<(), ApplyError> {
        let id = QueryGroupId::new(self.query_group_id);
        let name: Arc<str> = Arc::from(self.query_group_name.as_str());

        if !catalog.query_groups.contains_id(&id) {
            return Err(ApplyError(format!(
                "{}: query group id {} does not exist",
                Self::NAME,
                self.query_group_id,
            )));
        }
        // Fail the rename if another query group already uses that name.
        // If the query group is the same one that already has the name, allow it.
        // The update may be trying to change other properties without renaming.
        if catalog
            .query_groups
            .name_to_id(&name)
            .is_some_and(|owner| owner != id)
        {
            return Err(ApplyError(format!(
                "{}: query group name '{}' already exists",
                Self::NAME,
                self.query_group_name,
            )));
        }

        let members: Vec<_> = self.members.iter().copied().map(NodeId::new).collect();
        let replication_factor = decode_replication_factor(Self::NAME, self.replication_factor)?;

        let definition = QueryGroupDefinition::new(id, name, members, replication_factor);
        catalog.query_groups.update(id, definition)?;
        Ok(())
    }

    fn event(&self) -> CatalogEvent {
        CatalogEvent::QueryGroupUpdated {
            query_group_id: QueryGroupId::new(self.query_group_id),
        }
    }
}

inventory::submit! {
    RegisteredRecord::new::<UpdateQueryGroup>()
}

/// Remove an existing query group from the catalog.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, bitcode::Encode, bitcode::Decode)]
pub struct DeleteQueryGroup {
    /// The query group's catalog ID.
    pub query_group_id: u32,
}

impl CatalogRecord for DeleteQueryGroup {
    const ID: RecordId = record_ids::DELETE_QUERY_GROUP;
    const FLAGS: RecordFlags = RecordFlags::none();
    const NAME: &'static str = "DeleteQueryGroup";

    fn apply(&self, catalog: &mut InnerCatalog) -> Result<(), ApplyError> {
        let id = QueryGroupId::new(self.query_group_id);
        if !catalog.query_groups.contains_id(&id) {
            return Err(ApplyError(format!(
                "{}: query group id {} does not exist",
                Self::NAME,
                self.query_group_id,
            )));
        }
        catalog.query_groups.remove(&id);
        Ok(())
    }

    fn event(&self) -> CatalogEvent {
        CatalogEvent::QueryGroupDeleted {
            query_group_id: QueryGroupId::new(self.query_group_id),
        }
    }
}

inventory::submit! {
    RegisteredRecord::new::<DeleteQueryGroup>()
}

impl_bitcode_encoding!(CreateQueryGroup, UpdateQueryGroup, DeleteQueryGroup);

#[cfg(test)]
mod tests;
