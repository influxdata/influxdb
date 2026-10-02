//! Query group catalog operations.

use std::sync::Arc;

use std::num::NonZeroUsize;

use influxdb3_id::{NodeId, QueryGroupId};

use super::CatalogOp;
use crate::CatalogError;
use crate::catalog::versions::v3::inner::InnerCatalog;
use crate::catalog::versions::v3::schema::query_group::{
    QueryGroupDefinition, QueryGroupInsertPosition, QueryGroupUpdate,
};
use crate::format::RecordBatch;
use crate::format::records::{CreateQueryGroup, DeleteQueryGroup, UpdateQueryGroup};
use crate::resource::CatalogResource;

/// Reject a member that already belongs to a different query group.
///
/// `skip` names the group being created or edited. A node already in that group
/// is not a conflict, so a rename or a member reorder still goes through.
fn reject_member_of_another_group(
    catalog: &InnerCatalog,
    members: &[NodeId],
    skip: Option<QueryGroupId>,
) -> Result<(), CatalogError> {
    for member in members {
        let Some(group) = catalog
            .query_groups
            .resource_iter()
            .filter(|group| Some(group.id()) != skip)
            .find(|group| group.members().contains(member))
        else {
            continue;
        };

        // Name the node the way the operator does. A stored member always
        // resolves, so the catalog id is only a fallback.
        let node_id = catalog
            .nodes
            .get_by_id(member)
            .map_or_else(|| Arc::from(member.to_string()), |node| node.node_id());

        return Err(CatalogError::NodeAlreadyInQueryGroup {
            node_id,
            query_group_name: group.name(),
            query_group_id: group.id(),
        });
    }

    Ok(())
}

/// Check that every member is a registered node that advertises a connection
/// address.
///
/// Members dial each other directly, so a node the others cannot reach serves
/// nothing and makes a distributed query fail. Reject it at the write, naming
/// the node, rather than leaving the group in a state only a log warning
/// reports.
///
/// Only the member list an operation writes is checked. An operation that
/// leaves members alone, or that removes one, must keep working: those are how
/// an operator repairs a group whose member lost its address.
fn validate_members_reachable(
    members: &[NodeId],
    catalog: &InnerCatalog,
) -> Result<(), CatalogError> {
    for member in members {
        let node = catalog
            .nodes
            .get_by_id(member)
            .ok_or_else(|| CatalogError::NotFound(format!("query group member node {member}")))?;
        if node.conn_info().is_none() {
            return Err(CatalogError::QueryGroupMemberNotConnectable {
                node_id: node.node_id(),
            });
        }
    }
    Ok(())
}

fn push_update_record(
    records: &mut RecordBatch,
    id: QueryGroupId,
    name: &str,
    members: &[NodeId],
    replication_factor: NonZeroUsize,
) {
    records.push(&UpdateQueryGroup {
        query_group_id: id.get(),
        query_group_name: name.to_string(),
        members: members.iter().map(|m| m.get()).collect(),
        replication_factor: replication_factor.get() as u64,
    });
}

// ---------------------------------------------------------------------------
// CreateQueryGroup
// ---------------------------------------------------------------------------

#[derive(Debug)]
pub(crate) struct CreateQueryGroupArgs {
    /// Unique operator-facing name for the new group.
    pub name: Arc<str>,
    /// Query nodes in operator-supplied order.
    pub members: Vec<NodeId>,
    /// Number of in-memory copies the group keeps of each ingester stream and shard.
    pub replication_factor: NonZeroUsize,
}

pub(crate) struct CreateQueryGroupOp {
    query_group_id: QueryGroupId,
}

impl CatalogOp for CreateQueryGroupOp {
    type Input = CreateQueryGroupArgs;
    type Output = Arc<QueryGroupDefinition>;

    fn prepare(
        args: &Self::Input,
        catalog: &InnerCatalog,
        records: &mut RecordBatch,
    ) -> Result<Self, CatalogError> {
        // Reject a duplicate name early so callers get AlreadyExists, not an
        // internal apply error.
        if catalog.query_groups.name_to_id(&args.name).is_some() {
            return Err(CatalogError::AlreadyExists);
        }

        reject_member_of_another_group(catalog, &args.members, None)?;

        validate_members_reachable(&args.members, catalog)?;

        let query_group_id = catalog.query_groups.next_id();

        records.push(&CreateQueryGroup {
            query_group_id: query_group_id.get(),
            query_group_name: args.name.to_string(),
            members: args.members.iter().map(|m| m.get()).collect(),
            replication_factor: args.replication_factor.get() as u64,
        });

        Ok(Self { query_group_id })
    }

    fn output(&self, catalog: &InnerCatalog) -> Self::Output {
        catalog
            .query_groups
            .get_by_id(&self.query_group_id)
            .expect("query group should exist after create")
    }
}

// ---------------------------------------------------------------------------
// UpdateQueryGroup
// ---------------------------------------------------------------------------

#[derive(Debug)]
pub(crate) struct UpdateQueryGroupArgs {
    /// ID of the query group to update.
    pub id: QueryGroupId,
    /// PATCH-style update payload.
    pub update: QueryGroupUpdate,
}

pub(crate) struct UpdateQueryGroupOp {
    query_group_id: QueryGroupId,
}

impl CatalogOp for UpdateQueryGroupOp {
    type Input = UpdateQueryGroupArgs;
    type Output = Arc<QueryGroupDefinition>;

    fn prepare(
        args: &Self::Input,
        catalog: &InnerCatalog,
        records: &mut RecordBatch,
    ) -> Result<Self, CatalogError> {
        let group = catalog
            .query_groups
            .get_by_id(&args.id)
            .ok_or_else(|| CatalogError::NotFound(format!("query group id {}", args.id)))?;

        let name = args
            .update
            .name
            .as_ref()
            .map_or_else(|| Arc::<str>::clone(&group.name), Arc::clone);
        if catalog
            .query_groups
            .name_to_id(&name)
            .is_some_and(|owner| owner != args.id)
        {
            // Reject a duplicate name that is already owned by a different query group.
            return Err(CatalogError::AlreadyExists);
        }

        let members = match args.update.members.as_deref() {
            Some(new_members) => {
                reject_member_of_another_group(catalog, new_members, Some(args.id))?;
                validate_members_reachable(new_members, catalog)?;
                new_members
            }
            None => &group.members,
        };

        let replication_factor = args
            .update
            .replication_factor
            .unwrap_or(group.replication_factor);

        push_update_record(records, args.id, &name, members, replication_factor);

        Ok(Self {
            query_group_id: args.id,
        })
    }

    fn output(&self, catalog: &InnerCatalog) -> Self::Output {
        catalog
            .query_groups
            .get_by_id(&self.query_group_id)
            .expect("query group should exist after update")
    }
}

// ---------------------------------------------------------------------------
// UpdateQueryGroupMembers
// ---------------------------------------------------------------------------

#[derive(Debug)]
pub(crate) enum QueryGroupMemberMutation {
    Add {
        member: NodeId,
        position: QueryGroupInsertPosition,
    },
    Remove {
        member: NodeId,
    },
}

#[derive(Debug)]
pub(crate) struct UpdateQueryGroupMembersArgs {
    /// ID of the query group whose members should be updated.
    pub id: QueryGroupId,
    /// Member-list mutation to apply.
    pub mutation: QueryGroupMemberMutation,
}

pub(crate) struct UpdateQueryGroupMembersOp {
    query_group_id: QueryGroupId,
}

impl CatalogOp for UpdateQueryGroupMembersOp {
    type Input = UpdateQueryGroupMembersArgs;
    type Output = Arc<QueryGroupDefinition>;

    fn prepare(
        args: &Self::Input,
        catalog: &InnerCatalog,
        records: &mut RecordBatch,
    ) -> Result<Self, CatalogError> {
        let group = catalog
            .query_groups
            .get_by_id(&args.id)
            .ok_or_else(|| CatalogError::NotFound(format!("query group id {}", args.id)))?;

        let mut members = group.members.clone();
        match &args.mutation {
            QueryGroupMemberMutation::Add { member, position } => {
                reject_member_of_another_group(
                    catalog,
                    std::slice::from_ref(member),
                    Some(args.id),
                )?;
                validate_members_reachable(std::slice::from_ref(member), catalog)?;
                let insert_at = match *position {
                    QueryGroupInsertPosition::Append => members.len(),
                    QueryGroupInsertPosition::Index(index) => {
                        if index > members.len() {
                            return Err(CatalogError::invalid_configuration(format!(
                                "query group insert index {index} is greater than member count {}",
                                members.len()
                            )));
                        }
                        index
                    }
                };
                members.insert(insert_at, *member);
            }
            QueryGroupMemberMutation::Remove { member } => {
                let index = members.iter().position(|existing| existing == member);
                match index {
                    Some(index) => {
                        members.remove(index);
                    }
                    None => {
                        return Err(CatalogError::NotFound(format!(
                            "query group member node {member}"
                        )));
                    }
                }
            }
        }

        push_update_record(
            records,
            args.id,
            &group.name,
            &members,
            group.replication_factor,
        );

        Ok(Self {
            query_group_id: args.id,
        })
    }

    fn output(&self, catalog: &InnerCatalog) -> Self::Output {
        catalog
            .query_groups
            .get_by_id(&self.query_group_id)
            .expect("query group should exist after member update")
    }
}

// ---------------------------------------------------------------------------
// DeleteQueryGroup
// ---------------------------------------------------------------------------

#[derive(Debug)]
pub(crate) struct DeleteQueryGroupArgs {
    /// Catalog ID of the group to remove.
    pub id: QueryGroupId,
}

pub(crate) struct DeleteQueryGroupOp {
    // Captured during prepare so output() can return it after the group is
    // removed from the catalog by record apply.
    removed: Arc<QueryGroupDefinition>,
}

impl CatalogOp for DeleteQueryGroupOp {
    type Input = DeleteQueryGroupArgs;
    type Output = Arc<QueryGroupDefinition>;

    fn prepare(
        args: &Self::Input,
        catalog: &InnerCatalog,
        records: &mut RecordBatch,
    ) -> Result<Self, CatalogError> {
        let group = catalog
            .query_groups
            .get_by_id(&args.id)
            .ok_or_else(|| CatalogError::NotFound(format!("query group id {}", args.id)))?;

        records.push(&DeleteQueryGroup {
            query_group_id: group.id.get(),
        });

        Ok(Self { removed: group })
    }

    fn output(&self, _catalog: &InnerCatalog) -> Self::Output {
        Arc::clone(&self.removed)
    }
}

#[cfg(test)]
mod tests;
