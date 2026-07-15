use std::num::NonZeroUsize;
use std::sync::Arc;

use influxdb3_id::{NodeId, QueryGroupId};

use crate::resource::CatalogResource;

/// An operator-defined query group in the catalog.
///
/// A query group names a set of query nodes that collectively buffer data from
/// ingesters and compacted shards. The [Distributed Query design doc](https://github.com/influxdata/influxdb_pro/blob/main/docs/distributed-queries.md)
/// describes how these inputs feed deterministic query data placement.
///
/// Member order is operator-supplied and semantically significant, so it is
/// preserved exactly and never sorted by `NodeId`.
#[derive(Debug, Eq, PartialEq, Clone)]
pub struct QueryGroupDefinition {
    /// Unique catalog identifier for the query group.
    pub(crate) id: QueryGroupId,
    /// Unique operator-facing name for the query group.
    pub(crate) name: Arc<str>,
    /// Query nodes that belong to the group, in operator-supplied order.
    pub(crate) members: Vec<NodeId>,
    /// Number of in-memory copies the group keeps of each ingester stream and
    /// compacted shard.
    pub(crate) replication_factor: NonZeroUsize,
}

impl QueryGroupDefinition {
    /// Create a new query group definition.
    ///
    /// Member order is preserved exactly as supplied.
    pub fn new(
        id: QueryGroupId,
        name: Arc<str>,
        members: Vec<NodeId>,
        replication_factor: NonZeroUsize,
    ) -> Self {
        Self {
            id,
            name,
            members,
            replication_factor,
        }
    }

    /// Query nodes that belong to the group, in operator-supplied order.
    pub fn members(&self) -> &[NodeId] {
        &self.members
    }

    /// Number of in-memory copies the group keeps of each ingester stream and
    /// compacted shard.
    pub fn replication_factor(&self) -> NonZeroUsize {
        self.replication_factor
    }
}

impl CatalogResource for QueryGroupDefinition {
    type Identifier = QueryGroupId;

    const CATEGORY: &'static str = "query_groups";

    fn id(&self) -> Self::Identifier {
        self.id
    }

    fn name(&self) -> Arc<str> {
        Arc::clone(&self.name)
    }
}

/// Position at which to insert a query node into a query group.
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum QueryGroupInsertPosition {
    /// Append the query node after the current last member.
    Append,
    /// Insert the query node at the given zero-based member index.
    Index(usize),
}

/// Partial query group update used by catalog mutation producers.
///
/// Each omitted field keeps the current value from the stored
/// [`QueryGroupDefinition`]. Producers turn this PATCH-style input into a full
/// `UpdateQueryGroup` record before persistence.
#[derive(Debug, Default, Clone, Eq, PartialEq)]
pub struct QueryGroupUpdate {
    /// New operator-facing name. `None` keeps the current name.
    pub name: Option<Arc<str>>,
    /// New complete member list. `None` keeps the current members.
    pub members: Option<Vec<NodeId>>,
    /// New replication factor. `None` keeps the current replication factor.
    pub replication_factor: Option<NonZeroUsize>,
}
