use std::collections::BTreeMap;

use regex::Regex;

use crate::{
    consistent_hasher::ConsistentHasher, server_id::ServerId, write_buffer::WriteBufferConnection,
};

#[derive(Debug, Eq, PartialEq, Hash, PartialOrd, Ord, Clone, Copy)]
pub struct ShardId(u32);

impl ShardId {
    pub fn new(id: u32) -> Self {
        Self(id)
    }

    pub fn get(&self) -> u32 {
        self.0
    }
}

/// ShardConfig defines rules for assigning a line/row to an individual
/// host or a group of hosts. A shard
/// is a logical concept, but the usage is meant to split data into
/// mutually exclusive areas. The rough order of organization is:
/// database -> shard -> partition -> chunk. For example, you could shard
/// based on table name and assign to 1 of 10 shards. Within each
/// shard you would have partitions, which would likely be based off time.
/// This makes it possible to horizontally scale out writes.
#[derive(Debug, Eq, PartialEq, Clone, Default)]
pub struct ShardConfig {
    /// Each matcher, if any, is evaluated in order.
    /// If there is a match, the route will be evaluated to
    /// the given targets, otherwise the hash ring will be evaluated.
    /// This is useful for overriding the hashring function on some hot spot. For
    /// example, if you use the table name as the input to the hash function
    /// and your ring has 4 slots. If two tables that are very hot get
    /// assigned to the same slot you can override that by putting in a
    /// specific matcher to pull that table over to a different node.
    pub specific_targets: Vec<MatcherToShard>,

    /// An optional default hasher which will route to one in a collection of
    /// nodes.
    pub hash_ring: Option<HashRing>,
}

/// Maps a matcher with specific shard. If the line/row matches
/// it should be sent to the group.
#[derive(Debug, Eq, PartialEq, Clone)]
pub struct MatcherToShard {
    pub matcher: Matcher,
    pub shard: ShardId,
}

/// HashRing is a rule for creating a hash key for a row and mapping that to
/// an individual node on a ring.
#[derive(Debug, Eq, PartialEq, Clone, Default)]
pub struct HashRing {
    /// ring of shard ids
    pub shards: ConsistentHasher<ShardId>,
}

/// A matcher is used to match routing rules or subscriptions on a row-by-row
/// (or line) basis.
#[derive(Debug, Clone, Default)]
pub struct Matcher {
    /// if provided, match if the table name matches against the regex
    pub table_name_regex: Option<Regex>,
}

impl PartialEq for Matcher {
    fn eq(&self, other: &Self) -> bool {
        // this is kind of janky, but it's only used during tests and should get the job
        // done
        format!("{:?}", self.table_name_regex) == format!("{:?}", other.table_name_regex)
    }
}
impl Eq for Matcher {}

/// Sinks for query requests.
///
/// Queries are sent to one of these sinks and the resulting data is received from it.
///
/// Note that the query results are flowing into the opposite direction (aka a query sink is a result source).
#[derive(Debug, Eq, PartialEq, Clone, Default)]
pub struct QuerySinks {
    pub grpc_remotes: Vec<ServerId>,
}

#[derive(Debug, Eq, PartialEq, Clone)]
pub enum WriteSinkVariant {
    /// gRPC-based remote, addressed by its server ID.
    GrpcRemote(ServerId),

    /// Write buffer connection.
    WriteBuffer(WriteBufferConnection),
}

/// Sink of write requests aka new data.
///
/// Data is sent to this sink and a status is received from it.
#[derive(Debug, Eq, PartialEq, Clone)]
pub struct WriteSink {
    pub sink: WriteSinkVariant,

    /// If set, errors during writing to this sink are ignored and do NOT lead to an overall failure.
    pub ignore_errors: bool,
}

/// Set of write sinks.
#[derive(Debug, Eq, PartialEq, Clone)]
pub struct WriteSinkSet {
    /// Sinks within the set.
    pub sinks: Vec<WriteSink>,
}

/// Router for writes and queries.
#[derive(Debug, Eq, PartialEq, Clone)]
pub struct Router {
    /// Router name.
    ///
    /// The name corresponds to the database name on the database node.
    ///
    /// The router name is unique for this router node.
    pub name: String,

    /// Write sharder.
    pub write_sharder: ShardConfig,

    /// Sinks for write requests.
    pub write_sinks: BTreeMap<ShardId, WriteSinkSet>,

    /// Sinks for query requests.
    pub query_sinks: QuerySinks,
}
