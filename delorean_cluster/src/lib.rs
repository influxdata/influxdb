//! This module contains code that defines how Delorean servers talk to each other.
//! This includes replication, subscriptions, querying, and traits that abstract these
//! methods away for testing purposes.
//!
//! This diagram shows the lifecycle of a write coming into a set of delorean servers
//! configured in different roles. This doesn't include ensuring that the replicated
//! writes are durable, or snapshotting partitions in the write buffer. Have a read
//! through the comments in the source before trying to make sense of this diagram.
//!
//! Each level of servers exists to serve a specific function, ideally isolating the
//! kinds of failures that would cause one portion to go down.
//!
//! The router level simply converts the line protocol to the flatbuffer format and
//! computes the partition key. It keeps no state.
//!
//! The HostGroup/AZ level is for receiving the replicated writes and keeping multiple
//! copies of those writes in memory before they are persisted to object storage. Individual
//! databases or groups of databases can be routed to the same set of host groups, which
//! will limit the blast radius for databases that overload the system with writes or
//! for situations where subscribers lag too far behind.
//!
//! The Subscriber level is for actually pulling in the data and making it available for
//! query through indexing in the write buffer or writing that data out to Parquet in object
//! storage. Subscribers can also be used for real-time alerting and monitoring.
//!
//! ```text
//!                                    ┌ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─
//!            ┌────────┐  ┌────────┐   Step 1:                 │
//!            │Router 1│  │Router 2│  │  Parse LP
//!            │        │  │        │     Create ReplicatedWrite│
//!            └───┬─┬──┘  └────────┘  └ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─
//!                │ │
//!                │ │                     ┌ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─
//!       ┌────────┘ └───┬──────────────┐   Step 2:                 │
//!       │              │              │  │  Replicate to
//!       │              │              │     all host groups       │
//!       ▼              ▼              ▼  └ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─
//! ┌───────────┐  ┌───────────┐  ┌───────────┐
//! │HostGroup 1│  │HostGroup 2│  │HostGroup 3│
//! │(AZ 1)     │  │(AZ 2)     │  │(AZ 3)     │
//! └───────────┘  └───────────┘  └───────────┘
//!       │
//!       │
//!       │     ┌ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─
//!       │      Step 3:                 │
//!       └──┐  │  Push subscription
//!          │                           │
//!          │  └ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─
//!          │
//!          ▼
//!   ┌────────────┐  ┌ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─
//!   │Query Server│   Step 4:                 │
//!   │(subscriber)│  │  Store in WriteBuffer
//!   │            │                           │
//!   └────────────┘  └ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─
//! ```

#![deny(rust_2018_idioms)]

use std::collections::BTreeMap;

use delorean_generated_types::wal::ReplicatedWrite;
use delorean_line_parser::ParsedLine;
use delorean_storage::{Database, DatabaseStore};

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use snafu::{ResultExt, Snafu};

type DatabaseError = Box<dyn std::error::Error + Send + Sync + 'static>;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Server error: {}", source))]
    ServerError { source: std::io::Error },
    #[snafu(display("database not found: {}", db))]
    DatabaseNotFound { db: String },
    #[snafu(display("database error: {}", source))]
    UnknownDatabaseError { source: DatabaseError },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

/// Server is the container struct for how Delorean servers store data internally
/// as well as how they communicate with other Delorean servers. Each server
/// will have one of these structs, which keeps track of all replication and query rules.
pub struct Server<M: ConnectionManager, S: DatabaseStore> {
    #[allow(dead_code)]
    database_rules: BTreeMap<String, DatabaseRules>,
    local_store: S,
    #[allow(dead_code)]
    connection_manager: M,
}

impl<P: ConnectionManager, S: DatabaseStore> Server<P, S> {
    pub fn local_store(&self) -> &S {
        &self.local_store
    }

    /// write_lines takes in raw line protocol and converts it to a ReplicatedWrite, which
    /// is then replicated to other delorean servers based on the configuration of the db.
    /// This is step #1 from the above diagram.
    pub fn write_lines(&self, _db: &str, _lines: &[ParsedLine<'_>]) -> Result<()> {
        unimplemented!("implement")
    }
}

/// The Server will ask the ConnectionManager for connections to a specific remote server.
/// These connections can be used to communicate with other Delorean servers.
/// This is implemented as a trait for dependency injection in testing.
#[async_trait]
pub trait ConnectionManager {
    type Error: std::error::Error + Send + Sync + 'static;

    type RemoteServer: RemoteServer;

    async fn remote_server(&self, connect: &str) -> Result<&Self::RemoteServer, Self::Error>;
}

/// The RemoteServer represents the API for replicating, subscribing, and querying other
/// delorean servers.
#[async_trait]
pub trait RemoteServer {
    type Error: std::error::Error + Send + Sync + 'static;

    /// replicate will send a replicated write to a remote server. This is step #2 from the diagram.
    async fn replicate(
        &self,
        db: &str,
        replicated_write: &ReplicatedWrite<'_>,
    ) -> Result<(), Self::Error>;
}

impl<M: ConnectionManager, S: DatabaseStore> Server<M, S> {
    pub fn new(connection_manager: M, local_store: S) -> Server<M, S> {
        Server {
            database_rules: BTreeMap::new(),
            local_store,
            connection_manager,
        }
    }

    pub async fn write(&self, db: &str, lines: &[ParsedLine<'_>]) -> Result<()> {
        let local = self
            .local_store
            .db_or_create(db)
            .await
            .map_err(|e| Box::new(e) as DatabaseError)
            .context(UnknownDatabaseError {})?;

        local
            .write_lines(lines)
            .await
            .map_err(|e| Box::new(e) as DatabaseError)
            .context(UnknownDatabaseError {})?;

        Ok(())
    }
}

/// DatabaseRules contains the rules for replicating data, sending data to subscribers, and
/// querying data for a single database.
#[derive(Debug, Serialize, Deserialize)]
pub struct DatabaseRules {
    // Handlebars template for generating a partition key for each row
    partition_template: Option<String>,
    // store_locally if set to true will cause this delorean server to store writes and replicated
    // writes in a local write buffer database. This is step #4 from the diagram.
    store_locally: bool,
    // replication is the set of host groups that data should be replicated to. Which host a
    // write goes to within a host group is determined by consistent hashing the partition key.
    // We'd use this to create a host group per availability zone. So you might have 5 availability
    // zones with 2 hosts in each. Replication will ensure that N of those zones get a write. For
    // each zone, only a single host needs to get the write. Replication is for ensuring a write
    // exists across multiple hosts before returning success. Its purpose is to ensure write
    // durability, rather than write availability for query (this is covered by subscriptions).
    replication: Vec<HostGroupId>,
    // replication_count is the minimum number of host groups to replicate a write to before
    // success is returned. This can be overridden on a per request basis. Replication will
    // continue to write to the other host groups in the background.
    replication_count: u8,
    // replication_queue_max_size is used to determine how far back replication can back up before
    // either rejecting writes or dropping missed writes. The queue is kept in memory on a per
    // database basis. A queue size of zero means it will only try to replicate synchronously and
    // drop any failures.
    replication_queue_max_size: usize,
    // subscriptions are used for query servers to get data via either push or pull as it arrives.
    // They are separate from replication as they have a different purpose. They're for query
    // servers or other clients that want to subscribe to some subset of data being written in.
    // This could either be specific partitions, ranges of partitions, tables, or rows matching
    // some predicate. This is step #3 from the diagram.
    subscriptions: Vec<Subscription>,

    // query local is set to true if this server should answer queries from either its local
    // write buffer and/or read-only partitions that it knows about. If set to true, results
    // will be merged with any others from the remote goups or read only partitions.
    query_local: bool,
    // primary_query_group should be set to a host group if remote servers should be issued
    // queries for this database. All hosts in the group should be queried with this server
    // acting as the coordinator that merges results together. If a specific host in the group
    // is unavailable, another host int he same position from a secondary group should be
    // queried. For example, if we've partitioned the data in this DB into 4 partitions and
    // we are replicating the data across 3 availability zones. Say we have 4 hosts in each
    // of those AZs, thus they each have 1 partition. We'd set the primary group to be the 4
    // hosts in the same AZ as this one. And the secondary groups as the hosts in the other
    // 2 AZs.
    primary_query_group: Option<HostGroupId>,
    secondary_query_groups: Vec<HostGroupId>,

    // read_only_partitions are used when a server should answer queries for partitions that
    // come from object storage. This can be used to start up a new query server to handle
    // queries by pointing it at a collection of partitions and then telling it to also pull
    // data from the replication servers (writes that haven't been snapshotted into a partition).
    read_only_partitions: Vec<PartitionId>,
}

/// PartitionId is the object storage identifier for a specific partition. It should be a
/// path that can be used against an object store to local all the files and subdirectories
/// for a partition. It takes the form of /<writer ID>/<database>/<partition key>/
pub type PartitionId = String;
pub type WriterId = String;

#[derive(Debug, Serialize, Deserialize)]
enum SubscriptionType {
    Push,
    Pull,
}

/// Subscription represent a group of hosts that want to either receive data pushed
/// as it arrives or want to pull it periodically. The subscription has a matcher
/// that is used to determine what data will match it, and an optional queue for
/// storing matched writes.
#[derive(Debug, Serialize, Deserialize)]
pub struct Subscription {
    name: String,
    host_group: HostGroupId,
    subscription_type: SubscriptionType,
    matcher: Matcher,
    // max_queue_size is used for subscriptions that can potentially get queued up either for
    // pulling later, or in the case of a temporary outage for push subscriptions.
    max_queue_size: usize,
}

/// Matcher specifies the rule against the table name and/or a predicate
/// against the row to determine if it matches the write rule.
#[derive(Debug, Serialize, Deserialize)]
struct Matcher {
    #[serde(flatten)]
    tables: MatchTables,
    // TODO: make this work with delorean_storage::Predicate
    #[serde(skip_serializing_if = "Option::is_none")]
    predicate: Option<String>,
}

/// MatchTables looks at the table name of a row to determine if it should
/// match the rule.
#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
enum MatchTables {
    #[serde(rename = "*")]
    All,
    Table(String),
    Regex(String),
}

type HostGroupId = String;

#[derive(Debug, Serialize, Deserialize)]
pub struct HostGroup {
    id: HostGroupId,
    // hosts is a vec of connection strings for remote hosts
    hosts: Vec<String>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use delorean_line_parser::parse_lines;
    use delorean_storage::test::TestDatabaseStore;
    use snafu::Snafu;

    //    type TestError = Box<dyn std::error::Error + Send + Sync + 'static>;
    type Result<T = (), E = TestError> = std::result::Result<T, E>;

    #[tokio::test(threaded_scheduler)]
    async fn write_rule_match_all_local() -> Result {
        let store = TestDatabaseStore::new();
        let manager = TestConnectionManager::new();
        let server = Server::new(manager, store);

        let line = "cpu foo=1 10";
        let lines: Vec<_> = parse_lines(line).map(|l| l.unwrap()).collect();
        server.write("foo", &lines).await.unwrap();
        let db = server.local_store().db("foo").await.unwrap();
        assert_eq!(db.get_lines().await, vec![line]);

        Ok(())
    }

    #[derive(Snafu, Debug, Clone)]
    enum TestError {
        #[snafu(display("Test database error:  {}", message))]
        General { message: String },
    }

    struct TestConnectionManager {
        remote: TestRemoteServer,
    }

    impl TestConnectionManager {
        fn new() -> TestConnectionManager {
            TestConnectionManager {
                remote: TestRemoteServer {},
            }
        }
    }

    #[async_trait]
    impl ConnectionManager for TestConnectionManager {
        type Error = TestError;
        type RemoteServer = TestRemoteServer;

        async fn remote_server<'a>(
            &'a self,
            _id: &str,
        ) -> Result<&'a TestRemoteServer, Self::Error> {
            Ok(&self.remote)
        }
    }

    struct TestRemoteServer;
    #[async_trait]
    impl RemoteServer for TestRemoteServer {
        type Error = TestError;

        async fn replicate(
            &self,
            _db: &str,
            _replicated_write: &ReplicatedWrite<'_>,
        ) -> Result<(), Self::Error> {
            Ok(())
        }
    }
}
