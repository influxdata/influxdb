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
#![warn(
    missing_copy_implementations,
    missing_debug_implementations,
    clippy::explicit_iter_loop,
    clippy::use_self
)]

use std::{collections::BTreeMap, sync::Arc};

use delorean_data_types::{
    data::{lines_to_replicated_write, ReplicatedWrite},
    database_rules::{DatabaseRules, HostGroup, HostGroupId},
};
use delorean_generated_types::wal as wb;
use delorean_line_parser::ParsedLine;
use delorean_storage::Database;
use delorean_write_buffer::Db as WriteBufferDb;

use async_trait::async_trait;
use delorean_arrow::arrow::record_batch::RecordBatch;
use snafu::{OptionExt, ResultExt, Snafu};

type DatabaseError = Box<dyn std::error::Error + Send + Sync + 'static>;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Server error: {}", source))]
    ServerError { source: std::io::Error },
    #[snafu(display("database not found: {}", db))]
    DatabaseNotFound { db: String },
    #[snafu(display("database error: {}", source))]
    UnknownDatabaseError { source: DatabaseError },
    #[snafu(display("no local buffer for database: {}", db))]
    NoLocalBuffer { db: String },
    #[snafu(display("host group not found: {}", id))]
    HostGroupNotFound { id: HostGroupId },
    #[snafu(display("no hosts in group: {}", id))]
    NoHostInGroup { id: HostGroupId },
    #[snafu(display("unable to get connection to remote server: {}", server))]
    UnableToGetConnection {
        server: String,
        source: DatabaseError,
    },
    #[snafu(display("error replicating to remote: {}", source))]
    ErrorReplicating { source: DatabaseError },
    #[snafu(display("unable to use server until id is set"))]
    IdNotSet,
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

/// `Server` is the container struct for how servers store data internally, as well as how they
/// communicate with other servers. Each server will have one of these structs, which keeps track
/// of all replication and query rules.
#[derive(Debug)]
pub struct Server<M: ConnectionManager> {
    // id is optional because this may not be set on startup. It might be set via an API call
    id: Option<u32>,
    databases: BTreeMap<String, Db>,
    host_groups: BTreeMap<HostGroupId, HostGroup>,
    connection_manager: M,
}

impl<M: ConnectionManager> Server<M> {
    pub fn new(connection_manager: M) -> Self {
        Self {
            id: None,
            databases: BTreeMap::new(),
            host_groups: BTreeMap::new(),
            connection_manager,
        }
    }

    /// sets the id of the server, which is used for replication and the base path in object storage
    pub fn set_id(&mut self, id: u32) {
        self.id = Some(id);
    }

    fn require_id(&self) -> Result<u32> {
        Ok(self.id.context(IdNotSet)?)
    }

    /// Tells the server the set of rules for a database. Currently, this is not persisted and
    /// is for in-memory processing rules only.
    pub async fn create_database(
        &mut self,
        db_name: impl Into<String>,
        rules: DatabaseRules,
    ) -> Result<()> {
        self.require_id()?;

        let db_name = db_name.into();

        let buffer = if rules.store_locally {
            Some(WriteBufferDb::new(&db_name))
        } else {
            None
        };

        let db = Db { rules, buffer };

        self.databases.insert(db_name, db);

        Ok(())
    }

    /// Creates a host group with a set of connection strings to hosts. These host connection
    /// strings should be something that the connection manager can use to return a remote server
    /// to work with.
    pub async fn create_host_group(&mut self, id: HostGroupId, hosts: Vec<String>) -> Result<()> {
        self.require_id()?;

        self.host_groups.insert(id.clone(), HostGroup { id, hosts });

        Ok(())
    }

    /// `write_lines` takes in raw line protocol and converts it to a `ReplicatedWrite`, which
    /// is then replicated to other servers based on the configuration of the `db`.
    /// This is step #1 from the above diagram.
    pub async fn write_lines(&self, db_name: &str, lines: &[ParsedLine<'_>]) -> Result<()> {
        let id = self.require_id()?;

        let db = self
            .databases
            .get(db_name)
            .context(DatabaseNotFound { db: db_name })?;

        let write = lines_to_replicated_write(id, 0, lines, &db.rules);

        if let Some(buf) = &db.buffer {
            buf.store_replicated_write(&write)
                .await
                .map_err(|e| Box::new(e) as DatabaseError)
                .context(UnknownDatabaseError {})?;
        }

        for host_group_id in &db.rules.replication {
            let group = self
                .host_groups
                .get(host_group_id)
                .context(HostGroupNotFound { id: host_group_id })?;

            let host = group
                .hosts
                .get(0)
                .context(NoHostInGroup { id: host_group_id })?;

            let connection = self
                .connection_manager
                .remote_server(host)
                .await
                .map_err(|e| Box::new(e) as DatabaseError)
                .context(UnableToGetConnection { server: host })?;

            connection
                .replicate(db_name, &write)
                .await
                .map_err(|e| Box::new(e) as DatabaseError)
                .context(ErrorReplicating {})?;
        }

        Ok(())
    }

    /// Executes a query against the local write buffer database, if one exists.
    pub async fn query_local(&self, db_name: &str, query: &str) -> Result<Vec<RecordBatch>> {
        let db = self
            .databases
            .get(db_name)
            .context(DatabaseNotFound { db: db_name })?;

        let buff = db.buffer.as_ref().context(NoLocalBuffer { db: db_name })?;
        buff.query(query)
            .await
            .map_err(|e| Box::new(e) as DatabaseError)
            .context(UnknownDatabaseError {})
    }

    pub async fn handle_replicated_write(
        &self,
        _db: &str,
        _write: &wb::ReplicatedWrite<'_>,
    ) -> Result<()> {
        unimplemented!()
    }
}

/// The `Server` will ask the `ConnectionManager` for connections to a specific remote server.
/// These connections can be used to communicate with other servers.
/// This is implemented as a trait for dependency injection in testing.
#[async_trait]
pub trait ConnectionManager {
    type Error: std::error::Error + Send + Sync + 'static;

    type RemoteServer: RemoteServer;

    async fn remote_server(&self, connect: &str) -> Result<Arc<Self::RemoteServer>, Self::Error>;
}

/// The `RemoteServer` represents the API for replicating, subscribing, and querying other servers.
#[async_trait]
pub trait RemoteServer {
    type Error: std::error::Error + Send + Sync + 'static;

    /// Sends a replicated write to a remote server. This is step #2 from the diagram.
    async fn replicate(
        &self,
        db: &str,
        replicated_write: &ReplicatedWrite,
    ) -> Result<(), Self::Error>;
}

#[derive(Debug)]
struct Db {
    pub rules: DatabaseRules,
    pub buffer: Option<WriteBufferDb>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use delorean_arrow::arrow::{csv, util::string_writer::StringWriter};
    use delorean_line_parser::parse_lines;
    use snafu::Snafu;
    use std::sync::Mutex;

    type TestError = Box<dyn std::error::Error + Send + Sync + 'static>;
    type Result<T = (), E = TestError> = std::result::Result<T, E>;

    #[tokio::test]
    async fn server_api_calls_return_error_with_no_id_set() -> Result {
        let manager = TestConnectionManager::new();
        let mut server = Server::new(manager);

        let rules = DatabaseRules::default();
        let resp = server.create_database("foo", rules).await.unwrap_err();
        assert!(matches!(resp, Error::IdNotSet));

        let lines = parsed_lines("cpu foo=1 10");
        let resp = server.write_lines("foo", &lines).await.unwrap_err();
        assert!(matches!(resp, Error::IdNotSet));

        let resp = server
            .create_host_group("group1".to_string(), vec!["serverA".to_string()])
            .await
            .unwrap_err();
        assert!(matches!(resp, Error::IdNotSet));

        Ok(())
    }

    #[tokio::test]
    async fn writes_local() -> Result {
        let manager = TestConnectionManager::new();
        let mut server = Server::new(manager);
        server.set_id(1);
        let rules = DatabaseRules {
            store_locally: true,
            ..Default::default()
        };
        server.create_database("foo", rules).await?;

        let line = "cpu bar=1 10";
        let lines: Vec<_> = parse_lines(line).map(|l| l.unwrap()).collect();
        server.write_lines("foo", &lines).await.unwrap();

        let results = server
            .query_local("foo", "select * from cpu")
            .await
            .unwrap();

        let mut sw = StringWriter::new();
        {
            let mut writer = csv::Writer::new(&mut sw);
            for r in results {
                writer.write(&r).unwrap();
            }
        }
        assert_eq!(&sw.to_string(), "bar,time\n1,10\n");

        Ok(())
    }

    #[tokio::test]
    async fn replicate_to_single_group() -> Result {
        let mut manager = TestConnectionManager::new();
        let remote = Arc::new(TestRemoteServer::default());
        let remote_id = "serverA";
        manager
            .remotes
            .insert(remote_id.to_string(), remote.clone());

        let mut server = Server::new(manager);
        server.set_id(1);
        let host_group_id = "az1".to_string();
        let rules = DatabaseRules {
            replication: vec![host_group_id.clone()],
            replication_count: 1,
            ..Default::default()
        };
        server
            .create_host_group(host_group_id.clone(), vec![remote_id.to_string()])
            .await
            .unwrap();
        let db_name = "foo";
        server.create_database(db_name, rules).await.unwrap();

        let line = "cpu bar=1 10";
        let lines: Vec<_> = parse_lines(line).map(|l| l.unwrap()).collect();
        server.write_lines("foo", &lines).await.unwrap();

        let writes = remote.writes.lock().unwrap().get(db_name).unwrap().clone();

        let write_text = r#"
writer:1, sequence:0, checksum:226387645
partition_key:
  table:cpu
    bar:1 time:10
"#;

        assert_eq!(write_text, writes[0].to_string());

        Ok(())
    }

    #[derive(Snafu, Debug, Clone)]
    enum TestClusterError {
        #[snafu(display("Test delorean_cluster error:  {}", message))]
        General { message: String },
    }

    struct TestConnectionManager {
        remotes: BTreeMap<String, Arc<TestRemoteServer>>,
    }

    impl TestConnectionManager {
        fn new() -> Self {
            Self {
                remotes: BTreeMap::new(),
            }
        }
    }

    #[async_trait]
    impl ConnectionManager for TestConnectionManager {
        type Error = TestClusterError;
        type RemoteServer = TestRemoteServer;

        async fn remote_server(&self, id: &str) -> Result<Arc<TestRemoteServer>, Self::Error> {
            Ok(self.remotes.get(id).unwrap().clone())
        }
    }

    #[derive(Default)]
    struct TestRemoteServer {
        writes: Mutex<BTreeMap<String, Vec<ReplicatedWrite>>>,
    }

    #[async_trait]
    impl RemoteServer for TestRemoteServer {
        type Error = TestClusterError;

        async fn replicate(
            &self,
            db: &str,
            replicated_write: &ReplicatedWrite,
        ) -> Result<(), Self::Error> {
            let mut writes = self.writes.lock().unwrap();
            let entries = writes.entry(db.to_string()).or_insert_with(Vec::new);
            entries.push(replicated_write.clone());

            Ok(())
        }
    }

    fn parsed_lines(lp: &str) -> Vec<ParsedLine<'_>> {
        parse_lines(lp).map(|l| l.unwrap()).collect()
    }
}
