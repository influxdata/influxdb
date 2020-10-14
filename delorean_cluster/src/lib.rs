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

use delorean_data_types::{data::lines_to_replicated_write, database_rules::DatabaseRules};
use delorean_generated_types::wal as wb;
use delorean_line_parser::ParsedLine;
use delorean_storage::Database;
use delorean_write_buffer::Db as WriteBufferDb;

use async_trait::async_trait;
use delorean_arrow::arrow::record_batch::RecordBatch;
use snafu::{OptionExt, ResultExt, Snafu};
use tokio::sync::RwLock;

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
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

/// Server is the container struct for how Delorean servers store data internally
/// as well as how they communicate with other Delorean servers. Each server
/// will have one of these structs, which keeps track of all replication and query rules.
#[derive(Debug)]
pub struct Server<M: ConnectionManager> {
    databases: RwLock<BTreeMap<String, Arc<Db>>>,
    #[allow(dead_code)]
    connection_manager: M,
}

impl<M: ConnectionManager> Server<M> {
    pub fn new(connection_manager: M) -> Self {
        Self {
            databases: RwLock::new(BTreeMap::new()),
            connection_manager,
        }
    }

    pub async fn create_database(&self, db_name: &str, rules: DatabaseRules) -> Result<()> {
        let mut database_map = self.databases.write().await;
        let buffer = if rules.store_locally {
            Some(WriteBufferDb::new(db_name))
        } else {
            None
        };

        let db = Db { rules, buffer };

        database_map.insert(db_name.into(), Arc::new(db));

        Ok(())
    }

    /// write_lines takes in raw line protocol and converts it to a ReplicatedWrite, which
    /// is then replicated to other delorean servers based on the configuration of the db.
    /// This is step #1 from the above diagram.
    pub async fn write_lines(&self, db: &str, lines: &[ParsedLine<'_>]) -> Result<()> {
        let db = self
            .databases
            .read()
            .await
            .get(db)
            .context(DatabaseNotFound { db })?
            .clone();

        let data = lines_to_replicated_write(0, 0, lines, &db.rules);

        if let Some(buf) = &db.buffer {
            buf.store_replicated_write(&data)
                .await
                .map_err(|e| Box::new(e) as DatabaseError)
                .context(UnknownDatabaseError {})?;
        }

        Ok(())
    }

    pub async fn query_local(&self, db_name: &str, query: &str) -> Result<Vec<RecordBatch>> {
        let db = self
            .databases
            .read()
            .await
            .get(db_name)
            .context(DatabaseNotFound { db: db_name })?
            .clone();

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
        replicated_write: &wb::ReplicatedWrite<'_>,
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

    //    type TestError = Box<dyn std::error::Error + Send + Sync + 'static>;
    type Result<T = (), E = TestError> = std::result::Result<T, E>;

    #[tokio::test(threaded_scheduler)]
    async fn writes_local() -> Result {
        // TODO: update this to use an actual database store and database backed entirely by memory
        let manager = TestConnectionManager::new();
        let server = Server::new(manager);
        let rules = DatabaseRules {
            store_locally: true,
            ..Default::default()
        };
        server.create_database("foo", rules).await.unwrap();

        let line = "cpu bar=1 10";
        let lines: Vec<_> = parse_lines(line).map(|l| l.unwrap()).collect();
        server.write_lines("foo", &lines).await.unwrap();

        //        panic!("blah {:?}", server.db("foo").await.buffer.as_ref().unwrap().tag_column_names(Some("cpu".to_string()), None, None).await.unwrap());

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

    #[derive(Snafu, Debug, Clone)]
    enum TestError {
        #[snafu(display("Test delorean_cluster error:  {}", message))]
        General { message: String },
    }

    struct TestConnectionManager {
        remote: TestRemoteServer,
    }

    impl TestConnectionManager {
        fn new() -> Self {
            Self {
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
            _replicated_write: &wb::ReplicatedWrite<'_>,
        ) -> Result<(), Self::Error> {
            Ok(())
        }
    }
}
