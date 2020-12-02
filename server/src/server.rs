//! This module contains code for organizing the running server

use std::{
    collections::BTreeMap,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
};

use crate::buffer::Buffer;
use arrow_deps::arrow::record_batch::RecordBatch;
use data_types::{
    data::{lines_to_replicated_write, ReplicatedWrite},
    database_rules::{DatabaseRules, HostGroup, HostGroupId, MatchTables},
};
use influxdb_line_protocol::ParsedLine;
use object_store::ObjectStore;
use query::{SQLDatabase, TSDatabase};
use write_buffer::Db as WriteBufferDb;

use async_trait::async_trait;
use bytes::Bytes;
use futures::stream::TryStreamExt;
use serde::{Deserialize, Serialize};
use snafu::{ensure, OptionExt, ResultExt, Snafu};

type DatabaseError = Box<dyn std::error::Error + Send + Sync + 'static>;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Server error: {}", source))]
    ServerError { source: std::io::Error },
    #[snafu(display("database not found: {}", db_name))]
    DatabaseNotFound { db_name: String },
    #[snafu(display(
        "invalid database name {} can only contain alphanumeric, _ and - characters",
        db_name
    ))]
    InvalidDatabaseName { db_name: String },
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
    #[snafu(display("error serializing configuration {}", source))]
    ErrorSerializing { source: serde_json::Error },
    #[snafu(display("error deserializing configuration {}", source))]
    ErrorDeserializing { source: serde_json::Error },
    #[snafu(display("store error: {}", source))]
    StoreError { source: object_store::Error },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

/// `Server` is the container struct for how servers store data internally, as well as how they
/// communicate with other servers. Each server will have one of these structs, which keeps track
/// of all replication and query rules.
#[derive(Debug)]
pub struct Server<M: ConnectionManager> {
    config: Config,
    connection_manager: M,
    store: ObjectStore,
}

#[derive(Debug, Default, Serialize, Deserialize, Eq, PartialEq)]
struct Config {
    // id is optional because this may not be set on startup. It might be set via an API call
    id: Option<u32>,
    databases: BTreeMap<String, Db>,
    host_groups: BTreeMap<HostGroupId, HostGroup>,
}

impl<M: ConnectionManager> Server<M> {
    pub fn new(connection_manager: M, store: ObjectStore) -> Self {
        Self {
            config: Config::default(),
            store,
            connection_manager,
        }
    }

    /// sets the id of the server, which is used for replication and the base path in object storage
    pub fn set_id(&mut self, id: u32) {
        self.config.id = Some(id);
    }

    fn require_id(&self) -> Result<u32> {
        self.config.id.context(IdNotSet)
    }

    /// Tells the server the set of rules for a database. Currently, this is not persisted and
    /// is for in-memory processing rules only.
    pub async fn create_database(
        &mut self,
        db_name: impl Into<String>,
        rules: DatabaseRules,
    ) -> Result<()> {
        // Return an error if this server hasn't yet been setup with an id
        self.require_id()?;

        let db_name = db_name.into();
        ensure!(
            db_name
                .chars()
                .all(|c| c.is_alphanumeric() || c == '_' || c == '-'),
            InvalidDatabaseName { db_name }
        );

        let buffer = if rules.store_locally {
            Some(WriteBufferDb::new(&db_name))
        } else {
            None
        };

        let sequence = AtomicU64::new(STARTING_SEQUENCE);
        let db = Db {
            rules,
            local_store: buffer,
            wal_buffer: None,
            sequence,
        };

        self.config.databases.insert(db_name, db);

        Ok(())
    }

    /// Creates a host group with a set of connection strings to hosts. These host connection
    /// strings should be something that the connection manager can use to return a remote server
    /// to work with.
    pub async fn create_host_group(&mut self, id: HostGroupId, hosts: Vec<String>) -> Result<()> {
        // Return an error if this server hasn't yet been setup with an id
        self.require_id()?;

        self.config
            .host_groups
            .insert(id.clone(), HostGroup { id, hosts });

        Ok(())
    }

    /// Saves the configuration of database rules and host groups to a single JSON file in
    /// the configured store under a directory /<writer ID/config.json
    pub async fn store_configuration(&self) -> Result<()> {
        let id = self.require_id()?;

        let data = Bytes::from(serde_json::to_vec(&self.config).context(ErrorSerializing)?);
        let len = data.len();
        let location = config_location(id);

        let stream_data = std::io::Result::Ok(data);
        self.store
            .put(
                &location,
                futures::stream::once(async move { stream_data }),
                len,
            )
            .await
            .context(StoreError)?;

        Ok(())
    }

    /// Loads the configuration for this server from the configured store. This replaces
    /// any in-memory configuration that might already be set.
    pub async fn load_configuration(&mut self, id: u32) -> Result<()> {
        let location = config_location(id);

        let read_data = self
            .store
            .get(&location)
            .await
            .context(StoreError)?
            .map_ok(|b| bytes::BytesMut::from(&b[..]))
            .try_concat()
            .await
            .context(StoreError)?;

        let config: Config = serde_json::from_slice(&read_data).context(ErrorDeserializing)?;
        self.config = config;

        Ok(())
    }

    /// `write_lines` takes in raw line protocol and converts it to a `ReplicatedWrite`, which
    /// is then replicated to other servers based on the configuration of the `db`.
    /// This is step #1 from the crate level documentation.
    pub async fn write_lines(&self, db_name: &str, lines: &[ParsedLine<'_>]) -> Result<()> {
        let id = self.require_id()?;

        let db = self
            .config
            .databases
            .get(db_name)
            .context(DatabaseNotFound { db_name })?;

        let sequence = db.next_sequence();
        let write = lines_to_replicated_write(id, sequence, lines, &db.rules);

        self.handle_replicated_write(db_name, db, write).await?;

        Ok(())
    }

    /// Executes a query against the local write buffer database, if one exists.
    pub async fn query_local(&self, db_name: &str, query: &str) -> Result<Vec<RecordBatch>> {
        let db = self
            .config
            .databases
            .get(db_name)
            .context(DatabaseNotFound { db_name })?;

        let buff = db
            .local_store
            .as_ref()
            .context(NoLocalBuffer { db: db_name })?;
        buff.query(query)
            .await
            .map_err(|e| Box::new(e) as DatabaseError)
            .context(UnknownDatabaseError {})
    }

    pub async fn handle_replicated_write(
        &self,
        db_name: &str,
        db: &Db,
        write: ReplicatedWrite,
    ) -> Result<()> {
        if let Some(buf) = &db.local_store {
            buf.store_replicated_write(&write)
                .await
                .map_err(|e| Box::new(e) as DatabaseError)
                .context(UnknownDatabaseError {})?;
        }

        for host_group_id in &db.rules.replication {
            self.replicate_to_host_group(host_group_id, db_name, &write)
                .await?;
        }

        for subscription in &db.rules.subscriptions {
            match subscription.matcher.tables {
                MatchTables::All => {
                    self.replicate_to_host_group(&subscription.host_group_id, db_name, &write)
                        .await?
                }
                MatchTables::Table(_) => unimplemented!(),
                MatchTables::Regex(_) => unimplemented!(),
            }
        }

        Ok(())
    }

    // replicates to a single host in the group based on hashing rules. If that host is unavailable
    // an error will be returned. The request may still succeed if enough of the other host groups
    // have returned a success.
    async fn replicate_to_host_group(
        &self,
        host_group_id: &str,
        db_name: &str,
        write: &ReplicatedWrite,
    ) -> Result<()> {
        let group = self
            .config
            .host_groups
            .get(host_group_id)
            .context(HostGroupNotFound { id: host_group_id })?;

        // TODO: handle hashing rules to determine which host in the group should get the write.
        //       for now, just write to the first one.
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
            .replicate(db_name, write)
            .await
            .map_err(|e| Box::new(e) as DatabaseError)
            .context(ErrorReplicating {})?;

        Ok(())
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

#[derive(Debug, Serialize, Deserialize)]
pub struct Db {
    #[serde(flatten)]
    pub rules: DatabaseRules,
    #[serde(skip)]
    pub local_store: Option<WriteBufferDb>,
    #[serde(skip)]
    wal_buffer: Option<Buffer>,
    #[serde(skip)]
    sequence: AtomicU64,
}

impl PartialEq for Db {
    fn eq(&self, other: &Self) -> bool {
        self.rules == other.rules
    }
}
impl Eq for Db {}

const STARTING_SEQUENCE: u64 = 1;

impl Db {
    fn next_sequence(&self) -> u64 {
        self.sequence.fetch_add(1, Ordering::SeqCst)
    }
}

// location in the store for the configuration file
fn config_location(id: u32) -> String {
    format!("{}/config.json", id)
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_deps::arrow::{csv, util::string_writer::StringWriter};
    use async_trait::async_trait;
    use data_types::database_rules::{MatchTables, Matcher, Subscription};
    use futures::TryStreamExt;
    use influxdb_line_protocol::parse_lines;
    use object_store::{InMemory, ObjectStoreIntegration};
    use snafu::Snafu;
    use std::sync::Mutex;

    type TestError = Box<dyn std::error::Error + Send + Sync + 'static>;
    type Result<T = (), E = TestError> = std::result::Result<T, E>;

    #[tokio::test]
    async fn server_api_calls_return_error_with_no_id_set() -> Result {
        let manager = TestConnectionManager::new();
        let store = ObjectStore::new_in_memory(InMemory::new());
        let mut server = Server::new(manager, store);

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
    async fn database_name_validation() -> Result {
        let manager = TestConnectionManager::new();
        let store = ObjectStore::new_in_memory(InMemory::new());
        let mut server = Server::new(manager, store);
        server.set_id(1);

        let reject: [&str; 5] = [
            "bananas!",
            r#""bananas\"are\"great"#,
            "bananas:good",
            "bananas/cavendish",
            "bananas\n",
        ];

        for &name in &reject {
            let rules = DatabaseRules {
                store_locally: true,
                ..Default::default()
            };
            let got = server.create_database(name, rules).await.unwrap_err();
            if let Error::InvalidDatabaseName { db_name: got } = got {
                if got != name {
                    panic!(format!("expected name {} got {}", name, got));
                }
            } else {
                panic!("unexpected error");
            }
        }

        Ok(())
    }

    #[tokio::test]
    async fn writes_local() -> Result {
        let manager = TestConnectionManager::new();
        let store = ObjectStore::new_in_memory(InMemory::new());
        let mut server = Server::new(manager, store);
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

        let store = ObjectStore::new_in_memory(InMemory::new());

        let mut server = Server::new(manager, store);
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

        let lines = parsed_lines("cpu bar=1 10");
        server.write_lines("foo", &lines).await.unwrap();

        let writes = remote.writes.lock().unwrap().get(db_name).unwrap().clone();

        let write_text = r#"
writer:1, sequence:1, checksum:226387645
partition_key:
  table:cpu
    bar:1 time:10
"#;

        assert_eq!(write_text, writes[0].to_string());

        // ensure sequence number goes up
        let lines = parsed_lines("mem,server=A,region=west user=232 12");
        server.write_lines("foo", &lines).await.unwrap();

        let writes = remote.writes.lock().unwrap().get(db_name).unwrap().clone();
        assert_eq!(2, writes.len());

        let write_text = r#"
writer:1, sequence:2, checksum:3759030699
partition_key:
  table:mem
    server:A region:west user:232 time:12
"#;

        assert_eq!(write_text, writes[1].to_string());

        Ok(())
    }

    #[tokio::test]
    async fn sends_all_to_subscriber() -> Result {
        let mut manager = TestConnectionManager::new();
        let remote = Arc::new(TestRemoteServer::default());
        let remote_id = "serverA";
        manager
            .remotes
            .insert(remote_id.to_string(), remote.clone());

        let store = ObjectStore::new_in_memory(InMemory::new());

        let mut server = Server::new(manager, store);
        server.set_id(1);
        let host_group_id = "az1".to_string();
        let rules = DatabaseRules {
            subscriptions: vec![Subscription {
                name: "query_server_1".to_string(),
                host_group_id: host_group_id.clone(),
                matcher: Matcher {
                    tables: MatchTables::All,
                    predicate: None,
                },
            }],
            ..Default::default()
        };
        server
            .create_host_group(host_group_id.clone(), vec![remote_id.to_string()])
            .await
            .unwrap();
        let db_name = "foo";
        server.create_database(db_name, rules).await.unwrap();

        let lines = parsed_lines("cpu bar=1 10");
        server.write_lines("foo", &lines).await.unwrap();

        let writes = remote.writes.lock().unwrap().get(db_name).unwrap().clone();

        let write_text = r#"
writer:1, sequence:1, checksum:226387645
partition_key:
  table:cpu
    bar:1 time:10
"#;

        assert_eq!(write_text, writes[0].to_string());

        // ensure sequence number goes up
        let lines = parsed_lines("mem,server=A,region=west user=232 12");
        server.write_lines("foo", &lines).await.unwrap();

        let writes = remote.writes.lock().unwrap().get(db_name).unwrap().clone();
        assert_eq!(2, writes.len());

        let write_text = r#"
writer:1, sequence:2, checksum:3759030699
partition_key:
  table:mem
    server:A region:west user:232 time:12
"#;

        assert_eq!(write_text, writes[1].to_string());

        Ok(())
    }

    #[tokio::test]
    async fn store_and_load_configuration() -> Result {
        let manager = TestConnectionManager::new();
        let store = ObjectStore::new_in_memory(InMemory::new());

        let mut server = Server::new(manager, store);
        server.set_id(1);
        let host_group_id = "az1".to_string();
        let remote_id = "serverA";
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

        server.store_configuration().await.unwrap();

        let location = "1/config.json";
        let read_data = server
            .store
            .get(location)
            .await
            .unwrap()
            .map_ok(|b| bytes::BytesMut::from(&b[..]))
            .try_concat()
            .await
            .unwrap();

        let config = r#"{"id":1,"databases":{"foo":{"partition_template":{"parts":[]},"store_locally":false,"replication":["az1"],"replication_count":1,"replication_queue_max_size":0,"subscriptions":[],"query_local":false,"primary_query_group":null,"secondary_query_groups":[],"read_only_partitions":[],"wal_buffer_config":null}},"host_groups":{"az1":{"id":"az1","hosts":["serverA"]}}}"#;
        let read_data = std::str::from_utf8(&*read_data).unwrap();
        println!("\n\n{}\n", read_data);
        assert_eq!(read_data, config);

        let manager = TestConnectionManager::new();
        let store = match server.store.0 {
            ObjectStoreIntegration::InMemory(in_mem) => in_mem.clone().await,
            _ => panic!("wrong type"),
        };
        let store = ObjectStore::new_in_memory(store);

        let mut recovered_server = Server::new(manager, store);
        assert_ne!(server.config, recovered_server.config);
        recovered_server.load_configuration(1).await.unwrap();
        assert_eq!(server.config, recovered_server.config);

        Ok(())
    }

    #[derive(Snafu, Debug, Clone)]
    enum TestClusterError {
        #[snafu(display("Test cluster error:  {}", message))]
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
