//! This crate contains code that defines the logic for a running InfluxDB IOx
//! server. It also has the logic for how servers talk to each other, which
//! includes replication, subscriptions, querying, and traits that abstract
//! these methods away for testing purposes.
//!
//! This diagram shows the lifecycle of a write coming into a set of IOx servers
//! configured in different roles. This doesn't include ensuring that the
//! replicated writes are durable, or snapshotting partitions in the write
//! buffer. Have a read through the comments in the source before trying to make
//! sense of this diagram.
//!
//! Each level of servers exists to serve a specific function, ideally isolating
//! the kinds of failures that would cause one portion to go down.
//!
//! The router level simply converts the line protocol to the flatbuffer format
//! and computes the partition key. It keeps no state.
//!
//! The HostGroup/AZ level is for receiving the replicated writes and keeping
//! multiple copies of those writes in memory before they are persisted to
//! object storage. Individual databases or groups of databases can be routed to
//! the same set of host groups, which will limit the blast radius for databases
//! that overload the system with writes or for situations where subscribers lag
//! too far behind.
//!
//! The Subscriber level is for actually pulling in the data and making it
//! available for query through indexing in the write buffer or writing that
//! data out to Parquet in object storage. Subscribers can also be used for
//! real-time alerting and monitoring.
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
    missing_debug_implementations,
    clippy::explicit_iter_loop,
    clippy::use_self,
    clippy::clone_on_ref_ptr
)]

use std::sync::{
    atomic::{AtomicU32, Ordering},
    Arc,
};

use async_trait::async_trait;
use bytes::Bytes;
use futures::stream::TryStreamExt;
use parking_lot::Mutex;
use snafu::{OptionExt, ResultExt, Snafu};
use tracing::{debug, error, info};

use data_types::{
    data::{lines_to_replicated_write, ReplicatedWrite},
    database_rules::{DatabaseRules, WriterId},
    job::Job,
    {DatabaseName, DatabaseNameError},
};
use influxdb_line_protocol::ParsedLine;
use object_store::{path::ObjectStorePath, ObjectStore, ObjectStoreApi};
use query::{exec::Executor, DatabaseStore};

use crate::{
    config::{
        object_store_path_for_database_config, Config, GRPCConnectionString, DB_RULES_FILE_NAME,
    },
    db::Db,
    tracker::{TrackedFutureExt, Tracker, TrackerId, TrackerRegistryWithHistory},
};

pub mod buffer;
mod config;
pub mod db;
pub mod snapshot;
pub mod tracker;

#[cfg(test)]
mod query_tests;

type DatabaseError = Box<dyn std::error::Error + Send + Sync + 'static>;

/// A server ID of 0 is reserved and indicates no ID has been configured.
const SERVER_ID_NOT_SET: u32 = 0;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Server error: {}", source))]
    ServerError { source: std::io::Error },
    #[snafu(display("database not found: {}", db_name))]
    DatabaseNotFound { db_name: String },
    #[snafu(display("invalid database: {}", source))]
    InvalidDatabaseName { source: DatabaseNameError },
    #[snafu(display("database error: {}", source))]
    UnknownDatabaseError { source: DatabaseError },
    #[snafu(display("getting mutable buffer chunk: {}", source))]
    MutableBufferChunk { source: DatabaseError },
    #[snafu(display("no local buffer for database: {}", db))]
    NoLocalBuffer { db: String },
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
    #[snafu(display("database already exists"))]
    DatabaseAlreadyExists { db_name: String },
    #[snafu(display("error appending to wal buffer: {}", source))]
    WalError { source: buffer::Error },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

const STORE_ERROR_PAUSE_SECONDS: u64 = 100;
const JOB_HISTORY_SIZE: usize = 1000;

/// `Server` is the container struct for how servers store data internally, as
/// well as how they communicate with other servers. Each server will have one
/// of these structs, which keeps track of all replication and query rules.
#[derive(Debug)]
pub struct Server<M: ConnectionManager> {
    id: AtomicU32,
    config: Arc<Config>,
    connection_manager: Arc<M>,
    pub store: Arc<ObjectStore>,
    executor: Arc<Executor>,
    jobs: Mutex<TrackerRegistryWithHistory<Job>>,
}

impl<M: ConnectionManager> Server<M> {
    pub fn new(connection_manager: M, store: Arc<ObjectStore>) -> Self {
        Self {
            id: AtomicU32::new(SERVER_ID_NOT_SET),
            config: Arc::new(Config::default()),
            store,
            connection_manager: Arc::new(connection_manager),
            executor: Arc::new(Executor::new()),
            jobs: Mutex::new(TrackerRegistryWithHistory::new(JOB_HISTORY_SIZE)),
        }
    }

    /// sets the id of the server, which is used for replication and the base
    /// path in object storage.
    ///
    /// A valid server ID Must be non-zero.
    pub fn set_id(&self, id: u32) {
        self.id.store(id, Ordering::Release)
    }

    /// Returns the current server ID, or an error if not yet set.
    pub fn require_id(&self) -> Result<u32> {
        match self.id.load(Ordering::Acquire) {
            SERVER_ID_NOT_SET => Err(Error::IdNotSet),
            v => Ok(v),
        }
    }

    /// Tells the server the set of rules for a database. Currently, this is not
    /// persisted and is for in-memory processing rules only.
    pub async fn create_database(
        &self,
        db_name: impl Into<String>,
        mut rules: DatabaseRules,
    ) -> Result<()> {
        // Return an error if this server hasn't yet been setup with an id
        self.require_id()?;

        let name = db_name.into();
        let db_name = DatabaseName::new(name.clone()).context(InvalidDatabaseName)?;
        rules.name = name;

        let db_reservation = self.config.create_db(db_name, rules)?;

        let data =
            Bytes::from(serde_json::to_vec(&db_reservation.db.rules).context(ErrorSerializing)?);
        let len = data.len();
        let location =
            object_store_path_for_database_config(&self.root_path()?, &db_reservation.name);

        let stream_data = std::io::Result::Ok(data);
        self.store
            .put(
                &location,
                futures::stream::once(async move { stream_data }),
                Some(len),
            )
            .await
            .context(StoreError)?;

        db_reservation.commit();

        Ok(())
    }

    // base location in object store for this writer
    fn root_path(&self) -> Result<object_store::path::Path> {
        let id = self.require_id()?;

        let mut path = self.store.new_path();
        path.push_dir(format!("{}", id));
        Ok(path)
    }

    /// Loads the database configurations based on the databases in the
    /// object store. Any databases in the config already won't be
    /// replaced.
    pub async fn load_database_configs(&self) -> Result<()> {
        // get the database names from the object store prefixes
        // TODO: update object store to pull back all common prefixes by
        //       following the next tokens.
        let list_result = self
            .store
            .list_with_delimiter(&self.root_path()?)
            .await
            .context(StoreError)?;

        let handles: Vec<_> = list_result
            .common_prefixes
            .into_iter()
            .map(|mut path| {
                let store = Arc::clone(&self.store);
                let config = Arc::clone(&self.config);

                path.set_file_name(DB_RULES_FILE_NAME);

                tokio::task::spawn(async move {
                    let mut res = get_store_bytes(&path, &store).await;
                    while let Err(e) = &res {
                        error!(
                            "error getting database config {:?} from object store: {}",
                            path, e
                        );
                        tokio::time::sleep(tokio::time::Duration::from_secs(
                            STORE_ERROR_PAUSE_SECONDS,
                        ))
                        .await;
                        res = get_store_bytes(&path, &store).await;
                    }

                    let res = res.unwrap();

                    match serde_json::from_slice::<DatabaseRules>(&res) {
                        Err(e) => {
                            error!("error parsing database config {:?} from store: {}", path, e)
                        }
                        Ok(rules) => match DatabaseName::new(rules.name.clone()) {
                            Err(e) => error!("error parsing name {} from rules: {}", rules.name, e),
                            Ok(name) => match config.create_db(name, rules) {
                                Err(e) => error!("error adding database to config: {}", e),
                                Ok(handle) => handle.commit(),
                            },
                        },
                    }
                })
            })
            .collect();

        futures::future::join_all(handles).await;

        Ok(())
    }

    /// `write_lines` takes in raw line protocol and converts it to a
    /// `ReplicatedWrite`, which is then replicated to other servers based
    /// on the configuration of the `db`. This is step #1 from the crate
    /// level documentation.
    pub async fn write_lines(&self, db_name: &str, lines: &[ParsedLine<'_>]) -> Result<()> {
        let id = self.require_id()?;

        let db_name = DatabaseName::new(db_name).context(InvalidDatabaseName)?;
        let db = self
            .config
            .db(&db_name)
            .context(DatabaseNotFound { db_name: &*db_name })?;

        let sequence = db.next_sequence();
        let write = lines_to_replicated_write(id, sequence, lines, &db.rules);

        self.handle_replicated_write(&db_name, &db, write).await?;

        Ok(())
    }

    pub async fn handle_replicated_write(
        &self,
        db_name: &DatabaseName<'_>,
        db: &Db,
        write: ReplicatedWrite,
    ) -> Result<()> {
        if let Some(buf) = &db.mutable_buffer {
            buf.store_replicated_write(&write)
                .await
                .map_err(|e| Box::new(e) as DatabaseError)
                .context(UnknownDatabaseError {})?;
        }

        let write = Arc::new(write);

        if let Some(wal_buffer) = &db.wal_buffer {
            let persist;
            let segment = {
                let mut wal_buffer = wal_buffer.lock();
                persist = wal_buffer.persist;

                // TODO: address this issue?
                // the mutable buffer and the wal buffer have different locking mechanisms,
                // which means that it's possible for a mutable buffer write to
                // succeed while a WAL buffer write fails, which would then
                // return an error. A single lock is probably undesirable, but
                // we need to figure out what semantics we want.
                wal_buffer.append(Arc::clone(&write)).context(WalError)?
            };

            if let Some(segment) = segment {
                if persist {
                    let writer_id = self.require_id()?;
                    let store = Arc::clone(&self.store);

                    let (_, tracker) = self.jobs.lock().register(Job::PersistSegment {
                        writer_id,
                        segment_id: segment.id,
                    });

                    segment
                        .persist_bytes_in_background(tracker, writer_id, db_name, store)
                        .context(WalError)?;
                }
            }
        }

        Ok(())
    }

    pub fn db(&self, name: &DatabaseName<'_>) -> Option<Arc<Db>> {
        self.config.db(name)
    }

    pub fn db_rules(&self, name: &DatabaseName<'_>) -> Option<DatabaseRules> {
        self.config.db(name).map(|d| d.rules.clone())
    }

    pub fn remotes_sorted(&self) -> Vec<(WriterId, String)> {
        self.config.remotes_sorted()
    }

    pub fn update_remote(&self, id: WriterId, addr: GRPCConnectionString) {
        self.config.update_remote(id, addr)
    }

    pub fn delete_remote(&self, id: WriterId) -> Option<GRPCConnectionString> {
        self.config.delete_remote(id)
    }

    pub fn spawn_dummy_job(&self, nanos: Vec<u64>) -> Tracker<Job> {
        let (tracker, registration) = self.jobs.lock().register(Job::Dummy {
            nanos: nanos.clone(),
        });

        for duration in nanos {
            tokio::spawn(
                tokio::time::sleep(tokio::time::Duration::from_nanos(duration))
                    .track(registration.clone()),
            );
        }

        tracker
    }

    /// Closes a chunk and starts moving its data to the read buffer, as a
    /// background job, dropping when complete.
    pub fn close_chunk(
        &self,
        db_name: DatabaseName<'_>,
        partition_key: impl Into<String>,
        chunk_id: u32,
    ) -> Result<Tracker<Job>> {
        let db_name = db_name.to_string();
        let name = DatabaseName::new(&db_name).context(InvalidDatabaseName)?;

        let partition_key = partition_key.into();

        let db = self
            .config
            .db(&name)
            .context(DatabaseNotFound { db_name: &db_name })?;

        let (tracker, registration) = self.jobs.lock().register(Job::CloseChunk {
            db_name: db_name.clone(),
            partition_key: partition_key.clone(),
            chunk_id,
        });

        let task = async move {
            // Close the chunk if it isn't already closed
            if db.is_open_chunk(&partition_key, chunk_id) {
                debug!(%db_name, %partition_key, %chunk_id, "Rolling over partition to close chunk");
                let result = db.rollover_partition(&partition_key).await;

                if let Err(e) = result {
                    info!(?e, %db_name, %partition_key, %chunk_id, "background task error during chunk closing");
                    return Err(e);
                }
            }

            debug!(%db_name, %partition_key, %chunk_id, "background task loading chunk to read buffer");
            let result = db.load_chunk_to_read_buffer(&partition_key, chunk_id).await;
            if let Err(e) = result {
                info!(?e, %db_name, %partition_key, %chunk_id, "background task error loading read buffer chunk");
                return Err(e);
            }

            // now, drop the chunk
            debug!(%db_name, %partition_key, %chunk_id, "background task dropping mutable buffer chunk");
            let result = db.drop_mutable_buffer_chunk(&partition_key, chunk_id).await;
            if let Err(e) = result {
                info!(?e, %db_name, %partition_key, %chunk_id, "background task error loading read buffer chunk");
                return Err(e);
            }

            debug!(%db_name, %partition_key, %chunk_id, "background task completed closing chunk");

            Ok(())
        };

        tokio::spawn(task.track(registration));

        Ok(tracker)
    }

    /// Returns a list of all jobs tracked by this server
    pub fn tracked_jobs(&self) -> Vec<Tracker<Job>> {
        self.jobs.lock().tracked()
    }

    /// Returns a specific job tracked by this server
    pub fn get_job(&self, id: TrackerId) -> Option<Tracker<Job>> {
        self.jobs.lock().get(id)
    }

    /// Background worker function
    ///
    /// TOOD: Handle termination (#827)
    pub async fn background_worker(&self) {
        let mut interval = tokio::time::interval(tokio::time::Duration::from_secs(1));

        loop {
            self.jobs.lock().reclaim();
            interval.tick().await;
        }
    }
}

#[async_trait]
impl<M> DatabaseStore for Server<M>
where
    M: ConnectionManager + std::fmt::Debug + Send + Sync,
{
    type Database = Db;
    type Error = Error;

    fn db_names_sorted(&self) -> Vec<String> {
        self.config
            .db_names_sorted()
            .iter()
            .map(|i| i.clone().into())
            .collect()
    }

    fn db(&self, name: &str) -> Option<Arc<Self::Database>> {
        if let Ok(name) = DatabaseName::new(name) {
            return self.db(&name);
        }

        None
    }

    // TODO: refactor usages of this to use the Server rather than this trait and to
    //       explicitly create a database.
    async fn db_or_create(&self, name: &str) -> Result<Arc<Self::Database>, Self::Error> {
        let db_name = DatabaseName::new(name.to_string()).context(InvalidDatabaseName)?;

        let db = match self.db(&db_name) {
            Some(db) => db,
            None => {
                self.create_database(name, DatabaseRules::new()).await?;
                self.db(&db_name).expect("db not inserted")
            }
        };

        Ok(db)
    }

    fn executor(&self) -> Arc<Executor> {
        Arc::clone(&self.executor)
    }
}

/// The `Server` will ask the `ConnectionManager` for connections to a specific
/// remote server. These connections can be used to communicate with other
/// servers. This is implemented as a trait for dependency injection in testing.
#[async_trait]
pub trait ConnectionManager {
    type Error: std::error::Error + Send + Sync + 'static;

    type RemoteServer: RemoteServer + Send + Sync + 'static;

    async fn remote_server(&self, connect: &str) -> Result<Arc<Self::RemoteServer>, Self::Error>;
}

/// The `RemoteServer` represents the API for replicating, subscribing, and
/// querying other servers.
#[async_trait]
pub trait RemoteServer {
    type Error: std::error::Error + Send + Sync + 'static;

    /// Sends a replicated write to a remote server. This is step #2 from the
    /// diagram.
    async fn replicate(
        &self,
        db: &str,
        replicated_write: &ReplicatedWrite,
    ) -> Result<(), Self::Error>;
}

/// The connection manager maps a host identifier to a remote server.
#[derive(Debug)]
pub struct ConnectionManagerImpl {}

#[async_trait]
impl ConnectionManager for ConnectionManagerImpl {
    type Error = Error;
    type RemoteServer = RemoteServerImpl;

    async fn remote_server(&self, _connect: &str) -> Result<Arc<Self::RemoteServer>, Self::Error> {
        unimplemented!()
    }
}

/// An implementation for communicating with other IOx servers. This should
/// be moved into and implemented in an influxdb_iox_client create at a later
/// date.
#[derive(Debug)]
pub struct RemoteServerImpl {}

#[async_trait]
impl RemoteServer for RemoteServerImpl {
    type Error = Error;

    async fn replicate(
        &self,
        _db: &str,
        _replicated_write: &ReplicatedWrite,
    ) -> Result<(), Self::Error> {
        unimplemented!()
    }
}

// get bytes from the location in object store
async fn get_store_bytes(
    location: &object_store::path::Path,
    store: &ObjectStore,
) -> Result<bytes::BytesMut> {
    let b = store
        .get(location)
        .await
        .context(StoreError)?
        .map_ok(|b| bytes::BytesMut::from(&b[..]))
        .try_concat()
        .await
        .context(StoreError)?;

    Ok(b)
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use async_trait::async_trait;
    use futures::TryStreamExt;
    use parking_lot::Mutex;
    use snafu::Snafu;

    use arrow_deps::{assert_table_eq, datafusion::physical_plan::collect};
    use data_types::database_rules::{
        PartitionTemplate, TemplatePart, WalBufferConfig, WalBufferRollover,
    };
    use influxdb_line_protocol::parse_lines;
    use object_store::{memory::InMemory, path::ObjectStorePath};
    use query::{frontend::sql::SQLQueryPlanner, Database};

    use crate::buffer::Segment;

    use super::*;

    type TestError = Box<dyn std::error::Error + Send + Sync + 'static>;
    type Result<T = (), E = TestError> = std::result::Result<T, E>;

    #[tokio::test]
    async fn server_api_calls_return_error_with_no_id_set() -> Result {
        let manager = TestConnectionManager::new();
        let store = Arc::new(ObjectStore::new_in_memory(InMemory::new()));
        let server = Server::new(manager, store);

        let rules = DatabaseRules::new();
        let resp = server.create_database("foo", rules).await.unwrap_err();
        assert!(matches!(resp, Error::IdNotSet));

        let lines = parsed_lines("cpu foo=1 10");
        let resp = server.write_lines("foo", &lines).await.unwrap_err();
        assert!(matches!(resp, Error::IdNotSet));

        Ok(())
    }

    #[tokio::test]
    async fn create_database_persists_rules() {
        let manager = TestConnectionManager::new();
        let store = Arc::new(ObjectStore::new_in_memory(InMemory::new()));
        let server = Server::new(manager, Arc::clone(&store));
        server.set_id(1);

        let name = "bananas";

        let rules = DatabaseRules {
            partition_template: PartitionTemplate {
                parts: vec![TemplatePart::TimeFormat("YYYY-MM".to_string())],
            },
            name: name.to_string(),
            ..Default::default()
        };

        // Create a database
        server
            .create_database(name, rules.clone())
            .await
            .expect("failed to create database");

        let mut rules_path = server.store.new_path();
        rules_path.push_all_dirs(&["1", name]);
        rules_path.set_file_name("rules.json");

        let read_data = server
            .store
            .get(&rules_path)
            .await
            .unwrap()
            .map_ok(|b| bytes::BytesMut::from(&b[..]))
            .try_concat()
            .await
            .unwrap();

        let read_data = std::str::from_utf8(&*read_data).unwrap();
        let read_rules = serde_json::from_str::<DatabaseRules>(read_data).unwrap();

        assert_eq!(rules, read_rules);

        let db2 = "db_awesome";
        server
            .create_database(db2, DatabaseRules::new())
            .await
            .expect("failed to create 2nd db");

        store.list_with_delimiter(&store.new_path()).await.unwrap();

        let manager = TestConnectionManager::new();
        let server2 = Server::new(manager, store);
        server2.set_id(1);
        server2.load_database_configs().await.unwrap();

        let _ = server2.db(&DatabaseName::new(db2).unwrap()).unwrap();
        let _ = server2.db(&DatabaseName::new(name).unwrap()).unwrap();
    }

    #[tokio::test]
    async fn duplicate_database_name_rejected() -> Result {
        // Covers #643

        let manager = TestConnectionManager::new();
        let store = Arc::new(ObjectStore::new_in_memory(InMemory::new()));
        let server = Server::new(manager, store);
        server.set_id(1);

        let name = "bananas";

        // Create a database
        server
            .create_database(name, DatabaseRules::new())
            .await
            .expect("failed to create database");

        // Then try and create another with the same name
        let got = server
            .create_database(name, DatabaseRules::new())
            .await
            .unwrap_err();

        if !matches!(got, Error::DatabaseAlreadyExists {..}) {
            panic!("expected already exists error");
        }

        Ok(())
    }

    #[tokio::test]
    async fn db_names_sorted() -> Result {
        let manager = TestConnectionManager::new();
        let store = Arc::new(ObjectStore::new_in_memory(InMemory::new()));
        let server = Server::new(manager, store);
        server.set_id(1);

        let names = vec!["bar", "baz"];

        for name in &names {
            server
                .create_database(*name, DatabaseRules::new())
                .await
                .expect("failed to create database");
        }

        let db_names_sorted = server.db_names_sorted();
        assert_eq!(names, db_names_sorted);

        Ok(())
    }

    #[tokio::test]
    async fn database_name_validation() -> Result {
        let manager = TestConnectionManager::new();
        let store = Arc::new(ObjectStore::new_in_memory(InMemory::new()));
        let server = Server::new(manager, store);
        server.set_id(1);

        let reject = vec![
            "bananas\t",
            "bananas\"are\u{0099}\"great",
            "bananas\nfoster",
        ];

        for name in reject {
            let got = server
                .create_database(name, DatabaseRules::new())
                .await
                .unwrap_err();
            if !matches!(got, Error::InvalidDatabaseName { .. }) {
                panic!("expected invalid name error");
            }
        }

        Ok(())
    }

    #[tokio::test]
    async fn writes_local() -> Result {
        let manager = TestConnectionManager::new();
        let store = Arc::new(ObjectStore::new_in_memory(InMemory::new()));
        let server = Server::new(manager, store);
        server.set_id(1);
        server.create_database("foo", DatabaseRules::new()).await?;

        let line = "cpu bar=1 10";
        let lines: Vec<_> = parse_lines(line).map(|l| l.unwrap()).collect();
        server.write_lines("foo", &lines).await.unwrap();

        let db_name = DatabaseName::new("foo").unwrap();
        let db = server.db(&db_name).unwrap();

        let planner = SQLQueryPlanner::default();
        let executor = server.executor();
        let physical_plan = planner
            .query(db.as_ref(), "select * from cpu", executor.as_ref())
            .await
            .unwrap();

        let batches = collect(physical_plan).await.unwrap();
        let expected = vec![
            "+-----+------+",
            "| bar | time |",
            "+-----+------+",
            "| 1   | 10   |",
            "+-----+------+",
        ];
        assert_table_eq!(expected, &batches);

        Ok(())
    }

    #[tokio::test]
    async fn close_chunk() -> Result {
        test_helpers::maybe_start_logging();
        let manager = TestConnectionManager::new();
        let store = Arc::new(ObjectStore::new_in_memory(InMemory::new()));
        let server = Arc::new(Server::new(manager, store));

        let captured_server = Arc::clone(&server);
        let background_handle =
            tokio::task::spawn(async move { captured_server.background_worker().await });

        server.set_id(1);

        let db_name = DatabaseName::new("foo").unwrap();
        server
            .create_database(db_name.as_str(), DatabaseRules::new())
            .await?;

        let line = "cpu bar=1 10";
        let lines: Vec<_> = parse_lines(line).map(|l| l.unwrap()).collect();
        server.write_lines(&db_name, &lines).await.unwrap();

        // start the close (note this is not an async)
        let partition_key = "";
        let db_name_string = db_name.to_string();
        let tracker = server.close_chunk(db_name, partition_key, 0).unwrap();

        let metadata = tracker.metadata();
        let expected_metadata = Job::CloseChunk {
            db_name: db_name_string,
            partition_key: partition_key.to_string(),
            chunk_id: 0,
        };
        assert_eq!(metadata, &expected_metadata);

        // wait for the job to complete
        tracker.join().await;

        // Data should be in the read buffer and not in mutable buffer
        let db_name = DatabaseName::new("foo").unwrap();
        let db = server.db(&db_name).unwrap();

        let mut chunk_summaries = db.chunk_summaries().unwrap();
        chunk_summaries.sort_unstable();

        let actual = chunk_summaries
            .into_iter()
            .map(|s| format!("{:?} {}", s.storage, s.id))
            .collect::<Vec<_>>();

        let expected = vec!["ReadBuffer 0", "OpenMutableBuffer 1"];

        assert_eq!(
            expected, actual,
            "expected:\n{:#?}\n\nactual:{:#?}\n\n",
            expected, actual
        );

        // ensure that we don't leave the server instance hanging around
        background_handle.abort();
        let _ = background_handle.await;

        Ok(())
    }

    #[tokio::test]
    async fn segment_persisted_on_rollover() {
        let manager = TestConnectionManager::new();
        let store = Arc::new(ObjectStore::new_in_memory(InMemory::new()));

        let server = Server::new(manager, Arc::clone(&store));
        server.set_id(1);
        let db_name = "my_db";
        let rules = DatabaseRules {
            wal_buffer_config: Some(WalBufferConfig {
                buffer_size: 500,
                segment_size: 10,
                buffer_rollover: WalBufferRollover::ReturnError,
                store_segments: true,
                close_segment_after: None,
            }),
            ..Default::default()
        };
        server.create_database(db_name, rules).await.unwrap();

        let lines = parsed_lines("disk,host=a used=10.1 12");
        server.write_lines(db_name, &lines).await.unwrap();

        // write lines should have caused a segment rollover and persist, wait
        tokio::task::yield_now().await;

        let mut path = store.new_path();
        path.push_all_dirs(&["1", "my_db", "wal", "000", "000"]);
        path.set_file_name("001.segment");

        let data = store
            .get(&path)
            .await
            .unwrap()
            .map_ok(|b| bytes::BytesMut::from(&b[..]))
            .try_concat()
            .await
            .unwrap();

        let segment = Segment::from_file_bytes(&data).unwrap();
        assert_eq!(segment.writes.len(), 1);
        let write = r#"
writer:1, sequence:1, checksum:2741956553
partition_key:
  table:disk
    host:a used:10.1 time:12
"#;
        assert_eq!(segment.writes[0].to_string(), write);
    }

    #[tokio::test]
    async fn background_task_cleans_jobs() -> Result {
        let manager = TestConnectionManager::new();
        let store = Arc::new(ObjectStore::new_in_memory(InMemory::new()));
        let server = Arc::new(Server::new(manager, store));
        let captured_server = Arc::clone(&server);
        let background_handle =
            tokio::task::spawn(async move { captured_server.background_worker().await });

        let wait_nanos = 1000;
        let job = server.spawn_dummy_job(vec![wait_nanos]);

        // Note: this will hang forwever if the background task has not been started
        job.join().await;

        assert!(job.is_complete());

        // ensure that we don't leave the server instance hanging around
        background_handle.abort();
        let _ = background_handle.await;

        Ok(())
    }

    #[derive(Snafu, Debug, Clone)]
    enum TestClusterError {
        #[snafu(display("Test cluster error:  {}", message))]
        General { message: String },
    }

    #[derive(Debug)]
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
            Ok(Arc::clone(&self.remotes.get(id).unwrap()))
        }
    }

    #[derive(Debug, Default)]
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
            let mut writes = self.writes.lock();
            let entries = writes.entry(db.to_string()).or_insert_with(Vec::new);
            entries.push(replicated_write.clone());

            Ok(())
        }
    }

    fn parsed_lines(lp: &str) -> Vec<ParsedLine<'_>> {
        parse_lines(lp).map(|l| l.unwrap()).collect()
    }
}
