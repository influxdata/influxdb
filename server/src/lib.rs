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
//!            │        │  │        │     Create SequencedEntry │
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

#![deny(rustdoc::broken_intra_doc_links, rustdoc::bare_urls, rust_2018_idioms)]
#![warn(
    missing_debug_implementations,
    clippy::explicit_iter_loop,
    clippy::use_self,
    clippy::clone_on_ref_ptr,
    clippy::future_not_send
)]

use ::lifecycle::{LockableChunk, LockablePartition};
use async_trait::async_trait;
use connection::{ConnectionManager, RemoteServer};
use data_types::{
    chunk_metadata::ChunkId,
    database_rules::{NodeGroup, RoutingRules, ShardId, Sink},
    detailed_database::DetailedDatabase,
    error::ErrorLogger,
    job::Job,
    server_id::ServerId,
    {DatabaseName, DatabaseNameError},
};
use database::{Database, DatabaseConfig};
use entry::{lines_to_sharded_entries, pb_to_entry, Entry};
use futures::future::{BoxFuture, Future, FutureExt, Shared, TryFutureExt};
use generated_types::{influxdata::iox::management, influxdata::pbdata::v1 as pb};
use hashbrown::HashMap;
use influxdb_line_protocol::ParsedLine;
use internal_types::freezable::Freezable;
use iox_object_store::IoxObjectStore;
use observability_deps::tracing::{error, info, warn};
use parking_lot::RwLock;
use rand::seq::SliceRandom;
use resolver::Resolver;
use rules::ProvidedDatabaseRules;
use snafu::{ensure, OptionExt, ResultExt, Snafu};
use std::sync::Arc;
use tokio::{sync::Notify, task::JoinError};
use tokio_util::sync::CancellationToken;
use tracker::{TaskTracker, TrackedFutureExt};
use uuid::Uuid;

pub use application::ApplicationState;
pub use db::Db;
pub use job::JobRegistry;
pub use resolver::{GrpcConnectionString, RemoteTemplate};

mod application;
pub mod connection;
pub mod database;
pub mod db;
mod job;
mod resolver;

pub mod rules;
/// Utility modules used by benchmarks and tests
pub mod utils;

mod write_buffer;

mod lifecycle;

type DatabaseError = Box<dyn std::error::Error + Send + Sync + 'static>;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("id not set"))]
    IdNotSet,

    #[snafu(display(
        "Server ID is set ({}) but server is not yet initialized (e.g. DBs and remotes are not \
         loaded). Server is not yet ready to read/write data.",
        server_id
    ))]
    ServerNotInitialized { server_id: ServerId },

    #[snafu(display("id already set"))]
    IdAlreadySet,

    #[snafu(display("database not initialized"))]
    DatabaseNotInitialized { db_name: String },

    #[snafu(display("cannot update database rules"))]
    CanNotUpdateRules {
        db_name: String,
        source: crate::database::Error,
    },

    #[snafu(display("cannot create database: {}", source))]
    CannotCreateDatabase { source: crate::database::InitError },

    #[snafu(display("database not found: {}", db_name))]
    DatabaseNotFound { db_name: String },

    #[snafu(display("{}", source))]
    CannotMarkDatabaseDeleted { source: crate::database::Error },

    #[snafu(display("{}", source))]
    CannotRestoreDatabase { source: crate::database::Error },

    #[snafu(display("database already exists: {}", db_name))]
    DatabaseAlreadyExists { db_name: String },

    #[snafu(display("Server error: {}", source))]
    ServerError { source: std::io::Error },

    #[snafu(display("invalid database: {}", source))]
    InvalidDatabaseName { source: DatabaseNameError },

    #[snafu(display("error wiping preserved catalog: {}", source))]
    WipePreservedCatalog { source: database::Error },

    #[snafu(display("database error: {}", source))]
    UnknownDatabaseError { source: DatabaseError },

    #[snafu(display("partition not found: {}", source))]
    PartitionNotFound { source: db::catalog::Error },

    #[snafu(display(
        "chunk: {} not found in partition '{}' and table '{}'",
        chunk_id,
        partition,
        table
    ))]
    ChunkNotFound {
        chunk_id: ChunkId,
        partition: String,
        table: String,
    },

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

    #[snafu(display("error converting line protocol to flatbuffers: {}", source))]
    LineConversion { source: entry::Error },

    #[snafu(display("error converting protobuf to flatbuffers: {}", source))]
    PBConversion { source: entry::Error },

    #[snafu(display("shard not found: {}", shard_id))]
    ShardNotFound { shard_id: ShardId },

    #[snafu(display("hard buffer limit reached"))]
    HardLimitReached {},

    #[snafu(display(
        "Storing sequenced entry failed with the following error(s), and possibly more: {}",
        errors.iter().map(ToString::to_string).collect::<Vec<_>>().join(", ")
    ))]
    StoreSequencedEntryFailures { errors: Vec<DatabaseError> },

    #[snafu(display(
        "Cannot write to database {}, it's configured to only read from the write buffer",
        db_name
    ))]
    WritingOnlyAllowedThroughWriteBuffer { db_name: String },

    #[snafu(display("Cannot write to write buffer: {}", source))]
    WriteBuffer {
        source: Box<dyn std::error::Error + Sync + Send>,
    },

    #[snafu(display("no remote configured for node group: {:?}", node_group))]
    NoRemoteConfigured { node_group: NodeGroup },

    #[snafu(display("all remotes failed connecting: {:?}", errors))]
    NoRemoteReachable {
        errors: HashMap<GrpcConnectionString, connection::ConnectionManagerError>,
    },

    #[snafu(display("remote error: {}", source))]
    RemoteError {
        source: connection::ConnectionManagerError,
    },

    #[snafu(display("database failed to initialize: {}", source))]
    DatabaseInit { source: Arc<database::InitError> },

    #[snafu(display(
        "Either invalid time range [{}, {}] or invalid delete expression {}",
        start_time,
        stop_time,
        predicate
    ))]
    DeleteExpression {
        start_time: String,
        stop_time: String,
        predicate: String,
    },

    #[snafu(display("error listing deleted databases in object storage: {}", source))]
    ListDeletedDatabases { source: object_store::Error },

    #[snafu(display("error listing detailed databases in object storage: {}", source))]
    ListDetailedDatabases { source: object_store::Error },

    #[snafu(display("error persisting server config to object storage: {}", source))]
    PersistServerConfig { source: object_store::Error },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

/// Storage for `Databases` which can be retrieved by name
#[async_trait]
pub trait DatabaseStore: std::fmt::Debug + Send + Sync {
    /// The type of database that is stored by this DatabaseStore
    type Database: query::QueryDatabase + query::exec::ExecutionContextProvider;

    /// The type of error this DataBase store generates
    type Error: std::error::Error + Send + Sync + 'static;

    /// Retrieve the database specified by `name` returning None if no
    /// such database exists
    fn db(&self, name: &str) -> Option<Arc<Self::Database>>;

    /// Retrieve the database specified by `name`, creating it if it
    /// doesn't exist.
    async fn db_or_create(&self, name: &str) -> Result<Arc<Self::Database>, Self::Error>;
}

/// Configuration options for `Server`
#[derive(Debug)]
pub struct ServerConfig {
    pub remote_template: Option<RemoteTemplate>,

    pub wipe_catalog_on_error: bool,

    pub skip_replay_and_seek_instead: bool,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            remote_template: None,
            wipe_catalog_on_error: false,
            skip_replay_and_seek_instead: false,
        }
    }
}

/// `Server` is the container struct for how servers store data internally, as
/// well as how they communicate with other servers. Each server will have one
/// of these structs, which keeps track of all replication and query rules.
#[derive(Debug)]
pub struct Server<M: ConnectionManager> {
    connection_manager: Arc<M>,

    /// Future that resolves when the background worker exits
    join: Shared<BoxFuture<'static, Result<(), Arc<JoinError>>>>,

    /// Resolver for mapping ServerId to gRPC connection strings
    resolver: RwLock<Resolver>,

    /// State shared with the background worker
    shared: Arc<ServerShared>,
}

impl<M: ConnectionManager> Drop for Server<M> {
    fn drop(&mut self) {
        if !self.shared.shutdown.is_cancelled() {
            warn!("server dropped without calling shutdown()");
            self.shared.shutdown.cancel();
        }

        if self.join.clone().now_or_never().is_none() {
            warn!("server dropped without waiting for worker termination");
        }
    }
}

#[derive(Debug)]
struct ServerShared {
    /// A token that is used to trigger shutdown of the background worker
    shutdown: CancellationToken,

    /// Application-global state
    application: Arc<ApplicationState>,

    /// The state of the `Server`
    state: RwLock<Freezable<ServerState>>,

    /// Notify that the database state has changed
    state_notify: Notify,
}

#[derive(Debug, Snafu)]
pub enum InitError {
    #[snafu(display("error listing databases in object storage: {}", source))]
    ListDatabases { source: object_store::Error },

    #[snafu(display("error persisting initial server config to object storage: {}", source))]
    PersistInitialServerConfig { source: object_store::Error },
}

/// The stage of the server in the startup process
///
/// The progression is linear Startup -> InitReady -> Initialized
///
/// If an error is encountered trying to transition InitReady -> Initialized it enters
/// state InitError and the background worker will continue to try to advance to Initialized
///
/// Any error encountered is exposed by Server::init_error()
///
#[derive(Debug)]
enum ServerState {
    /// Server has started but doesn't have a server id yet
    Startup(ServerStateStartup),

    /// Server can be initialized
    InitReady(ServerStateInitReady),

    /// Server encountered error initializing
    InitError(ServerStateInitReady, Arc<InitError>),

    /// Server has finish initializing
    Initialized(ServerStateInitialized),
}

impl ServerState {
    fn initialized(&self) -> Result<&ServerStateInitialized> {
        match self {
            ServerState::Startup(_) => Err(Error::IdNotSet),
            ServerState::InitReady(state) | ServerState::InitError(state, _) => {
                Err(Error::ServerNotInitialized {
                    server_id: state.server_id,
                })
            }
            ServerState::Initialized(state) => Ok(state),
        }
    }

    fn server_id(&self) -> Option<ServerId> {
        match self {
            ServerState::Startup(_) => None,
            ServerState::InitReady(state) => Some(state.server_id),
            ServerState::InitError(state, _) => Some(state.server_id),
            ServerState::Initialized(state) => Some(state.server_id),
        }
    }
}

#[derive(Debug, Clone)]
struct ServerStateStartup {
    wipe_catalog_on_error: bool,
    skip_replay_and_seek_instead: bool,
}

#[derive(Debug, Clone)]
struct ServerStateInitReady {
    wipe_catalog_on_error: bool,
    skip_replay_and_seek_instead: bool,
    server_id: ServerId,
}

#[derive(Debug)]
struct ServerStateInitialized {
    server_id: ServerId,

    /// A map of possibly initialized `Database` owned by this `Server`
    databases: HashMap<DatabaseName<'static>, Arc<Database>>,
}

impl ServerStateInitialized {
    /// Add a new database to the state
    ///
    /// Returns an error if an active database (either initialized or errored, but not deleted)
    /// with the same name already exists
    fn new_database(
        &mut self,
        shared: &ServerShared,
        config: DatabaseConfig,
    ) -> Result<&Arc<Database>> {
        let db_name = config.name.clone();
        let database = match self.databases.entry(db_name.clone()) {
            hashbrown::hash_map::Entry::Vacant(vacant) => vacant.insert(Arc::new(Database::new(
                Arc::clone(&shared.application),
                config,
            ))),
            hashbrown::hash_map::Entry::Occupied(mut existing) => {
                if let Some(init_error) = existing.get().init_error() {
                    if matches!(&*init_error, database::InitError::NoActiveDatabase) {
                        existing.insert(Arc::new(Database::new(
                            Arc::clone(&shared.application),
                            config,
                        )));
                        existing.into_mut()
                    } else {
                        return DatabaseAlreadyExists {
                            db_name: config.name,
                        }
                        .fail();
                    }
                } else {
                    return DatabaseAlreadyExists {
                        db_name: config.name,
                    }
                    .fail();
                }
            }
        };

        // Spawn a task to monitor the Database and trigger server shutdown if it fails
        let fut = database.join();
        let shutdown = shared.shutdown.clone();
        let _ = tokio::spawn(async move {
            match fut.await {
                Ok(_) => info!(%db_name, "server observed clean shutdown of database worker"),
                Err(e) => {
                    if e.is_panic() {
                        error!(
                            %db_name,
                            %e,
                            "panic in database worker - shutting down server"
                        );
                    } else {
                        error!(
                            %db_name,
                            %e,
                            "unexpected database worker shut down - shutting down server"
                        );
                    }

                    shutdown.cancel();
                }
            }
        });

        Ok(database)
    }

    /// Serialize the list of databases this server owns with their names and object storage
    /// locations into protobuf.
    fn server_config(&self) -> bytes::Bytes {
        let data = management::v1::ServerConfig {
            databases: self
                .databases
                .iter()
                .map(|(name, database)| management::v1::Database {
                    name: name.to_string(),
                    location: database.location(),
                })
                .collect(),
        };

        let mut encoded = bytes::BytesMut::new();
        generated_types::server_config::encode_persisted_server_config(&data, &mut encoded)
            .expect("server config serialization should be valid");
        encoded.freeze()
    }
}

impl<M> Server<M>
where
    M: ConnectionManager + Send + Sync,
{
    pub fn new(
        connection_manager: M,
        application: Arc<ApplicationState>,
        config: ServerConfig,
    ) -> Self {
        let shared = Arc::new(ServerShared {
            shutdown: Default::default(),
            application,
            state: RwLock::new(Freezable::new(ServerState::Startup(ServerStateStartup {
                wipe_catalog_on_error: config.wipe_catalog_on_error,
                skip_replay_and_seek_instead: config.skip_replay_and_seek_instead,
            }))),
            state_notify: Default::default(),
        });

        let handle = tokio::spawn(background_worker(Arc::clone(&shared)));
        let join = handle.map_err(Arc::new).boxed().shared();

        Self {
            shared,
            join,
            connection_manager: Arc::new(connection_manager),
            resolver: RwLock::new(Resolver::new(config.remote_template)),
        }
    }

    /// sets the id of the server, which is used for replication and the base
    /// path in object storage.
    ///
    /// A valid server ID Must be non-zero.
    pub fn set_id(&self, server_id: ServerId) -> Result<()> {
        let mut state = self.shared.state.write();
        let startup = match &**state {
            ServerState::Startup(startup) => startup.clone(),
            _ => return Err(Error::IdAlreadySet),
        };

        *state.get_mut().expect("transaction in progress") =
            ServerState::InitReady(ServerStateInitReady {
                wipe_catalog_on_error: startup.wipe_catalog_on_error,
                skip_replay_and_seek_instead: startup.skip_replay_and_seek_instead,
                server_id,
            });

        Ok(())
    }

    /// Returns the server id for this server if set
    pub fn server_id(&self) -> Option<ServerId> {
        self.shared.state.read().server_id()
    }

    /// Returns true if the server is initialized
    ///
    /// NB: not all databases may be initialized
    pub fn initialized(&self) -> bool {
        self.shared.state.read().initialized().is_ok()
    }

    /// Triggers shutdown of this `Server`
    pub fn shutdown(&self) {
        info!("server shutting down");
        self.shared.shutdown.cancel()
    }

    /// Waits for this `Server` background worker to exit
    pub fn join(&self) -> impl Future<Output = Result<(), Arc<JoinError>>> {
        self.join.clone()
    }

    /// Returns Ok(()) when the Server is initialized, or the error
    /// if one is encountered
    pub async fn wait_for_init(&self) -> Result<(), Arc<InitError>> {
        loop {
            // Register interest before checking to avoid race
            let notify = self.shared.state_notify.notified();

            // Note: this is not guaranteed to see non-terminal states
            // as the state machine may advance past them between
            // the notification being fired, and this task waking up
            match &**self.shared.state.read() {
                ServerState::InitError(_, e) => return Err(Arc::clone(e)),
                ServerState::Initialized(_) => return Ok(()),
                _ => {}
            }

            notify.await;
        }
    }

    /// Error occurred during generic server init (e.g. listing store content).
    pub fn server_init_error(&self) -> Option<Arc<InitError>> {
        match &**self.shared.state.read() {
            ServerState::InitError(_, e) => Some(Arc::clone(e)),
            _ => None,
        }
    }

    /// Returns a list of `Database` for this `Server` sorted by name
    pub fn databases(&self) -> Result<Vec<Arc<Database>>> {
        let state = self.shared.state.read();
        let initialized = state.initialized()?;
        let mut databases: Vec<_> = initialized.databases.iter().collect();

        // ensure the databases come back sorted by name
        databases.sort_by_key(|(name, _db)| name.as_str());

        let databases = databases
            .into_iter()
            .map(|(_name, db)| Arc::clone(db))
            .collect();

        Ok(databases)
    }

    /// Get the `Database` by name
    pub fn database(&self, db_name: &DatabaseName<'_>) -> Result<Arc<Database>> {
        let state = self.shared.state.read();
        let initialized = state.initialized()?;
        let db = initialized
            .databases
            .get(db_name)
            .context(DatabaseNotFound { db_name })?;

        Ok(Arc::clone(db))
    }

    /// Returns an active `Database` by name
    pub fn active_database(&self, db_name: &DatabaseName<'_>) -> Result<Arc<Database>> {
        let database = self.database(db_name)?;
        ensure!(database.is_active(), DatabaseNotFound { db_name });
        Ok(database)
    }

    /// Returns an initialized `Db` by name
    pub fn db(&self, db_name: &DatabaseName<'_>) -> Result<Arc<Db>> {
        let database = self.active_database(db_name)?;

        database
            .initialized_db()
            .context(DatabaseNotInitialized { db_name })
    }

    /// Tells the server the set of rules for a database.
    ///
    /// Waits until the database has initialized or failed to do so
    pub async fn create_database(&self, rules: ProvidedDatabaseRules) -> Result<Arc<Database>> {
        let uuid = Uuid::new_v4();
        let db_name = rules.db_name();

        // Wait for exclusive access to mutate server state
        let handle_fut = self.shared.state.read().freeze();
        let handle = handle_fut.await;

        let server_id = {
            let state = self.shared.state.read();
            let initialized = state.initialized()?;

            if let Some(existing) = initialized.databases.get(db_name) {
                if let Some(init_error) = existing.init_error() {
                    if !matches!(&*init_error, database::InitError::NoActiveDatabase) {
                        return DatabaseAlreadyExists { db_name }.fail();
                    }
                }
            }
            initialized.server_id
        };

        let res = Database::create(
            Arc::clone(&self.shared.application),
            uuid,
            &rules,
            server_id,
        )
        .await;

        ensure!(
            !matches!(res, Err(database::InitError::DatabaseAlreadyExists { .. })),
            DatabaseAlreadyExists { db_name }
        );

        res.context(CannotCreateDatabase)?;

        let database = {
            let mut state = self.shared.state.write();

            // Exchange FreezeHandle for mutable access via WriteGuard
            let mut state = state.unfreeze(handle);

            let database = match &mut *state {
                ServerState::Initialized(initialized) => initialized
                    .new_database(
                        &self.shared,
                        DatabaseConfig {
                            name: db_name.clone(),
                            uuid: Some(uuid),
                            server_id,
                            wipe_catalog_on_error: false,
                            skip_replay: false,
                        },
                    )
                    .expect("database unique"),
                _ => unreachable!(),
            };
            Arc::clone(database)
        };

        // Save the database to the server config as soon as it's added to the `ServerState`
        self.persist_server_config().await?;

        database.wait_for_init().await.context(DatabaseInit)?;

        Ok(database)
    }

    /// Delete an existing, active database with this name. Return an error if no active database
    /// with this name can be found.
    pub async fn delete_database(&self, db_name: &DatabaseName<'static>) -> Result<()> {
        let database = self.database(db_name)?;
        database.delete().await.context(CannotMarkDatabaseDeleted)?;
        Ok(())
    }

    /// Restore a database and generation that has been marked as deleted. Return an error if no
    /// database with this generation can be found, or if there's already an active database with
    /// this name.
    pub async fn restore_database(
        &self,
        db_name: &DatabaseName<'static>,
        generation_id: u64,
    ) -> Result<()> {
        let database = {
            let state = self.shared.state.read();
            let initialized = state.initialized()?;

            let database = Arc::clone(
                initialized
                    .databases
                    .get(db_name)
                    .context(DatabaseNotFound { db_name })?,
            );

            if let Some(init_error) = database.init_error() {
                if !matches!(&*init_error, database::InitError::NoActiveDatabase) {
                    return DatabaseAlreadyExists { db_name }.fail();
                }
            }

            database
        };

        database
            .restore(generation_id as usize)
            .await
            .context(CannotRestoreDatabase)?;

        database.wait_for_init().await.context(DatabaseInit)?;

        Ok(())
    }

    /// List deleted databases in object storage.
    pub async fn list_deleted_databases(&self) -> Result<Vec<DetailedDatabase>> {
        let server_id = {
            let state = self.shared.state.read();
            let initialized = state.initialized()?;
            initialized.server_id
        };

        Ok(IoxObjectStore::list_deleted_databases(
            self.shared.application.object_store(),
            server_id,
        )
        .await
        .context(ListDeletedDatabases)?)
    }

    /// List all databases, active and deleted, in object storage, including their generation IDs.
    pub async fn list_detailed_databases(&self) -> Result<Vec<DetailedDatabase>> {
        let server_id = {
            let state = self.shared.state.read();
            let initialized = state.initialized()?;
            initialized.server_id
        };

        Ok(IoxObjectStore::list_detailed_databases(
            self.shared.application.object_store(),
            server_id,
        )
        .await
        .context(ListDetailedDatabases)?)
    }

    /// Write this server's databases out to the server config in object storage.
    async fn persist_server_config(&self) -> Result<()> {
        let (server_id, bytes) = {
            let state = self.shared.state.read();
            let initialized = state.initialized()?;
            (initialized.server_id, initialized.server_config())
        };

        IoxObjectStore::put_server_config_file(
            self.shared.application.object_store(),
            server_id,
            bytes,
        )
        .await
        .context(PersistServerConfig)?;

        Ok(())
    }

    pub async fn write_pb(&self, database_batch: pb::DatabaseBatch) -> Result<()> {
        let db_name = DatabaseName::new(database_batch.database_name.as_str())
            .context(InvalidDatabaseName)?;

        let entry = pb_to_entry(&database_batch).context(PBConversion)?;

        // TODO: Apply routing/sharding logic (#2139)
        self.write_entry_local(&db_name, entry).await?;

        Ok(())
    }

    /// `write_lines` takes in raw line protocol and converts it to a collection
    /// of ShardedEntry which are then sent to other IOx servers based on
    /// the ShardConfig or sent to the local database for buffering in the
    /// WriteBuffer and/or the MutableBuffer if configured.
    ///
    /// The provided `default_time` is nanoseconds since the epoch and will be assigned
    /// to any lines that don't have a timestamp.
    ///
    /// TODO: Move this routing logic into a subsystem owned by `Database`
    pub async fn write_lines(
        &self,
        db_name: &DatabaseName<'_>,
        lines: &[ParsedLine<'_>],
        default_time: i64,
    ) -> Result<()> {
        let db = self.db(db_name)?;
        let rules = db.rules();

        let shard_config = match &rules.routing_rules {
            Some(RoutingRules::ShardConfig(shard_config)) => Some(shard_config),
            _ => None,
        };

        let sharded_entries =
            lines_to_sharded_entries(lines, default_time, shard_config, rules.as_ref())
                .context(LineConversion)?;

        // Write to all shards in parallel; as soon as one fails return error
        // immediately to the client and abort all other outstanding requests.
        futures_util::future::try_join_all(sharded_entries.into_iter().map(
            |sharded_entry| async {
                let sink = match &rules.routing_rules {
                    Some(RoutingRules::ShardConfig(shard_config)) => {
                        let id = sharded_entry.shard_id.expect("sharded entry");
                        Some(shard_config.shards.get(&id).expect("valid shard"))
                    }
                    Some(RoutingRules::RoutingConfig(config)) => Some(&config.sink),
                    None => None,
                };

                match sink {
                    Some(sink) => {
                        self.write_entry_sink(db_name, sink, sharded_entry.entry)
                            .await
                    }
                    None => self.write_entry_local(db_name, sharded_entry.entry).await,
                }
            },
        ))
        .await?;
        Ok(())
    }

    async fn write_entry_sink(
        &self,
        db_name: &DatabaseName<'_>,
        sink: &Sink,
        entry: Entry,
    ) -> Result<()> {
        match sink {
            Sink::Iox(node_group) => {
                self.write_entry_downstream(db_name, node_group, entry)
                    .await
            }
            Sink::Kafka(_) => {
                // The write buffer write path is currently implemented in "db", so confusingly we
                // need to invoke write_entry_local.
                // TODO(mkm): tracked in #2134
                self.write_entry_local(db_name, entry).await
            }
            Sink::DevNull => {
                // write is silently ignored, as requested by the configuration.
                Ok(())
            }
        }
    }

    async fn write_entry_downstream(
        &self,
        db_name: &str,
        node_group: &[ServerId],
        entry: Entry,
    ) -> Result<()> {
        // Return an error if this server is not yet ready
        self.shared.state.read().initialized()?;

        let addrs: Vec<_> = {
            let resolver = self.resolver.read();
            node_group
                .iter()
                .filter_map(|&node| resolver.resolve_remote(node))
                .collect()
        };

        if addrs.is_empty() {
            return NoRemoteConfigured { node_group }.fail();
        }

        let mut errors = HashMap::new();
        // this needs to be in its own statement because rand::thread_rng is not Send and the loop below is async.
        // braces around the expression would work but clippy don't know that and complains the braces are useless.
        let random_addrs_iter = addrs.choose_multiple(&mut rand::thread_rng(), addrs.len());
        for addr in random_addrs_iter {
            match self.connection_manager.remote_server(addr).await {
                Err(err) => {
                    info!("error obtaining remote for {}: {}", addr, err);
                    errors.insert(addr.to_owned(), err);
                }
                Ok(remote) => {
                    return remote
                        .write_entry(db_name, entry)
                        .await
                        .context(RemoteError)
                }
            };
        }
        NoRemoteReachable { errors }.fail()
    }

    /// Write an entry to the local `Db`
    ///
    /// TODO: Remove this and migrate callers to `Database::write_entry`
    pub async fn write_entry_local(&self, db_name: &DatabaseName<'_>, entry: Entry) -> Result<()> {
        use database::WriteError;

        self.active_database(db_name)?
            .write_entry(entry)
            .await
            .map_err(|e| match e {
                WriteError::NotInitialized { .. } => Error::DatabaseNotInitialized {
                    db_name: db_name.to_string(),
                },
                WriteError::WriteBuffer { source } => Error::WriteBuffer { source },
                WriteError::WritingOnlyAllowedThroughWriteBuffer => {
                    Error::WritingOnlyAllowedThroughWriteBuffer {
                        db_name: db_name.to_string(),
                    }
                }
                WriteError::DbError { source } => Error::UnknownDatabaseError {
                    source: Box::new(source),
                },
                WriteError::HardLimitReached { .. } => Error::HardLimitReached {},
                WriteError::StoreSequencedEntryFailures { errors } => {
                    Error::StoreSequencedEntryFailures {
                        errors: errors.into_iter().map(|e| Box::new(e) as _).collect(),
                    }
                }
            })
    }

    /// Update database rules and save on success.
    pub async fn update_db_rules(
        &self,
        db_name: &DatabaseName<'_>,
        rules: ProvidedDatabaseRules,
    ) -> Result<Arc<ProvidedDatabaseRules>> {
        let database = self.database(db_name)?;

        // attempt to save provided rules in the current state
        Ok(database
            .update_provided_rules(rules)
            .await
            .context(CanNotUpdateRules { db_name })?)
    }

    pub fn remotes_sorted(&self) -> Vec<(ServerId, String)> {
        self.resolver.read().remotes_sorted()
    }

    pub fn update_remote(&self, id: ServerId, addr: GrpcConnectionString) {
        self.resolver.write().update_remote(id, addr)
    }

    pub fn delete_remote(&self, id: ServerId) -> Option<GrpcConnectionString> {
        self.resolver.write().delete_remote(id)
    }

    /// Closes a chunk and starts moving its data to the read buffer, as a
    /// background job, dropping when complete.
    pub fn close_chunk(
        &self,
        db_name: &DatabaseName<'_>,
        table_name: impl Into<String>,
        partition_key: impl Into<String>,
        chunk_id: ChunkId,
    ) -> Result<TaskTracker<Job>> {
        let db = self.db(db_name)?;
        let partition_key = partition_key.into();
        let table_name = table_name.into();

        let partition = db
            .lockable_partition(&table_name, &partition_key)
            .context(PartitionNotFound)?;

        let partition = partition.read();
        let chunk =
            LockablePartition::chunk(&partition, chunk_id).ok_or_else(|| Error::ChunkNotFound {
                chunk_id,
                partition: partition_key.to_string(),
                table: table_name.to_string(),
            })?;

        let partition = partition.upgrade();
        let chunk = chunk.write();

        LockablePartition::compact_chunks(partition, vec![chunk]).map_err(|e| {
            Error::UnknownDatabaseError {
                source: Box::new(e),
            }
        })
    }

    /// Recover database that has failed to load its catalog by wiping it
    ///
    /// The DB must exist in the server and have failed to load the catalog for this to work
    /// This is done to prevent race conditions between DB jobs and this command
    pub fn wipe_preserved_catalog(
        &self,
        db_name: &DatabaseName<'static>,
    ) -> Result<TaskTracker<Job>> {
        let database = self.database(db_name)?;
        let registry = self.shared.application.job_registry();

        let (tracker, registration) = registry.register(Job::WipePreservedCatalog {
            db_name: Arc::from(db_name.as_str()),
        });

        let fut = database
            .wipe_preserved_catalog()
            .context(WipePreservedCatalog)?;

        let _ = tokio::spawn(fut.track(registration));

        Ok(tracker)
    }
}

/// Background worker function for the server
async fn background_worker(shared: Arc<ServerShared>) {
    info!("started server background worker");

    let mut interval = tokio::time::interval(tokio::time::Duration::from_secs(1));

    // TODO: Move out of Server background worker
    let job_registry = shared.application.job_registry();

    while !shared.shutdown.is_cancelled() {
        maybe_initialize_server(shared.as_ref()).await;
        job_registry.reclaim();

        crate::utils::panic_test(|| {
            let server_id = shared.state.read().initialized().ok()?.server_id;
            Some(format!("server background worker: {}", server_id))
        });

        tokio::select! {
            _ = interval.tick() => {},
            _ = shared.shutdown.cancelled() => break
        }
    }

    info!("shutting down databases");
    let databases: Vec<_> = shared
        .state
        .read()
        .initialized()
        .into_iter()
        .flat_map(|x| x.databases.values().cloned())
        .collect();

    for database in databases {
        database.shutdown();
        let _ = database
            .join()
            .await
            .log_if_error("database background worker");
    }

    info!("draining tracker registry");

    // Wait for any outstanding jobs to finish - frontend shutdown should be
    // sequenced before shutting down the background workers and so there
    // shouldn't be any
    while job_registry.reclaim() != 0 {
        interval.tick().await;
    }

    info!("drained tracker registry");
}

/// Loads the database configurations based on the databases in the
/// object store. Any databases in the config already won't be
/// replaced.
///
/// This requires the serverID to be set.
///
/// It will be a no-op if the configs are already loaded and the server is ready.
///
async fn maybe_initialize_server(shared: &ServerShared) {
    if shared.state.read().initialized().is_ok() {
        return;
    }

    let (init_ready, handle) = {
        let state = shared.state.read();

        let init_ready = match &**state {
            ServerState::Startup(_) => {
                info!("server not initialized - ID not set");
                return;
            }
            ServerState::InitReady(state) => {
                info!(server_id=%state.server_id, "server init ready");
                state.clone()
            }
            ServerState::InitError(state, e) => {
                info!(server_id=%state.server_id, %e, "retrying server init");
                state.clone()
            }
            ServerState::Initialized(_) => return,
        };

        let handle = match state.try_freeze() {
            Some(handle) => handle,
            None => return,
        };

        (init_ready, handle)
    };

    let maybe_databases = IoxObjectStore::list_possible_databases(
        shared.application.object_store(),
        init_ready.server_id,
    )
    .await;

    let next_state = match maybe_databases {
        Ok(databases) => {
            let mut state = ServerStateInitialized {
                server_id: init_ready.server_id,
                databases: HashMap::with_capacity(databases.len()),
            };

            for db_name in databases {
                state
                    .new_database(
                        shared,
                        DatabaseConfig {
                            name: db_name,
                            // TODO: this will be the UUID from the object store path once we
                            // make that switch; we'll be guaranteed to have it then and this
                            // won't be an `Option`
                            uuid: None,
                            server_id: init_ready.server_id,
                            wipe_catalog_on_error: init_ready.wipe_catalog_on_error,
                            skip_replay: init_ready.skip_replay_and_seek_instead,
                        },
                    )
                    .expect("database unique");
            }

            let bytes = state.server_config();

            let config_written = IoxObjectStore::put_server_config_file(
                shared.application.object_store(),
                init_ready.server_id,
                bytes,
            )
            .await;

            match config_written {
                Ok(_) => {
                    info!(server_id=%init_ready.server_id, "server initialized");
                    ServerState::Initialized(state)
                }
                Err(e) => {
                    error!(
                        server_id=%init_ready.server_id,
                        %e,
                        "error persisting initial server config to object storage"
                    );
                    ServerState::InitError(
                        init_ready,
                        Arc::new(InitError::PersistInitialServerConfig { source: e }),
                    )
                }
            }
        }
        Err(e) => {
            error!(server_id=%init_ready.server_id, %e, "error initializing server");
            ServerState::InitError(init_ready, Arc::new(InitError::ListDatabases { source: e }))
        }
    };

    *shared.state.write().unfreeze(handle) = next_state;
    shared.state_notify.notify_waiters();
}

/// TODO: Revisit this trait's API
#[async_trait]
impl<M> DatabaseStore for Server<M>
where
    M: ConnectionManager + std::fmt::Debug + Send + Sync,
{
    type Database = Db;
    type Error = Error;

    fn db(&self, name: &str) -> Option<Arc<Self::Database>> {
        DatabaseName::new(name)
            .ok()
            .and_then(|name| self.db(&name).ok())
    }

    // TODO: refactor usages of this to use the Server rather than this trait and to
    //       explicitly create a database.
    async fn db_or_create(&self, name: &str) -> Result<Arc<Self::Database>, Self::Error> {
        let db_name = DatabaseName::new(name.to_string()).context(InvalidDatabaseName)?;

        let db = match self.db(&db_name) {
            Ok(db) => db,
            Err(Error::DatabaseNotFound { .. }) => {
                self.create_database(ProvidedDatabaseRules::new_empty(db_name.clone()))
                    .await?;
                self.db(&db_name).expect("db not inserted")
            }
            Err(e) => return Err(e),
        };

        Ok(db)
    }
}

#[cfg(test)]
impl<M> Server<M>
where
    M: ConnectionManager + Send + Sync,
{
    /// For tests:  list of database names in this server, regardless
    /// of their initialization state
    fn db_names_sorted(&self) -> Vec<String> {
        self.shared
            .state
            .read()
            .initialized()
            .map(|initialized| {
                let mut keys: Vec<_> = initialized
                    .databases
                    .keys()
                    .map(ToString::to_string)
                    .collect();

                keys.sort_unstable();
                keys
            })
            .unwrap_or_default()
    }
}

pub mod test_utils {
    use super::*;
    use crate::connection::test_helpers::TestConnectionManager;
    use object_store::ObjectStore;

    /// Create a new [`ApplicationState`] with an in-memory object store
    pub fn make_application() -> Arc<ApplicationState> {
        Arc::new(ApplicationState::new(
            Arc::new(ObjectStore::new_in_memory()),
            None,
        ))
    }

    /// Creates a new server with the provided [`ApplicationState`]
    pub fn make_server(application: Arc<ApplicationState>) -> Arc<Server<TestConnectionManager>> {
        Arc::new(Server::new(
            TestConnectionManager::new(),
            application,
            Default::default(),
        ))
    }

    /// Creates a new server with the provided [`ApplicationState`]
    ///
    /// Sets the `server_id` provided and waits for it to initialize
    pub async fn make_initialized_server(
        server_id: ServerId,
        application: Arc<ApplicationState>,
    ) -> Arc<Server<TestConnectionManager>> {
        let server = make_server(application);
        server.set_id(server_id).unwrap();
        server.wait_for_init().await.unwrap();
        server
    }
}

#[cfg(test)]
mod tests {
    use super::{
        test_utils::{make_application, make_server},
        *,
    };
    use arrow::record_batch::RecordBatch;
    use arrow_util::assert_batches_eq;
    use bytes::Bytes;
    use connection::test_helpers::{TestConnectionManager, TestRemoteServer};
    use data_types::{
        chunk_metadata::{ChunkAddr, ChunkStorage},
        database_rules::{
            DatabaseRules, HashRing, LifecycleRules, PartitionTemplate, ShardConfig, TemplatePart,
            WriteBufferConnection, WriteBufferDirection, NO_SHARD_CONFIG,
        },
    };
    use entry::test_helpers::lp_to_entry;
    use futures::TryStreamExt;
    use influxdb_line_protocol::parse_lines;
    use iox_object_store::IoxObjectStore;
    use metric::{Attributes, Metric, U64Counter};
    use object_store::{
        path::{parsed::DirsAndFileName, ObjectStorePath},
        ObjectStore, ObjectStoreApi,
    };
    use parquet_catalog::{
        core::{PreservedCatalog, PreservedCatalogConfig},
        test_helpers::{load_ok, new_empty},
    };
    use query::{exec::ExecutionContextProvider, frontend::sql::SqlQueryPlanner, QueryDatabase};
    use std::{
        convert::TryFrom,
        sync::{
            atomic::{AtomicBool, Ordering},
            Arc,
        },
        time::{Duration, Instant},
    };
    use test_helpers::assert_contains;

    const ARBITRARY_DEFAULT_TIME: i64 = 456;

    #[tokio::test]
    async fn server_api_calls_return_error_with_no_id_set() {
        let server = make_server(make_application());

        let lines = parsed_lines("cpu foo=1 10");
        let resp = server
            .write_lines(
                &DatabaseName::new("foo").unwrap(),
                &lines,
                ARBITRARY_DEFAULT_TIME,
            )
            .await
            .unwrap_err();
        assert!(matches!(resp, Error::IdNotSet));
    }

    async fn server_config_contents(object_store: &ObjectStore, server_id: ServerId) -> Bytes {
        IoxObjectStore::get_server_config_file(object_store, server_id)
            .await
            .unwrap_or_else(|_| Bytes::new())
    }

    async fn server_config(
        object_store: &ObjectStore,
        server_id: ServerId,
    ) -> management::v1::ServerConfig {
        let server_config_contents = server_config_contents(object_store, server_id).await;
        generated_types::server_config::decode_persisted_server_config(server_config_contents)
            .unwrap()
    }

    fn assert_config_contents(
        config: &management::v1::ServerConfig,
        expected: &[(&DatabaseName<'_>, String)],
    ) {
        assert_eq!(config.databases.len(), expected.len());

        for entry in expected {
            let (expected_name, expected_location) = entry;
            let db = config
                .databases
                .iter()
                .find(|db| db.name == expected_name.as_str())
                .unwrap_or_else(|| {
                    panic!(
                        "Could not find database named {} in server config",
                        expected_name
                    )
                });

            assert_eq!(&db.location, expected_location);
        }
    }

    #[tokio::test]
    async fn create_database_persists_rules_and_server_config() {
        let application = make_application();
        let server = make_server(Arc::clone(&application));
        let server_id = ServerId::try_from(1).unwrap();
        server.set_id(server_id).unwrap();
        server.wait_for_init().await.unwrap();

        // assert server config file either doesn't exist or exists but has 0 entries
        let server_config_contents =
            server_config_contents(application.object_store(), server_id).await;
        assert_eq!(server_config_contents.len(), 0);

        let name = DatabaseName::new("bananas").unwrap();

        let rules = DatabaseRules {
            name: name.clone(),
            partition_template: PartitionTemplate {
                parts: vec![TemplatePart::TimeFormat("YYYY-MM".to_string())],
            },
            lifecycle_rules: LifecycleRules {
                catalog_transactions_until_checkpoint: std::num::NonZeroU64::new(13).unwrap(),
                ..Default::default()
            },
            routing_rules: None,
            worker_cleanup_avg_sleep: Duration::from_secs(2),
            write_buffer_connection: None,
        };

        let provided_rules = make_provided_rules(rules);

        // Create a database
        let bananas = server
            .create_database(provided_rules.clone())
            .await
            .expect("failed to create database");

        let iox_object_store = bananas.iox_object_store().unwrap();
        let read_rules = ProvidedDatabaseRules::load(&iox_object_store)
            .await
            .unwrap();

        // Same rules that were provided are read
        assert_eq!(provided_rules.original(), read_rules.original());

        // rules that are being used are the same
        assert_eq!(provided_rules.rules(), read_rules.rules());

        // assert server config file exists and has 1 entry
        let config = server_config(application.object_store(), server_id).await;
        assert_config_contents(&config, &[(&name, iox_object_store.root_path())]);

        let db2 = DatabaseName::new("db_awesome").unwrap();
        let rules2 = DatabaseRules::new(db2.clone());
        let provided_rules2 = make_provided_rules(rules2);

        let awesome = server
            .create_database(provided_rules2)
            .await
            .expect("failed to create 2nd db");

        // assert server config file exists and has 2 entries
        let config = server_config(application.object_store(), server_id).await;
        assert_config_contents(
            &config,
            &[
                (&name, iox_object_store.root_path()),
                (&db2, awesome.iox_object_store().unwrap().root_path()),
            ],
        );

        let server2 = make_server(Arc::clone(&application));
        server2.set_id(ServerId::try_from(1).unwrap()).unwrap();
        server2.wait_for_init().await.unwrap();

        let database1 = server2.database(&name).unwrap();
        let database2 = server2.database(&db2).unwrap();

        database1.wait_for_init().await.unwrap();
        database2.wait_for_init().await.unwrap();

        assert!(server2.db(&db2).is_ok());
        assert!(server2.db(&name).is_ok());

        // assert server config file still exists and has 2 entries
        let config = server_config(application.object_store(), server_id).await;
        assert_config_contents(
            &config,
            &[
                (&name, iox_object_store.root_path()),
                (&db2, awesome.iox_object_store().unwrap().root_path()),
            ],
        );
    }

    #[tokio::test]
    async fn duplicate_database_name_rejected() {
        // Covers #643

        let server = make_server(make_application());
        server.set_id(ServerId::try_from(1).unwrap()).unwrap();
        server.wait_for_init().await.unwrap();

        let name = DatabaseName::new("bananas").unwrap();

        // Create a database
        server
            .create_database(default_rules(name.clone()))
            .await
            .expect("failed to create database");

        // Then try and create another with the same name
        let got = server
            .create_database(default_rules(name.clone()))
            .await
            .unwrap_err();

        if !matches!(got, Error::DatabaseAlreadyExists { .. }) {
            panic!("expected already exists error");
        }
    }

    async fn create_simple_database<M>(
        server: &Server<M>,
        name: impl Into<String> + Send,
    ) -> Result<Arc<Database>>
    where
        M: ConnectionManager + Send + Sync,
    {
        let name = DatabaseName::new(name.into()).unwrap();

        let rules = DatabaseRules {
            name,
            partition_template: PartitionTemplate {
                parts: vec![TemplatePart::TimeFormat("YYYY-MM".to_string())],
            },
            lifecycle_rules: Default::default(),
            routing_rules: None,
            worker_cleanup_avg_sleep: Duration::from_secs(2),
            write_buffer_connection: None,
        };

        // Create a database
        server.create_database(make_provided_rules(rules)).await
    }

    #[tokio::test]
    async fn load_databases_and_transition_to_server_config() {
        let application = make_application();

        let server = make_server(Arc::clone(&application));
        let server_id = ServerId::try_from(1).unwrap();
        server.set_id(server_id).unwrap();
        server.wait_for_init().await.unwrap();
        let bananas = create_simple_database(&server, "bananas")
            .await
            .expect("failed to create database");

        std::mem::drop(server);

        let server = make_server(Arc::clone(&application));
        server.set_id(ServerId::try_from(1).unwrap()).unwrap();
        server.wait_for_init().await.unwrap();

        let apples = create_simple_database(&server, "apples")
            .await
            .expect("failed to create database");

        assert_eq!(server.db_names_sorted(), vec!["apples", "bananas"]);

        std::mem::drop(server);

        bananas
            .iox_object_store()
            .unwrap()
            .delete_database_rules_file()
            .await
            .expect("cannot delete rules file");

        // delete server config file - this is not something that's supposed to happen but is
        // what will happen during the transition to using the server config file
        let mut path = application.object_store().new_path();
        path.push_dir(server_id.to_string());
        path.set_file_name("config.pb");
        application.object_store().delete(&path).await.unwrap();

        let server = make_server(Arc::clone(&application));
        server.set_id(ServerId::try_from(1).unwrap()).unwrap();
        server.wait_for_init().await.unwrap();

        assert_eq!(server.db_names_sorted(), vec!["apples", "bananas"]);

        // assert server config file has been recreated and contains 2 entries, even though
        // the databases fail to initialize
        let config = server_config(application.object_store(), server_id).await;
        assert_config_contents(
            &config,
            &[
                (
                    &apples.config().name,
                    apples.iox_object_store().unwrap().root_path(),
                ),
                (
                    &bananas.config().name,
                    bananas.iox_object_store().unwrap().root_path(),
                ),
            ],
        );

        let apples_name = DatabaseName::new("apples").unwrap();
        let bananas_name = DatabaseName::new("bananas").unwrap();

        let apples_database = server.database(&apples_name).unwrap();
        let bananas_database = server.database(&bananas_name).unwrap();

        apples_database.wait_for_init().await.unwrap();
        let err = bananas_database.wait_for_init().await.unwrap_err();

        assert!(apples_database.init_error().is_none());
        assert_contains!(err.to_string(), "error fetching rules");
        assert!(Arc::ptr_eq(&err, &bananas_database.init_error().unwrap()));
    }

    #[tokio::test]
    async fn db_names_sorted() {
        let server = make_server(make_application());
        server.set_id(ServerId::try_from(1).unwrap()).unwrap();
        server.wait_for_init().await.unwrap();

        let names = vec!["bar", "baz"];

        for name in &names {
            let name = DatabaseName::new(name.to_string()).unwrap();
            server
                .create_database(default_rules(name))
                .await
                .expect("failed to create database");
        }

        let db_names_sorted = server.db_names_sorted();
        assert_eq!(names, db_names_sorted);
    }

    #[tokio::test]
    async fn writes_local() {
        let server = make_server(make_application());
        server.set_id(ServerId::try_from(1).unwrap()).unwrap();
        server.wait_for_init().await.unwrap();

        let db_name = DatabaseName::new("foo".to_string()).unwrap();
        server
            .create_database(default_rules(db_name.clone()))
            .await
            .unwrap();

        let line = "cpu bar=1 10";
        let lines: Vec<_> = parse_lines(line).map(|l| l.unwrap()).collect();
        server
            .write_lines(&db_name, &lines, ARBITRARY_DEFAULT_TIME)
            .await
            .unwrap();

        let db_name = DatabaseName::new("foo").unwrap();
        let db = server.db(&db_name).unwrap();
        let batches = run_query(db, "select * from cpu").await;

        let expected = vec![
            "+-----+--------------------------------+",
            "| bar | time                           |",
            "+-----+--------------------------------+",
            "| 1   | 1970-01-01T00:00:00.000000010Z |",
            "+-----+--------------------------------+",
        ];
        assert_batches_eq!(expected, &batches);
    }

    #[tokio::test]
    async fn write_entry_local() {
        let application = make_application();
        let registry = Arc::clone(application.metric_registry());
        let server = make_server(application);
        server.set_id(ServerId::try_from(1).unwrap()).unwrap();
        server.wait_for_init().await.unwrap();

        let name = DatabaseName::new("foo".to_string()).unwrap();
        server
            .create_database(default_rules(name.clone()))
            .await
            .unwrap();

        let db_name = DatabaseName::new("foo").unwrap();
        let db = server.db(&db_name).unwrap();
        let rules = db.rules();

        let line = "cpu bar=1 10";
        let lines: Vec<_> = parse_lines(line).map(|l| l.unwrap()).collect();
        let sharded_entries = lines_to_sharded_entries(
            &lines,
            ARBITRARY_DEFAULT_TIME,
            NO_SHARD_CONFIG,
            rules.as_ref(),
        )
        .expect("sharded entries");

        let entry = &sharded_entries[0].entry;
        server
            .write_entry_local(&name, entry.clone())
            .await
            .expect("write entry");

        let batches = run_query(db, "select * from cpu").await;
        let expected = vec![
            "+-----+--------------------------------+",
            "| bar | time                           |",
            "+-----+--------------------------------+",
            "| 1   | 1970-01-01T00:00:00.000000010Z |",
            "+-----+--------------------------------+",
        ];
        assert_batches_eq!(expected, &batches);

        let bytes = registry
            .get_instrument::<Metric<U64Counter>>("ingest_entries_bytes")
            .unwrap()
            .get_observer(&Attributes::from(&[("status", "ok"), ("db_name", "foo")]))
            .unwrap()
            .fetch();
        assert_eq!(bytes, 240)
    }

    // This tests sets up a database with a sharding config which defines exactly one shard
    // backed by 3 remote nodes. One of the nodes is modeled to be "down", while the other two
    // can record write entry events.
    // This tests goes through a few trivial error cases before checking that the both working
    // mock remote servers actually receive write entry events.
    //
    // This test is theoretically flaky, low probability though (in the order of 1e-30)
    #[tokio::test]
    async fn write_entry_downstream() {
        const TEST_SHARD_ID: ShardId = 1;
        const GOOD_REMOTE_ADDR_1: &str = "http://localhost:111";
        const GOOD_REMOTE_ADDR_2: &str = "http://localhost:222";
        const BAD_REMOTE_ADDR: &str = "http://localhost:666";

        let good_remote_id_1 = ServerId::try_from(1).unwrap();
        let good_remote_id_2 = ServerId::try_from(2).unwrap();
        let bad_remote_id = ServerId::try_from(666).unwrap();

        let mut manager = TestConnectionManager::new();
        let written_1 = Arc::new(AtomicBool::new(false));
        manager.remotes.insert(
            GOOD_REMOTE_ADDR_1.to_owned(),
            Arc::new(TestRemoteServer {
                written: Arc::clone(&written_1),
            }),
        );
        let written_2 = Arc::new(AtomicBool::new(false));
        manager.remotes.insert(
            GOOD_REMOTE_ADDR_2.to_owned(),
            Arc::new(TestRemoteServer {
                written: Arc::clone(&written_2),
            }),
        );

        let server = Server::new(manager, make_application(), Default::default());
        server.set_id(ServerId::try_from(1).unwrap()).unwrap();
        server.wait_for_init().await.unwrap();

        let db_name = DatabaseName::new("foo").unwrap();
        server
            .create_database(default_rules(db_name.clone()))
            .await
            .unwrap();

        let remote_ids = vec![bad_remote_id, good_remote_id_1, good_remote_id_2];
        let db = server.db(&db_name).unwrap();

        let shard_config = ShardConfig {
            hash_ring: Some(HashRing {
                shards: vec![TEST_SHARD_ID].into(),
                ..Default::default()
            }),
            shards: vec![(TEST_SHARD_ID, Sink::Iox(remote_ids.clone()))]
                .into_iter()
                .collect(),
            ..Default::default()
        };

        let mut rules = db.rules().as_ref().clone();
        rules.routing_rules = Some(RoutingRules::ShardConfig(shard_config));
        let rules = Arc::new(rules);

        db.update_rules(rules);

        let line = "cpu bar=1 10";
        let lines: Vec<_> = parse_lines(line).map(|l| l.unwrap()).collect();

        let err = server
            .write_lines(&db_name, &lines, ARBITRARY_DEFAULT_TIME)
            .await
            .unwrap_err();
        assert!(
            matches!(err, Error::NoRemoteConfigured { node_group } if node_group == remote_ids)
        );

        // one remote is configured but it's down and we'll get connection error
        server.update_remote(bad_remote_id, BAD_REMOTE_ADDR.into());
        let err = server
            .write_lines(&db_name, &lines, ARBITRARY_DEFAULT_TIME)
            .await
            .unwrap_err();
        assert!(matches!(
            err,
            Error::NoRemoteReachable { errors } if matches!(
                errors[BAD_REMOTE_ADDR],
                connection::ConnectionManagerError::RemoteServerConnectError {..}
            )
        ));
        assert!(!written_1.load(Ordering::Relaxed));
        assert!(!written_2.load(Ordering::Relaxed));

        // We configure the address for the other remote, this time connection will succeed
        // despite the bad remote failing to connect.
        server.update_remote(good_remote_id_1, GOOD_REMOTE_ADDR_1.into());
        server.update_remote(good_remote_id_2, GOOD_REMOTE_ADDR_2.into());

        // Remotes are tried in random order, so we need to repeat the test a few times to have a reasonable
        // probability both the remotes will get hit.
        for _ in 0..100 {
            server
                .write_lines(&db_name, &lines, ARBITRARY_DEFAULT_TIME)
                .await
                .expect("cannot write lines");
        }
        assert!(written_1.load(Ordering::Relaxed));
        assert!(written_2.load(Ordering::Relaxed));
    }

    #[tokio::test]
    async fn close_chunk() {
        test_helpers::maybe_start_logging();
        let server = make_server(make_application());

        server.set_id(ServerId::try_from(1).unwrap()).unwrap();
        server.wait_for_init().await.unwrap();

        let db_name = DatabaseName::new("foo").unwrap();
        server
            .create_database(default_rules(db_name.clone()))
            .await
            .unwrap();

        let line = "cpu bar=1 10";
        let lines: Vec<_> = parse_lines(line).map(|l| l.unwrap()).collect();
        server
            .write_lines(&db_name, &lines, ARBITRARY_DEFAULT_TIME)
            .await
            .unwrap();

        // get chunk ID
        let db = server.db(&db_name).unwrap();
        let chunks = db.chunk_summaries().unwrap();
        assert_eq!(chunks.len(), 1);
        let chunk_id = chunks[0].id;

        // start the close (note this is not an async)
        let chunk_addr = ChunkAddr {
            db_name: Arc::from(db_name.as_str()),
            table_name: Arc::from("cpu"),
            partition_key: Arc::from(""),
            chunk_id,
        };
        let tracker = server
            .close_chunk(
                &db_name,
                chunk_addr.table_name.as_ref(),
                chunk_addr.partition_key.as_ref(),
                chunk_addr.chunk_id,
            )
            .unwrap();

        let metadata = tracker.metadata();
        let expected_metadata = Job::CompactChunks {
            partition: chunk_addr.clone().into_partition(),
            chunks: vec![chunk_addr.chunk_id],
        };
        assert_eq!(metadata, &expected_metadata);

        // wait for the job to complete
        tracker.join().await;

        // Data should be in the read buffer and not in mutable buffer
        let db_name = DatabaseName::new("foo").unwrap();
        let db = server.db(&db_name).unwrap();

        let chunk_summaries = db.chunk_summaries().unwrap();
        assert_eq!(chunk_summaries.len(), 1);
        assert_eq!(chunk_summaries[0].storage, ChunkStorage::ReadBuffer);
    }

    #[tokio::test]
    async fn background_task_cleans_jobs() {
        let application = make_application();
        let server = make_server(Arc::clone(&application));

        let wait_nanos = 1000;
        let job = application
            .job_registry()
            .spawn_dummy_job(vec![wait_nanos], None);

        job.join().await;

        assert!(job.is_complete());

        server.shutdown();
        server.join().await.unwrap();
    }

    fn parsed_lines(lp: &str) -> Vec<ParsedLine<'_>> {
        parse_lines(lp).map(|l| l.unwrap()).collect()
    }

    #[tokio::test]
    async fn hard_buffer_limit() {
        let server = make_server(make_application());
        server.set_id(ServerId::try_from(1).unwrap()).unwrap();
        server.wait_for_init().await.unwrap();

        let name = DatabaseName::new("foo").unwrap();
        server
            .create_database(default_rules(name.clone()))
            .await
            .unwrap();

        let db = server.db(&name).unwrap();

        let mut rules: DatabaseRules = db.rules().as_ref().clone();

        rules.lifecycle_rules.buffer_size_hard = Some(std::num::NonZeroUsize::new(10).unwrap());

        let rules = Arc::new(rules);
        db.update_rules(Arc::clone(&rules));

        // inserting first line does not trigger hard buffer limit
        let line_1 = "cpu bar=1 10";
        let lines_1: Vec<_> = parse_lines(line_1).map(|l| l.unwrap()).collect();
        let sharded_entries_1 = lines_to_sharded_entries(
            &lines_1,
            ARBITRARY_DEFAULT_TIME,
            NO_SHARD_CONFIG,
            rules.as_ref(),
        )
        .expect("first sharded entries");

        let entry_1 = &sharded_entries_1[0].entry;
        server
            .write_entry_local(&name, entry_1.clone())
            .await
            .expect("write first entry");

        // inserting second line will
        let line_2 = "cpu bar=2 20";
        let lines_2: Vec<_> = parse_lines(line_2).map(|l| l.unwrap()).collect();
        let sharded_entries_2 = lines_to_sharded_entries(
            &lines_2,
            ARBITRARY_DEFAULT_TIME,
            NO_SHARD_CONFIG,
            rules.as_ref(),
        )
        .expect("second sharded entries");
        let entry_2 = &sharded_entries_2[0].entry;
        let res = server.write_entry_local(&name, entry_2.clone()).await;
        assert!(matches!(res, Err(super::Error::HardLimitReached {})));
    }

    #[tokio::test]
    async fn cannot_create_db_until_server_is_initialized() {
        let server = make_server(make_application());

        // calling before serverID set leads to `IdNotSet`
        let err = create_simple_database(&server, "bananas")
            .await
            .unwrap_err();
        assert!(matches!(err, Error::IdNotSet));

        server.set_id(ServerId::try_from(1).unwrap()).unwrap();
        // do NOT call `server.maybe_load_database_configs` so DBs are not loaded and server is not ready

        // calling with serverId but before loading is done leads to
        let err = create_simple_database(&server, "bananas")
            .await
            .unwrap_err();
        assert!(matches!(err, Error::ServerNotInitialized { .. }));
    }

    #[tokio::test]
    async fn background_worker_eventually_inits_server() {
        let server = make_server(make_application());

        server.set_id(ServerId::try_from(1).unwrap()).unwrap();

        let t_0 = Instant::now();
        loop {
            if server.initialized() {
                break;
            }
            assert!(t_0.elapsed() < Duration::from_secs(10));
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }

    #[tokio::test]
    async fn init_error_generic() {
        // use an object store that will hopefully fail to read
        let store = Arc::new(ObjectStore::new_failing_store().unwrap());
        let application = Arc::new(ApplicationState::new(store, None));
        let server = make_server(application);

        server.set_id(ServerId::try_from(1).unwrap()).unwrap();
        let err = server.wait_for_init().await.unwrap_err();
        assert!(matches!(err.as_ref(), InitError::ListDatabases { .. }));
        assert_contains!(
            server.server_init_error().unwrap().to_string(),
            "error listing databases in object storage:"
        );
    }

    #[tokio::test]
    async fn init_error_database() {
        let application = make_application();
        let server_id = ServerId::try_from(1).unwrap();

        let server = make_server(Arc::clone(&application));
        server.set_id(server_id).unwrap();
        server.wait_for_init().await.unwrap();

        let foo_db_name = DatabaseName::new("foo").unwrap();
        let bar_db_name = DatabaseName::new("bar").unwrap();

        // create database foo
        create_simple_database(&server, "foo")
            .await
            .expect("failed to create database");

        // create invalid db rules for bar
        let iox_object_store = IoxObjectStore::new(
            Arc::clone(application.object_store()),
            server_id,
            &bar_db_name,
        )
        .await
        .unwrap();
        iox_object_store
            .put_database_rules_file(Bytes::from("x"))
            .await
            .unwrap();
        iox_object_store.get_database_rules_file().await.unwrap();

        // start server
        let server = make_server(application);
        server.set_id(server_id).unwrap();
        server.wait_for_init().await.unwrap();

        // generic error MUST NOT be set
        assert!(server.server_init_error().is_none());

        // server is initialized
        assert!(server.initialized());

        // DB names contain all DBs
        assert_eq!(
            server.db_names_sorted(),
            vec!["bar".to_string(), "foo".to_string()]
        );

        let foo_database = server.database(&foo_db_name).unwrap();
        let bar_database = server.database(&bar_db_name).unwrap();

        foo_database.wait_for_init().await.unwrap();
        assert!(foo_database.init_error().is_none());

        let err = bar_database.wait_for_init().await.unwrap_err();
        assert_contains!(err.to_string(), "error deserializing database rules");
        assert_contains!(
            err.to_string(),
            "failed to decode Protobuf message: invalid varint"
        );
        assert!(Arc::ptr_eq(&err, &bar_database.init_error().unwrap()));

        // can only write to successfully created DBs
        let lines = parsed_lines("cpu foo=1 10");
        server
            .write_lines(&foo_db_name, &lines, ARBITRARY_DEFAULT_TIME)
            .await
            .unwrap();

        let err = server
            .write_lines(&bar_db_name, &lines, ARBITRARY_DEFAULT_TIME)
            .await
            .unwrap_err();
        assert!(matches!(err, Error::DatabaseNotInitialized { .. }));

        // creating failed DBs does not work
        let err = create_simple_database(&server, "bar").await.unwrap_err();
        assert!(matches!(err, Error::DatabaseAlreadyExists { .. }));
    }

    #[tokio::test]
    async fn init_deleted_database() {
        test_helpers::maybe_start_logging();
        let application = make_application();
        let server_id = ServerId::try_from(1).unwrap();

        let foo_db_name = DatabaseName::new("foo").unwrap();

        // create a directory in object storage that looks like it could
        // be a database directory, but doesn't have any valid generation
        // directories in it
        let mut fake_db_path = application.object_store().new_path();
        fake_db_path.push_all_dirs(&[server_id.to_string().as_str(), foo_db_name.as_str()]);
        let mut not_generation_file = fake_db_path.clone();
        not_generation_file.set_file_name("not-a-generation");
        application
            .object_store()
            .put(&not_generation_file, Bytes::new())
            .await
            .unwrap();

        // start server
        let server = make_server(Arc::clone(&application));
        server.set_id(server_id).unwrap();
        server.wait_for_init().await.unwrap();

        // generic error MUST NOT be set
        assert!(server.server_init_error().is_none());

        // server is initialized
        assert!(server.initialized());

        // DB names contains foo
        assert_eq!(server.db_names_sorted().len(), 1);
        assert!(server.db_names_sorted().contains(&String::from("foo")));

        // server config contains foo
        let config = server_config(application.object_store(), server_id).await;
        assert_config_contents(&config, &[(&foo_db_name, fake_db_path.to_string())]);

        // can't delete an inactive database
        let err = server.delete_database(&foo_db_name).await;
        assert!(
            matches!(&err, Err(Error::CannotMarkDatabaseDeleted { .. })),
            "got {:?}",
            err
        );

        let foo_database = server.database(&foo_db_name).unwrap();
        let err = foo_database.wait_for_init().await.unwrap_err();
        assert!(
            matches!(err.as_ref(), database::InitError::NoActiveDatabase),
            "got {:?}",
            err
        );
        assert!(Arc::ptr_eq(&err, &foo_database.init_error().unwrap()));

        // creating a new DB with the deleted db's name works
        let new_foo_db = create_simple_database(&server, &foo_db_name)
            .await
            .expect("failed to create database");

        // DB names contains foo
        assert_eq!(server.db_names_sorted().len(), 1);
        assert!(server.db_names_sorted().contains(&String::from("foo")));

        // server config contains foo
        let config = server_config(application.object_store(), server_id).await;
        assert_config_contents(
            &config,
            &[(
                &foo_db_name,
                new_foo_db.iox_object_store().unwrap().root_path(),
            )],
        );

        // calling delete database works
        server.delete_database(&foo_db_name).await.unwrap();

        // DB names still contains foo
        assert_eq!(server.db_names_sorted().len(), 1);
        assert!(server.db_names_sorted().contains(&String::from("foo")));

        // creating another new DB with the deleted db's name works
        create_simple_database(&server, &foo_db_name)
            .await
            .expect("failed to create database");

        // DB names still contains foo
        assert_eq!(server.db_names_sorted().len(), 1);
        assert!(server.db_names_sorted().contains(&String::from("foo")));
    }

    #[tokio::test]
    async fn init_too_many_active_generation_directories() {
        let application = make_application();
        let server_id = ServerId::try_from(1).unwrap();

        let server = make_server(Arc::clone(&application));
        server.set_id(server_id).unwrap();
        server.wait_for_init().await.unwrap();

        let foo_db_name = DatabaseName::new("foo").unwrap();

        // Create database
        create_simple_database(&server, &foo_db_name)
            .await
            .expect("failed to create database");

        // Delete it
        server.delete_database(&foo_db_name).await.unwrap();

        // Create it again
        create_simple_database(&server, &foo_db_name)
            .await
            .expect("failed to create database");

        // Delete it again
        server.delete_database(&foo_db_name).await.unwrap();

        std::mem::drop(server);

        // Remove the tombstone files from both database generation directories
        let mut db_path = application.object_store().new_path();
        db_path.push_all_dirs(&[server_id.to_string().as_str(), foo_db_name.as_str()]);
        let database_files: Vec<_> = application
            .object_store()
            .list(Some(&db_path))
            .await
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .unwrap()
            .into_iter()
            .flatten()
            .collect();

        // Delete all tombstone files
        let mut deleted_something = false;
        for file in database_files {
            let parsed: DirsAndFileName = file.clone().into();
            if parsed.file_name.unwrap().to_string() == "DELETED" {
                application.object_store().delete(&file).await.unwrap();
                deleted_something = true;
            }
        }
        assert!(deleted_something);

        // Restart the server
        let server = make_server(Arc::clone(&application));
        server.set_id(server_id).unwrap();
        server.wait_for_init().await.unwrap();
        // generic error MUST NOT be set
        assert!(server.server_init_error().is_none());
        // server is initialized
        assert!(server.initialized());

        // The database should be in an error state
        let foo_database = server.database(&foo_db_name).unwrap();
        let err = foo_database.wait_for_init().await.unwrap_err();
        assert!(
            matches!(
                err.as_ref(),
                database::InitError::DatabaseObjectStoreLookup {
                    source: iox_object_store::IoxObjectStoreError::MultipleActiveDatabasesFound
                }
            ),
            "got {:?}",
            err
        );
        assert!(Arc::ptr_eq(&err, &foo_database.init_error().unwrap()));
    }

    #[tokio::test]
    async fn init_without_uuid() {
        let application = make_application();
        let server_id = ServerId::try_from(1).unwrap();

        let server = make_server(Arc::clone(&application));
        server.set_id(server_id).unwrap();
        server.wait_for_init().await.unwrap();

        let db_name = DatabaseName::new("foo").unwrap();

        // Create database
        create_simple_database(&server, &db_name)
            .await
            .expect("failed to create database");

        // restart the server
        std::mem::drop(server);
        let server = make_server(Arc::clone(&application));
        server.set_id(server_id).unwrap();
        server.wait_for_init().await.unwrap();
        assert!(server.initialized());

        // database should not be in an error state
        let database = server.database(&db_name).unwrap();
        database.wait_for_init().await.unwrap();

        // update the database's rules
        let rules = DatabaseRules {
            name: db_name.clone(),
            partition_template: PartitionTemplate {
                parts: vec![TemplatePart::TimeFormat("YYYY-MM".to_string())],
            },
            lifecycle_rules: Default::default(),
            routing_rules: None,
            worker_cleanup_avg_sleep: Duration::from_secs(2),
            write_buffer_connection: Some(WriteBufferConnection {
                direction: WriteBufferDirection::Write,
                type_: "mock".to_string(),
                connection: "my_mock".to_string(),
                ..Default::default()
            }),
        };
        let provided_rules = make_provided_rules(rules);

        server
            .update_db_rules(&db_name, provided_rules)
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn wipe_preserved_catalog() {
        // have the following DBs:
        // 1. existing => cannot be wiped
        // 2. non-existing => can be wiped, will not exist afterwards
        // 3. existing one, but rules file is broken => can be wiped, will not exist afterwards
        // 4. existing one, but catalog is broken => can be wiped, will exist afterwards
        // 5. recently (during server lifecycle) created one => cannot be wiped
        let db_name_existing = DatabaseName::new("db_existing").unwrap();
        let db_name_non_existing = DatabaseName::new("db_non_existing").unwrap();
        let db_name_rules_broken = DatabaseName::new("db_broken_rules").unwrap();
        let db_name_catalog_broken = DatabaseName::new("db_broken_catalog").unwrap();
        let db_name_created = DatabaseName::new("db_created").unwrap();

        // setup
        let application = make_application();
        let server_id = ServerId::try_from(1).unwrap();

        // Create temporary server to create existing databases
        let server = make_server(Arc::clone(&application));
        server.set_id(server_id).unwrap();
        server.wait_for_init().await.unwrap();

        let existing = create_simple_database(&server, db_name_existing.clone())
            .await
            .expect("failed to create database");

        let rules_broken = create_simple_database(&server, db_name_rules_broken.clone())
            .await
            .expect("failed to create database");

        let catalog_broken = create_simple_database(&server, db_name_catalog_broken.clone())
            .await
            .expect("failed to create database");

        // tamper store to break one database
        rules_broken
            .iox_object_store()
            .unwrap()
            .put_database_rules_file(Bytes::from("x"))
            .await
            .unwrap();

        let config = PreservedCatalogConfig::new(
            catalog_broken.iox_object_store().unwrap(),
            db_name_catalog_broken.to_string(),
            Arc::clone(application.time_provider()),
        );

        let (preserved_catalog, _catalog) = load_ok(config).await.unwrap();

        parquet_catalog::test_helpers::break_catalog_with_weird_version(&preserved_catalog).await;
        drop(preserved_catalog);

        rules_broken
            .iox_object_store()
            .unwrap()
            .get_database_rules_file()
            .await
            .unwrap();

        // boot actual test server
        let server = make_server(Arc::clone(&application));

        // cannot wipe if server ID is not set
        assert_eq!(
            server
                .wipe_preserved_catalog(&db_name_non_existing)
                .unwrap_err()
                .to_string(),
            "id not set"
        );

        server.set_id(ServerId::try_from(1).unwrap()).unwrap();
        server.wait_for_init().await.unwrap();

        // Wait for databases to finish startup
        let databases = server.databases().unwrap();
        assert_eq!(databases.len(), 3);

        for database in databases {
            let name = &database.config().name;
            if name == &db_name_existing {
                database.wait_for_init().await.unwrap();
            } else if name == &db_name_catalog_broken {
                let err = database.wait_for_init().await.unwrap_err();
                assert!(matches!(
                    err.as_ref(),
                    database::InitError::CatalogLoad { .. }
                ))
            } else if name == &db_name_rules_broken {
                let err = database.wait_for_init().await.unwrap_err();
                assert_contains!(err.to_string(), "error deserializing database rules");
            } else {
                unreachable!()
            }
        }

        // 1. cannot wipe if DB exists
        assert_eq!(
            server
                .wipe_preserved_catalog(&db_name_existing)
                .unwrap_err()
                .to_string(),
            "error wiping preserved catalog: database (db_existing) in invalid state (Initialized) \
            for transition (WipePreservedCatalog)"
        );
        assert!(
            PreservedCatalog::exists(&existing.iox_object_store().unwrap())
                .await
                .unwrap()
        );

        // 2. cannot wipe non-existent DB
        assert!(matches!(
            server.database(&db_name_non_existing).unwrap_err(),
            Error::DatabaseNotFound { .. }
        ));
        let non_existing_iox_object_store = Arc::new(
            IoxObjectStore::new(
                Arc::clone(application.object_store()),
                server_id,
                &db_name_non_existing,
            )
            .await
            .unwrap(),
        );

        let config = PreservedCatalogConfig::new(
            non_existing_iox_object_store,
            db_name_non_existing.to_string(),
            Arc::clone(application.time_provider()),
        );
        new_empty(config).await;

        assert_eq!(
            server
                .wipe_preserved_catalog(&db_name_non_existing)
                .unwrap_err()
                .to_string(),
            "database not found: db_non_existing"
        );

        // 3. cannot wipe DB with broken rules file
        assert!(server
            .database(&db_name_rules_broken)
            .unwrap()
            .init_error()
            .is_some());

        assert_eq!(
            server
                .wipe_preserved_catalog(&db_name_rules_broken)
                .unwrap_err()
                .to_string(),
            "error wiping preserved catalog: database (db_broken_rules) in invalid state \
            (RulesLoadError) for transition (WipePreservedCatalog)"
        );

        // 4. wipe DB with broken catalog, this will bring the DB back to life
        let database = server.database(&db_name_catalog_broken).unwrap();
        assert!(database.init_error().is_some());

        let tracker = server
            .wipe_preserved_catalog(&db_name_catalog_broken)
            .unwrap();

        let metadata = tracker.metadata();
        let expected_metadata = Job::WipePreservedCatalog {
            db_name: Arc::from(db_name_catalog_broken.as_str()),
        };
        assert_eq!(metadata, &expected_metadata);
        tracker.join().await;

        database.wait_for_init().await.unwrap();

        assert!(
            PreservedCatalog::exists(&catalog_broken.iox_object_store().unwrap())
                .await
                .unwrap()
        );
        assert!(database.init_error().is_none());

        assert!(server.db(&db_name_catalog_broken).is_ok());
        let line = "cpu bar=1 10";
        let lines: Vec<_> = parse_lines(line).map(|l| l.unwrap()).collect();
        server
            .write_lines(&db_name_catalog_broken, &lines, ARBITRARY_DEFAULT_TIME)
            .await
            .expect("DB writable");

        // 5. cannot wipe if DB was just created
        let created = server
            .create_database(default_rules(db_name_created.clone()))
            .await
            .unwrap();

        assert_eq!(
            server
                .wipe_preserved_catalog(&db_name_created)
                .unwrap_err()
                .to_string(),
            "error wiping preserved catalog: database (db_created) in invalid state (Initialized) \
            for transition (WipePreservedCatalog)"
        );
        assert!(
            PreservedCatalog::exists(&created.iox_object_store().unwrap())
                .await
                .unwrap()
        );
    }

    #[tokio::test]
    async fn cannot_create_db_when_catalog_is_present() {
        let application = make_application();
        let server_id = ServerId::try_from(1).unwrap();
        let db_name = DatabaseName::new("my_db").unwrap();

        // setup server
        let server = make_server(Arc::clone(&application));
        server.set_id(server_id).unwrap();
        server.wait_for_init().await.unwrap();

        let iox_object_store = Arc::new(
            IoxObjectStore::new(Arc::clone(application.object_store()), server_id, &db_name)
                .await
                .unwrap(),
        );

        let config = PreservedCatalogConfig::new(
            iox_object_store,
            db_name.to_string(),
            Arc::clone(application.time_provider()),
        );

        // create catalog
        new_empty(config).await;

        // creating database will now result in an error
        let err = create_simple_database(&server, db_name).await.unwrap_err();
        assert!(
            matches!(err, Error::DatabaseAlreadyExists { .. }),
            "got: {:?}",
            err
        );
    }

    #[tokio::test]
    async fn write_buffer_errors_propagate() {
        let application = make_application();

        application
            .write_buffer_factory()
            .register_always_fail_mock("my_mock".to_string());

        let server = make_server(application);
        server.set_id(ServerId::try_from(1).unwrap()).unwrap();
        server.wait_for_init().await.unwrap();

        let db_name = DatabaseName::new("my_db").unwrap();
        let rules = DatabaseRules {
            name: db_name.clone(),
            partition_template: PartitionTemplate {
                parts: vec![TemplatePart::TimeFormat("YYYY-MM".to_string())],
            },
            lifecycle_rules: Default::default(),
            routing_rules: None,
            worker_cleanup_avg_sleep: Duration::from_secs(2),
            write_buffer_connection: Some(WriteBufferConnection {
                direction: WriteBufferDirection::Write,
                type_: "mock".to_string(),
                connection: "my_mock".to_string(),
                ..Default::default()
            }),
        };
        server
            .create_database(make_provided_rules(rules))
            .await
            .unwrap();

        let entry = lp_to_entry("cpu bar=1 10");

        let res = server.write_entry_local(&db_name, entry).await;
        assert!(
            matches!(res, Err(Error::WriteBuffer { .. })),
            "Expected Err(Error::WriteBuffer {{ .. }}), got: {:?}",
            res
        );
    }

    // run a sql query against the database, returning the results as record batches
    async fn run_query(db: Arc<Db>, query: &str) -> Vec<RecordBatch> {
        let planner = SqlQueryPlanner::default();
        let ctx = db.new_query_context(None);

        let physical_plan = planner.query(query, &ctx).await.unwrap();
        ctx.collect(physical_plan).await.unwrap()
    }

    fn default_rules(db_name: DatabaseName<'static>) -> ProvidedDatabaseRules {
        make_provided_rules(DatabaseRules::new(db_name))
    }

    /// Normally database rules are provided as grpc messages, but in
    /// tests they are constructed from database rules structures
    /// themselves.
    fn make_provided_rules(rules: DatabaseRules) -> ProvidedDatabaseRules {
        ProvidedDatabaseRules::new_rules(rules.into())
            .expect("Tests should create valid DatabaseRules")
    }

    #[tokio::test]
    async fn job_metrics() {
        let application = make_application();
        let server = make_server(Arc::clone(&application));

        let wait_nanos = 1000;
        let job = application
            .job_registry()
            .spawn_dummy_job(vec![wait_nanos], Some(Arc::from("some_db")));

        job.join().await;

        // need to force-update metrics
        application.job_registry().reclaim();

        let mut reporter = metric::RawReporter::default();
        application.metric_registry().report(&mut reporter);

        server.shutdown();
        server.join().await.unwrap();

        // ========== influxdb_iox_job_count ==========
        let metric = reporter.metric("influxdb_iox_job_count").unwrap();
        assert_eq!(metric.kind, metric::MetricKind::U64Gauge);
        let observation = metric
            .observation(&[
                ("description", "Dummy Job, for testing"),
                ("status", "Success"),
                ("db_name", "some_db"),
            ])
            .unwrap();
        assert_eq!(observation, &metric::Observation::U64Gauge(1));

        // ========== influxdb_iox_job_completed_cpu ==========
        let metric = reporter.metric("influxdb_iox_job_completed_cpu").unwrap();
        assert_eq!(metric.kind, metric::MetricKind::DurationHistogram);
        metric
            .observation(&[
                ("description", "Dummy Job, for testing"),
                ("status", "Success"),
                ("db_name", "some_db"),
            ])
            .unwrap();

        // ========== influxdb_iox_job_completed_wall ==========
        let metric = reporter.metric("influxdb_iox_job_completed_wall").unwrap();
        assert_eq!(metric.kind, metric::MetricKind::DurationHistogram);
        metric
            .observation(&[
                ("description", "Dummy Job, for testing"),
                ("status", "Success"),
                ("db_name", "some_db"),
            ])
            .unwrap();
    }
}
