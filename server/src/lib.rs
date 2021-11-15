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
    database_rules::{NodeGroup, RoutingRules, Sink},
    detailed_database::ActiveDatabase,
    error::ErrorLogger,
    job::Job,
    server_id::ServerId,
    {DatabaseName, DatabaseNameError},
};
use database::{Database, DatabaseConfig};
use futures::future::{BoxFuture, Future, FutureExt, Shared, TryFutureExt};
use generated_types::{google::FieldViolation, influxdata::iox::management};
use hashbrown::HashMap;
use internal_types::freezable::Freezable;
use iox_object_store::IoxObjectStore;
use observability_deps::tracing::{error, info, warn};
use parking_lot::RwLock;
use rand::seq::SliceRandom;
use resolver::Resolver;
use snafu::{ensure, OptionExt, ResultExt, Snafu};
use std::sync::Arc;
use tokio::{sync::Notify, task::JoinError};
use tokio_util::sync::CancellationToken;
use tracker::{TaskTracker, TrackedFutureExt};
use uuid::Uuid;

pub use application::ApplicationState;
pub use db::Db;
use dml::DmlWrite;
pub use job::JobRegistry;
pub use resolver::{GrpcConnectionString, RemoteTemplate};

mod application;
pub mod connection;
pub mod database;
pub mod db;
mod job;
mod resolver;

pub mod rules;
use rules::{PersistedDatabaseRules, ProvidedDatabaseRules};

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

    #[snafu(display("database uuid not found: {}", uuid))]
    DatabaseUuidNotFound { uuid: Uuid },

    #[snafu(display("cannot get database name from rules: {}", source))]
    CouldNotGetDatabaseNameFromRules { source: DatabaseNameFromRulesError },

    #[snafu(display("{}", source))]
    CannotMarkDatabaseDeleted { source: crate::database::Error },

    #[snafu(display("{}", source))]
    CannotReleaseDatabase { source: crate::database::Error },

    #[snafu(display("{}", source))]
    CannotRestoreDatabase { source: crate::database::InitError },

    #[snafu(display("{}", source))]
    CannotClaimDatabase { source: crate::database::InitError },

    #[snafu(display("A database with the name `{}` already exists", db_name))]
    DatabaseAlreadyExists { db_name: String },

    #[snafu(display("The database with UUID `{}` named `{}` is already active", uuid, name))]
    DatabaseAlreadyActive { name: String, uuid: Uuid },

    #[snafu(display("The database with UUID `{}` is already owned by this server", uuid))]
    DatabaseAlreadyOwnedByThisServer { uuid: Uuid },

    #[snafu(display(
        "Could not release {}: the UUID specified ({}) does not match the current UUID ({})",
        db_name,
        specified,
        current
    ))]
    UuidMismatch {
        db_name: String,
        specified: Uuid,
        current: Uuid,
    },

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

    #[snafu(display("hard buffer limit reached"))]
    HardLimitReached {},

    #[snafu(display(
        "Storing database write failed with the following error(s), and possibly more: {}",
        errors.iter().map(ToString::to_string).collect::<Vec<_>>().join(", ")
    ))]
    StoreWriteErrors { errors: Vec<DatabaseError> },

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

    #[snafu(display("error persisting server config to object storage: {}", source))]
    PersistServerConfig { source: object_store::Error },

    #[snafu(display("Error sharding write: {}", source))]
    ShardWrite {
        source: data_types::database_rules::Error,
    },
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

    #[snafu(display("error getting server config from object storage: {}", source))]
    GetServerConfig { source: object_store::Error },

    #[snafu(display("error deserializing server config from protobuf: {}", source))]
    DeserializeServerConfig {
        source: generated_types::DecodeError,
    },

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
                .map(|(name, database)| (name.to_string(), database.location()))
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
        let db_name = rules.db_name().clone();

        info!(%db_name, %uuid, "creating new database");

        // Wait for exclusive access to mutate server state
        let handle_fut = self.shared.state.read().freeze();
        let handle = handle_fut.await;

        let server_id = {
            let state = self.shared.state.read();
            let initialized = state.initialized()?;

            ensure!(
                !initialized.databases.contains_key(&db_name),
                DatabaseAlreadyExists { db_name }
            );

            initialized.server_id
        };

        let res =
            Database::create(Arc::clone(&self.shared.application), uuid, rules, server_id).await;

        let location = res.context(CannotCreateDatabase)?;

        let database = {
            let mut state = self.shared.state.write();

            // Exchange FreezeHandle for mutable access via WriteGuard
            let mut state = state.unfreeze(handle);

            let database = match &mut *state {
                ServerState::Initialized(initialized) => initialized
                    .new_database(
                        &self.shared,
                        DatabaseConfig {
                            name: db_name,
                            location,
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
    pub async fn delete_database(&self, db_name: &DatabaseName<'static>) -> Result<Uuid> {
        // Wait for exclusive access to mutate server state
        let handle_fut = self.shared.state.read().freeze();
        let handle = handle_fut.await;

        let database = self.database(db_name)?;
        let uuid = database.delete().await.context(CannotMarkDatabaseDeleted)?;
        database.shutdown();
        let _ = database
            .join()
            .await
            .log_if_error("database background worker while deleting database");

        {
            let mut state = self.shared.state.write();

            // Exchange FreezeHandle for mutable access via WriteGuard
            let mut state = state.unfreeze(handle);

            match &mut *state {
                ServerState::Initialized(initialized) => {
                    initialized.databases.remove(db_name);
                }
                _ => unreachable!(),
            }
        }

        self.persist_server_config().await?;

        Ok(uuid)
    }

    /// Release an existing, active database with this name from this server. Return an error if no
    /// active database with this name can be found.
    pub async fn release_database(
        &self,
        db_name: &DatabaseName<'static>,
        uuid: Option<Uuid>,
    ) -> Result<Uuid> {
        // Wait for exclusive access to mutate server state
        let handle_fut = self.shared.state.read().freeze();
        let handle = handle_fut.await;

        let database = self.database(db_name)?;
        let current = database
            .uuid()
            .expect("Previous line should return not found if the database is inactive");

        // If a UUID has been specified, it has to match this database's UUID
        // Should this check be here or in database.release?
        if matches!(uuid, Some(specified) if specified != current) {
            return UuidMismatch {
                db_name: db_name.to_string(),
                specified: uuid.unwrap(),
                current,
            }
            .fail();
        }

        let returned_uuid = database.release().await.context(CannotReleaseDatabase)?;
        database.shutdown();
        let _ = database
            .join()
            .await
            .log_if_error("database background worker while releasing database");

        {
            let mut state = self.shared.state.write();

            // Exchange FreezeHandle for mutable access via WriteGuard
            let mut state = state.unfreeze(handle);

            match &mut *state {
                ServerState::Initialized(initialized) => {
                    initialized.databases.remove(db_name);
                }
                _ => unreachable!(),
            }
        }

        self.persist_server_config().await?;

        Ok(returned_uuid)
    }

    /// Restore a database that has been marked as deleted. Return an error if no database with
    /// this UUID can be found, or if there's already an active database with this name.
    pub async fn restore_database(&self, uuid: Uuid) -> Result<()> {
        // Wait for exclusive access to mutate server state
        let handle_fut = self.shared.state.read().freeze();
        let handle = handle_fut.await;

        // Don't proceed without a server ID
        let server_id = {
            let state = self.shared.state.read();
            let initialized = state.initialized()?;

            initialized.server_id
        };

        // Read the database's rules from object storage to get the database name
        let db_name =
            database_name_from_rules_file(Arc::clone(self.shared.application.object_store()), uuid)
                .await
                .context(CouldNotGetDatabaseNameFromRules)?;

        info!(%db_name, %uuid, "start restoring database");

        // Check that this name is unique among currently active databases
        if let Ok(existing_db) = self.database(&db_name) {
            if matches!(dbg!(existing_db.uuid()),
                    Some(existing_uuid) if existing_uuid == dbg!(uuid))
            {
                return DatabaseAlreadyActive {
                    name: db_name,
                    uuid,
                }
                .fail();
            } else {
                return DatabaseAlreadyExists { db_name }.fail();
            }
        }

        // Mark the database as restored in object storage and get its location for the server
        // config file
        let location = Database::restore(
            Arc::clone(&self.shared.application),
            &db_name,
            uuid,
            server_id,
        )
        .await
        .context(CannotRestoreDatabase)?;

        let database = {
            let mut state = self.shared.state.write();

            // Exchange FreezeHandle for mutable access via WriteGuard
            let mut state = state.unfreeze(handle);

            let database = match &mut *state {
                ServerState::Initialized(initialized) => {
                    if initialized.databases.contains_key(&db_name) {
                        return DatabaseAlreadyExists { db_name }.fail();
                    }

                    initialized
                        .new_database(
                            &self.shared,
                            DatabaseConfig {
                                name: db_name.clone(),
                                location,
                                server_id,
                                wipe_catalog_on_error: false,
                                skip_replay: false,
                            },
                        )
                        .expect("database unique")
                }
                _ => unreachable!(),
            };
            Arc::clone(database)
        };

        // Save the database to the server config as soon as it's added to the `ServerState`
        self.persist_server_config().await?;

        database.wait_for_init().await.context(DatabaseInit)?;

        Ok(())
    }

    /// Claim a database that has been released. Return an error if:
    ///
    /// * No database with this UUID can be found
    /// * There's already an active database with this name
    /// * This database is already owned by this server
    /// * This database is already owned by a different server
    pub async fn claim_database(&self, uuid: Uuid) -> Result<DatabaseName<'static>> {
        // Wait for exclusive access to mutate server state
        let handle_fut = self.shared.state.read().freeze();
        let handle = handle_fut.await;

        // Don't proceed without a server ID
        let server_id = {
            let state = self.shared.state.read();
            let initialized = state.initialized()?;

            initialized.server_id
        };

        // Read the database's rules from object storage to get the database name
        let db_name =
            database_name_from_rules_file(Arc::clone(self.shared.application.object_store()), uuid)
                .await
                .map_err(|e| match e {
                    DatabaseNameFromRulesError::DatabaseRulesNotFound { .. } => {
                        Error::DatabaseUuidNotFound { uuid }
                    }
                    _ => Error::CouldNotGetDatabaseNameFromRules { source: e },
                })?;

        info!(%db_name, %uuid, "start restoring database");

        // Check that this name is unique among currently active databases
        if let Ok(existing_db) = self.database(&db_name) {
            if matches!(existing_db.uuid(), Some(existing_uuid) if existing_uuid == uuid) {
                return DatabaseAlreadyOwnedByThisServer { uuid }.fail();
            } else {
                return DatabaseAlreadyExists { db_name }.fail();
            }
        }

        // Mark the database as claimed in object storage and get its location for the server
        // config file
        let location = Database::claim(
            Arc::clone(&self.shared.application),
            &db_name,
            uuid,
            server_id,
        )
        .await
        .context(CannotClaimDatabase)?;

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
                            location,
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

        Ok(db_name)
    }

    /// List active databases owned by this server, including their UUIDs.
    pub async fn list_detailed_databases(&self) -> Result<Vec<ActiveDatabase>> {
        Ok(self
            .databases()?
            .iter()
            .filter_map(|db| {
                db.uuid().map(|uuid| ActiveDatabase {
                    name: db.config().name.clone(),
                    uuid,
                })
            })
            .collect())
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

    /// `write_lines` takes in raw line protocol and converts it to a collection
    /// of ShardedEntry which are then sent to other IOx servers based on
    /// the ShardConfig or sent to the local database for buffering in the
    /// WriteBuffer and/or the MutableBuffer if configured.
    ///
    /// The provided `default_time` is nanoseconds since the epoch and will be assigned
    /// to any lines that don't have a timestamp.
    ///
    /// TODO: Replace with dedicated router in terms of MutableBatch
    pub async fn write(&self, db_name: &DatabaseName<'_>, write: DmlWrite) -> Result<()> {
        let db = self.db(db_name)?;
        let rules = db.rules();

        let sharded_writes = match &rules.routing_rules {
            Some(RoutingRules::ShardConfig(shard_config)) => {
                let sharded_writes = write.shard(shard_config).context(ShardWrite)?;
                itertools::Either::Left(sharded_writes.into_iter().map(|(s, w)| (Some(s), w)))
            }
            _ => itertools::Either::Right(std::iter::once((None, write))),
        };

        // Write to all shards in parallel; as soon as one fails return error
        // immediately to the client and abort all other outstanding requests.
        futures_util::future::try_join_all(sharded_writes.map(|(shard, write)| {
            let sink = match &rules.routing_rules {
                Some(RoutingRules::ShardConfig(shard_config)) => {
                    let id = shard.expect("sharded entry");
                    Some(shard_config.shards.get(&id).expect("valid shard"))
                }
                Some(RoutingRules::RoutingConfig(config)) => Some(&config.sink),
                None => None,
            };

            async move {
                match sink {
                    Some(sink) => self.write_sink(db_name, sink, &write).await,
                    None => self.write_local(db_name, &write).await,
                }
            }
        }))
        .await?;
        Ok(())
    }

    async fn write_sink(
        &self,
        db_name: &DatabaseName<'_>,
        sink: &Sink,
        write: &DmlWrite,
    ) -> Result<()> {
        match sink {
            Sink::Iox(node_group) => self.write_downstream(db_name, node_group, write).await,
            Sink::Kafka(_) => {
                // The write buffer write path is currently implemented in "db", so confusingly we
                // need to invoke write_entry_local.
                // TODO(mkm): tracked in #2134
                self.write_local(db_name, write).await
            }
            Sink::DevNull => {
                // write is silently ignored, as requested by the configuration.
                Ok(())
            }
        }
    }

    async fn write_downstream(
        &self,
        db_name: &str,
        node_group: &[ServerId],
        write: &DmlWrite,
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
                Ok(remote) => return remote.write(db_name, write).await.context(RemoteError),
            };
        }
        NoRemoteReachable { errors }.fail()
    }

    /// Write an entry to the local `Db`
    ///
    /// TODO: Remove this and migrate callers to `Database::route_write`
    async fn write_local(&self, db_name: &DatabaseName<'_>, write: &DmlWrite) -> Result<()> {
        use database::WriteError;

        self.active_database(db_name)?
            .route_write(write)
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
                WriteError::StoreWriteErrors { errors } => Error::StoreWriteErrors {
                    errors: errors.into_iter().map(|e| Box::new(e) as _).collect(),
                },
            })
    }

    /// Update database rules and save on success.
    pub async fn update_db_rules(
        &self,
        rules: ProvidedDatabaseRules,
    ) -> Result<Arc<ProvidedDatabaseRules>> {
        let db_name = rules.db_name().clone();
        let database = self.database(&db_name)?;

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

    let maybe_databases = IoxObjectStore::get_server_config_file(
        shared.application.object_store(),
        init_ready.server_id,
    )
    .await
    .or_else(|e| match e {
        // If this is the first time starting up this server and there is no config file yet,
        // this isn't a problem. Start an empty server config.
        object_store::Error::NotFound { .. } => Ok(bytes::Bytes::new()),
        // Any other error is a problem.
        _ => Err(InitError::GetServerConfig { source: e }),
    })
    .and_then(|config| {
        generated_types::server_config::decode_persisted_server_config(config)
            .map_err(|e| InitError::DeserializeServerConfig { source: e })
    })
    .map(|server_config| {
        server_config
            .databases
            .into_iter()
            .map(|(name, location)| {
                (
                    DatabaseName::new(name).expect("serialized db names should be valid"),
                    location,
                )
            })
            .collect::<Vec<_>>()
    });

    let next_state = match maybe_databases {
        Ok(databases) => {
            let mut state = ServerStateInitialized {
                server_id: init_ready.server_id,
                databases: HashMap::with_capacity(databases.len()),
            };

            for (db_name, location) in databases {
                state
                    .new_database(
                        shared,
                        DatabaseConfig {
                            name: db_name,
                            location,
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
            ServerState::InitError(init_ready, Arc::new(e))
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

#[derive(Snafu, Debug)]
pub enum DatabaseNameFromRulesError {
    #[snafu(display(
        "database rules for UUID {} not found at expected location `{}`",
        uuid,
        location
    ))]
    DatabaseRulesNotFound { uuid: Uuid, location: String },

    #[snafu(display("error loading rules from object storage: {} ({:?})", source, source))]
    CannotLoadRules { source: object_store::Error },

    #[snafu(display("error deserializing database rules from protobuf: {}", source))]
    CannotDeserializeRules {
        source: generated_types::DecodeError,
    },

    #[snafu(display("error converting grpc to database rules: {}", source))]
    ConvertingRules { source: FieldViolation },
}

async fn database_name_from_rules_file(
    object_store: Arc<object_store::ObjectStore>,
    uuid: Uuid,
) -> Result<DatabaseName<'static>, DatabaseNameFromRulesError> {
    let rules_bytes = IoxObjectStore::load_database_rules(object_store, uuid)
        .await
        .map_err(|e| match e {
            object_store::Error::NotFound { location, .. } => {
                DatabaseNameFromRulesError::DatabaseRulesNotFound { uuid, location }
            }
            other => DatabaseNameFromRulesError::CannotLoadRules { source: other },
        })?;

    let rules: PersistedDatabaseRules =
        generated_types::database_rules::decode_persisted_database_rules(rules_bytes)
            .context(CannotDeserializeRules)?
            .try_into()
            .context(ConvertingRules)?;

    Ok(rules.db_name().to_owned())
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
            DatabaseRules, HashRing, LifecycleRules, PartitionTemplate, ShardConfig, ShardId,
            TemplatePart,
        },
        write_buffer::{WriteBufferConnection, WriteBufferDirection},
    };
    use iox_object_store::IoxObjectStore;
    use mutable_batch_lp::lines_to_batches;
    use object_store::{path::ObjectStorePath, ObjectStore, ObjectStoreApi};
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
    use test_helpers::{assert_contains, assert_error};

    #[tokio::test]
    async fn server_api_calls_return_error_with_no_id_set() {
        let server = make_server(make_application());

        let tables = lines_to_batches("cpu foo=1 10", 0).unwrap();
        let write = DmlWrite::new(tables, Default::default());
        let resp = server
            .write(&DatabaseName::new("foo").unwrap(), write)
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
            let location = config
                .databases
                .get(expected_name.as_str())
                .unwrap_or_else(|| {
                    panic!(
                        "Could not find database named {} in server config",
                        expected_name
                    )
                });

            assert_eq!(location, expected_location);
        }
    }

    #[tokio::test]
    async fn create_database_persists_rules_owner_and_server_config() {
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
        let read_rules = PersistedDatabaseRules::load(&iox_object_store)
            .await
            .unwrap();
        let bananas_uuid = read_rules.uuid();

        // Same rules that were provided are read
        assert_eq!(provided_rules.original(), read_rules.original());

        // rules that are being used are the same
        assert_eq!(provided_rules.rules(), read_rules.rules());

        // assert this database knows it's owned by this server
        let owner_info = bananas.owner_info().unwrap();
        assert_eq!(owner_info.id, server_id.get_u32());
        assert_eq!(
            owner_info.location,
            IoxObjectStore::server_config_path(application.object_store(), server_id).to_string()
        );

        // assert server config file exists and has 1 entry
        let config = server_config(application.object_store(), server_id).await;
        assert_config_contents(&config, &[(&name, format!("dbs/{}/", bananas_uuid))]);

        let db2 = DatabaseName::new("db_awesome").unwrap();
        let rules2 = DatabaseRules::new(db2.clone());
        let provided_rules2 = make_provided_rules(rules2);

        let awesome = server
            .create_database(provided_rules2)
            .await
            .expect("failed to create 2nd db");
        let awesome_uuid = awesome.uuid().unwrap();

        // assert server config file exists and has 2 entries
        let config = server_config(application.object_store(), server_id).await;
        assert_config_contents(
            &config,
            &[
                (&name, format!("dbs/{}/", bananas_uuid)),
                (&db2, format!("dbs/{}/", awesome_uuid)),
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
                (&name, format!("dbs/{}/", bananas_uuid)),
                (&db2, format!("dbs/{}/", awesome_uuid)),
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
    async fn load_databases() {
        let application = make_application();

        let server = make_server(Arc::clone(&application));
        let server_id = ServerId::try_from(1).unwrap();
        server.set_id(server_id).unwrap();
        server.wait_for_init().await.unwrap();
        let bananas = create_simple_database(&server, "bananas")
            .await
            .expect("failed to create database");
        let bananas_uuid = bananas.uuid().unwrap();

        std::mem::drop(server);

        let server = make_server(Arc::clone(&application));
        server.set_id(ServerId::try_from(1).unwrap()).unwrap();
        server.wait_for_init().await.unwrap();

        let apples = create_simple_database(&server, "apples")
            .await
            .expect("failed to create database");
        let apples_uuid = apples.uuid().unwrap();

        assert_eq!(server.db_names_sorted(), vec!["apples", "bananas"]);

        std::mem::drop(server);

        bananas
            .iox_object_store()
            .unwrap()
            .delete_database_rules_file()
            .await
            .expect("cannot delete rules file");

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
                (&apples.config().name, format!("dbs/{}/", apples_uuid)),
                (&bananas.config().name, format!("dbs/{}/", bananas_uuid)),
            ],
        );

        let apples_name = DatabaseName::new("apples").unwrap();
        let bananas_name = DatabaseName::new("bananas").unwrap();

        let apples_database = server.database(&apples_name).unwrap();
        let bananas_database = server.database(&bananas_name).unwrap();

        apples_database.wait_for_init().await.unwrap();
        assert!(apples_database.init_error().is_none());

        let err = bananas_database.wait_for_init().await.unwrap_err();
        assert_contains!(err.to_string(), "No rules found to load");
        assert!(Arc::ptr_eq(&err, &bananas_database.init_error().unwrap()));
    }

    #[tokio::test]
    async fn old_database_object_store_path() {
        let application = make_application();
        let server = make_server(Arc::clone(&application));
        let server_id = ServerId::try_from(1).unwrap();
        let object_store = application.object_store();

        // Databases used to be stored under the server ID. Construct a database in the old
        // location and list that in the serialized server config.
        let old_loc_db_uuid = Uuid::new_v4();
        let old_loc_db_name = DatabaseName::new("old").unwrap();

        // Construct path in the old database location containing server ID
        let mut old_root = object_store.new_path();
        old_root.push_dir(&server_id.to_string());
        old_root.push_dir(&old_loc_db_uuid.to_string());

        // Write out a database owner file in the old location
        let mut old_owner_path = old_root.clone();
        old_owner_path.set_file_name("owner.pb");
        let owner_info = management::v1::OwnerInfo {
            id: server_id.get_u32(),
            location: IoxObjectStore::server_config_path(object_store, server_id).to_string(),
            transactions: vec![],
        };
        let mut encoded_owner_info = bytes::BytesMut::new();
        generated_types::server_config::encode_database_owner_info(
            &owner_info,
            &mut encoded_owner_info,
        )
        .expect("owner info serialization should be valid");
        let encoded_owner_info = encoded_owner_info.freeze();
        object_store
            .put(&old_owner_path, encoded_owner_info)
            .await
            .unwrap();

        // Write out a database rules file in the old location
        let mut old_db_rules_path = old_root.clone();
        old_db_rules_path.set_file_name("rules.pb");
        let rules = management::v1::DatabaseRules {
            name: old_loc_db_name.to_string(),
            ..Default::default()
        };
        let persisted_database_rules = management::v1::PersistedDatabaseRules {
            uuid: old_loc_db_uuid.as_bytes().to_vec(),
            rules: Some(rules),
        };
        let mut encoded_rules = bytes::BytesMut::new();
        generated_types::database_rules::encode_persisted_database_rules(
            &persisted_database_rules,
            &mut encoded_rules,
        )
        .unwrap();
        let encoded_rules = encoded_rules.freeze();
        object_store
            .put(&old_db_rules_path, encoded_rules)
            .await
            .unwrap();

        // Write out the server config with the database name and pointing to the old location
        let old_location = old_root.to_raw().to_string();
        let server_config = management::v1::ServerConfig {
            databases: [(old_loc_db_name.to_string(), old_location.clone())]
                .into_iter()
                .collect(),
        };
        let mut encoded_server_config = bytes::BytesMut::new();
        generated_types::server_config::encode_persisted_server_config(
            &server_config,
            &mut encoded_server_config,
        )
        .unwrap();
        let encoded_server_config = encoded_server_config.freeze();
        IoxObjectStore::put_server_config_file(object_store, server_id, encoded_server_config)
            .await
            .unwrap();

        // The server should initialize successfully
        server.set_id(server_id).unwrap();
        server.wait_for_init().await.unwrap();

        // The database should initialize successfully
        let old_loc_db = server.database(&old_loc_db_name).unwrap();
        old_loc_db.wait_for_init().await.unwrap();

        // Database rules can be updated
        let mut new_rules = DatabaseRules::new(old_loc_db_name.clone());
        new_rules.worker_cleanup_avg_sleep = Duration::from_secs(22);
        server
            .update_db_rules(make_provided_rules(new_rules.clone()))
            .await
            .unwrap();
        let updated = old_loc_db.provided_rules().unwrap();
        assert_eq!(
            updated.rules().worker_cleanup_avg_sleep,
            new_rules.worker_cleanup_avg_sleep
        );

        // Location remains the same
        assert_eq!(old_loc_db.location(), old_location);

        // New databases are created in the current database location, `dbs`
        let new_loc_db_name = DatabaseName::new("new").unwrap();
        let new_loc_rules = DatabaseRules::new(new_loc_db_name.clone());
        let new_loc_db = server
            .create_database(make_provided_rules(new_loc_rules))
            .await
            .unwrap();
        let new_loc_db_uuid = new_loc_db.uuid().unwrap();
        assert_eq!(new_loc_db.location(), format!("dbs/{}/", new_loc_db_uuid));

        // Restarting the server with a database in the "old" location and a database in the "new"
        // location works
        std::mem::drop(server);
        let server = make_server(Arc::clone(&application));
        server.set_id(ServerId::try_from(1).unwrap()).unwrap();
        server.wait_for_init().await.unwrap();
        let old_loc_db = server.database(&old_loc_db_name).unwrap();
        old_loc_db.wait_for_init().await.unwrap();
        let new_loc_db = server.database(&new_loc_db_name).unwrap();
        new_loc_db.wait_for_init().await.unwrap();
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

        let tables = lines_to_batches("cpu bar=1 10", 0).unwrap();
        let write = DmlWrite::new(tables, Default::default());
        server.write(&db_name, write).await.unwrap();

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

        let tables = lines_to_batches("cpu bar=1 10", 0).unwrap();
        let write = DmlWrite::new(tables, Default::default());
        let err = server.write(&db_name, write.clone()).await.unwrap_err();
        assert!(
            matches!(err, Error::NoRemoteConfigured { node_group } if node_group == remote_ids)
        );

        // one remote is configured but it's down and we'll get connection error
        server.update_remote(bad_remote_id, BAD_REMOTE_ADDR.into());
        let err = server.write(&db_name, write.clone()).await.unwrap_err();
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
                .write(&db_name, write.clone())
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

        let tables = lines_to_batches("cpu bar=1 10", 0).unwrap();
        let write = DmlWrite::new(tables, Default::default());
        server.write(&db_name, write).await.unwrap();

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
        let tables = lines_to_batches("cpu bar=1 10", 0).unwrap();
        let write = DmlWrite::new(tables, Default::default());
        server.write(&name, write).await.unwrap();

        // inserting second line will
        let tables = lines_to_batches("cpu bar=2 20", 0).unwrap();
        let write = DmlWrite::new(tables, Default::default());
        let res = server.write(&name, write).await.unwrap_err();
        assert!(matches!(res, super::Error::HardLimitReached {}));
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
        let application = Arc::new(ApplicationState::new(store, None, None));
        let server = make_server(application);

        server.set_id(ServerId::try_from(1).unwrap()).unwrap();
        let err = server.wait_for_init().await.unwrap_err();
        assert!(
            matches!(err.as_ref(), InitError::GetServerConfig { .. }),
            "got: {:?}",
            err
        );
        assert_contains!(
            server.server_init_error().unwrap().to_string(),
            "error getting server config from object storage:"
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
        let baz_db_name = DatabaseName::new("baz").unwrap();

        // create database foo
        create_simple_database(&server, "foo")
            .await
            .expect("failed to create database");

        // create database bar so it gets written to the server config
        let bar = create_simple_database(&server, "bar")
            .await
            .expect("failed to create database");

        // make the db rules for bar invalid
        let iox_object_store = bar.iox_object_store().unwrap();

        iox_object_store
            .put_database_rules_file(Bytes::from("x"))
            .await
            .unwrap();
        iox_object_store.get_database_rules_file().await.unwrap();

        // create database bar so it gets written to the server config
        let baz = create_simple_database(&server, "baz")
            .await
            .expect("failed to create database");

        // make the owner info for baz say it's owned by a different server
        let baz_iox_object_store = baz.iox_object_store().unwrap();
        let owner_info = management::v1::OwnerInfo {
            id: 2,
            location: "2/config.pb".to_string(),
            transactions: vec![],
        };
        let mut encoded = bytes::BytesMut::new();
        generated_types::server_config::encode_database_owner_info(&owner_info, &mut encoded)
            .expect("owner info serialization should be valid");
        let encoded = encoded.freeze();

        baz_iox_object_store.put_owner_file(encoded).await.unwrap();

        // restart server
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
            vec!["bar".to_string(), "baz".to_string(), "foo".to_string()]
        );

        let foo_database = server.database(&foo_db_name).unwrap();
        let bar_database = server.database(&bar_db_name).unwrap();
        let baz_database = server.database(&baz_db_name).unwrap();

        foo_database.wait_for_init().await.unwrap();
        assert!(foo_database.init_error().is_none());

        let err = bar_database.wait_for_init().await.unwrap_err();
        assert_contains!(err.to_string(), "error deserializing database rules");
        assert_contains!(
            err.to_string(),
            "failed to decode Protobuf message: invalid varint"
        );
        assert!(Arc::ptr_eq(&err, &bar_database.init_error().unwrap()));

        let baz_err = baz_database.wait_for_init().await.unwrap_err();
        assert_contains!(
            baz_err.to_string(),
            "Server ID in the database's owner info file (2) does not match this server's ID (1)"
        );

        // can only write to successfully created DBs
        let tables = lines_to_batches("cpu foo=1 10", 0).unwrap();
        let write = DmlWrite::new(tables, Default::default());
        server.write(&foo_db_name, write.clone()).await.unwrap();

        let err = server.write(&bar_db_name, write).await.unwrap_err();
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

        // start server
        let server = make_server(Arc::clone(&application));
        server.set_id(server_id).unwrap();
        server.wait_for_init().await.unwrap();

        // create database
        let foo = create_simple_database(&server, &foo_db_name)
            .await
            .expect("failed to create database");
        let foo_uuid = foo.uuid().unwrap();

        // delete database
        server
            .delete_database(&foo_db_name)
            .await
            .expect("failed to delete database");

        // restart server
        let server = make_server(Arc::clone(&application));
        server.set_id(server_id).unwrap();
        server.wait_for_init().await.unwrap();

        // generic error MUST NOT be set
        assert!(server.server_init_error().is_none());

        // server is initialized
        assert!(server.initialized());

        // DB names is empty
        assert!(server.db_names_sorted().is_empty());

        // can't delete an inactive database
        let err = server.delete_database(&foo_db_name).await;
        assert!(
            matches!(&err, Err(Error::DatabaseNotFound { .. })),
            "got {:?}",
            err
        );

        // creating a new DB with the deleted db's name works
        let new_foo = create_simple_database(&server, &foo_db_name)
            .await
            .expect("failed to create database");
        let new_foo_uuid = new_foo.uuid().unwrap();
        assert_ne!(foo_uuid, new_foo_uuid);

        // DB names contains foo
        assert_eq!(server.db_names_sorted().len(), 1);
        assert!(server.db_names_sorted().contains(&String::from("foo")));

        // server config contains foo
        let config = server_config(application.object_store(), server_id).await;
        assert_config_contents(&config, &[(&foo_db_name, format!("dbs/{}/", new_foo_uuid))]);

        // calling delete database works
        server.delete_database(&foo_db_name).await.unwrap();
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

        server.update_db_rules(provided_rules).await.unwrap();
    }

    #[tokio::test]
    async fn release_database_removes_from_memory_and_persisted_config() {
        let application = make_application();
        let server_id = ServerId::try_from(1).unwrap();

        let foo_db_name = DatabaseName::new("foo").unwrap();

        // start server
        let server = make_server(Arc::clone(&application));
        server.set_id(server_id).unwrap();
        server.wait_for_init().await.unwrap();

        // create database
        let foo = create_simple_database(&server, &foo_db_name).await.unwrap();
        let first_foo_uuid = foo.uuid().unwrap();

        // release database by name
        let released_uuid = server.release_database(&foo_db_name, None).await.unwrap();
        assert_eq!(first_foo_uuid, released_uuid);

        assert_error!(
            server.database(&foo_db_name),
            Error::DatabaseNotFound { .. },
        );

        let config = server_config(application.object_store(), server_id).await;
        assert_config_contents(&config, &[]);

        // create another database
        let foo = create_simple_database(&server, &foo_db_name).await.unwrap();
        let second_foo_uuid = foo.uuid().unwrap();

        // release database specifying UUID; error if UUID doesn't match
        let incorrect_uuid = Uuid::new_v4();
        assert_error!(
            server
                .release_database(&foo_db_name, Some(incorrect_uuid))
                .await,
            Error::UuidMismatch { .. }
        );

        // release database specifying UUID works if UUID *does* match
        server
            .release_database(&foo_db_name, Some(second_foo_uuid))
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn cant_release_nonexistent_database() {
        let application = make_application();
        let server_id = ServerId::try_from(1).unwrap();

        let foo_db_name = DatabaseName::new("foo").unwrap();

        // start server
        let server = make_server(Arc::clone(&application));
        server.set_id(server_id).unwrap();
        server.wait_for_init().await.unwrap();

        assert_error!(
            server.release_database(&foo_db_name, None).await,
            Error::DatabaseNotFound { .. },
        );
    }

    #[tokio::test]
    async fn claim_database_adds_to_memory_and_persisted_config() {
        let application = make_application();
        let server_id = ServerId::try_from(1).unwrap();

        let foo_db_name = DatabaseName::new("foo").unwrap();

        // start server
        let server = make_server(Arc::clone(&application));
        server.set_id(server_id).unwrap();
        server.wait_for_init().await.unwrap();

        // create database
        create_simple_database(&server, &foo_db_name).await.unwrap();

        // release database by name
        let released_uuid = server.release_database(&foo_db_name, None).await.unwrap();

        // claim database by UUID
        server.claim_database(released_uuid).await.unwrap();

        let claimed = server.database(&foo_db_name).unwrap();
        claimed.wait_for_init().await.unwrap();

        let config = server_config(application.object_store(), server_id).await;
        assert_config_contents(
            &config,
            &[(&foo_db_name, format!("dbs/{}/", released_uuid))],
        );
    }

    #[tokio::test]
    async fn cant_claim_nonexistent_database() {
        let application = make_application();
        let server_id = ServerId::try_from(1).unwrap();

        let invalid_uuid = Uuid::new_v4();

        // start server
        let server = make_server(Arc::clone(&application));
        server.set_id(server_id).unwrap();
        server.wait_for_init().await.unwrap();

        assert_error!(
            server.claim_database(invalid_uuid).await,
            Error::DatabaseUuidNotFound { .. },
        );
    }

    #[tokio::test]
    async fn cant_claim_database_owned_by_another_server() {
        let application = make_application();
        let server_id1 = ServerId::try_from(1).unwrap();
        let server_id2 = ServerId::try_from(2).unwrap();
        let foo_db_name = DatabaseName::new("foo").unwrap();

        // start server 1
        let server1 = make_server(Arc::clone(&application));
        server1.set_id(server_id1).unwrap();
        server1.wait_for_init().await.unwrap();

        // create database owned by server 1
        let database = create_simple_database(&server1, &foo_db_name)
            .await
            .unwrap();
        let uuid = database.uuid().unwrap();

        // start server 2
        let server2 = make_server(Arc::clone(&application));
        server2.set_id(server_id2).unwrap();
        server2.wait_for_init().await.unwrap();

        // Attempting to claim on server 2 will fail
        assert_error!(
            server2.claim_database(uuid).await,
            Error::CannotClaimDatabase {
                source: database::InitError::CantClaimDatabaseCurrentlyOwned { server_id, .. }
            } if server_id == server_id1.get_u32()
        );

        // Have to release from server 1 first
        server1.release_database(&foo_db_name, None).await.unwrap();

        // Then claiming on server 2 will work
        server2.claim_database(uuid).await.unwrap();
    }

    #[tokio::test]
    async fn cant_restore_nonexistent_database() {
        let application = make_application();
        let server_id = ServerId::try_from(1).unwrap();

        let server = make_server(Arc::clone(&application));
        server.set_id(server_id).unwrap();
        server.wait_for_init().await.unwrap();

        let nonexistent_uuid = Uuid::new_v4();

        assert_error!(
            server.restore_database(nonexistent_uuid).await,
            Error::CouldNotGetDatabaseNameFromRules {
                source: DatabaseNameFromRulesError::DatabaseRulesNotFound { .. },
            }
        );
    }

    #[tokio::test]
    async fn cant_restore_database_with_same_name_as_existing_database() {
        test_helpers::maybe_start_logging();
        let application = make_application();
        let server_id = ServerId::try_from(1).unwrap();

        let foo_db_name = DatabaseName::new("foo").unwrap();

        // start server
        let server = make_server(Arc::clone(&application));
        server.set_id(server_id).unwrap();
        server.wait_for_init().await.unwrap();

        // create database
        let foo = create_simple_database(&server, &foo_db_name)
            .await
            .expect("failed to create database");
        let first_foo_uuid = foo.uuid().unwrap();

        // delete database
        server
            .delete_database(&foo_db_name)
            .await
            .expect("failed to delete database");

        // create another database named foo
        create_simple_database(&server, &foo_db_name)
            .await
            .expect("failed to create database");

        // trying to restore the first foo fails
        assert_error!(
            server.restore_database(first_foo_uuid).await,
            Error::DatabaseAlreadyExists { .. },
        );
    }

    #[tokio::test]
    async fn cant_restore_database_twice() {
        test_helpers::maybe_start_logging();
        let application = make_application();
        let server_id = ServerId::try_from(1).unwrap();

        let foo_db_name = DatabaseName::new("foo").unwrap();

        // start server
        let server = make_server(Arc::clone(&application));
        server.set_id(server_id).unwrap();
        server.wait_for_init().await.unwrap();

        // create database
        let foo = create_simple_database(&server, &foo_db_name)
            .await
            .expect("failed to create database");
        let foo_uuid = foo.uuid().unwrap();

        // delete database
        server
            .delete_database(&foo_db_name)
            .await
            .expect("failed to delete database");

        // restore database
        server.restore_database(foo_uuid).await.unwrap();

        // restoring again fails
        assert_error!(
            server.restore_database(foo_uuid).await,
            Error::DatabaseAlreadyActive { .. },
        );
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
        let db_uuid_non_existing = Uuid::new_v4();

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
            IoxObjectStore::create(Arc::clone(application.object_store()), db_uuid_non_existing)
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
        let tables = lines_to_batches("cpu bar=1 10", 0).unwrap();
        let write = DmlWrite::new(tables, Default::default());
        server.write(&db_name_catalog_broken, write).await.unwrap();

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

        let tables = lines_to_batches("cpu bar=1 10", 0).unwrap();
        let write = DmlWrite::new(tables, Default::default());
        assert_error!(
            server.write(&db_name, write).await,
            Error::WriteBuffer { .. },
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
