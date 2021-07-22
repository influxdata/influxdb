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

#![deny(broken_intra_doc_links, rustdoc::bare_urls, rust_2018_idioms)]
#![warn(
    missing_debug_implementations,
    clippy::explicit_iter_loop,
    clippy::use_self,
    clippy::clone_on_ref_ptr,
    clippy::future_not_send
)]

use std::convert::{Infallible, TryInto};
use std::sync::Arc;

use async_trait::async_trait;
use bytes::BytesMut;
use db::load::create_preserved_catalog;
use observability_deps::tracing::{debug, error, info, warn};
use parking_lot::{Mutex, RwLockUpgradableReadGuard};
use snafu::{OptionExt, ResultExt, Snafu};

use data_types::{
    database_rules::{
        DatabaseRules, NodeGroup, RoutingRules, ShardConfig, ShardId, Sink, WriteBufferConnection,
    },
    database_state::DatabaseStateCode,
    job::Job,
    server_id::ServerId,
    {DatabaseName, DatabaseNameError},
};
use entry::{lines_to_sharded_entries, pb_to_entry, Entry, ShardedEntry};
use generated_types::influxdata::transfer::column::v1 as pb;
use influxdb_line_protocol::ParsedLine;
use metrics::{KeyValue, MetricObserverBuilder, MetricRegistry};
use object_store::{ObjectStore, ObjectStoreApi};
use parking_lot::RwLock;
use query::{exec::Executor, DatabaseStore};
use tracker::{TaskId, TaskRegistration, TaskRegistryWithHistory, TaskTracker, TrackedFutureExt};
use write_buffer::config::WriteBufferConfig;

pub use crate::config::RemoteTemplate;
use crate::config::{object_store_path_for_database_config, Config, GRpcConnectionString};
use cache_loader_async::cache_api::LoadingCache;
pub use db::Db;
use generated_types::database_rules::encode_database_rules;
use influxdb_iox_client::{connection::Builder, write};
use lifecycle::LockableChunk;
use rand::seq::SliceRandom;
use std::collections::HashMap;

mod config;
pub mod db;
mod init;

/// Utility modules used by benchmarks and tests
pub mod utils;

type DatabaseError = Box<dyn std::error::Error + Send + Sync + 'static>;

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

    #[snafu(display("chunk not found: {}", source))]
    ChunkNotFound { source: db::catalog::Error },

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

    #[snafu(display(
        "Server ID is set ({}) but server is not yet initialized (e.g. DBs and remotes are not loaded). Server is not yet ready to read/write data.", server_id
    ))]
    ServerNotInitialized { server_id: ServerId },

    #[snafu(display("error serializing database rules to protobuf: {}", source))]
    ErrorSerializingRulesProtobuf {
        source: generated_types::database_rules::EncodeError,
    },

    #[snafu(display("error deserializing configuration {}", source))]
    ErrorDeserializing { source: serde_json::Error },

    #[snafu(display("store error: {}", source))]
    StoreError { source: object_store::Error },

    #[snafu(display("database already exists: {}", db_name))]
    DatabaseAlreadyExists { db_name: String },

    #[snafu(display("database currently reserved: {}", db_name))]
    DatabaseReserved { db_name: String },

    #[snafu(display("no rules loaded for database: {}", db_name))]
    NoRulesLoaded { db_name: String },

    #[snafu(display(
        "Database names in deserialized rules ({}) does not match expected value ({})",
        actual,
        expected
    ))]
    RulesDatabaseNameMismatch { actual: String, expected: String },

    #[snafu(display("error converting line protocol to flatbuffers: {}", source))]
    LineConversion { source: entry::Error },

    #[snafu(display("error converting protobuf to flatbuffers: {}", source))]
    PBConversion { source: entry::Error },

    #[snafu(display("error decoding entry flatbuffers: {}", source))]
    DecodingEntry {
        source: flatbuffers::InvalidFlatbuffer,
    },

    #[snafu(display("shard not found: {}", shard_id))]
    ShardNotFound { shard_id: ShardId },

    #[snafu(display("hard buffer limit reached"))]
    HardLimitReached {},

    #[snafu(display(
        "Cannot write to database {}, it's configured to only read from the write buffer",
        db_name
    ))]
    WritingOnlyAllowedThroughWriteBuffer { db_name: String },

    #[snafu(display("Cannot write to write buffer, bytes {}: {}", bytes, source))]
    WriteBuffer { source: db::Error, bytes: u64 },

    #[snafu(display("no remote configured for node group: {:?}", node_group))]
    NoRemoteConfigured { node_group: NodeGroup },

    #[snafu(display("all remotes failed connecting: {:?}", errors))]
    NoRemoteReachable {
        errors: HashMap<GRpcConnectionString, ConnectionManagerError>,
    },

    #[snafu(display("remote error: {}", source))]
    RemoteError { source: ConnectionManagerError },

    #[snafu(display("cannot create preserved catalog: {}", source))]
    CannotCreatePreservedCatalog { source: DatabaseError },

    #[snafu(display("id already set"))]
    IdAlreadySet,

    #[snafu(display("id not set"))]
    IdNotSet,

    #[snafu(display(
        "cannot create write buffer with config: {:?}, error: {}",
        config,
        source
    ))]
    CreatingWriteBuffer {
        config: Option<WriteBufferConnection>,
        source: DatabaseError,
    },

    #[snafu(display(
        "Invalid database state transition, expected {:?} but got {:?}",
        expected,
        actual
    ))]
    InvalidDatabaseStateTransition {
        actual: DatabaseStateCode,
        expected: DatabaseStateCode,
    },

    #[snafu(display("server is shutting down"))]
    ServerShuttingDown,
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

const JOB_HISTORY_SIZE: usize = 1000;

/// The global job registry
#[derive(Debug)]
pub struct JobRegistry {
    inner: Mutex<TaskRegistryWithHistory<Job>>,
}

impl Default for JobRegistry {
    fn default() -> Self {
        Self {
            inner: Mutex::new(TaskRegistryWithHistory::new(JOB_HISTORY_SIZE)),
        }
    }
}

impl JobRegistry {
    pub fn new() -> Self {
        Default::default()
    }

    pub fn register(&self, job: Job) -> (TaskTracker<Job>, TaskRegistration) {
        self.inner.lock().register(job)
    }

    /// Returns a list of recent Jobs, including some that are no
    /// longer running
    pub fn tracked(&self) -> Vec<TaskTracker<Job>> {
        self.inner.lock().tracked()
    }
}

/// Used to configure a server instance
#[derive(Debug)]
pub struct ServerConfig {
    // number of executor worker threads. If not specified, defaults
    // to number of cores on the system.
    num_worker_threads: Option<usize>,

    /// The `ObjectStore` instance to use for persistence
    object_store: Arc<ObjectStore>,

    metric_registry: Arc<MetricRegistry>,

    remote_template: Option<RemoteTemplate>,

    wipe_catalog_on_error: bool,
}

impl ServerConfig {
    /// Create a new config using the specified store.
    pub fn new(
        object_store: Arc<ObjectStore>,
        metric_registry: Arc<MetricRegistry>,
        remote_template: Option<RemoteTemplate>,
    ) -> Self {
        Self {
            num_worker_threads: None,
            object_store,
            metric_registry,
            remote_template,
            wipe_catalog_on_error: true,
        }
    }

    /// Use `num` worker threads for running queries
    pub fn with_num_worker_threads(mut self, num: usize) -> Self {
        self.num_worker_threads = Some(num);
        self
    }

    /// return a reference to the object store in this configuration
    pub fn store(&self) -> Arc<ObjectStore> {
        Arc::clone(&self.object_store)
    }
}

// A collection of metrics used to instrument the Server.
#[derive(Debug)]
pub struct ServerMetrics {
    /// This metric tracks all requests to the Server
    pub http_requests: metrics::RedMetric,

    /// The number of LP lines ingested
    pub ingest_lines_total: metrics::Counter,

    /// The number of LP fields ingested
    pub ingest_fields_total: metrics::Counter,

    /// The number of LP bytes ingested
    pub ingest_points_bytes_total: metrics::Counter,

    /// The number of Entry bytes ingested
    pub ingest_entries_bytes_total: metrics::Counter,
}

impl ServerMetrics {
    pub fn new(registry: Arc<metrics::MetricRegistry>) -> Self {
        // Server manages multiple domains.
        let http_domain = registry.register_domain("http");
        let ingest_domain = registry.register_domain("ingest");
        let jemalloc_domain = registry.register_domain("jemalloc");

        // This isn't really a property of the server, perhaps it should be somewhere else?
        jemalloc_domain.register_observer(None, &[], |observer: MetricObserverBuilder<'_>| {
            observer.register_gauge_u64(
                "memstats",
                Some("bytes"),
                "jemalloc memstats",
                |observer| {
                    use tikv_jemalloc_ctl::{epoch, stats};
                    epoch::advance().unwrap();

                    let active = stats::active::read().unwrap();
                    observer.observe(active as u64, &[KeyValue::new("stat", "active")]);

                    let allocated = stats::allocated::read().unwrap();
                    observer.observe(allocated as u64, &[KeyValue::new("stat", "alloc")]);

                    let metadata = stats::metadata::read().unwrap();
                    observer.observe(metadata as u64, &[KeyValue::new("stat", "metadata")]);

                    let mapped = stats::mapped::read().unwrap();
                    observer.observe(mapped as u64, &[KeyValue::new("stat", "mapped")]);

                    let resident = stats::resident::read().unwrap();
                    observer.observe(resident as u64, &[KeyValue::new("stat", "resident")]);

                    let retained = stats::retained::read().unwrap();
                    observer.observe(retained as u64, &[KeyValue::new("stat", "retained")]);
                },
            )
        });

        Self {
            http_requests: http_domain.register_red_metric(None),
            ingest_lines_total: ingest_domain.register_counter_metric(
                "points",
                None,
                "total LP points ingested",
            ),
            ingest_fields_total: ingest_domain.register_counter_metric(
                "fields",
                None,
                "total LP field values ingested",
            ),
            ingest_points_bytes_total: ingest_domain.register_counter_metric(
                "points",
                Some("bytes"),
                "total LP points bytes ingested",
            ),
            ingest_entries_bytes_total: ingest_domain.register_counter_metric(
                "entries",
                Some("bytes"),
                "total Entry bytes ingested",
            ),
        }
    }
}

/// `Server` is the container struct for how servers store data internally, as
/// well as how they communicate with other servers. Each server will have one
/// of these structs, which keeps track of all replication and query rules.
#[derive(Debug)]
pub struct Server<M: ConnectionManager> {
    connection_manager: Arc<M>,
    pub store: Arc<ObjectStore>,
    exec: Arc<Executor>,
    jobs: Arc<JobRegistry>,
    pub metrics: Arc<ServerMetrics>,

    /// The metrics registry associated with the server. This is needed not for
    /// recording telemetry, but because the server hosts the /metric endpoint
    /// and populates the endpoint with this data.
    pub registry: Arc<metrics::MetricRegistry>,

    /// The state machine for server startup
    stage: Arc<RwLock<ServerStage>>,
}

/// The stage of the server in the startup process
///
/// The progression is linear Startup -> InitReady -> Initializing -> Initialized
/// with the sole exception that on failure Initializing -> InitReady
///
/// Errors encountered on server init will be retried, however, errors encountered
/// during database init will require operator intervention
///
/// These errors are exposed via `Server::error_generic` and `Server::error_database` respectively
///
/// They do not impact the state machine's progression, but instead are exposed to the
/// gRPC management API to allow an operator to assess the state of the system
#[derive(Debug)]
enum ServerStage {
    /// Server has started but doesn't have a server id yet
    Startup {
        remote_template: Option<RemoteTemplate>,
        wipe_catalog_on_error: bool,
    },

    /// Server can be initialized
    InitReady {
        wipe_catalog_on_error: bool,
        config: Arc<Config>,
        last_error: Option<Arc<init::Error>>,
    },

    /// Server has a server id, has started loading
    Initializing {
        wipe_catalog_on_error: bool,
        config: Arc<Config>,
        last_error: Option<Arc<init::Error>>,
    },

    /// Server has finish initializing, possibly with errors
    Initialized {
        config: Arc<Config>,
        /// Errors that occurred during some DB init.
        database_errors: HashMap<String, Arc<init::Error>>,
    },
}

#[derive(Debug)]
pub enum UpdateError<E> {
    Update(Error),
    Closure(E),
}

impl<E> From<Error> for UpdateError<E> {
    fn from(e: Error) -> Self {
        Self::Update(e)
    }
}

impl<M> Server<M>
where
    M: ConnectionManager + Send + Sync,
{
    pub fn new(connection_manager: M, config: ServerConfig) -> Self {
        let jobs = Arc::new(JobRegistry::new());

        let ServerConfig {
            num_worker_threads,
            object_store,
            // to test the metrics provide a different registry to the `ServerConfig`.
            metric_registry,
            remote_template,
            wipe_catalog_on_error,
        } = config;

        let num_worker_threads = num_worker_threads.unwrap_or_else(num_cpus::get);
        let exec = Arc::new(Executor::new(num_worker_threads));

        Self {
            store: object_store,
            connection_manager: Arc::new(connection_manager),
            exec,
            jobs,
            metrics: Arc::new(ServerMetrics::new(Arc::clone(&metric_registry))),
            registry: Arc::clone(&metric_registry),
            stage: Arc::new(RwLock::new(ServerStage::Startup {
                remote_template,
                wipe_catalog_on_error,
            })),
        }
    }

    /// sets the id of the server, which is used for replication and the base
    /// path in object storage.
    ///
    /// A valid server ID Must be non-zero.
    pub fn set_id(&self, id: ServerId) -> Result<()> {
        let mut stage = self.stage.write();
        match &mut *stage {
            ServerStage::Startup {
                remote_template,
                wipe_catalog_on_error,
            } => {
                let remote_template = remote_template.take();

                *stage = ServerStage::InitReady {
                    wipe_catalog_on_error: *wipe_catalog_on_error,
                    config: Arc::new(Config::new(
                        Arc::clone(&self.jobs),
                        Arc::clone(&self.store),
                        Arc::clone(&self.exec),
                        id,
                        Arc::clone(&self.registry),
                        remote_template,
                    )),
                    last_error: None,
                };
                Ok(())
            }
            _ => Err(Error::IdAlreadySet),
        }
    }

    /// Check if server is loaded. Databases are loaded and server is ready to read/write.
    pub fn initialized(&self) -> bool {
        matches!(&*self.stage.read(), ServerStage::Initialized { .. })
    }

    /// Require that server is loaded. Databases are loaded and server is ready to read/write.
    fn require_initialized(&self) -> Result<Arc<Config>> {
        match &*self.stage.read() {
            ServerStage::Startup { .. } => Err(Error::IdNotSet),
            ServerStage::InitReady { config, .. } | ServerStage::Initializing { config, .. } => {
                Err(Error::ServerNotInitialized {
                    server_id: config.server_id(),
                })
            }
            ServerStage::Initialized { config, .. } => Ok(Arc::clone(&config)),
        }
    }

    /// Returns the config for this server if server id has been set
    fn config(&self) -> Result<Arc<Config>> {
        let stage = self.stage.read();
        match &*stage {
            ServerStage::Startup { .. } => Err(Error::IdNotSet),
            ServerStage::InitReady { config, .. }
            | ServerStage::Initializing { config, .. }
            | ServerStage::Initialized { config, .. } => Ok(Arc::clone(&config)),
        }
    }

    /// Returns the server id for this server if set
    pub fn server_id(&self) -> Option<ServerId> {
        self.config().map(|x| x.server_id()).ok()
    }

    /// Error occurred during generic server init (e.g. listing store content).
    pub fn error_generic(&self) -> Option<Arc<crate::init::Error>> {
        let stage = self.stage.read();
        match &*stage {
            ServerStage::InitReady { last_error, .. } => last_error.clone(),
            ServerStage::Initializing { last_error, .. } => last_error.clone(),
            _ => None,
        }
    }

    /// List all databases with errors in sorted order.
    pub fn databases_with_errors(&self) -> Vec<String> {
        let stage = self.stage.read();
        match &*stage {
            ServerStage::Initialized {
                database_errors, ..
            } => database_errors.keys().cloned().collect(),
            _ => Default::default(),
        }
    }

    /// Error that occurred during initialization of a specific database.
    pub fn error_database(&self, db_name: &str) -> Option<Arc<crate::init::Error>> {
        let stage = self.stage.read();
        match &*stage {
            ServerStage::Initialized {
                database_errors, ..
            } => database_errors.get(db_name).cloned(),
            _ => None,
        }
    }

    /// Current database init state.
    pub fn database_state(&self, name: &str) -> Option<DatabaseStateCode> {
        let db_name = DatabaseName::new(name).ok()?;
        let config = self.config().ok()?;
        config.db_state(&db_name)
    }

    /// Tells the server the set of rules for a database.
    pub async fn create_database(&self, rules: DatabaseRules) -> Result<()> {
        // Return an error if this server is not yet ready
        let config = self.require_initialized()?;

        // Reserve name before expensive IO (e.g. loading the preserved catalog)
        let mut db_reservation = config.create_db(rules.name.clone())?;

        // register rules
        db_reservation.advance_rules_loaded(rules.clone())?;

        // load preserved catalog
        let (preserved_catalog, catalog, replay_plan) = create_preserved_catalog(
            rules.db_name(),
            Arc::clone(&self.store),
            config.server_id(),
            config.metrics_registry(),
        )
        .await
        .map_err(|e| Box::new(e) as _)
        .context(CannotCreatePreservedCatalog)?;

        let write_buffer = WriteBufferConfig::new(config.server_id(), &rules)
            .await
            .map_err(|e| Error::CreatingWriteBuffer {
                config: rules.write_buffer_connection.clone(),
                source: e,
            })?;
        info!(write_buffer_enabled=?write_buffer.is_some(), db_name=rules.db_name(), "write buffer config");
        db_reservation.advance_replay(preserved_catalog, catalog, replay_plan, write_buffer)?;

        // no actual replay required
        db_reservation.advance_init()?;

        // ready to commit
        self.persist_database_rules(rules.clone()).await?;
        db_reservation.commit();

        Ok(())
    }

    pub async fn persist_database_rules<'a>(&self, rules: DatabaseRules) -> Result<()> {
        let config = self.config()?;
        let location = object_store_path_for_database_config(&config.root_path(), &rules.name);

        let mut data = BytesMut::new();
        encode_database_rules(rules, &mut data).context(ErrorSerializingRulesProtobuf)?;

        let len = data.len();

        let stream_data = std::io::Result::Ok(data.freeze());
        self.store
            .put(
                &location,
                futures::stream::once(async move { stream_data }),
                Some(len),
            )
            .await
            .context(StoreError)?;
        Ok(())
    }

    /// Loads the database configurations based on the databases in the
    /// object store. Any databases in the config already won't be
    /// replaced.
    ///
    /// This requires the serverID to be set.
    ///
    /// It will be a no-op if the configs are already loaded and the server is ready.
    pub async fn maybe_initialize_server(&self) {
        // Explicit scope to help async generator
        let (wipe_catalog_on_error, config) = {
            let state = self.stage.upgradable_read();
            match &*state {
                ServerStage::InitReady {
                    wipe_catalog_on_error,
                    config,
                    last_error,
                } => {
                    let config = Arc::clone(config);
                    let last_error = last_error.clone();
                    let wipe_catalog_on_error = *wipe_catalog_on_error;

                    // Mark the server as initializing and drop lock

                    let mut state = RwLockUpgradableReadGuard::upgrade(state);
                    *state = ServerStage::Initializing {
                        config: Arc::clone(&config),
                        wipe_catalog_on_error,
                        last_error,
                    };
                    (wipe_catalog_on_error, config)
                }
                _ => return,
            }
        };

        let init_result = init::initialize_server(Arc::clone(&config), wipe_catalog_on_error).await;
        let new_stage = match init_result {
            // Success -> move to next stage
            Ok(results) => {
                info!(server_id=%config.server_id(), "server initialized");
                ServerStage::Initialized {
                    config,
                    database_errors: results
                        .into_iter()
                        .filter_map(|(name, res)| Some((name.to_string(), Arc::new(res.err()?))))
                        .collect(),
                }
            }
            // Error -> return to InitReady
            Err(err) => {
                error!(%err, "error during server init");
                ServerStage::InitReady {
                    wipe_catalog_on_error,
                    config,
                    last_error: Some(Arc::new(err)),
                }
            }
        };

        *self.stage.write() = new_stage;
    }

    pub async fn write_pb(&self, database_batch: pb::DatabaseBatch) -> Result<()> {
        // Return an error if this server is not yet ready
        self.require_initialized()?;

        let entry = pb_to_entry(&database_batch).context(PBConversion)?;
        self.write_entry(&database_batch.database_name, entry.data().into())
            .await?;

        Ok(())
    }

    /// `write_lines` takes in raw line protocol and converts it to a collection
    /// of ShardedEntry which are then sent to other IOx servers based on
    /// the ShardConfig or sent to the local database for buffering in the
    /// WriteBuffer and/or the MutableBuffer if configured.
    ///
    /// The provided `default_time` is nanoseconds since the epoch and will be assigned
    /// to any lines that don't have a timestamp.
    pub async fn write_lines(
        &self,
        db_name: &str,
        lines: &[ParsedLine<'_>],
        default_time: i64,
    ) -> Result<()> {
        // Return an error if this server is not yet ready
        let config = self.require_initialized()?;

        let db_name = DatabaseName::new(db_name).context(InvalidDatabaseName)?;
        let db = config
            .db_initialized(&db_name)
            .context(DatabaseNotFound { db_name: &*db_name })?;

        // need to split this in two blocks because we cannot hold a lock across an async call.
        let routing_config_target = {
            let rules = db.rules();
            if let Some(RoutingRules::RoutingConfig(routing_config)) = &rules.routing_rules {
                let sharded_entries = lines_to_sharded_entries(
                    lines,
                    default_time,
                    None as Option<&ShardConfig>,
                    &*rules,
                )
                .context(LineConversion)?;
                Some((routing_config.sink.clone(), sharded_entries))
            } else {
                None
            }
        };

        if let Some((sink, sharded_entries)) = routing_config_target {
            for i in sharded_entries {
                self.write_entry_sink(&db_name, &sink, i.entry).await?;
            }
            return Ok(());
        }

        // Split lines into shards while holding a read lock on the sharding config.
        // Once the lock is released we have a vector of entries, each associated with a
        // shard id, and an Arc to the mapping between shard ids and node
        // groups. This map is atomically replaced every time the sharding
        // config is updated, hence it's safe to use after we release the shard config
        // lock.
        let (sharded_entries, shards) = {
            let rules = db.rules();

            let shard_config = rules.routing_rules.as_ref().map(|cfg| match cfg {
                RoutingRules::RoutingConfig(_) => unreachable!("routing config handled above"),
                RoutingRules::ShardConfig(shard_config) => shard_config,
            });

            let sharded_entries =
                lines_to_sharded_entries(lines, default_time, shard_config, &*rules)
                    .context(LineConversion)?;

            let shards = shard_config
                .as_ref()
                .map(|cfg| Arc::clone(&cfg.shards))
                .unwrap_or_default();

            (sharded_entries, shards)
        };

        // Write to all shards in parallel; as soon as one fails return error
        // immediately to the client and abort all other outstanding requests.
        // This can take some time, but we're no longer holding the lock to the shard
        // config.
        futures_util::future::try_join_all(
            sharded_entries
                .into_iter()
                .map(|e| self.write_sharded_entry(&db_name, &db, Arc::clone(&shards), e)),
        )
        .await?;

        Ok(())
    }

    async fn write_sharded_entry(
        &self,
        db_name: &str,
        db: &Db,
        shards: Arc<HashMap<u32, Sink>>,
        sharded_entry: ShardedEntry,
    ) -> Result<()> {
        match sharded_entry.shard_id {
            Some(shard_id) => {
                let sink = shards.get(&shard_id).context(ShardNotFound { shard_id })?;
                self.write_entry_sink(db_name, sink, sharded_entry.entry)
                    .await?
            }
            None => {
                self.write_entry_local(&db_name, db, sharded_entry.entry)
                    .await?
            }
        }
        Ok(())
    }

    async fn write_entry_sink(&self, db_name: &str, sink: &Sink, entry: Entry) -> Result<()> {
        match sink {
            Sink::Iox(node_group) => {
                self.write_entry_downstream(db_name, node_group, entry)
                    .await
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
        let config = self.config()?;

        let addrs: Vec<_> = node_group
            .iter()
            .filter_map(|&node| config.resolve_remote(node))
            .collect();
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
                        .write_entry(&db_name, entry)
                        .await
                        .context(RemoteError)
                }
            };
        }
        NoRemoteReachable { errors }.fail()
    }

    pub async fn write_entry(&self, db_name: &str, entry_bytes: Vec<u8>) -> Result<()> {
        // Return an error if this server is not yet ready
        let config = self.require_initialized()?;

        let db_name = DatabaseName::new(db_name).context(InvalidDatabaseName)?;
        let db = config
            .db_initialized(&db_name)
            .context(DatabaseNotFound { db_name: &*db_name })?;

        let entry = entry_bytes.try_into().context(DecodingEntry)?;
        self.write_entry_local(&db_name, &db, entry).await
    }

    pub async fn write_entry_local(&self, db_name: &str, db: &Db, entry: Entry) -> Result<()> {
        let bytes = entry.data().len() as u64;
        db.store_entry(entry).await.map_err(|e| {
            self.metrics.ingest_entries_bytes_total.add_with_labels(
                bytes,
                &[
                    metrics::KeyValue::new("status", "error"),
                    metrics::KeyValue::new("db_name", db_name.to_string()),
                ],
            );
            match e {
                db::Error::HardLimitReached {} => Error::HardLimitReached {},
                db::Error::WritingOnlyAllowedThroughWriteBuffer {} => {
                    Error::WritingOnlyAllowedThroughWriteBuffer {
                        db_name: db_name.into(),
                    }
                }
                db::Error::WriteBufferWritingError { .. } => {
                    Error::WriteBuffer { source: e, bytes }
                }
                _ => Error::UnknownDatabaseError {
                    source: Box::new(e),
                },
            }
        })?;

        self.metrics.ingest_entries_bytes_total.add_with_labels(
            bytes,
            &[
                metrics::KeyValue::new("status", "ok"),
                metrics::KeyValue::new("db_name", db_name.to_string()),
            ],
        );

        Ok(())
    }

    pub fn db(&self, name: &DatabaseName<'_>) -> Option<Arc<Db>> {
        self.config().ok()?.db_initialized(name)
    }

    pub fn db_rules(&self, name: &DatabaseName<'_>) -> Option<Arc<DatabaseRules>> {
        self.db(name).map(|d| d.rules())
    }

    // Update database rules and save on success.
    pub async fn update_db_rules<F, E>(
        &self,
        db_name: &DatabaseName<'static>,
        update: F,
    ) -> std::result::Result<Arc<DatabaseRules>, UpdateError<E>>
    where
        F: FnOnce(DatabaseRules) -> Result<DatabaseRules, E> + Send,
    {
        let config = self.config()?;
        let rules = config
            .update_db_rules(db_name, update)
            .map_err(|e| match e {
                crate::config::UpdateError::Closure(e) => UpdateError::Closure(e),
                crate::config::UpdateError::Update(e) => UpdateError::Update(e),
            })?;

        // TODO: Move into DB (#2053)
        self.persist_database_rules(rules.as_ref().clone()).await?;
        Ok(rules)
    }

    pub fn remotes_sorted(&self) -> Result<Vec<(ServerId, String)>> {
        // TODO: Should these be on ConnectionManager and not Config
        let config = self.config()?;
        Ok(config.remotes_sorted())
    }

    pub fn update_remote(&self, id: ServerId, addr: GRpcConnectionString) -> Result<()> {
        // TODO: Should these be on ConnectionManager and not Config
        let config = self.config()?;
        config.update_remote(id, addr);
        Ok(())
    }

    pub fn delete_remote(&self, id: ServerId) -> Result<Option<GRpcConnectionString>> {
        // TODO: Should these be on ConnectionManager and not Config
        let config = self.config()?;
        Ok(config.delete_remote(id))
    }

    pub fn spawn_dummy_job(&self, nanos: Vec<u64>) -> TaskTracker<Job> {
        let (tracker, registration) = self.jobs.register(Job::Dummy {
            nanos: nanos.clone(),
        });

        for duration in nanos {
            tokio::spawn(
                async move {
                    tokio::time::sleep(tokio::time::Duration::from_nanos(duration)).await;
                    Ok::<_, Infallible>(())
                }
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
        table_name: impl Into<String>,
        partition_key: impl Into<String>,
        chunk_id: u32,
    ) -> Result<TaskTracker<Job>> {
        let config = self.require_initialized()?;

        let db_name = db_name.to_string();
        let name = DatabaseName::new(&db_name).context(InvalidDatabaseName)?;

        let partition_key = partition_key.into();
        let table_name = table_name.into();

        let db = config
            .db_initialized(&name)
            .context(DatabaseNotFound { db_name: &db_name })?;

        let chunk = db
            .lockable_chunk(&table_name, &partition_key, chunk_id)
            .context(ChunkNotFound)?;

        let guard = chunk.write();

        LockableChunk::move_to_read_buffer(guard).map_err(|e| Error::UnknownDatabaseError {
            source: Box::new(e),
        })
    }

    /// Wipe preserved catalog of specific DB.
    ///
    /// The DB must not yet exist within this server for this to work! This is done to prevent race conditions between
    /// DB jobs and this command.
    pub fn wipe_preserved_catalog(
        &self,
        db_name: &DatabaseName<'static>,
    ) -> Result<TaskTracker<Job>> {
        // Can only wipe catalog of database that failed to initialize
        let config = match &*self.stage.read() {
            ServerStage::Initialized {
                config,
                database_errors,
            } => {
                if config.db_initialized(db_name).is_some() {
                    return Err(Error::DatabaseAlreadyExists {
                        db_name: db_name.to_string(),
                    });
                }

                if !database_errors.contains_key(db_name.as_str()) {
                    // TODO: Should this be an error? Some end-to-end tests assume it is non-fatal
                    warn!(%db_name, "wiping database not present at startup");
                }
                Arc::clone(config)
            }
            ServerStage::Startup { .. } => return Err(Error::IdNotSet),
            ServerStage::Initializing { config, .. } | ServerStage::InitReady { config, .. } => {
                return Err(Error::ServerNotInitialized {
                    server_id: config.server_id(),
                })
            }
        };

        let (tracker, registration) = self.jobs.register(Job::WipePreservedCatalog {
            db_name: Arc::from(db_name.as_str()),
        });

        let state = Arc::clone(&self.stage);
        let db_name = db_name.clone();

        let task = async move {
            let result = init::wipe_preserved_catalog_and_maybe_recover(config, &db_name).await;

            match &mut *state.write() {
                ServerStage::Initialized {
                    database_errors, ..
                } => match result {
                    Ok(_) => {
                        info!(%db_name, "wiped preserved catalog of registered database and recovered");
                        database_errors.remove(db_name.as_str());
                        Ok(())
                    }
                    Err(e) => {
                        warn!(%db_name, %e, "wiped preserved catalog of registered database but still cannot recover");
                        let e = Arc::new(e);
                        database_errors.insert(db_name.to_string(), Arc::clone(&e));
                        Err(e)
                    }
                },
                _ => unreachable!("server cannot become uninitialized"),
            }
        };
        tokio::spawn(task.track(registration));

        Ok(tracker)
    }

    /// Returns a list of all jobs tracked by this server
    pub fn tracked_jobs(&self) -> Vec<TaskTracker<Job>> {
        self.jobs.inner.lock().tracked()
    }

    /// Returns a specific job tracked by this server
    pub fn get_job(&self, id: TaskId) -> Option<TaskTracker<Job>> {
        self.jobs.inner.lock().get(id)
    }

    /// Background worker function for the server
    pub async fn background_worker(&self, shutdown: tokio_util::sync::CancellationToken) {
        info!("started background worker");

        let mut interval = tokio::time::interval(tokio::time::Duration::from_secs(1));

        while !shutdown.is_cancelled() {
            self.maybe_initialize_server().await;
            self.jobs.inner.lock().reclaim();

            tokio::select! {
                _ = interval.tick() => {},
                _ = shutdown.cancelled() => break
            }
        }

        info!("shutting down background workers");
        if let Ok(config) = self.config() {
            config.drain().await;
        }

        info!("draining tracker registry");

        // Wait for any outstanding jobs to finish - frontend shutdown should be
        // sequenced before shutting down the background workers and so there
        // shouldn't be any
        while self.jobs.inner.lock().tracked_len() != 0 {
            self.jobs.inner.lock().reclaim();

            interval.tick().await;
        }

        info!("drained tracker registry");
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
        self.config()
            .map(|config| {
                config
                    .db_names_sorted()
                    .iter()
                    .map(ToString::to_string)
                    .collect()
            })
            .unwrap_or_default()
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
                self.create_database(DatabaseRules::new(db_name.clone()))
                    .await?;
                self.db(&db_name).expect("db not inserted")
            }
        };

        Ok(db)
    }

    /// Return a handle to the query executor
    fn executor(&self) -> Arc<Executor> {
        Arc::clone(&self.exec)
    }
}

type RemoteServerError = Box<dyn std::error::Error + Send + Sync + 'static>;

#[derive(Debug, Snafu)]
pub enum ConnectionManagerError {
    #[snafu(display("cannot connect to remote: {}", source))]
    RemoteServerConnectError { source: RemoteServerError },
    #[snafu(display("cannot write to remote: {}", source))]
    RemoteServerWriteError { source: write::WriteError },
}

/// The `Server` will ask the `ConnectionManager` for connections to a specific
/// remote server. These connections can be used to communicate with other
/// servers. This is implemented as a trait for dependency injection in testing.
#[async_trait]
pub trait ConnectionManager {
    type RemoteServer: RemoteServer + Send + Sync + 'static;

    async fn remote_server(
        &self,
        connect: &str,
    ) -> Result<Arc<Self::RemoteServer>, ConnectionManagerError>;
}

/// The `RemoteServer` represents the API for replicating, subscribing, and
/// querying other servers.
#[async_trait]
pub trait RemoteServer {
    /// Sends an Entry to the remote server. An IOx server acting as a
    /// router/sharder will call this method to send entries to remotes.
    async fn write_entry(&self, db: &str, entry: Entry) -> Result<(), ConnectionManagerError>;
}

/// The connection manager maps a host identifier to a remote server.
#[derive(Debug)]
pub struct ConnectionManagerImpl {
    cache: LoadingCache<String, Arc<RemoteServerImpl>, CacheFillError>,
}

// Error must be Clone because LoadingCache requires so.
#[derive(Debug, Snafu, Clone)]
pub enum CacheFillError {
    #[snafu(display("gRPC error: {}", source))]
    GrpcError {
        source: Arc<dyn std::error::Error + Send + Sync + 'static>,
    },
}

impl ConnectionManagerImpl {
    pub fn new() -> Self {
        let (cache, _) = LoadingCache::new(Self::cached_remote_server);
        Self { cache }
    }

    async fn cached_remote_server(
        connect: String,
    ) -> Result<Arc<RemoteServerImpl>, CacheFillError> {
        let connection = Builder::default()
            .build(&connect)
            .await
            .map_err(|e| Arc::new(e) as _)
            .context(GrpcError)?;
        let client = write::Client::new(connection);
        Ok(Arc::new(RemoteServerImpl { client }))
    }
}

impl Default for ConnectionManagerImpl {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl ConnectionManager for ConnectionManagerImpl {
    type RemoteServer = RemoteServerImpl;

    async fn remote_server(
        &self,
        connect: &str,
    ) -> Result<Arc<Self::RemoteServer>, ConnectionManagerError> {
        let ret = self
            .cache
            .get_with_meta(connect.to_string())
            .await
            .map_err(|e| Box::new(e) as _)
            .context(RemoteServerConnectError)?;
        debug!(was_cached=%ret.cached, %connect, "getting remote connection");
        Ok(ret.result)
    }
}

/// An implementation for communicating with other IOx servers. This should
/// be moved into and implemented in an influxdb_iox_client create at a later
/// date.
#[derive(Debug)]
pub struct RemoteServerImpl {
    client: write::Client,
}

#[async_trait]
impl RemoteServer for RemoteServerImpl {
    /// Sends an Entry to the remote server. An IOx server acting as a
    /// router/sharder will call this method to send entries to remotes.
    async fn write_entry(&self, db_name: &str, entry: Entry) -> Result<(), ConnectionManagerError> {
        self.client
            .clone() // cheap, see https://docs.rs/tonic/0.4.2/tonic/client/index.html#concurrent-usage
            .write_entry(db_name, entry)
            .await
            .context(RemoteServerWriteError)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::utils::TestDb;
    use arrow::record_batch::RecordBatch;
    use arrow_util::assert_batches_eq;
    use async_trait::async_trait;
    use bytes::Bytes;
    use data_types::chunk_metadata::ChunkAddr;
    use data_types::database_rules::{
        HashRing, LifecycleRules, PartitionTemplate, ShardConfig, TemplatePart, NO_SHARD_CONFIG,
    };
    use entry::test_helpers::lp_to_entry;
    use futures::TryStreamExt;
    use generated_types::database_rules::decode_database_rules;
    use influxdb_line_protocol::parse_lines;
    use metrics::MetricRegistry;
    use object_store::path::ObjectStorePath;
    use parquet_file::catalog::{test_helpers::TestCatalogState, PreservedCatalog};
    use query::{exec::ExecutorType, frontend::sql::SqlQueryPlanner, QueryDatabase};
    use snafu::Snafu;
    use std::{
        collections::BTreeMap,
        convert::TryFrom,
        sync::{
            atomic::{AtomicBool, Ordering},
            Arc,
        },
        time::{Duration, Instant},
    };
    use tempfile::TempDir;
    use tokio::task::JoinHandle;
    use tokio_util::sync::CancellationToken;
    use write_buffer::mock::MockBufferForWritingThatAlwaysErrors;

    const ARBITRARY_DEFAULT_TIME: i64 = 456;

    // TODO: perhaps switch to a builder pattern.
    fn config_with_metric_registry_and_store(
        object_store: ObjectStore,
    ) -> (metrics::TestMetricRegistry, ServerConfig) {
        let registry = Arc::new(metrics::MetricRegistry::new());
        let test_registry = metrics::TestMetricRegistry::new(Arc::clone(&registry));
        (
            test_registry,
            ServerConfig::new(Arc::new(object_store), registry, None).with_num_worker_threads(1),
        )
    }

    fn config_with_metric_registry() -> (metrics::TestMetricRegistry, ServerConfig) {
        config_with_metric_registry_and_store(ObjectStore::new_in_memory())
    }

    fn config() -> ServerConfig {
        config_with_metric_registry().1
    }

    fn config_with_store(object_store: ObjectStore) -> ServerConfig {
        config_with_metric_registry_and_store(object_store).1
    }

    #[tokio::test]
    async fn server_api_calls_return_error_with_no_id_set() {
        let manager = TestConnectionManager::new();
        let server = Server::new(manager, config());

        let resp = server.config().unwrap_err();
        assert!(matches!(resp, Error::IdNotSet));

        let lines = parsed_lines("cpu foo=1 10");
        let resp = server
            .write_lines("foo", &lines, ARBITRARY_DEFAULT_TIME)
            .await
            .unwrap_err();
        assert!(matches!(resp, Error::IdNotSet));
    }

    #[tokio::test]
    async fn create_database_persists_rules() {
        let manager = TestConnectionManager::new();
        let config = config();
        let store = config.store();
        let server = Server::new(manager, config);
        server.set_id(ServerId::try_from(1).unwrap()).unwrap();
        server.maybe_initialize_server().await;

        let name = DatabaseName::new("bananas").unwrap();

        let rules = DatabaseRules {
            name: name.clone(),
            partition_template: PartitionTemplate {
                parts: vec![TemplatePart::TimeFormat("YYYY-MM".to_string())],
            },
            lifecycle_rules: LifecycleRules {
                catalog_transactions_until_checkpoint: 13.try_into().unwrap(),
                ..Default::default()
            },
            routing_rules: None,
            worker_cleanup_avg_sleep: Duration::from_secs(2),
            write_buffer_connection: None,
        };

        // Create a database
        server
            .create_database(rules.clone())
            .await
            .expect("failed to create database");

        let mut rules_path = server.store.new_path();
        rules_path.push_all_dirs(&["1", name.as_str()]);
        rules_path.set_file_name("rules.pb");

        let read_data = server
            .store
            .get(&rules_path)
            .await
            .unwrap()
            .map_ok(|b| bytes::BytesMut::from(&b[..]))
            .try_concat()
            .await
            .unwrap()
            .freeze();

        let read_rules = decode_database_rules(read_data).unwrap();

        assert_eq!(rules, read_rules);

        let db2 = DatabaseName::new("db_awesome").unwrap();
        server
            .create_database(DatabaseRules::new(db2.clone()))
            .await
            .expect("failed to create 2nd db");

        store.list_with_delimiter(&store.new_path()).await.unwrap();

        let manager = TestConnectionManager::new();
        let config2 = ServerConfig::new(store, Arc::new(MetricRegistry::new()), Option::None)
            .with_num_worker_threads(1);
        let server2 = Server::new(manager, config2);
        server2.set_id(ServerId::try_from(1).unwrap()).unwrap();
        server2.maybe_initialize_server().await;

        let _ = server2.db(&db2).unwrap();
        let _ = server2.db(&name).unwrap();
    }

    #[tokio::test]
    async fn duplicate_database_name_rejected() {
        // Covers #643

        let manager = TestConnectionManager::new();
        let server = Server::new(manager, config());
        server.set_id(ServerId::try_from(1).unwrap()).unwrap();
        server.maybe_initialize_server().await;

        let name = DatabaseName::new("bananas").unwrap();

        // Create a database
        server
            .create_database(DatabaseRules::new(name.clone()))
            .await
            .expect("failed to create database");

        // Then try and create another with the same name
        let got = server
            .create_database(DatabaseRules::new(name.clone()))
            .await
            .unwrap_err();

        if !matches!(got, Error::DatabaseAlreadyExists { .. }) {
            panic!("expected already exists error");
        }
    }

    async fn create_simple_database<M>(
        server: &Server<M>,
        name: impl Into<String> + Send,
    ) -> Result<()>
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
        server.create_database(rules.clone()).await
    }

    #[tokio::test]
    async fn load_databases() {
        let temp_dir = TempDir::new().unwrap();

        let store = ObjectStore::new_file(temp_dir.path());
        let manager = TestConnectionManager::new();
        let config = config_with_store(store);
        let server = Server::new(manager, config);
        server.set_id(ServerId::try_from(1).unwrap()).unwrap();
        server.maybe_initialize_server().await;
        create_simple_database(&server, "bananas")
            .await
            .expect("failed to create database");

        let mut rules_path = server.store.new_path();
        rules_path.push_all_dirs(&["1", "bananas"]);
        rules_path.set_file_name("rules.pb");

        std::mem::drop(server);

        let store = ObjectStore::new_file(temp_dir.path());
        let manager = TestConnectionManager::new();
        let config = config_with_store(store);
        let server = Server::new(manager, config);
        server.set_id(ServerId::try_from(1).unwrap()).unwrap();
        server.maybe_initialize_server().await;

        create_simple_database(&server, "apples")
            .await
            .expect("failed to create database");

        assert_eq!(server.db_names_sorted(), vec!["apples", "bananas"]);

        std::mem::drop(server);

        let store = ObjectStore::new_file(temp_dir.path());
        store
            .delete(&rules_path)
            .await
            .expect("cannot delete rules file");

        let manager = TestConnectionManager::new();
        let config = config_with_store(store);
        let server = Server::new(manager, config);
        server.set_id(ServerId::try_from(1).unwrap()).unwrap();
        server.maybe_initialize_server().await;

        assert_eq!(server.db_names_sorted(), vec!["apples"]);
    }

    #[tokio::test]
    async fn db_names_sorted() {
        let manager = TestConnectionManager::new();
        let server = Server::new(manager, config());
        server.set_id(ServerId::try_from(1).unwrap()).unwrap();
        server.maybe_initialize_server().await;

        let names = vec!["bar", "baz"];

        for name in &names {
            let name = DatabaseName::new(name.to_string()).unwrap();
            server
                .create_database(DatabaseRules::new(name))
                .await
                .expect("failed to create database");
        }

        let db_names_sorted = server.db_names_sorted();
        assert_eq!(names, db_names_sorted);
    }

    #[tokio::test]
    async fn writes_local() {
        let manager = TestConnectionManager::new();
        let server = Server::new(manager, config());
        server.set_id(ServerId::try_from(1).unwrap()).unwrap();
        server.maybe_initialize_server().await;

        let name = DatabaseName::new("foo".to_string()).unwrap();
        server
            .create_database(DatabaseRules::new(name))
            .await
            .unwrap();

        let line = "cpu bar=1 10";
        let lines: Vec<_> = parse_lines(line).map(|l| l.unwrap()).collect();
        server
            .write_lines("foo", &lines, ARBITRARY_DEFAULT_TIME)
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
        let (metric_registry, config) = config_with_metric_registry();
        let manager = TestConnectionManager::new();
        let server = Server::new(manager, config);
        server.set_id(ServerId::try_from(1).unwrap()).unwrap();
        server.maybe_initialize_server().await;

        let name = DatabaseName::new("foo".to_string()).unwrap();
        server
            .create_database(DatabaseRules::new(name))
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
            .write_entry("foo", entry.data().into())
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

        metric_registry
            .has_metric_family("ingest_entries_bytes_total")
            .with_labels(&[("status", "ok"), ("db_name", "foo")])
            .counter()
            .eq(240.0)
            .unwrap();
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

        let server = Server::new(manager, config());
        server.set_id(ServerId::try_from(1).unwrap()).unwrap();
        server.maybe_initialize_server().await;

        let db_name = DatabaseName::new("foo").unwrap();
        server
            .create_database(DatabaseRules::new(db_name.clone()))
            .await
            .unwrap();

        let remote_ids = vec![bad_remote_id, good_remote_id_1, good_remote_id_2];
        let db = server.db(&db_name).unwrap();
        db.update_rules(|mut rules| {
            let shard_config = ShardConfig {
                hash_ring: Some(HashRing {
                    shards: vec![TEST_SHARD_ID].into(),
                    ..Default::default()
                }),
                shards: Arc::new(
                    vec![(TEST_SHARD_ID, Sink::Iox(remote_ids.clone()))]
                        .into_iter()
                        .collect(),
                ),
                ..Default::default()
            };
            rules.routing_rules = Some(RoutingRules::ShardConfig(shard_config));
            Ok::<_, Infallible>(rules)
        })
        .unwrap();

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
        server
            .update_remote(bad_remote_id, BAD_REMOTE_ADDR.into())
            .unwrap();
        let err = server
            .write_lines(&db_name, &lines, ARBITRARY_DEFAULT_TIME)
            .await
            .unwrap_err();
        assert!(matches!(
            err,
            Error::NoRemoteReachable { errors } if matches!(
                errors[BAD_REMOTE_ADDR],
                ConnectionManagerError::RemoteServerConnectError {..}
            )
        ));
        assert!(!written_1.load(Ordering::Relaxed));
        assert!(!written_2.load(Ordering::Relaxed));

        // We configure the address for the other remote, this time connection will succeed
        // despite the bad remote failing to connect.
        server
            .update_remote(good_remote_id_1, GOOD_REMOTE_ADDR_1.into())
            .unwrap();
        server
            .update_remote(good_remote_id_2, GOOD_REMOTE_ADDR_2.into())
            .unwrap();

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
        let manager = TestConnectionManager::new();
        let server = Arc::new(Server::new(manager, config()));

        let cancel_token = CancellationToken::new();
        let background_handle = spawn_worker(Arc::clone(&server), cancel_token.clone());

        server.set_id(ServerId::try_from(1).unwrap()).unwrap();
        server.maybe_initialize_server().await;

        let db_name = DatabaseName::new("foo").unwrap();
        server
            .create_database(DatabaseRules::new(db_name.clone()))
            .await
            .unwrap();

        let line = "cpu bar=1 10";
        let lines: Vec<_> = parse_lines(line).map(|l| l.unwrap()).collect();
        server
            .write_lines(&db_name, &lines, ARBITRARY_DEFAULT_TIME)
            .await
            .unwrap();

        // start the close (note this is not an async)
        let chunk_addr = ChunkAddr {
            db_name: Arc::from(db_name.as_str()),
            table_name: Arc::from("cpu"),
            partition_key: Arc::from(""),
            chunk_id: 0,
        };
        let tracker = server
            .close_chunk(
                db_name,
                chunk_addr.table_name.as_ref(),
                chunk_addr.partition_key.as_ref(),
                chunk_addr.chunk_id,
            )
            .unwrap();

        let metadata = tracker.metadata();
        let expected_metadata = Job::CompactChunk {
            chunk: chunk_addr.clone(),
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

        let expected = vec!["ReadBuffer 0"];

        assert_eq!(
            expected, actual,
            "expected:\n{:#?}\n\nactual:{:#?}\n\n",
            expected, actual
        );

        // ensure that we don't leave the server instance hanging around
        cancel_token.cancel();
        let _ = background_handle.await;
    }

    #[tokio::test]
    async fn background_task_cleans_jobs() {
        let manager = TestConnectionManager::new();
        let server = Arc::new(Server::new(manager, config()));

        let cancel_token = CancellationToken::new();
        let background_handle = spawn_worker(Arc::clone(&server), cancel_token.clone());

        let wait_nanos = 1000;
        let job = server.spawn_dummy_job(vec![wait_nanos]);

        // Note: this will hang forever if the background task has not been started
        job.join().await;

        assert!(job.is_complete());

        // ensure that we don't leave the server instance hanging around
        cancel_token.cancel();
        let _ = background_handle.await;
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
        type RemoteServer = TestRemoteServer;

        async fn remote_server(
            &self,
            id: &str,
        ) -> Result<Arc<TestRemoteServer>, ConnectionManagerError> {
            #[derive(Debug, Snafu)]
            enum TestRemoteError {
                #[snafu(display("remote not found"))]
                NotFound,
            }
            Ok(Arc::clone(self.remotes.get(id).ok_or_else(|| {
                ConnectionManagerError::RemoteServerConnectError {
                    source: Box::new(TestRemoteError::NotFound),
                }
            })?))
        }
    }

    #[derive(Debug)]
    struct TestRemoteServer {
        written: Arc<AtomicBool>,
    }

    #[async_trait]
    impl<'a> RemoteServer for TestRemoteServer {
        async fn write_entry(
            &self,
            _db: &str,
            _entry: Entry,
        ) -> Result<(), ConnectionManagerError> {
            self.written.store(true, Ordering::Relaxed);
            Ok(())
        }
    }

    fn parsed_lines(lp: &str) -> Vec<ParsedLine<'_>> {
        parse_lines(lp).map(|l| l.unwrap()).collect()
    }

    fn spawn_worker<M>(server: Arc<Server<M>>, token: CancellationToken) -> JoinHandle<()>
    where
        M: ConnectionManager + Send + Sync + 'static,
    {
        tokio::task::spawn(async move { server.background_worker(token).await })
    }

    #[tokio::test]
    async fn hard_buffer_limit() {
        let manager = TestConnectionManager::new();
        let server = Server::new(manager, config());
        server.set_id(ServerId::try_from(1).unwrap()).unwrap();
        server.maybe_initialize_server().await;

        let name = DatabaseName::new("foo".to_string()).unwrap();
        server
            .create_database(DatabaseRules::new(name))
            .await
            .unwrap();

        let db_name = DatabaseName::new("foo").unwrap();
        let db = server.db(&db_name).unwrap();
        let rules = db
            .update_rules(|mut rules| {
                rules.lifecycle_rules.buffer_size_hard =
                    Some(std::num::NonZeroUsize::new(10).unwrap());
                Ok::<_, Infallible>(rules)
            })
            .unwrap();

        // inserting first line does not trigger hard buffer limit
        let line_1 = "cpu bar=1 10";
        let lines_1: Vec<_> = parse_lines(line_1).map(|l| l.unwrap()).collect();
        let sharded_entries_1 =
            lines_to_sharded_entries(&lines_1, ARBITRARY_DEFAULT_TIME, NO_SHARD_CONFIG, &*rules)
                .expect("first sharded entries");

        let entry_1 = &sharded_entries_1[0].entry;
        server
            .write_entry("foo", entry_1.data().into())
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
        let res = server.write_entry("foo", entry_2.data().into()).await;
        assert!(matches!(res, Err(super::Error::HardLimitReached {})));
    }

    #[tokio::test]
    async fn cannot_create_db_until_server_is_initialized() {
        let temp_dir = TempDir::new().unwrap();

        let store = ObjectStore::new_file(temp_dir.path());
        let manager = TestConnectionManager::new();
        let config = config_with_store(store);
        let server = Server::new(manager, config);

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
        let manager = TestConnectionManager::new();
        let server = Arc::new(Server::new(manager, config()));

        let cancel_token = CancellationToken::new();
        let background_handle = spawn_worker(Arc::clone(&server), cancel_token.clone());

        server.set_id(ServerId::try_from(1).unwrap()).unwrap();

        let t_0 = Instant::now();
        loop {
            if server.config().is_ok() {
                break;
            }
            assert!(t_0.elapsed() < Duration::from_secs(10));
            tokio::time::sleep(Duration::from_millis(100)).await;
        }

        // ensure that we don't leave the server instance hanging around
        cancel_token.cancel();
        let _ = background_handle.await;
    }

    #[tokio::test]
    async fn init_error_generic() {
        // use an object store that will hopefully fail to read
        let store = ObjectStore::new_failing_store().unwrap();

        let manager = TestConnectionManager::new();
        let config = config_with_store(store);
        let server = Server::new(manager, config);

        server.set_id(ServerId::try_from(1).unwrap()).unwrap();
        server.maybe_initialize_server().await;
        assert!(dbg!(server.error_generic().unwrap().to_string()).starts_with("store error:"));
    }

    #[tokio::test]
    async fn init_error_database() {
        let store = ObjectStore::new_in_memory();
        let server_id = ServerId::try_from(1).unwrap();

        // Create temporary server to create single database
        let manager = TestConnectionManager::new();
        let config = config_with_store(store);
        let store = config.store();

        let server = Server::new(manager, config);
        server.set_id(server_id).unwrap();
        server.maybe_initialize_server().await;

        create_simple_database(&server, "foo")
            .await
            .expect("failed to create database");

        let config = server.require_initialized().unwrap();
        let root = config.root_path();
        config.drain().await;
        drop(server);
        drop(config);

        // tamper store
        let path = object_store_path_for_database_config(&root, &DatabaseName::new("bar").unwrap());
        let data = Bytes::from("x");
        let len = data.len();
        store
            .put(
                &path,
                futures::stream::once(async move { Ok(data) }),
                Some(len),
            )
            .await
            .unwrap();

        // start server
        let store = Arc::try_unwrap(store).unwrap();
        store.get(&path).await.unwrap();
        let manager = TestConnectionManager::new();
        let config = config_with_store(store);

        let server = Server::new(manager, config);
        server.set_id(server_id).unwrap();
        server.maybe_initialize_server().await;

        // generic error MUST NOT be set
        assert!(server.error_generic().is_none());

        // server is initialized
        assert!(server.initialized());

        // DB-specific error is set for `bar` but not for `foo`
        assert_eq!(server.databases_with_errors(), vec!["bar".to_string()]);
        assert!(dbg!(server.error_database("foo")).is_none());
        assert!(dbg!(server.error_database("bar").unwrap().to_string())
            .starts_with("error deserializing database rules from protobuf:"));

        // DB names contain all DBs
        assert_eq!(
            server.db_names_sorted(),
            vec!["bar".to_string(), "foo".to_string()]
        );

        // can only write to successfully created DBs
        let lines = parsed_lines("cpu foo=1 10");
        server
            .write_lines("foo", &lines, ARBITRARY_DEFAULT_TIME)
            .await
            .unwrap();
        let err = server
            .write_lines("bar", &lines, ARBITRARY_DEFAULT_TIME)
            .await
            .unwrap_err();
        assert_eq!(err.to_string(), "database not found: bar");

        // creating failed DBs does not work
        let err = create_simple_database(&server, "bar").await.unwrap_err();
        assert_eq!(err.to_string(), "database already exists: bar");
    }

    #[tokio::test]
    async fn wipe_preserved_catalog() {
        // have the following DBs:
        // 1. existing => cannot be wiped
        // 2. non-existing => can be wiped, will not exist afterwards
        // 3. existing one, but rules file is broken => can be wiped, will not exist afterwards
        // 4. existing one, but catalog is broken => can be wiped, will exist afterwards
        // 5. recently (during server lifecycle) created one => cannot be wiped
        let db_name_existing = DatabaseName::new("db_existing".to_string()).unwrap();
        let db_name_non_existing = DatabaseName::new("db_non_existing".to_string()).unwrap();
        let db_name_rules_broken = DatabaseName::new("db_broken_rules".to_string()).unwrap();
        let db_name_catalog_broken = DatabaseName::new("db_broken_catalog".to_string()).unwrap();
        let db_name_created = DatabaseName::new("db_created".to_string()).unwrap();

        // setup
        let store = ObjectStore::new_in_memory();
        let server_id = ServerId::try_from(1).unwrap();

        // Create temporary server to create existing databases
        let manager = TestConnectionManager::new();
        let config = config_with_store(store);
        let store = config.store();
        let server = Server::new(manager, config);
        server.set_id(server_id).unwrap();
        server.maybe_initialize_server().await;

        create_simple_database(&server, db_name_existing.clone())
            .await
            .expect("failed to create database");

        create_simple_database(&server, db_name_rules_broken.clone())
            .await
            .expect("failed to create database");

        create_simple_database(&server, db_name_catalog_broken.clone())
            .await
            .expect("failed to create database");

        let config = server.require_initialized().unwrap();
        let root = config.root_path();
        config.drain().await;
        drop(server);
        drop(config);

        // tamper store to break one database
        let path = object_store_path_for_database_config(&root, &db_name_rules_broken);
        let data = Bytes::from("x");
        let len = data.len();
        store
            .put(
                &path,
                futures::stream::once(async move { Ok(data) }),
                Some(len),
            )
            .await
            .unwrap();
        let (preserved_catalog, _catalog) = PreservedCatalog::load::<TestCatalogState>(
            Arc::clone(&store),
            server_id,
            db_name_catalog_broken.to_string(),
            (),
        )
        .await
        .unwrap()
        .unwrap();
        parquet_file::catalog::test_helpers::break_catalog_with_weird_version(&preserved_catalog)
            .await;
        drop(preserved_catalog);

        // boot actual test server
        let store = Arc::try_unwrap(store).unwrap();
        store.get(&path).await.unwrap();
        let manager = TestConnectionManager::new();
        // need to disable auto-wipe for this test
        let mut config = config_with_store(store);
        config.wipe_catalog_on_error = false;
        let server = Server::new(manager, config);

        // cannot wipe if server ID is not set
        assert_eq!(
            server
                .wipe_preserved_catalog(&db_name_non_existing)
                .unwrap_err()
                .to_string(),
            "id not set"
        );

        server.set_id(ServerId::try_from(1).unwrap()).unwrap();
        server.maybe_initialize_server().await;

        // 1. cannot wipe if DB exists
        assert_eq!(
            server
                .wipe_preserved_catalog(&db_name_existing)
                .unwrap_err()
                .to_string(),
            "database already exists: db_existing"
        );
        assert!(
            PreservedCatalog::exists(&server.store, server_id, db_name_existing.as_str())
                .await
                .unwrap()
        );

        // 2. wiping a non-existing DB just works, but won't bring DB into existence
        assert!(server.error_database(&db_name_non_existing).is_none());
        PreservedCatalog::new_empty::<TestCatalogState>(
            Arc::clone(&server.store),
            server_id,
            db_name_non_existing.to_string(),
            (),
        )
        .await
        .unwrap();
        let tracker = server
            .wipe_preserved_catalog(&db_name_non_existing)
            .unwrap();
        let metadata = tracker.metadata();
        let expected_metadata = Job::WipePreservedCatalog {
            db_name: Arc::from(db_name_non_existing.as_str()),
        };
        assert_eq!(metadata, &expected_metadata);
        tracker.join().await;
        assert!(!PreservedCatalog::exists(
            &server.store,
            server_id,
            &db_name_non_existing.to_string()
        )
        .await
        .unwrap());
        assert!(server.error_database(&db_name_non_existing).is_none());
        assert!(server.db(&db_name_non_existing).is_none());

        // 3. wipe DB with broken rules file, this won't bring DB back to life
        assert!(server.error_database(&db_name_rules_broken).is_some());
        let tracker = server
            .wipe_preserved_catalog(&db_name_rules_broken)
            .unwrap();
        let metadata = tracker.metadata();
        let expected_metadata = Job::WipePreservedCatalog {
            db_name: Arc::from(db_name_rules_broken.as_str()),
        };
        assert_eq!(metadata, &expected_metadata);
        tracker.join().await;
        assert!(!PreservedCatalog::exists(
            &server.store,
            server_id,
            &db_name_rules_broken.to_string()
        )
        .await
        .unwrap());
        assert!(server.error_database(&db_name_rules_broken).is_some());
        assert!(server.db(&db_name_rules_broken).is_none());

        // 4. wipe DB with broken catalog, this will bring the DB back to life
        assert!(server.error_database(&db_name_catalog_broken).is_some());
        let tracker = server
            .wipe_preserved_catalog(&db_name_catalog_broken)
            .unwrap();
        let metadata = tracker.metadata();
        let expected_metadata = Job::WipePreservedCatalog {
            db_name: Arc::from(db_name_catalog_broken.as_str()),
        };
        assert_eq!(metadata, &expected_metadata);
        tracker.join().await;
        assert!(PreservedCatalog::exists(
            &server.store,
            server_id,
            &db_name_catalog_broken.to_string()
        )
        .await
        .unwrap());
        assert!(server.error_database(&db_name_catalog_broken).is_none());
        assert!(server.db(&db_name_catalog_broken).is_some());
        let line = "cpu bar=1 10";
        let lines: Vec<_> = parse_lines(line).map(|l| l.unwrap()).collect();
        server
            .write_lines(&db_name_catalog_broken, &lines, ARBITRARY_DEFAULT_TIME)
            .await
            .expect("DB writable");

        // 5. cannot wipe if DB was just created
        server
            .create_database(DatabaseRules::new(db_name_created.clone()))
            .await
            .unwrap();
        assert_eq!(
            server
                .wipe_preserved_catalog(&db_name_created)
                .unwrap_err()
                .to_string(),
            "database already exists: db_created"
        );
        assert!(
            PreservedCatalog::exists(&server.store, server_id, &db_name_created.to_string())
                .await
                .unwrap()
        );
    }

    #[tokio::test]
    async fn cannot_create_db_when_catalog_is_present() {
        let store = Arc::new(ObjectStore::new_in_memory());
        let server_id = ServerId::try_from(1).unwrap();
        let db_name = "my_db";

        // create catalog
        PreservedCatalog::new_empty::<TestCatalogState>(
            Arc::clone(&store),
            server_id,
            db_name.to_string(),
            (),
        )
        .await
        .unwrap();

        // setup server
        let store = Arc::try_unwrap(store).unwrap();
        let manager = TestConnectionManager::new();
        let config = config_with_store(store);
        let server = Server::new(manager, config);
        server.set_id(server_id).unwrap();
        server.maybe_initialize_server().await;

        // creating database will now result in an error
        let err = create_simple_database(&server, db_name).await.unwrap_err();
        assert!(matches!(err, Error::CannotCreatePreservedCatalog { .. }));
    }

    #[tokio::test]
    async fn write_buffer_errors_propagate() {
        let manager = TestConnectionManager::new();
        let server = Server::new(manager, config());
        server.set_id(ServerId::try_from(1).unwrap()).unwrap();
        server.maybe_initialize_server().await;

        let write_buffer = Arc::new(MockBufferForWritingThatAlwaysErrors {});

        let db = TestDb::builder()
            .write_buffer(WriteBufferConfig::Writing(Arc::clone(&write_buffer) as _))
            .build()
            .await
            .db;

        let entry = lp_to_entry("cpu bar=1 10");

        let res = server.write_entry_local("arbitrary", &db, entry).await;
        assert!(
            matches!(res, Err(Error::WriteBuffer { .. })),
            "Expected Err(Error::WriteBuffer {{ .. }}), got: {:?}",
            res
        );
    }

    // run a sql query against the database, returning the results as record batches
    async fn run_query(db: Arc<Db>, query: &str) -> Vec<RecordBatch> {
        let planner = SqlQueryPlanner::default();
        let executor = db.executor();

        let physical_plan = planner.query(db, query, &executor).unwrap();

        executor
            .collect(physical_plan, ExecutorType::Query)
            .await
            .unwrap()
    }
}
