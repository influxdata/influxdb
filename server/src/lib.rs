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

#![deny(broken_intra_doc_links, rust_2018_idioms)]
#![warn(
    missing_debug_implementations,
    clippy::explicit_iter_loop,
    clippy::use_self,
    clippy::clone_on_ref_ptr
)]

use std::convert::TryInto;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use std::sync::Arc;

use async_trait::async_trait;
use bytes::BytesMut;
use cached::proc_macro::cached;
use db::load_or_create_preserved_catalog;
use futures::stream::TryStreamExt;
use object_store::path::Path;
use observability_deps::tracing::{debug, error, info, warn};
use parking_lot::Mutex;
use snafu::{OptionExt, ResultExt, Snafu};

use data_types::{
    database_rules::DatabaseRules,
    job::Job,
    server_id::ServerId,
    {DatabaseName, DatabaseNameError},
};
use entry::{lines_to_sharded_entries, Entry, OwnedSequencedEntry, ShardedEntry};
use influxdb_line_protocol::ParsedLine;
use internal_types::once::OnceNonZeroU32;
use metrics::{KeyValue, MetricObserverBuilder, MetricRegistry};
use object_store::{path::ObjectStorePath, ObjectStore, ObjectStoreApi};
use query::{exec::Executor, DatabaseStore};
use tokio::sync::Semaphore;
use tracker::{TaskId, TaskRegistration, TaskRegistryWithHistory, TaskTracker, TrackedFutureExt};

pub use crate::config::RemoteTemplate;
use crate::{
    config::{
        object_store_path_for_database_config, Config, GRpcConnectionString, DB_RULES_FILE_NAME,
    },
    db::Db,
};
use cached::Return;
use data_types::database_rules::{NodeGroup, RoutingRules, Shard, ShardConfig, ShardId};
use generated_types::database_rules::{decode_database_rules, encode_database_rules};
use influxdb_iox_client::{connection::Builder, write};
use rand::seq::SliceRandom;
use std::collections::HashMap;

pub mod buffer;
mod config;
pub mod db;
mod query_tests;

// This module exposes `query_tests` outside of the crate so that it may be used
// in benchmarks. Do not import this module for non-benchmark purposes!
pub mod benchmarks {
    pub use crate::query_tests::*;
}

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

    #[snafu(display("id already set"))]
    IdAlreadySet { id: ServerId },

    #[snafu(display("unable to use server until id is set"))]
    IdNotSet,

    #[snafu(display(
        "server ID is set but DBs are not yet loaded. Server is not yet ready to read/write data."
    ))]
    DatabasesNotLoaded,

    #[snafu(display("error serializing database rules to protobuf: {}", source))]
    ErrorSerializingRulesProtobuf {
        source: generated_types::database_rules::EncodeError,
    },

    #[snafu(display("error deserializing database rules from protobuf: {}", source))]
    ErrorDeserializingRulesProtobuf {
        source: generated_types::database_rules::DecodeError,
    },

    #[snafu(display("error deserializing configuration {}", source))]
    ErrorDeserializing { source: serde_json::Error },

    #[snafu(display("store error: {}", source))]
    StoreError { source: object_store::Error },

    #[snafu(display(
        "no database configuration present in directory that contains data: {:?}",
        location
    ))]
    NoDatabaseConfigError { location: object_store::path::Path },

    #[snafu(display("database already exists"))]
    DatabaseAlreadyExists { db_name: String },

    #[snafu(display("error converting line protocol to flatbuffers: {}", source))]
    LineConversion { source: entry::Error },

    #[snafu(display("error decoding entry flatbuffers: {}", source))]
    DecodingEntry {
        source: flatbuffers::InvalidFlatbuffer,
    },

    #[snafu(display("shard not found: {}", shard_id))]
    ShardNotFound { shard_id: ShardId },

    #[snafu(display("hard buffer limit reached"))]
    HardLimitReached {},

    #[snafu(display("no remote configured for node group: {:?}", node_group))]
    NoRemoteConfigured { node_group: NodeGroup },

    #[snafu(display("all remotes failed connecting: {:?}", errors))]
    NoRemoteReachable {
        errors: HashMap<GRpcConnectionString, ConnectionManagerError>,
    },

    #[snafu(display("remote error: {}", source))]
    RemoteError { source: ConnectionManagerError },

    #[snafu(display("cannot load catalog: {}", source))]
    CatalogLoadError { source: DatabaseError },
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
    fn new() -> Self {
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

const STORE_ERROR_PAUSE_SECONDS: u64 = 100;

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

#[derive(Debug, Default)]
struct CurrentServerId(OnceNonZeroU32);

impl CurrentServerId {
    fn set(&self, id: ServerId) -> Result<()> {
        self.0.set(id.get()).map_err(|id| Error::IdAlreadySet {
            id: ServerId::new(id),
        })
    }

    fn get(&self) -> Result<ServerId> {
        self.0.get().map(ServerId::new).context(IdNotSet)
    }
}

/// `Server` is the container struct for how servers store data internally, as
/// well as how they communicate with other servers. Each server will have one
/// of these structs, which keeps track of all replication and query rules.
#[derive(Debug)]
pub struct Server<M: ConnectionManager> {
    id: CurrentServerId,
    config: Arc<Config>,
    connection_manager: Arc<M>,
    pub store: Arc<ObjectStore>,
    exec: Arc<Executor>,
    jobs: Arc<JobRegistry>,
    pub metrics: Arc<ServerMetrics>,

    /// The metrics registry associated with the server. This is needed not for
    /// recording telemetry, but because the server hosts the /metric endpoint
    /// and populates the endpoint with this data.
    pub registry: Arc<metrics::MetricRegistry>,

    /// Flags that databases are loaded and server is ready to read/write data.
    initialized: AtomicBool,

    /// Semaphore that limits the number of jobs that load DBs when the serverID is set.
    ///
    /// Note that this semaphore is more of a "lock" than an arbitrary semaphore. All the other sync structures (mutex,
    /// rwlock) require something to be wrapped which we don't have in our case, so we're using a semaphore here. We
    /// want exactly 1 background worker to mess with the server init / DB loading, otherwise everything in the critical
    /// section (in [`maybe_initialize_server`](Self::maybe_initialize_server)) will break apart. So this semaphore
    /// cannot be configured.
    initialize_semaphore: Semaphore,
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

impl<M: ConnectionManager> Server<M> {
    pub fn new(connection_manager: M, config: ServerConfig) -> Self {
        let jobs = Arc::new(JobRegistry::new());

        let ServerConfig {
            num_worker_threads,
            object_store,
            // to test the metrics provide a different registry to the `ServerConfig`.
            metric_registry,
            remote_template,
        } = config;
        let num_worker_threads = num_worker_threads.unwrap_or_else(num_cpus::get);

        Self {
            id: Default::default(),
            config: Arc::new(Config::new(
                Arc::clone(&jobs),
                Arc::clone(&metric_registry),
                remote_template,
            )),
            store: object_store,
            connection_manager: Arc::new(connection_manager),
            exec: Arc::new(Executor::new(num_worker_threads)),
            jobs,
            metrics: Arc::new(ServerMetrics::new(Arc::clone(&metric_registry))),
            registry: Arc::clone(&metric_registry),
            initialized: AtomicBool::new(false),
            // Always set semaphore permits to `1`, see design comments in `Server::initialize_semaphore`.
            initialize_semaphore: Semaphore::new(1),
        }
    }

    /// sets the id of the server, which is used for replication and the base
    /// path in object storage.
    ///
    /// A valid server ID Must be non-zero.
    pub fn set_id(&self, id: ServerId) -> Result<()> {
        self.id.set(id)
    }

    /// Returns the current server ID, or an error if not yet set.
    pub fn require_id(&self) -> Result<ServerId> {
        self.id.get()
    }

    /// Check if server is loaded. Databases are loaded and server is ready to read/write.
    pub fn initialized(&self) -> bool {
        // ordering here isn't that important since this method is not used to check-and-modify the flag
        self.initialized.load(Ordering::Relaxed)
    }

    /// Require that server is loaded. Databases are loaded and server is ready to read/write.
    fn require_initialized(&self) -> Result<ServerId> {
        // since a server ID is the pre-requirement for init, check this first
        let server_id = self.require_id()?;

        // ordering here isn't that important since this method is not used to check-and-modify the flag
        if self.initialized() {
            Ok(server_id)
        } else {
            Err(Error::DatabasesNotLoaded)
        }
    }

    /// Tells the server the set of rules for a database.
    pub async fn create_database(&self, rules: DatabaseRules) -> Result<()> {
        // Return an error if this server is not yet ready
        let server_id = self.require_initialized()?;

        let preserved_catalog = load_or_create_preserved_catalog(
            rules.db_name(),
            Arc::clone(&self.store),
            server_id,
            self.config.metrics_registry(),
        )
        .await
        .map_err(|e| Box::new(e) as _)
        .context(CatalogLoadError)?;

        let db_reservation = self.config.create_db(rules)?;
        self.persist_database_rules(db_reservation.rules().clone())
            .await?;
        db_reservation.commit(
            server_id,
            Arc::clone(&self.store),
            Arc::clone(&self.exec),
            preserved_catalog,
        );

        Ok(())
    }

    pub async fn persist_database_rules<'a>(&self, rules: DatabaseRules) -> Result<()> {
        let location = object_store_path_for_database_config(&self.root_path()?, &rules.name);

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
    ///
    /// This requires the serverID to be set. It will be a no-op if the configs are already loaded and the server is ready.
    pub async fn maybe_initialize_server(&self) -> Result<()> {
        let _guard = self
            .initialize_semaphore
            .acquire()
            .await
            .expect("semaphore should not be closed");

        // Note that we use Acquire-Release ordering for the atomic within the semaphore to ensure that another thread
        // that enters this semaphore after we've left actually sees the correct `is_ready` flag.
        if self.initialized.load(Ordering::Acquire) {
            // already loaded, so do nothing
            return Ok(());
        }

        // get the database names from the object store prefixes
        // TODO: update object store to pull back all common prefixes by
        //       following the next tokens.
        let list_result = self
            .store
            .list_with_delimiter(&self.root_path()?)
            .await
            .context(StoreError)?;

        let server_id = self.require_id()?;

        let handles: Vec<_> = list_result
            .common_prefixes
            .into_iter()
            .map(|mut path| {
                let store = Arc::clone(&self.store);
                let config = Arc::clone(&self.config);
                let exec = Arc::clone(&self.exec);

                path.set_file_name(DB_RULES_FILE_NAME);

                tokio::task::spawn(async move {
                    if let Err(e) =
                        Self::load_database_config(server_id, store, config, exec, path).await
                    {
                        error!(%e, "cannot load database");
                    }
                })
            })
            .collect();

        futures::future::join_all(handles).await;

        // mark as ready (use correct ordering for Acquire-Release)
        self.initialized.store(true, Ordering::Release);
        info!("loaded databases, server is initalized");

        Ok(())
    }

    async fn load_database_config(
        server_id: ServerId,
        store: Arc<ObjectStore>,
        config: Arc<Config>,
        exec: Arc<Executor>,
        path: Path,
    ) -> Result<()> {
        let serialized_rules = loop {
            match get_database_config_bytes(&path, &store).await {
                Ok(data) => break data,
                Err(e) => {
                    if let Error::NoDatabaseConfigError { location } = &e {
                        warn!(?location, "{}", e);
                        return Ok(());
                    }
                    error!(
                        "error getting database config {:?} from object store: {}",
                        path, e
                    );
                    tokio::time::sleep(tokio::time::Duration::from_secs(STORE_ERROR_PAUSE_SECONDS))
                        .await;
                }
            }
        };
        let rules = decode_database_rules(serialized_rules.freeze())
            .context(ErrorDeserializingRulesProtobuf)?;

        let preserved_catalog = load_or_create_preserved_catalog(
            rules.db_name(),
            Arc::clone(&store),
            server_id,
            config.metrics_registry(),
        )
        .await
        .map_err(|e| Box::new(e) as _)
        .context(CatalogLoadError)?;

        let handle = config.create_db(rules)?;
        handle.commit(server_id, store, exec, preserved_catalog);

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
        self.require_initialized()?;

        let db_name = DatabaseName::new(db_name).context(InvalidDatabaseName)?;
        let db = self
            .config
            .db(&db_name)
            .context(DatabaseNotFound { db_name: &*db_name })?;

        // need to split this in two blocks because we cannot hold a lock across an async call.
        let routing_config_target = {
            let rules = db.rules.read();
            if let Some(RoutingRules::RoutingConfig(routing_config)) = &rules.routing_rules {
                let sharded_entries = lines_to_sharded_entries(
                    lines,
                    default_time,
                    None as Option<&ShardConfig>,
                    &*rules,
                )
                .context(LineConversion)?;
                Some((routing_config.target.clone(), sharded_entries))
            } else {
                None
            }
        };

        if let Some((target, sharded_entries)) = routing_config_target {
            for i in sharded_entries {
                self.write_entry_downstream(&db_name, &target, i.entry)
                    .await?;
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
            let rules = db.rules.read();

            let shard_config = rules.routing_rules.as_ref().map(|cfg| match cfg {
                RoutingRules::RoutingConfig(_) => todo!("routing config"),
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
        shards: Arc<HashMap<u32, Shard>>,
        sharded_entry: ShardedEntry,
    ) -> Result<()> {
        match sharded_entry.shard_id {
            Some(shard_id) => {
                let shard = shards.get(&shard_id).context(ShardNotFound { shard_id })?;
                match shard {
                    Shard::Iox(node_group) => {
                        self.write_entry_downstream(db_name, node_group, sharded_entry.entry)
                            .await?
                    }
                }
            }
            None => {
                self.write_entry_local(&db_name, db, sharded_entry.entry)
                    .await?
            }
        }
        Ok(())
    }

    async fn write_entry_downstream(
        &self,
        db_name: &str,
        node_group: &[ServerId],
        entry: Entry,
    ) -> Result<()> {
        let addrs: Vec<_> = node_group
            .iter()
            .filter_map(|&node| self.config.resolve_remote(node))
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
        return NoRemoteReachable { errors }.fail();
    }

    pub async fn write_entry(&self, db_name: &str, entry_bytes: Vec<u8>) -> Result<()> {
        // Return an error if this server is not yet ready
        self.require_initialized()?;

        let db_name = DatabaseName::new(db_name).context(InvalidDatabaseName)?;
        let db = self
            .config
            .db(&db_name)
            .context(DatabaseNotFound { db_name: &*db_name })?;

        let entry = entry_bytes.try_into().context(DecodingEntry)?;
        self.write_entry_local(&db_name, &db, entry).await
    }

    pub async fn write_entry_local(&self, db_name: &str, db: &Db, entry: Entry) -> Result<()> {
        let bytes = entry.data().len() as u64;
        db.store_entry(entry).map_err(|e| {
            self.metrics.ingest_entries_bytes_total.add_with_labels(
                bytes,
                &[
                    metrics::KeyValue::new("status", "error"),
                    metrics::KeyValue::new("db_name", db_name.to_string()),
                ],
            );
            match e {
                db::Error::HardLimitReached {} => Error::HardLimitReached {},
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

    pub async fn handle_sequenced_entry(
        &self,
        db: &Db,
        sequenced_entry: OwnedSequencedEntry,
    ) -> Result<()> {
        db.store_sequenced_entry(Arc::new(sequenced_entry))
            .map_err(|e| Error::UnknownDatabaseError {
                source: Box::new(e),
            })?;

        Ok(())
    }

    pub fn db(&self, name: &DatabaseName<'_>) -> Option<Arc<Db>> {
        self.config.db(name)
    }

    pub fn db_rules(&self, name: &DatabaseName<'_>) -> Option<DatabaseRules> {
        self.config.db(name).map(|d| d.rules.read().clone())
    }

    // Update database rules and save on success.
    pub async fn update_db_rules<F, E>(
        &self,
        db_name: &DatabaseName<'static>,
        update: F,
    ) -> std::result::Result<DatabaseRules, UpdateError<E>>
    where
        F: FnOnce(DatabaseRules) -> Result<DatabaseRules, E>,
    {
        let rules = self
            .config
            .update_db_rules(db_name, update)
            .map_err(|e| match e {
                crate::config::UpdateError::Closure(e) => UpdateError::Closure(e),
                crate::config::UpdateError::Update(e) => UpdateError::Update(e),
            })?;
        self.persist_database_rules(rules.clone()).await?;
        Ok(rules)
    }

    pub fn remotes_sorted(&self) -> Vec<(ServerId, String)> {
        self.config.remotes_sorted()
    }

    pub fn update_remote(&self, id: ServerId, addr: GRpcConnectionString) {
        self.config.update_remote(id, addr)
    }

    pub fn delete_remote(&self, id: ServerId) -> Option<GRpcConnectionString> {
        self.config.delete_remote(id)
    }

    pub fn spawn_dummy_job(&self, nanos: Vec<u64>) -> TaskTracker<Job> {
        let (tracker, registration) = self.jobs.register(Job::Dummy {
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
        table_name: impl Into<String>,
        chunk_id: u32,
    ) -> Result<TaskTracker<Job>> {
        let db_name = db_name.to_string();
        let name = DatabaseName::new(&db_name).context(InvalidDatabaseName)?;

        let partition_key = partition_key.into();
        let table_name = table_name.into();

        let db = self
            .config
            .db(&name)
            .context(DatabaseNotFound { db_name: &db_name })?;

        Ok(db.load_chunk_to_read_buffer_in_background(partition_key, table_name, chunk_id))
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
            if self.require_id().is_ok() {
                if let Err(e) = self.maybe_initialize_server().await {
                    error!(%e, "error during DB loading");
                }
            }

            self.jobs.inner.lock().reclaim();

            tokio::select! {
                _ = interval.tick() => {},
                _ = shutdown.cancelled() => break
            }
        }

        info!("shutting down background workers");
        self.config.drain().await;

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

    /// Sends a SequencedEntry to the remote server. An IOx server acting as a
    /// write buffer will call this method to replicate to other write
    /// buffer servers or to send data to downstream subscribers.
    async fn write_sequenced_entry(
        &self,
        db: &str,
        sequenced_entry: OwnedSequencedEntry,
    ) -> Result<(), ConnectionManagerError>;
}

/// The connection manager maps a host identifier to a remote server.
#[derive(Debug)]
pub struct ConnectionManagerImpl {}

#[async_trait]
impl ConnectionManager for ConnectionManagerImpl {
    type RemoteServer = RemoteServerImpl;

    async fn remote_server(
        &self,
        connect: &str,
    ) -> Result<Arc<Self::RemoteServer>, ConnectionManagerError> {
        let ret = cached_remote_server(connect.to_string()).await?;
        debug!(was_cached=%ret.was_cached, %connect, "getting remote connection");
        Ok(ret.value)
    }
}

// cannot be an associated function
// argument need to have static lifetime because they become caching keys
#[cached(result = true, with_cached_flag = true)]
async fn cached_remote_server(
    connect: String,
) -> Result<Return<Arc<RemoteServerImpl>>, ConnectionManagerError> {
    let connection = Builder::default()
        .build(&connect)
        .await
        .map_err(|e| Box::new(e) as _)
        .context(RemoteServerConnectError)?;
    let client = write::Client::new(connection);
    Ok(Return::new(Arc::new(RemoteServerImpl { client })))
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

    /// Sends a SequencedEntry to the remote server. An IOx server acting as a
    /// write buffer will call this method to replicate to other write
    /// buffer servers or to send data to downstream subscribers.
    async fn write_sequenced_entry(
        &self,
        _db: &str,
        _sequenced_entry: OwnedSequencedEntry,
    ) -> Result<(), ConnectionManagerError> {
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

// get the bytes for the database rule config file, if it exists,
// otherwise it returns none.
async fn get_database_config_bytes(
    location: &object_store::path::Path,
    store: &ObjectStore,
) -> Result<bytes::BytesMut> {
    let list_result = store
        .list_with_delimiter(location)
        .await
        .context(StoreError)?;
    if list_result.objects.is_empty() {
        return NoDatabaseConfigError {
            location: location.clone(),
        }
        .fail();
    }
    get_store_bytes(location, store).await
}

#[cfg(test)]
mod tests {
    use std::{
        collections::BTreeMap,
        convert::TryFrom,
        time::{Duration, Instant},
    };

    use async_trait::async_trait;
    use futures::TryStreamExt;
    use snafu::Snafu;
    use tokio::task::JoinHandle;
    use tokio_util::sync::CancellationToken;

    use arrow_util::assert_batches_eq;
    use data_types::database_rules::{
        HashRing, PartitionTemplate, ShardConfig, TemplatePart, NO_SHARD_CONFIG,
    };
    use influxdb_line_protocol::parse_lines;
    use metrics::MetricRegistry;
    use object_store::{memory::InMemory, path::ObjectStorePath};
    use query::{frontend::sql::SqlQueryPlanner, Database};

    use super::*;
    use std::sync::atomic::{AtomicBool, Ordering};
    use tempfile::TempDir;

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
        config_with_metric_registry_and_store(ObjectStore::new_in_memory(InMemory::new()))
    }

    fn config() -> ServerConfig {
        config_with_metric_registry().1
    }

    fn config_with_store(object_store: ObjectStore) -> ServerConfig {
        config_with_metric_registry_and_store(object_store).1
    }

    #[tokio::test]
    async fn test_get_database_config_bytes() {
        let object_store = ObjectStore::new_in_memory(InMemory::new());
        let mut rules_path = object_store.new_path();
        rules_path.push_all_dirs(&["1", "foo_bar"]);
        rules_path.set_file_name("rules.pb");

        let res = get_database_config_bytes(&rules_path, &object_store)
            .await
            .unwrap_err();
        assert!(matches!(res, Error::NoDatabaseConfigError { .. }));
    }

    #[tokio::test]
    async fn server_api_calls_return_error_with_no_id_set() {
        let manager = TestConnectionManager::new();
        let server = Server::new(manager, config());

        let resp = server.require_id().unwrap_err();
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
        server.maybe_initialize_server().await.unwrap();

        let name = DatabaseName::new("bananas").unwrap();

        let rules = DatabaseRules {
            name: name.clone(),
            partition_template: PartitionTemplate {
                parts: vec![TemplatePart::TimeFormat("YYYY-MM".to_string())],
            },
            write_buffer_config: None,
            lifecycle_rules: Default::default(),
            routing_rules: None,
            worker_cleanup_avg_sleep: Duration::from_secs(2),
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
        server2.maybe_initialize_server().await.unwrap();

        let _ = server2.db(&db2).unwrap();
        let _ = server2.db(&name).unwrap();
    }

    #[tokio::test]
    async fn duplicate_database_name_rejected() {
        // Covers #643

        let manager = TestConnectionManager::new();
        let server = Server::new(manager, config());
        server.set_id(ServerId::try_from(1).unwrap()).unwrap();
        server.maybe_initialize_server().await.unwrap();

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

    async fn create_simple_database<M>(server: &Server<M>, name: impl Into<String>) -> Result<()>
    where
        M: ConnectionManager,
    {
        let name = DatabaseName::new(name.into()).unwrap();

        let rules = DatabaseRules {
            name,
            partition_template: PartitionTemplate {
                parts: vec![TemplatePart::TimeFormat("YYYY-MM".to_string())],
            },
            write_buffer_config: None,
            lifecycle_rules: Default::default(),
            routing_rules: None,
            worker_cleanup_avg_sleep: Duration::from_secs(2),
        };

        // Create a database
        server.create_database(rules.clone()).await
    }

    #[tokio::test]
    async fn load_databases() {
        let temp_dir = TempDir::new().unwrap();

        let store = ObjectStore::new_file(object_store::disk::File::new(temp_dir.path()));
        let manager = TestConnectionManager::new();
        let config = config_with_store(store);
        let server = Server::new(manager, config);
        server.set_id(ServerId::try_from(1).unwrap()).unwrap();
        server.maybe_initialize_server().await.unwrap();
        create_simple_database(&server, "bananas")
            .await
            .expect("failed to create database");

        let mut rules_path = server.store.new_path();
        rules_path.push_all_dirs(&["1", "bananas"]);
        rules_path.set_file_name("rules.pb");

        std::mem::drop(server);

        let store = ObjectStore::new_file(object_store::disk::File::new(temp_dir.path()));
        let manager = TestConnectionManager::new();
        let config = config_with_store(store);
        let server = Server::new(manager, config);
        server.set_id(ServerId::try_from(1).unwrap()).unwrap();
        server.maybe_initialize_server().await.expect("load config");

        create_simple_database(&server, "apples")
            .await
            .expect("failed to create database");

        assert_eq!(server.db_names_sorted(), vec!["apples", "bananas"]);

        std::mem::drop(server);

        let store = ObjectStore::new_file(object_store::disk::File::new(temp_dir.path()));
        store
            .delete(&rules_path)
            .await
            .expect("cannot delete rules file");

        let manager = TestConnectionManager::new();
        let config = config_with_store(store);
        let server = Server::new(manager, config);
        server.set_id(ServerId::try_from(1).unwrap()).unwrap();
        server.maybe_initialize_server().await.expect("load config");

        assert_eq!(server.db_names_sorted(), vec!["apples"]);
    }

    #[tokio::test]
    async fn db_names_sorted() {
        let manager = TestConnectionManager::new();
        let server = Server::new(manager, config());
        server.set_id(ServerId::try_from(1).unwrap()).unwrap();
        server.maybe_initialize_server().await.expect("load config");

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
        server.maybe_initialize_server().await.unwrap();

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

        let planner = SqlQueryPlanner::default();
        let executor = server.executor();
        let physical_plan = planner
            .query(db, "select * from cpu", executor.as_ref())
            .unwrap();

        let batches = executor.collect(physical_plan).await.unwrap();
        let expected = vec![
            "+-----+-------------------------------+",
            "| bar | time                          |",
            "+-----+-------------------------------+",
            "| 1   | 1970-01-01 00:00:00.000000010 |",
            "+-----+-------------------------------+",
        ];
        assert_batches_eq!(expected, &batches);
    }

    #[tokio::test]
    async fn write_entry_local() {
        let (metric_registry, config) = config_with_metric_registry();
        let manager = TestConnectionManager::new();
        let server = Server::new(manager, config);
        server.set_id(ServerId::try_from(1).unwrap()).unwrap();
        server.maybe_initialize_server().await.unwrap();

        let name = DatabaseName::new("foo".to_string()).unwrap();
        server
            .create_database(DatabaseRules::new(name))
            .await
            .unwrap();

        let db_name = DatabaseName::new("foo").unwrap();
        let db = server.db(&db_name).unwrap();

        let line = "cpu bar=1 10";
        let lines: Vec<_> = parse_lines(line).map(|l| l.unwrap()).collect();
        let sharded_entries = lines_to_sharded_entries(
            &lines,
            ARBITRARY_DEFAULT_TIME,
            NO_SHARD_CONFIG,
            &*db.rules.read(),
        )
        .expect("sharded entries");

        let entry = &sharded_entries[0].entry;
        server
            .write_entry("foo", entry.data().into())
            .await
            .expect("write entry");

        let planner = SqlQueryPlanner::default();
        let executor = server.executor();
        let physical_plan = planner
            .query(db, "select * from cpu", executor.as_ref())
            .unwrap();

        let batches = executor.collect(physical_plan).await.unwrap();
        let expected = vec![
            "+-----+-------------------------------+",
            "| bar | time                          |",
            "+-----+-------------------------------+",
            "| 1   | 1970-01-01 00:00:00.000000010 |",
            "+-----+-------------------------------+",
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
        server.maybe_initialize_server().await.unwrap();

        let db_name = DatabaseName::new("foo").unwrap();
        server
            .create_database(DatabaseRules::new(db_name.clone()))
            .await
            .unwrap();

        let remote_ids = vec![bad_remote_id, good_remote_id_1, good_remote_id_2];
        let db = server.db(&db_name).unwrap();
        {
            let mut rules = db.rules.write();
            let shard_config = ShardConfig {
                hash_ring: Some(HashRing {
                    shards: vec![TEST_SHARD_ID].into(),
                    ..Default::default()
                }),
                shards: Arc::new(
                    vec![(TEST_SHARD_ID, Shard::Iox(remote_ids.clone()))]
                        .into_iter()
                        .collect(),
                ),
                ..Default::default()
            };
            rules.routing_rules = Some(RoutingRules::ShardConfig(shard_config));
        }

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
                ConnectionManagerError::RemoteServerConnectError {..}
            )
        ));
        assert_eq!(written_1.load(Ordering::Relaxed), false);
        assert_eq!(written_2.load(Ordering::Relaxed), false);

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
        assert_eq!(written_1.load(Ordering::Relaxed), true);
        assert_eq!(written_2.load(Ordering::Relaxed), true);
    }

    #[tokio::test]
    async fn close_chunk() {
        test_helpers::maybe_start_logging();
        let manager = TestConnectionManager::new();
        let server = Arc::new(Server::new(manager, config()));

        let cancel_token = CancellationToken::new();
        let background_handle = spawn_worker(Arc::clone(&server), cancel_token.clone());

        server.set_id(ServerId::try_from(1).unwrap()).unwrap();
        server.maybe_initialize_server().await.unwrap();

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
        let partition_key = "";
        let table_name = "cpu";
        let db_name_string = db_name.to_string();
        let tracker = server
            .close_chunk(db_name, partition_key, table_name, 0)
            .unwrap();

        let metadata = tracker.metadata();
        let expected_metadata = Job::CloseChunk {
            db_name: db_name_string,
            partition_key: partition_key.to_string(),
            table_name: table_name.to_string(),
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

        async fn write_sequenced_entry(
            &self,
            _db: &str,
            _sequenced_entry: OwnedSequencedEntry,
        ) -> Result<(), ConnectionManagerError> {
            unimplemented!()
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
        server.maybe_initialize_server().await.unwrap();

        let name = DatabaseName::new("foo".to_string()).unwrap();
        server
            .create_database(DatabaseRules::new(name))
            .await
            .unwrap();

        let db_name = DatabaseName::new("foo").unwrap();
        let db = server.db(&db_name).unwrap();
        db.rules.write().lifecycle_rules.buffer_size_hard =
            Some(std::num::NonZeroUsize::new(10).unwrap());

        // inserting first line does not trigger hard buffer limit
        let line_1 = "cpu bar=1 10";
        let lines_1: Vec<_> = parse_lines(line_1).map(|l| l.unwrap()).collect();
        let sharded_entries_1 = lines_to_sharded_entries(
            &lines_1,
            ARBITRARY_DEFAULT_TIME,
            NO_SHARD_CONFIG,
            &*db.rules.read(),
        )
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
            &*db.rules.read(),
        )
        .expect("second sharded entries");
        let entry_2 = &sharded_entries_2[0].entry;
        let res = server.write_entry("foo", entry_2.data().into()).await;
        assert!(matches!(res, Err(super::Error::HardLimitReached {})));
    }

    #[tokio::test]
    async fn cannot_create_db_until_dbs_are_loaded() {
        let temp_dir = TempDir::new().unwrap();

        let store = ObjectStore::new_file(object_store::disk::File::new(temp_dir.path()));
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
        assert!(matches!(err, Error::DatabasesNotLoaded));
    }

    #[tokio::test]
    async fn background_worker_eventually_loads_dbs() {
        let manager = TestConnectionManager::new();
        let server = Arc::new(Server::new(manager, config()));

        let cancel_token = CancellationToken::new();
        let background_handle = spawn_worker(Arc::clone(&server), cancel_token.clone());

        server.set_id(ServerId::try_from(1).unwrap()).unwrap();

        let t_0 = Instant::now();
        loop {
            if server.require_initialized().is_ok() {
                break;
            }
            assert!(t_0.elapsed() < Duration::from_secs(10));
            tokio::time::sleep(Duration::from_millis(100)).await;
        }

        // ensure that we don't leave the server instance hanging around
        cancel_token.cancel();
        let _ = background_handle.await;
    }
}
