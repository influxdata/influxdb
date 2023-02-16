use crate::{
    dump_log_to_stdout, log_command, rand_id, write_to_ingester, write_to_router, ServerFixture,
    TestConfig, TestServer,
};
use arrow::{datatypes::SchemaRef, record_batch::RecordBatch};
use arrow_flight::{
    decode::{DecodedFlightData, DecodedPayload, FlightDataDecoder},
    error::FlightError,
    Ticket,
};
use assert_cmd::prelude::*;
use data_types::{NamespaceId, TableId};
use futures::{stream::FuturesOrdered, StreamExt};
use http::Response;
use hyper::Body;
use influxdb_iox_client::{
    connection::GrpcConnection,
    flight::generated_types::{IngesterQueryRequest, IngesterQueryResponseMetadata},
    schema::generated_types::{schema_service_client::SchemaServiceClient, GetSchemaRequest},
};
use observability_deps::tracing::{debug, info};
use once_cell::sync::Lazy;
use prost::Message;
use std::{
    process::Command,
    sync::{Arc, Weak},
    time::Instant,
};
use tempfile::NamedTempFile;
use tokio::sync::{Mutex, OnceCell};

/// Structure that holds services and helpful accessors. Does not start services for a compactor;
/// that is always run separately on-demand in tests.
#[derive(Debug, Default)]
pub struct MiniCluster {
    /// Standard optional router
    router: Option<ServerFixture>,

    /// Standard optional ingester(s)
    ingesters: Vec<ServerFixture>,

    /// Standard optional querier
    querier: Option<ServerFixture>,

    /// Standard optional compactor configuration, to be used on-demand
    compactor_config: Option<TestConfig>,

    // Potentially helpful data
    org_id: String,
    bucket_id: String,
    namespace: String,
    namespace_id: OnceCell<NamespaceId>,
}

impl MiniCluster {
    pub fn new() -> Self {
        let org_id = rand_id();
        let bucket_id = rand_id();
        let namespace = format!("{org_id}_{bucket_id}");

        Self {
            org_id,
            bucket_id,
            namespace,
            ..Self::default()
        }
    }

    /// Create a new MiniCluster that shares the same underlying servers but has a new unique
    /// namespace and set of connections
    ///
    /// Note this is an internal implementation -- please use
    /// [`create_shared2`](Self::create_shared2) and [`new`](Self::new) to create new MiniClusters.
    fn new_from_fixtures(
        router: Option<ServerFixture>,
        ingesters: Vec<ServerFixture>,
        querier: Option<ServerFixture>,
        compactor_config: Option<TestConfig>,
    ) -> Self {
        let org_id = rand_id();
        let bucket_id = rand_id();
        let namespace = format!("{org_id}_{bucket_id}");

        Self {
            router,
            ingesters,
            querier,
            compactor_config,

            org_id,
            bucket_id,
            namespace,
            namespace_id: Default::default(),
        }
    }

    /// Create a "standard" shared MiniCluster that starts a router, ingester, and querier. Save
    /// config for a compactor, but the compactor service should be run on-demand in tests using
    /// `compactor run-once` rather than using `run compactor`.
    ///
    /// Note: Because the underlying server processes are shared across multiple tests, all users
    /// of this `MiniCluster` instance should only modify their own unique namespace.
    pub async fn create_shared2(database_url: String) -> Self {
        let start = Instant::now();
        let mut shared_servers = GLOBAL_SHARED_SERVERS2.lock().await;
        debug!(mutex_wait=?start.elapsed(), "creating standard2 cluster");

        // try to reuse existing server processes
        if let Some(shared) = shared_servers.take() {
            if let Some(cluster) = shared.creatable_cluster().await {
                debug!("Reusing existing cluster");

                // Put the server back
                *shared_servers = Some(shared);
                let start = Instant::now();
                // drop the lock prior to calling `create()` to allow others to proceed
                std::mem::drop(shared_servers);
                let new_self = cluster.create().await;
                info!(
                    total_wait=?start.elapsed(),
                    "created new mini cluster2 from existing cluster"
                );
                return new_self;
            } else {
                info!("some server proceses of previous cluster2 have already returned");
            }
        }

        // Have to make a new one
        info!("Create a new server2");
        let new_cluster = Self::create_non_shared2(database_url).await;

        // Update the shared servers to point at the newly created server proesses
        *shared_servers = Some(SharedServers::new(&new_cluster));
        new_cluster
    }

    /// Create a shared "version 2" MiniCluster that has a router, ingester set to essentially
    /// never persist data (except on-demand), and querier. Save config for a compactor, but the
    /// compactor service should be run on-demand in tests using `compactor run-once` rather than
    /// using `run compactor`.
    ///
    /// Note: Because the underlying server processes are shared across multiple tests, all users
    /// of this `MiniCluster` instance should only modify their own unique namespace.
    pub async fn create_shared2_never_persist(database_url: String) -> Self {
        let start = Instant::now();
        let mut shared_servers = GLOBAL_SHARED_SERVERS2_NEVER_PERSIST.lock().await;
        debug!(mutex_wait=?start.elapsed(), "creating standard2 cluster");

        // try to reuse existing server processes
        if let Some(shared) = shared_servers.take() {
            if let Some(cluster) = shared.creatable_cluster().await {
                debug!("Reusing existing cluster");

                // Put the server back
                *shared_servers = Some(shared);
                let start = Instant::now();
                // drop the lock prior to calling `create()` to allow others to proceed
                std::mem::drop(shared_servers);
                let new_self = cluster.create().await;
                info!(
                    total_wait=?start.elapsed(),
                    "created new mini cluster2 from existing cluster"
                );
                return new_self;
            } else {
                info!("some server proceses of previous cluster2 have already returned");
            }
        }

        // Have to make a new one
        info!("Create a new server2 set to never persist");
        let new_cluster = Self::create_non_shared2_never_persist(database_url).await;

        // Update the shared servers to point at the newly created server proesses
        *shared_servers = Some(SharedServers::new(&new_cluster));
        new_cluster
    }

    /// Create a non-shared "version 2" "standard" MiniCluster that has a router, ingester,
    /// querier. Save config for a compactor, but the compactor service should be run on-demand in
    /// tests using `compactor run-once` rather than using `run compactor`.
    pub async fn create_non_shared2(database_url: String) -> Self {
        let ingester_config = TestConfig::new_ingester2(&database_url);
        let router_config = TestConfig::new_router2(&ingester_config);
        let querier_config = TestConfig::new_querier2(&ingester_config);
        let compactor_config = TestConfig::new_compactor2(&ingester_config);

        // Set up the cluster  ====================================
        Self::new()
            .with_ingester(ingester_config)
            .await
            .with_router(router_config)
            .await
            .with_querier(querier_config)
            .await
            .with_compactor_config(compactor_config)
    }

    /// Create a non-shared "version 2" MiniCluster that has a router, ingester set to essentially
    /// never persist data (except on-demand), and querier. Save config for a compactor, but the
    /// compactor service should be run on-demand in tests using `compactor run-once` rather than
    /// using `run compactor`.
    pub async fn create_non_shared2_never_persist(database_url: String) -> Self {
        let ingester_config = TestConfig::new_ingester2_never_persist(&database_url);
        let router_config = TestConfig::new_router2(&ingester_config);
        let querier_config = TestConfig::new_querier2(&ingester_config);
        let compactor_config = TestConfig::new_compactor2(&ingester_config);

        // Set up the cluster  ====================================
        Self::new()
            .with_ingester(ingester_config)
            .await
            .with_router(router_config)
            .await
            .with_querier(querier_config)
            .await
            .with_compactor_config(compactor_config)
    }

    /// Create an all-(minus compactor)-in-one server with the specified configuration
    pub async fn create_all_in_one(test_config: TestConfig) -> Self {
        Self::new()
            .with_ingester(test_config.clone())
            .await
            .with_router(test_config.clone())
            .await
            .with_querier(test_config.clone())
            .await
    }

    /// create a router with the specified configuration
    pub async fn with_router(mut self, router_config: TestConfig) -> Self {
        self.router = Some(ServerFixture::create(router_config).await);
        self
    }

    /// create an ingester with the specified configuration;
    pub async fn with_ingester(mut self, ingester_config: TestConfig) -> Self {
        self.ingesters
            .push(ServerFixture::create(ingester_config).await);
        self
    }

    /// create a querier with the specified configuration;
    pub async fn with_querier(mut self, querier_config: TestConfig) -> Self {
        self.querier = Some(ServerFixture::create(querier_config).await);
        self
    }

    pub fn with_compactor_config(mut self, compactor_config: TestConfig) -> Self {
        self.compactor_config = Some(compactor_config);
        self
    }

    /// Retrieve the underlying router server, if set
    pub fn router(&self) -> &ServerFixture {
        self.router.as_ref().expect("router not initialized")
    }

    /// Retrieve one of the underlying ingester servers, if there are any
    pub fn ingester(&self) -> &ServerFixture {
        self.ingesters.first().unwrap()
    }

    /// Retrieve all of the underlying ingester servers
    pub fn ingesters(&self) -> &[ServerFixture] {
        &self.ingesters
    }

    /// Restart router.
    ///
    /// This will break all currently connected clients!
    pub async fn restart_router(&mut self) {
        let router = self.router.take().unwrap();
        let router = router.restart_server().await;
        self.router = Some(router);
    }

    /// Restart ingesters.
    ///
    /// This will break all currently connected clients!
    pub async fn restart_ingesters(&mut self) {
        let mut restarted = Vec::with_capacity(self.ingesters.len());
        for ingester in self.ingesters.drain(..) {
            restarted.push(ingester.restart_server().await);
        }
        self.ingesters = restarted;
    }

    /// Restart querier.
    ///
    /// This will break all currently connected clients!
    pub async fn restart_querier(&mut self) {
        let querier = self.querier.take().unwrap();
        let querier = querier.restart_server().await;
        self.querier = Some(querier);
    }

    /// Retrieve the underlying querier server, if set
    pub fn querier(&self) -> &ServerFixture {
        self.querier.as_ref().expect("querier not initialized")
    }

    /// Retrieve the compactor config, if set
    pub fn compactor_config(&self) -> &TestConfig {
        self.compactor_config
            .as_ref()
            .expect("compactor config not set")
    }

    /// Get a reference to the mini cluster's org.
    pub fn org_id(&self) -> &str {
        self.org_id.as_ref()
    }

    /// Get a reference to the mini cluster's bucket.
    pub fn bucket_id(&self) -> &str {
        self.bucket_id.as_ref()
    }

    /// Get a reference to the mini cluster's namespace.
    pub fn namespace(&self) -> &str {
        self.namespace.as_ref()
    }

    /// Get a reference to the mini cluster's namespace ID.
    pub async fn namespace_id(&self) -> NamespaceId {
        *self
            .namespace_id
            .get_or_init(|| async {
                let c = self
                    .router
                    .as_ref()
                    .expect("no router instance running")
                    .router_grpc_connection()
                    .into_grpc_connection();

                let id = SchemaServiceClient::new(c)
                    .get_schema(GetSchemaRequest {
                        namespace: self.namespace().to_string(),
                    })
                    .await
                    .expect("failed to query for namespace ID")
                    .into_inner()
                    .schema
                    .unwrap()
                    .id;

                NamespaceId::new(id)
            })
            .await
    }

    /// Get a the table ID for the given table.
    pub async fn table_id(&self, name: &str) -> TableId {
        let c = self
            .router
            .as_ref()
            .expect("no router instance running")
            .router_grpc_connection()
            .into_grpc_connection();

        let id = SchemaServiceClient::new(c)
            .get_schema(GetSchemaRequest {
                namespace: self.namespace().to_string(),
            })
            .await
            .expect("failed to query for namespace ID")
            .into_inner()
            .schema
            .unwrap()
            .tables
            .get(name)
            .expect("table not found")
            .id;

        TableId::new(id)
    }

    /// Writes the line protocol to the write_base/api/v2/write endpoint on the router into the
    /// org/bucket
    pub async fn write_to_router(&self, line_protocol: impl Into<String>) -> Response<Body> {
        write_to_router(
            line_protocol,
            &self.org_id,
            &self.bucket_id,
            self.router().router_http_base(),
        )
        .await
    }

    /// Write to the ingester using the gRPC interface directly, rather than through a router.
    pub async fn write_to_ingester(&self, line_protocol: impl Into<String>, table_name: &str) {
        write_to_ingester(
            line_protocol,
            self.namespace_id().await,
            self.table_id(table_name).await,
            self.ingester().ingester_grpc_connection(),
        )
        .await;
    }

    /// Query the ingester using flight directly, rather than through a querier.
    pub async fn query_ingester(
        &self,
        query: IngesterQueryRequest,
    ) -> Result<IngesterResponse, FlightError> {
        let querier_flight =
            influxdb_iox_client::flight::Client::new(self.ingester().ingester_grpc_connection());

        let ticket = Ticket {
            ticket: query.encode_to_vec().into(),
        };

        let mut performed_query = querier_flight
            .into_inner()
            .do_get(ticket)
            .await?
            .into_inner();

        let (msg, app_metadata) = next_message(&mut performed_query).await.unwrap();
        assert!(matches!(msg, DecodedPayload::None), "{msg:?}");

        let schema = next_message(&mut performed_query)
            .await
            .map(|(msg, _)| unwrap_schema(msg));

        let mut record_batches = vec![];
        while let Some((msg, _md)) = next_message(&mut performed_query).await {
            let batch = unwrap_record_batch(msg);
            record_batches.push(batch);
        }

        Ok(IngesterResponse {
            app_metadata,
            schema,
            record_batches,
        })
    }

    /// Ask all of the ingesters to persist their data.
    pub async fn persist_ingesters(&self) {
        for ingester in &self.ingesters {
            let mut ingester_client =
                influxdb_iox_client::ingester::Client::new(ingester.ingester_grpc_connection());

            ingester_client
                .persist(self.namespace().into())
                .await
                .unwrap();
        }
    }

    pub fn run_compaction(&self) {
        let (log_file, log_path) = NamedTempFile::new()
            .expect("opening log file")
            .keep()
            .expect("expected to keep");

        let stdout_log_file = log_file
            .try_clone()
            .expect("cloning file handle for stdout");
        let stderr_log_file = log_file;

        info!("****************");
        info!("Compactor run-once logging to {:?}", log_path);
        info!("****************");

        // If set in test environment, use that value, else default to info
        let log_filter =
            std::env::var("LOG_FILTER").unwrap_or_else(|_| "info,sqlx=warn".to_string());

        let mut command = Command::cargo_bin("influxdb_iox").unwrap();
        let command = command
            .arg("compactor")
            .arg("run-once")
            .env("LOG_FILTER", log_filter)
            .env(
                "INFLUXDB_IOX_CATALOG_DSN",
                self.compactor_config()
                    .dsn()
                    .as_ref()
                    .expect("dsn is required to run compaction"),
            )
            .env(
                "INFLUXDB_IOX_CATALOG_POSTGRES_SCHEMA_NAME",
                self.compactor_config().catalog_schema_name(),
            )
            .envs(self.compactor_config().env())
            // redirect output to log file
            .stdout(stdout_log_file)
            .stderr(stderr_log_file);

        log_command(command);

        command.ok().unwrap();
        dump_log_to_stdout("compactor run-once", &log_path);
    }

    /// Create a storage client connected to the querier member of the cluster
    pub fn querier_storage_client(
        &self,
    ) -> generated_types::storage_client::StorageClient<GrpcConnection> {
        let grpc_connection = self
            .querier()
            .querier_grpc_connection()
            .into_grpc_connection();

        generated_types::storage_client::StorageClient::new(grpc_connection)
    }
}

/// Gathers data from ingester Flight queries
#[derive(Debug)]
pub struct IngesterResponse {
    pub app_metadata: IngesterQueryResponseMetadata,
    pub schema: Option<SchemaRef>,
    pub record_batches: Vec<RecordBatch>,
}

/// holds shared server processes to share across tests
#[derive(Clone)]
struct SharedServers {
    router: Option<Weak<TestServer>>,
    ingesters: Vec<Weak<TestServer>>,
    querier: Option<Weak<TestServer>>,
    compactor_config: Option<TestConfig>,
}

/// Deferred creation of a mini cluster
struct CreatableMiniCluster {
    router: Option<Arc<TestServer>>,
    ingesters: Vec<Arc<TestServer>>,
    querier: Option<Arc<TestServer>>,
    compactor_config: Option<TestConfig>,
}

async fn create_if_needed(server: Option<Arc<TestServer>>) -> Option<ServerFixture> {
    if let Some(server) = server {
        Some(ServerFixture::create_from_existing(server).await)
    } else {
        None
    }
}

impl CreatableMiniCluster {
    async fn create(self) -> MiniCluster {
        let Self {
            router,
            ingesters,
            querier,
            compactor_config,
        } = self;

        let router_fixture = create_if_needed(router).await;
        let ingester_fixtures = ingesters
            .into_iter()
            .map(|ingester| create_if_needed(Some(ingester)))
            .collect::<FuturesOrdered<_>>()
            .collect::<Vec<_>>()
            .await
            .into_iter()
            .flatten()
            .collect();
        let querier_fixture = create_if_needed(querier).await;

        MiniCluster::new_from_fixtures(
            router_fixture,
            ingester_fixtures,
            querier_fixture,
            compactor_config,
        )
    }
}

impl SharedServers {
    /// Save the server processes in this shared servers as weak references
    pub fn new(cluster: &MiniCluster) -> Self {
        Self {
            router: cluster.router.as_ref().map(|c| c.weak()),
            ingesters: cluster.ingesters.iter().map(|c| c.weak()).collect(),
            querier: cluster.querier.as_ref().map(|c| c.weak()),
            compactor_config: cluster.compactor_config.clone(),
        }
    }

    /// Returns a creatable MiniCluster that will reuse the existing
    /// [TestServer]s. Return None if they are no longer active
    async fn creatable_cluster(&self) -> Option<CreatableMiniCluster> {
        // The goal of the following code is to bail out (return None
        // from the function) if any of the optional weak references
        // aren't present so that the cluster is recreated correctly
        Some(CreatableMiniCluster {
            router: server_from_weak(self.router.as_ref())?,
            ingesters: self
                .ingesters
                .iter()
                .flat_map(|ingester| server_from_weak(Some(ingester)).unwrap())
                .collect(),
            querier: server_from_weak(self.querier.as_ref())?,
            compactor_config: self.compactor_config.clone(),
        })
    }
}

/// Returns None if there was a weak server but we couldn't upgrade.
/// Returns Some(None) if there was no weak server
/// Returns Some(Some(fixture)) if there was a weak server that we can upgrade and make a fixture from
fn server_from_weak(server: Option<&Weak<TestServer>>) -> Option<Option<Arc<TestServer>>> {
    if let Some(server) = server.as_ref() {
        // return None if can't upgrade
        let server = server.upgrade()?;

        Some(Some(server))
    } else {
        Some(None)
    }
}

// For the new server versions. `GLOBAL_SHARED_SERVERS` can be removed and this can be renamed
// when the migration to router2/etc is complete.
static GLOBAL_SHARED_SERVERS2: Lazy<Mutex<Option<SharedServers>>> = Lazy::new(|| Mutex::new(None));
static GLOBAL_SHARED_SERVERS2_NEVER_PERSIST: Lazy<Mutex<Option<SharedServers>>> =
    Lazy::new(|| Mutex::new(None));

async fn next_message(
    performed_query: &mut FlightDataDecoder,
) -> Option<(DecodedPayload, IngesterQueryResponseMetadata)> {
    let DecodedFlightData { inner, payload } = performed_query.next().await.transpose().unwrap()?;

    // extract the metadata from the underlying FlightData structure
    let app_metadata = &inner.app_metadata[..];
    let app_metadata: IngesterQueryResponseMetadata = Message::decode(app_metadata).unwrap();

    Some((payload, app_metadata))
}

fn unwrap_schema(msg: DecodedPayload) -> SchemaRef {
    match msg {
        DecodedPayload::Schema(s) => s,
        _ => panic!("Unexpected message type: {msg:?}"),
    }
}

fn unwrap_record_batch(msg: DecodedPayload) -> RecordBatch {
    match msg {
        DecodedPayload::RecordBatch(b) => b,
        _ => panic!("Unexpected message type: {msg:?}"),
    }
}
