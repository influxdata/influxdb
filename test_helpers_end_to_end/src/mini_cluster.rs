use crate::{
    dump_log_to_stdout, log_command, rand_id, server_type::AddAddrEnv, write_to_ingester,
    write_to_router, ServerFixture, TestConfig, TestServer,
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
    catalog::generated_types::{
        catalog_service_client::CatalogServiceClient, GetPartitionsByTableIdRequest,
    },
    connection::{Connection, GrpcConnection},
    schema::generated_types::{schema_service_client::SchemaServiceClient, GetSchemaRequest},
};
use ingester_query_grpc::influxdata::iox::ingester::v1::{
    IngesterQueryRequest, IngesterQueryResponseMetadata,
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

    pub fn new_based_on_tenancy(is_single_tenant: bool) -> Self {
        let org_id = rand_id();
        let bucket_id = rand_id();
        let namespace = match is_single_tenant {
            true => bucket_id.clone(),
            false => format!("{org_id}_{bucket_id}"),
        };

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
    /// [`create_shared`](Self::create_shared) and [`new`](Self::new) to create new MiniClusters.
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
    pub async fn create_shared(database_url: String) -> Self {
        let start = Instant::now();
        let mut shared_servers = GLOBAL_SHARED_SERVERS.lock().await;
        debug!(mutex_wait=?start.elapsed(), "creating standard cluster");

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
                    "created new mini cluster from existing cluster"
                );
                return new_self;
            } else {
                info!("some server proceses of previous cluster have already returned");
            }
        }

        // Have to make a new one
        info!("Create a new server");
        let new_cluster = Self::create_non_shared(database_url).await;

        // Update the shared servers to point at the newly created server proesses
        *shared_servers = Some(SharedServers::new(&new_cluster));
        new_cluster
    }

    /// Create a shared  MiniCluster that has a router, ingester set to essentially
    /// never persist data (except on-demand), and querier. Save config for a compactor, but the
    /// compactor service should be run on-demand in tests using `compactor run-once` rather than
    /// using `run compactor`.
    ///
    /// Note: Because the underlying server processes are shared across multiple tests, all users
    /// of this `MiniCluster` instance should only modify their own unique namespace.
    pub async fn create_shared_never_persist(database_url: String) -> Self {
        let start = Instant::now();
        let mut shared_servers = GLOBAL_SHARED_SERVERS_NEVER_PERSIST.lock().await;
        debug!(mutex_wait=?start.elapsed(), "creating standard cluster");

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
                    "created new mini cluster from existing cluster"
                );
                return new_self;
            } else {
                info!("some server proceses of previous cluster have already returned");
            }
        }

        // Have to make a new one
        info!("Create a new server set to never persist");
        let new_cluster = Self::create_non_shared_never_persist(database_url).await;

        // Update the shared servers to point at the newly created server proesses
        *shared_servers = Some(SharedServers::new(&new_cluster));
        new_cluster
    }

    /// Create a non-shared "standard" MiniCluster that has a router, ingester,
    /// querier. Save config for a compactor, but the compactor service should be run on-demand in
    /// tests using `compactor run-once` rather than using `run compactor`.
    pub async fn create_non_shared(database_url: String) -> Self {
        let ingester_config = TestConfig::new_ingester(&database_url);
        let router_config = TestConfig::new_router(&ingester_config);
        let querier_config = TestConfig::new_querier(&ingester_config);
        let compactor_config = TestConfig::new_compactor(&ingester_config);

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

    /// Create a non-shared MiniCluster that has a router, ingester set to essentially
    /// never persist data (except on-demand), and querier. Save config for a compactor, but the
    /// compactor service should be run on-demand in tests using `compactor run-once` rather than
    /// using `run compactor`.
    pub async fn create_non_shared_never_persist(database_url: String) -> Self {
        let ingester_config = TestConfig::new_ingester_never_persist(&database_url);
        let router_config = TestConfig::new_router(&ingester_config);
        let querier_config = TestConfig::new_querier(&ingester_config);
        let compactor_config = TestConfig::new_compactor(&ingester_config);

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

    /// Create a non-shared MiniCluster that has a router,
    /// ingester, and querier. The router and querier will be configured
    /// to use the authorization service and will require all requests to
    /// be authorized. Save config for a compactor, but the compactor service
    /// should be run on-demand in tests using `compactor run-once` rather
    /// than using `run compactor`.
    pub async fn create_non_shared_with_authz(
        database_url: String,
        authz_addr: impl Into<String> + Clone,
    ) -> Self {
        let ingester_config = TestConfig::new_ingester(&database_url);
        let router_config =
            TestConfig::new_router(&ingester_config).with_single_tenancy(authz_addr.clone());
        let querier_config =
            TestConfig::new_querier(&ingester_config).with_single_tenancy(authz_addr);
        let compactor_config = TestConfig::new_compactor(&ingester_config);

        // Set up the cluster  ====================================
        Self::new_based_on_tenancy(true)
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

    /// Gracefully stop all ingesters and wait for them to exit.
    ///
    /// If the shutdown does not complete within
    /// [`GRACEFUL_SERVER_STOP_TIMEOUT`] it is killed.
    ///
    /// [`GRACEFUL_SERVER_STOP_TIMEOUT`]:
    ///     crate::server_fixture::GRACEFUL_SERVER_STOP_TIMEOUT
    pub fn gracefully_stop_ingesters(&mut self) {
        self.ingesters = vec![];
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

    /// Get all partition keys for the given table.
    pub async fn partition_keys(
        &self,
        table_name: &str,
        namespace_name: Option<String>,
    ) -> Vec<String> {
        let namespace_name = namespace_name.unwrap_or(self.namespace().to_string());

        let c = self
            .router
            .as_ref()
            .expect("no router instance running")
            .router_grpc_connection()
            .into_grpc_connection();

        let table_id = SchemaServiceClient::new(c.clone())
            .get_schema(GetSchemaRequest {
                namespace: namespace_name.clone(),
            })
            .await
            .expect("failed to query for namespace ID")
            .into_inner()
            .schema
            .unwrap()
            .tables
            .get(table_name)
            .expect("table not found")
            .id;

        CatalogServiceClient::new(c)
            .get_partitions_by_table_id(GetPartitionsByTableIdRequest { table_id })
            .await
            .expect("failed to query for partitions")
            .into_inner()
            .partitions
            .into_iter()
            .map(|p| p.key)
            .collect()
    }

    /// Writes the line protocol to the write_base/api/v2/write endpoint on the router into the
    /// org/bucket
    pub async fn write_to_router(
        &self,
        line_protocol: impl Into<String>,
        authorization: Option<&str>,
    ) -> Response<Body> {
        write_to_router(
            line_protocol,
            &self.org_id,
            &self.bucket_id,
            self.router().router_http_base(),
            authorization,
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

    /// Query the ingester specified by the given gRPC connection using flight directly, rather than through a querier.
    pub async fn query_ingester(
        &self,
        query: IngesterQueryRequest,
        ingester_grpc_connection: Connection,
    ) -> Result<IngesterResponse, FlightError> {
        let querier_flight = influxdb_iox_client::flight::Client::new(ingester_grpc_connection);

        let ticket = Ticket {
            ticket: query.encode_to_vec().into(),
        };

        let mut performed_query = querier_flight
            .into_inner()
            .do_get(ticket)
            .await?
            .into_inner();

        let mut partitions = vec![];
        let mut current_partition = None;
        while let Some((msg, app_metadata)) = next_message(&mut performed_query).await {
            match msg {
                DecodedPayload::None => {
                    if let Some(p) = std::mem::take(&mut current_partition) {
                        partitions.push(p);
                    }
                    current_partition = Some(IngesterResponsePartition {
                        app_metadata,
                        schema: None,
                        record_batches: vec![],
                    });
                }
                DecodedPayload::Schema(schema) => {
                    let current_partition =
                        current_partition.as_mut().expect("schema w/o partition");
                    assert!(
                        current_partition.schema.is_none(),
                        "got two schemas for a single partition"
                    );
                    current_partition.schema = Some(schema);
                }
                DecodedPayload::RecordBatch(batch) => {
                    let current_partition =
                        current_partition.as_mut().expect("batch w/o partition");
                    assert!(current_partition.schema.is_some(), "batch w/o schema");
                    current_partition.record_batches.push(batch);
                }
            }
        }

        if let Some(p) = current_partition {
            partitions.push(p);
        }

        Ok(IngesterResponse { partitions })
    }

    /// Ask all of the ingesters to persist their data for the cluster namespace.
    pub async fn persist_ingesters(&self) {
        self.persist_ingesters_by_namespace(None).await;
    }

    /// Ask all of the ingesters to persist their data for a specified namespace, or the cluster
    /// namespace if none specified.
    pub async fn persist_ingesters_by_namespace(&self, namespace: Option<String>) {
        let namespace = namespace.unwrap_or_else(|| self.namespace().into());
        for ingester in &self.ingesters {
            let mut ingester_client =
                influxdb_iox_client::ingester::Client::new(ingester.ingester_grpc_connection());

            ingester_client.persist(namespace.clone()).await.unwrap();
        }
    }

    pub fn run_compaction(&self) -> Result<(), String> {
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
            .arg("run")
            .arg("compactor")
            .arg("--compaction-process-once")
            .arg("--compaction-process-all-partitions")
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
            .add_addr_env(
                self.compactor_config().server_type(),
                self.compactor_config().addrs(),
            )
            // redirect output to log file
            .stdout(stdout_log_file)
            .stderr(stderr_log_file);

        log_command(command);

        let run_result = command.ok();

        dump_log_to_stdout("compactor run-once", &log_path);

        // Return the command output from the log file as the error message to enable
        // assertions on the error message contents
        run_result.map_err(|_| std::fs::read_to_string(&log_path).unwrap())?;

        Ok(())
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
    pub partitions: Vec<IngesterResponsePartition>,
}

#[derive(Debug)]
pub struct IngesterResponsePartition {
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

static GLOBAL_SHARED_SERVERS: Lazy<Mutex<Option<SharedServers>>> = Lazy::new(|| Mutex::new(None));
static GLOBAL_SHARED_SERVERS_NEVER_PERSIST: Lazy<Mutex<Option<SharedServers>>> =
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
