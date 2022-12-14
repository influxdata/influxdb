use crate::{
    dump_log_to_stdout, log_command, rand_id, write_to_router, ServerFixture, TestConfig,
    TestServer,
};
use assert_cmd::prelude::*;
use data_types::{NamespaceId, TableId};
use futures::{stream::FuturesOrdered, StreamExt};
use http::Response;
use hyper::Body;
use influxdb_iox_client::{
    connection::GrpcConnection,
    schema::generated_types::{schema_service_client::SchemaServiceClient, GetSchemaRequest},
};
use observability_deps::tracing::{debug, info};
use once_cell::sync::Lazy;
use std::{
    process::Command,
    sync::{Arc, Weak},
    time::Instant,
};
use tempfile::NamedTempFile;
use tokio::sync::{Mutex, OnceCell};

/// Structure that holds services and helpful accessors. Does not include compactor; that is always
/// run separately on-demand in tests.
#[derive(Debug, Default)]
pub struct MiniCluster {
    /// Standard optional router
    router: Option<ServerFixture>,

    /// Standard optional ingester
    ingester: Option<ServerFixture>,

    /// Standard optional querier
    querier: Option<ServerFixture>,

    /// Standard optional compactor configuration, to be used on-demand
    compactor_config: Option<TestConfig>,

    /// Optional additional `ServerFixture`s that can be used for specific tests
    other_servers: Vec<ServerFixture>,

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
        let namespace = format!("{}_{}", org_id, bucket_id);

        Self {
            org_id,
            bucket_id,
            namespace,
            ..Self::default()
        }
    }

    /// Create a new MiniCluster that shares the same underlying
    /// servers but has a new unique namespace and set of connections
    ///
    /// Note this is an internal implementation -- please use
    /// [`create_shared`](Self::create_shared), and [`new`](Self::new) to create new MiniClusters.
    fn new_from_fixtures(
        router: Option<ServerFixture>,
        ingester: Option<ServerFixture>,
        querier: Option<ServerFixture>,
        compactor_config: Option<TestConfig>,
    ) -> Self {
        let org_id = rand_id();
        let bucket_id = rand_id();
        let namespace = format!("{}_{}", org_id, bucket_id);

        Self {
            router,
            ingester,
            querier,
            compactor_config,
            other_servers: vec![],

            org_id,
            bucket_id,
            namespace,
            namespace_id: Default::default(),
        }
    }

    /// Create a "standard" shared MiniCluster that has a router, ingester,
    /// querier (but no compactor as that should be run on-demand in tests)
    ///
    /// Note: Since the underlying server processes are shared across multiple
    /// tests so all users of this MiniCluster should only modify
    /// their namespace
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
                // drop the lock prior to calling create() to allow
                // others to proceed
                std::mem::drop(shared_servers);
                let new_self = cluster.create().await;
                info!(
                    total_wait=?start.elapsed(),
                    "created new new mini cluster from existing cluster"
                );
                return new_self;
            } else {
                info!("some server proceses of previous cluster have already returned");
            }
        }

        // Have to make a new one
        info!("Create a new server");
        let new_cluster = Self::create_non_shared_standard(database_url).await;

        // Update the shared servers to point at the newly created server proesses
        *shared_servers = Some(SharedServers::new(&new_cluster));
        new_cluster
    }

    /// Create a non shared "standard" MiniCluster that has a router, ingester, querier. Save
    /// config for a compactor, but the compactor should be run on-demand in tests using `compactor
    /// run-once` rather than using `run compactor`.
    pub async fn create_non_shared_standard(database_url: String) -> Self {
        let router_config = TestConfig::new_router(&database_url);
        let ingester_config = TestConfig::new_ingester(&router_config);
        let querier_config = TestConfig::new_querier(&ingester_config);
        let compactor_config = TestConfig::new_compactor(&ingester_config);

        // Set up the cluster  ====================================
        Self::new()
            .with_router(router_config)
            .await
            .with_ingester(ingester_config)
            .await
            .with_querier(querier_config)
            .await
            .with_compactor_config(compactor_config)
    }

    pub async fn create_non_shared2(database_url: String) -> Self {
        let ingester_config = TestConfig::new_ingester2(&database_url);
        let router_config = TestConfig::new_router2(&ingester_config);

        // Set up the cluster  ====================================
        Self::new()
            .with_ingester(ingester_config)
            .await
            .with_router(router_config)
            .await
    }

    /// Create an all-(minus compactor)-in-one server with the specified configuration
    pub async fn create_all_in_one(test_config: TestConfig) -> Self {
        Self::new()
            .with_router(test_config.clone())
            .await
            .with_ingester(test_config.clone())
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
        self.ingester = Some(ServerFixture::create(ingester_config).await);
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

    /// create another server compactor with the specified configuration;
    pub async fn with_other(mut self, config: TestConfig) -> Self {
        self.other_servers.push(ServerFixture::create(config).await);
        self
    }

    /// Retrieve the underlying router server, if set
    pub fn router(&self) -> &ServerFixture {
        self.router.as_ref().expect("router not initialized")
    }

    /// Retrieve the underlying ingester server, if set
    pub fn ingester(&self) -> &ServerFixture {
        self.ingester.as_ref().expect("ingester not initialized")
    }

    /// Restart ingester.
    ///
    /// This will break all currently connected clients!
    pub async fn restart_ingester(&mut self) {
        self.ingester = Some(
            self.ingester
                .take()
                .expect("ingester not initialized")
                .restart_server()
                .await,
        )
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

    /// Get a reference to the mini cluster's other servers.
    pub fn other_servers(&self) -> &[ServerFixture] {
        self.other_servers.as_ref()
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

/// holds shared server processes to share across tests
#[derive(Clone)]
struct SharedServers {
    router: Option<Weak<TestServer>>,
    ingester: Option<Weak<TestServer>>,
    querier: Option<Weak<TestServer>>,
    compactor_config: Option<TestConfig>,
}

/// Deferred creation of a mini cluster
struct CreatableMiniCluster {
    router: Option<Arc<TestServer>>,
    ingester: Option<Arc<TestServer>>,
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
            ingester,
            querier,
            compactor_config,
        } = self;

        let mut servers = [
            create_if_needed(router),
            create_if_needed(ingester),
            create_if_needed(querier),
        ]
        .into_iter()
        // Use futures ordered to run them all in parallel (hopfully)
        .collect::<FuturesOrdered<_>>()
        .collect::<Vec<Option<ServerFixture>>>()
        .await
        .into_iter();

        // ServerFixtures go in the same order as they came out
        MiniCluster::new_from_fixtures(
            servers.next().unwrap(),
            servers.next().unwrap(),
            servers.next().unwrap(),
            compactor_config,
        )
    }
}

impl SharedServers {
    /// Save the server processes in this shared servers as weak references
    pub fn new(cluster: &MiniCluster) -> Self {
        assert!(
            cluster.other_servers.is_empty(),
            "other servers not yet handled in shared mini clusters"
        );
        Self {
            router: cluster.router.as_ref().map(|c| c.weak()),
            ingester: cluster.ingester.as_ref().map(|c| c.weak()),
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
            ingester: server_from_weak(self.ingester.as_ref())?,
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
