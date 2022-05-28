use crate::{
    rand_id, write_to_router, write_to_router_grpc, ServerFixture, TestConfig, TestServer,
};
use futures::{stream::FuturesOrdered, StreamExt};
use http::Response;
use hyper::Body;
use influxdb_iox_client::write::generated_types::{TableBatch, WriteResponse};
use observability_deps::tracing::{debug, info};
use std::{
    sync::{Arc, Weak},
    time::Instant,
};
use tokio::sync::Mutex;

/// Structure that holds services and helpful accessors
#[derive(Debug, Default)]
pub struct MiniCluster {
    /// Standard optional router
    router: Option<ServerFixture>,

    /// Standard optional ingester
    ingester: Option<ServerFixture>,

    /// Standard optional querier
    querier: Option<ServerFixture>,

    /// Standard optional compactor
    compactor: Option<ServerFixture>,

    /// Optional additional `ServerFixture`s that can be used for specific tests
    other_servers: Vec<ServerFixture>,

    // Potentially helpful data
    org_id: String,
    bucket_id: String,
    namespace: String,
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
        compactor: Option<ServerFixture>,
    ) -> Self {
        let org_id = rand_id();
        let bucket_id = rand_id();
        let namespace = format!("{}_{}", org_id, bucket_id);

        Self {
            router,
            ingester,
            querier,
            compactor,
            other_servers: vec![],

            org_id,
            bucket_id,
            namespace,
        }
    }

    /// Create a "standard" shared MiniCluster that has a router, ingester,
    /// querier
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
                info!(total_wait=?start.elapsed(), "created new new mini cluster from existing cluster");
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

    /// Create a non shared "standard" MiniCluster that has a router, ingester,
    /// querier
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
            .with_compactor(compactor_config)
            .await
    }

    /// Create an all-in-one server with the specified configuration
    pub async fn create_all_in_one(test_config: TestConfig) -> Self {
        Self::new()
            .with_router(test_config.clone())
            .await
            .with_ingester(test_config.clone())
            .await
            .with_querier(test_config.clone())
            .await
            .with_compactor(test_config)
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

    /// create an querier with the specified configuration;
    pub async fn with_querier(mut self, querier_config: TestConfig) -> Self {
        self.querier = Some(ServerFixture::create(querier_config).await);
        self
    }

    /// create a compactor with the specified configuration;
    pub async fn with_compactor(mut self, compactor_config: TestConfig) -> Self {
        self.compactor = Some(ServerFixture::create(compactor_config).await);
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

    /// Retrieve the underlying compactor server, if set
    pub fn compactor(&self) -> &ServerFixture {
        self.compactor.as_ref().expect("compactor not initialized")
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

    /// Writes the table batch to the gRPC write API on the router into the org/bucket
    pub async fn write_to_router_grpc(
        &self,
        table_batches: Vec<TableBatch>,
    ) -> tonic::Response<WriteResponse> {
        write_to_router_grpc(
            table_batches,
            &self.namespace,
            self.router().router_grpc_connection(),
        )
        .await
    }

    /// Get a reference to the mini cluster's other servers.
    pub fn other_servers(&self) -> &[ServerFixture] {
        self.other_servers.as_ref()
    }
}

/// holds shared server processes to share across tests
#[derive(Clone)]
struct SharedServers {
    router: Option<Weak<TestServer>>,
    ingester: Option<Weak<TestServer>>,
    querier: Option<Weak<TestServer>>,
    compactor: Option<Weak<TestServer>>,
}

/// Deferred creaton of a mini cluster
struct CreatableMiniCluster {
    router: Option<Arc<TestServer>>,
    ingester: Option<Arc<TestServer>>,
    querier: Option<Arc<TestServer>>,
    compactor: Option<Arc<TestServer>>,
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
            compactor,
        } = self;

        let mut servers = [
            create_if_needed(router),
            create_if_needed(ingester),
            create_if_needed(querier),
            create_if_needed(compactor),
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
            servers.next().unwrap(),
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
            compactor: cluster.compactor.as_ref().map(|c| c.weak()),
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
            compactor: server_from_weak(self.compactor.as_ref())?,
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

lazy_static::lazy_static! {
    static ref GLOBAL_SHARED_SERVERS: Mutex<Option<SharedServers>> = Mutex::new(None);
}
