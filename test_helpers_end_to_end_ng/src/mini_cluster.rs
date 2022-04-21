use http::Response;
use hyper::Body;

use crate::{rand_id, write_to_router, ServerFixture, TestConfig, TestServer};

/// Structure that holds NG services and helpful accessors
#[derive(Debug, Default)]
pub struct MiniCluster {
    /// Standard optional router2
    router2: Option<ServerFixture>,

    /// Standard optional ingster2
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

    /// Create a "standard" MiniCluster that has a router, ingester,
    /// querier
    ///
    /// Long term plan is that this will be shared across multiple tests if possible
    pub async fn create_standard(database_url: String) -> MiniCluster {
        let router2_config = TestConfig::new_router2(&database_url);
        // fast parquet
        let ingester_config =
            TestConfig::new_ingester(&router2_config).with_fast_parquet_generation();
        let querier_config = TestConfig::new_querier(&ingester_config);

        // Set up the cluster  ====================================
        Self::new()
            .with_router2(router2_config)
            .await
            .with_ingester(ingester_config)
            .await
            .with_querier(querier_config)
            .await
    }

    /// return a "standard" MiniCluster that has a router, ingester,
    /// querier and quickly persists files to parquet
    pub async fn ccreate_quickly_peristing(database_url: String) -> MiniCluster {
        let router2_config = TestConfig::new_router2(&database_url);
        // fast parquet
        let ingester_config =
            TestConfig::new_ingester(&router2_config).with_fast_parquet_generation();
        let querier_config = TestConfig::new_querier(&ingester_config);

        // Set up the cluster  ====================================
        Self::new()
            .with_router2(router2_config)
            .await
            .with_ingester(ingester_config)
            .await
            .with_querier(querier_config)
            .await
    }

    /// create a router2 with the specified configuration
    pub async fn with_router2(mut self, router2_config: TestConfig) -> Self {
        self.router2 = Some(ServerFixture::create(router2_config).await);
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

    /// Retrieve the underlying router2 server, if set
    pub fn router2(&self) -> &TestServer {
        self.router2
            .as_ref()
            .expect("router2 not initialized")
            .server()
    }

    /// Retrieve the underlying ingester server, if set
    pub fn ingester(&self) -> &TestServer {
        self.ingester
            .as_ref()
            .expect("ingester not initialized")
            .server()
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
    pub fn querier(&self) -> &TestServer {
        self.querier
            .as_ref()
            .expect("querier not initialized")
            .server()
    }

    /// Retrieve the underlying compactor server, if set
    pub fn compactor(&self) -> &TestServer {
        self.compactor
            .as_ref()
            .expect("compactor not initialized")
            .server()
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

    /// Writes the line protocol to the write_base/api/v2/write endpoint on the router into the org/bucket
    pub async fn write_to_router(&self, line_protocol: impl Into<String>) -> Response<Body> {
        write_to_router(
            line_protocol,
            &self.org_id,
            &self.bucket_id,
            self.router2().router_http_base(),
        )
        .await
    }

    /// Get a reference to the mini cluster's other servers.
    pub fn other_servers(&self) -> &[ServerFixture] {
        self.other_servers.as_ref()
    }
}
