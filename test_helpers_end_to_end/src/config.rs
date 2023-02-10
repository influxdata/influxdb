use crate::{addrs::BindAddresses, ServerType, UdpCapture};
use http::{header::HeaderName, HeaderValue};
use rand::Rng;
use std::{collections::HashMap, sync::Arc};
use tempfile::TempDir;

/// Options for creating test servers (`influxdb_iox` processes)
#[derive(Debug, Clone)]
pub struct TestConfig {
    /// environment variables to pass to server process. HashMap to avoid duplication
    env: HashMap<String, String>,

    /// Headers to add to all client requests
    client_headers: Vec<(HeaderName, HeaderValue)>,

    /// Server type
    server_type: ServerType,

    /// Catalog DSN value. Required unless you're running all-in-one in ephemeral mode.
    dsn: Option<String>,

    /// Catalog schema name
    catalog_schema_name: String,

    /// Object store directory, if needed.
    object_store_dir: Option<Arc<TempDir>>,

    /// WAL directory, if needed.
    wal_dir: Option<Arc<TempDir>>,

    /// Which ports this server should use
    addrs: Arc<BindAddresses>,
}

impl TestConfig {
    /// Create a new TestConfig. Tests should use one of the specific
    /// configuration setup below, such as [new_router2](Self::new_router2).
    fn new(
        server_type: ServerType,
        dsn: Option<String>,
        catalog_schema_name: impl Into<String>,
    ) -> Self {
        Self {
            env: HashMap::new(),
            client_headers: vec![],
            server_type,
            dsn,
            catalog_schema_name: catalog_schema_name.into(),
            object_store_dir: None,
            wal_dir: None,
            addrs: Arc::new(BindAddresses::default()),
        }
    }

    /// Create a minimal router2 configuration sharing configuration with the ingester2 config
    pub fn new_router2(ingester_config: &TestConfig) -> Self {
        assert_eq!(ingester_config.server_type(), ServerType::Ingester2);

        Self::new(
            ServerType::Router2,
            ingester_config.dsn().to_owned(),
            ingester_config.catalog_schema_name(),
        )
        .with_existing_object_store(ingester_config)
        .with_env("INFLUXDB_IOX_RPC_MODE", "2")
        .with_ingester_addresses(&[ingester_config.ingester_base()])
    }

    /// Create a minimal ingester2 configuration, using the dsn configuration specified. Set the
    /// persistence options such that it will persist as quickly as possible.
    pub fn new_ingester2(dsn: impl Into<String>) -> Self {
        let dsn = Some(dsn.into());
        Self::new(ServerType::Ingester2, dsn, random_catalog_schema_name())
            .with_new_object_store()
            .with_new_wal()
            .with_env("INFLUXDB_IOX_WAL_ROTATION_PERIOD_SECONDS", "1")
    }

    /// Create a minimal ingester2 configuration, using the dsn configuration specified. Set the
    /// persistence options such that it will likely never persist, to be able to test when data
    /// only exists in the ingester's memory.
    pub fn new_ingester2_never_persist(dsn: impl Into<String>) -> Self {
        let dsn = Some(dsn.into());
        Self::new(ServerType::Ingester2, dsn, random_catalog_schema_name())
            .with_new_object_store()
            .with_new_wal()
            // I didn't run my tests for a day, because that would be too long
            .with_env("INFLUXDB_IOX_WAL_ROTATION_PERIOD_SECONDS", "86400")
    }

    /// Create another ingester with the same dsn, catalog schema name, and object store, but with
    /// its own WAL directory and own addresses.
    pub fn another_ingester(ingester_config: &TestConfig) -> Self {
        Self {
            env: ingester_config.env.clone(),
            client_headers: ingester_config.client_headers.clone(),
            server_type: ServerType::Ingester2,
            dsn: ingester_config.dsn.clone(),
            catalog_schema_name: ingester_config.catalog_schema_name.clone(),
            object_store_dir: None,
            wal_dir: None,
            addrs: Arc::new(BindAddresses::default()),
        }
        .with_existing_object_store(ingester_config)
        .with_new_wal()
    }

    /// Create a minimal querier2 configuration from the specified ingester2 configuration, using
    /// the same dsn and object store, and pointing at the specified ingester.
    pub fn new_querier2(ingester_config: &TestConfig) -> Self {
        assert_eq!(ingester_config.server_type(), ServerType::Ingester2);

        Self::new_querier2_without_ingester2(ingester_config)
            .with_ingester_addresses(&[ingester_config.ingester_base()])
    }

    /// Create a minimal compactor configuration, using the dsn configuration from other
    pub fn new_compactor2(other: &TestConfig) -> Self {
        Self::new(
            ServerType::Compactor2,
            other.dsn().to_owned(),
            other.catalog_schema_name(),
        )
        .with_existing_object_store(other)
    }

    /// Create a minimal querier2 configuration from the specified ingester2 configuration, using
    /// the same dsn and object store, but without specifying the ingester2 addresses
    pub fn new_querier2_without_ingester2(ingester_config: &TestConfig) -> Self {
        Self::new(
            ServerType::Querier2,
            ingester_config.dsn().to_owned(),
            ingester_config.catalog_schema_name(),
        )
        .with_existing_object_store(ingester_config)
        .with_env("INFLUXDB_IOX_RPC_MODE", "2")
        // Hard code query threads so query plans do not vary based on environment
        .with_env("INFLUXDB_IOX_NUM_QUERY_THREADS", "4")
    }

    /// Create a minimal all in one configuration
    pub fn new_all_in_one(dsn: Option<String>) -> Self {
        Self::new(ServerType::AllInOne, dsn, random_catalog_schema_name()).with_new_object_store()
    }

    /// Configure tracing capture
    pub fn with_tracing(self, udp_capture: &UdpCapture) -> Self {
        self.with_env("TRACES_EXPORTER", "jaeger")
            .with_env("TRACES_EXPORTER_JAEGER_AGENT_HOST", udp_capture.ip())
            .with_env("TRACES_EXPORTER_JAEGER_AGENT_PORT", udp_capture.port())
            .with_env(
                "TRACES_EXPORTER_JAEGER_TRACE_CONTEXT_HEADER_NAME",
                "custom-trace-header",
            )
            .with_client_header("custom-trace-header", "4:3:2:1")
    }

    /// Configure a custom debug name for tracing
    pub fn with_tracing_debug_name(self, custom_debug_name: &str) -> Self {
        // setup a custom debug name (to ensure it gets plumbed through)
        self.with_env("TRACES_EXPORTER_JAEGER_DEBUG_NAME", custom_debug_name)
            .with_client_header(custom_debug_name, "some-debug-id")
    }

    pub fn with_ingester_addresses(
        self,
        ingester_addresses: &[impl std::borrow::Borrow<str>],
    ) -> Self {
        self.with_env(
            "INFLUXDB_IOX_INGESTER_ADDRESSES",
            ingester_addresses.join(","),
        )
    }

    // Get the catalog DSN URL if set.
    pub fn dsn(&self) -> &Option<String> {
        &self.dsn
    }

    // Get the catalog postgres schema name
    pub fn catalog_schema_name(&self) -> &str {
        &self.catalog_schema_name
    }

    /// add a name=value environment variable when starting the server
    ///
    /// Should not be called directly, but instead all mapping to
    /// environment variables should be done via this structure
    fn with_env(mut self, name: impl Into<String>, value: impl Into<String>) -> Self {
        self.env.insert(name.into(), value.into());
        self
    }

    /// copy the specified environment variables from other; Panic's if they do not exist.
    ///
    /// Should not be called directly, but instead all mapping to
    /// environment variables should be done via this structure
    fn copy_env(self, name: impl Into<String>, other: &TestConfig) -> Self {
        let name = name.into();
        let value = match other.env.get(&name) {
            Some(v) => v.clone(),
            None => panic!(
                "Cannot copy {} from existing config. Available values are: {:#?}",
                name, other.env
            ),
        };

        self.with_env(name, value)
    }

    /// add a name=value http header to all client requests made to the server
    fn with_client_header(mut self, name: impl AsRef<str>, value: impl AsRef<str>) -> Self {
        self.client_headers.push((
            name.as_ref().parse().expect("valid header name"),
            value.as_ref().parse().expect("valid header value"),
        ));
        self
    }

    /// Configures a new WAL
    fn with_new_wal(mut self) -> Self {
        let tmpdir = TempDir::new().expect("cannot create tmp dir");

        let wal_string = tmpdir.path().display().to_string();
        self.wal_dir = Some(Arc::new(tmpdir));
        self.with_env("INFLUXDB_IOX_WAL_DIRECTORY", wal_string)
    }

    /// Configures a new object store
    fn with_new_object_store(mut self) -> Self {
        let tmpdir = TempDir::new().expect("cannot create tmp dir");

        let object_store_string = tmpdir.path().display().to_string();
        self.object_store_dir = Some(Arc::new(tmpdir));
        self.with_env("INFLUXDB_IOX_OBJECT_STORE", "file")
            .with_env("INFLUXDB_IOX_DB_DIR", object_store_string)
    }

    /// Configures this TestConfig to use the same object store as other
    fn with_existing_object_store(mut self, other: &TestConfig) -> Self {
        // copy a reference to the temp dir, if any
        self.object_store_dir = other.object_store_dir.clone();
        self.copy_env("INFLUXDB_IOX_OBJECT_STORE", other)
            .copy_env("INFLUXDB_IOX_DB_DIR", other)
    }

    /// Configure maximum per-table query bytes for the querier.
    pub fn with_querier_mem_pool_bytes(self, bytes: usize) -> Self {
        self.with_env("INFLUXDB_IOX_EXEC_MEM_POOL_BYTES", bytes.to_string())
    }

    /// Get the test config's server type.
    #[must_use]
    pub fn server_type(&self) -> ServerType {
        self.server_type
    }

    /// Get a reference to the test config's env.
    pub fn env(&self) -> impl Iterator<Item = (&str, &str)> {
        self.env.iter().map(|(k, v)| (k.as_str(), v.as_str()))
    }

    /// Get a reference to the test config's client headers.
    #[must_use]
    pub fn client_headers(&self) -> &[(HeaderName, HeaderValue)] {
        self.client_headers.as_ref()
    }

    /// Get a reference to the test config's addrs.
    #[must_use]
    pub fn addrs(&self) -> &BindAddresses {
        &self.addrs
    }

    /// return the base ingester gRPC address, such as
    /// `http://localhost:8082/`
    pub fn ingester_base(&self) -> Arc<str> {
        self.addrs().ingester_grpc_api().client_base()
    }
}

fn random_catalog_schema_name() -> String {
    let mut rng = rand::thread_rng();

    (&mut rng)
        .sample_iter(rand::distributions::Alphanumeric)
        .filter(|c| c.is_ascii_alphabetic())
        .take(20)
        .map(char::from)
        .collect::<String>()
}
