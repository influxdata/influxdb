use std::{collections::HashMap, sync::Arc};

use http::{header::HeaderName, HeaderValue};
use tempfile::TempDir;

use super::ServerType;

/// Options for creating test servers (`influxdb_iox` processes)
#[derive(Debug, Clone)]
pub struct TestConfig {
    /// environment variables to pass to server process. HashMap to avoid duplication
    env: HashMap<String, String>,

    /// Headers to add to all client requests
    client_headers: Vec<(HeaderName, HeaderValue)>,

    /// Server type
    server_type: ServerType,

    /// Catalog DSN value
    dsn: String,

    /// Write buffer directory, if needed
    write_buffer_dir: Option<Arc<TempDir>>,
}

impl TestConfig {
    /// Create a new TestConfig (tests should use one of the specific
    /// configuration setup below, such as [new_router2]
    fn new(server_type: ServerType, dsn: impl Into<String>) -> Self {
        Self {
            env: HashMap::new(),
            client_headers: vec![],
            server_type,
            dsn: dsn.into(),
            write_buffer_dir: None,
        }
    }

    /// Create a minimal router2 configuration
    pub fn new_router2(dsn: impl Into<String>) -> Self {
        Self::new(ServerType::Router2, dsn).with_new_write_buffer()
    }

    /// Create a minimal ingester configuration, using the dsn and
    /// write buffer configuration from other
    pub fn new_ingester(other: &TestConfig) -> Self {
        Self::new(ServerType::Ingester, other.dsn())
            .with_existing_write_buffer(other)
            .with_default_ingester_options()
    }

    /// Create a minimal all in one configuration
    pub fn new_all_in_one(dsn: impl Into<String>) -> Self {
        Self::new(ServerType::AllInOne, dsn)
            .with_new_write_buffer()
            .with_default_ingester_options()
            // Aggressive expulsion of parquet files
            .with_env("INFLUXDB_IOX_PAUSE_INGEST_SIZE_BYTES", "2")
            .with_env("INFLUXDB_IOX_PERSIST_MEMORY_THRESHOLD_BYTES", "1")
    }

    // Get the catalog DSN URL and panic if it's not set
    pub fn dsn(&self) -> &str {
        &self.dsn
    }

    /// Adds default ingester options
    fn with_default_ingester_options(self) -> Self {
        self.with_env("INFLUXDB_IOX_PAUSE_INGEST_SIZE_BYTES", "20")
            .with_env("INFLUXDB_IOX_PERSIST_MEMORY_THRESHOLD_BYTES", "10")
            .with_env("INFLUXDB_IOX_WRITE_BUFFER_PARTITION_RANGE_START", "0")
            .with_env("INFLUXDB_IOX_WRITE_BUFFER_PARTITION_RANGE_END", "0")
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
                "Can not copy {} from existing config. Available values are: {:#?}",
                name, other.env
            ),
        };

        self.with_env(name, value)
    }

    /// Configures a new write buffer
    pub fn with_new_write_buffer(mut self) -> Self {
        let n_sequencers = 1;
        let tmpdir = TempDir::new().expect("can not create tmp dir");
        let write_buffer_string = tmpdir.path().display().to_string();
        self.write_buffer_dir = Some(Arc::new(tmpdir));

        self.with_env("INFLUXDB_IOX_WRITE_BUFFER_TYPE", "file")
            .with_env(
                "INFLUXDB_IOX_WRITE_BUFFER_AUTO_CREATE_TOPICS",
                n_sequencers.to_string(),
            )
            .with_env("INFLUXDB_IOX_WRITE_BUFFER_ADDR", &write_buffer_string)
    }

    /// Configures this TestConfig to use the same write buffer as other
    pub fn with_existing_write_buffer(mut self, other: &TestConfig) -> Self {
        // get the directory, if any
        self.write_buffer_dir = other.write_buffer_dir.clone();

        // copy the environment variables
        self.copy_env("INFLUXDB_IOX_WRITE_BUFFER_TYPE", other)
            .copy_env("INFLUXDB_IOX_WRITE_BUFFER_AUTO_CREATE_TOPICS", other)
            .copy_env("INFLUXDB_IOX_WRITE_BUFFER_ADDR", other)
    }

    // add a name=value http header to all client requests made to the server
    pub fn with_client_header(mut self, name: impl AsRef<str>, value: impl AsRef<str>) -> Self {
        self.client_headers.push((
            name.as_ref().parse().expect("valid header name"),
            value.as_ref().parse().expect("valid header value"),
        ));
        self
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
}
