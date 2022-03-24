use http::{header::HeaderName, HeaderValue};

use super::ServerType;

/// Options for creating test servers (`influxdb_iox` processes)
#[derive(Debug, Clone)]
pub struct TestConfig {
    /// Additional environment variables to pass
    env: Vec<(String, String)>,

    /// Headers to add to all client requests
    client_headers: Vec<(HeaderName, HeaderValue)>,

    /// Server type
    server_type: ServerType,

    /// Catalog DSN value
    dsn: Option<String>,
}

impl TestConfig {
    pub fn new(server_type: ServerType) -> Self {
        Self {
            env: vec![],
            client_headers: vec![],
            server_type,
            dsn: None,
        }
    }

    // change server type
    pub fn with_server_type(mut self, server_type: ServerType) -> Self {
        self.server_type = server_type;
        self
    }

    /// Set Postgres catalog DSN URL
    pub fn with_postgres_catalog(mut self, dsn: &str) -> Self {
        self.dsn = Some(dsn.into());

        self
    }

    // Get the catalog DSN URL and panic if it's not set
    pub fn dsn(&self) -> &str {
        self.dsn
            .as_ref()
            .expect("Test Config must have a catalog configured")
    }

    // add a name=value environment variable when starting the server
    pub fn with_env(mut self, name: impl Into<String>, value: impl Into<String>) -> Self {
        self.env.push((name.into(), value.into()));
        self
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
    #[must_use]
    pub fn env(&self) -> &[(String, String)] {
        self.env.as_ref()
    }

    /// Get a reference to the test config's client headers.
    #[must_use]
    pub fn client_headers(&self) -> &[(HeaderName, HeaderValue)] {
        self.client_headers.as_ref()
    }
}
