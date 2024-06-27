use std::{
    net::{SocketAddr, SocketAddrV4, TcpListener},
    process::{Child, Command, Stdio},
    time::Duration,
};

use arrow::record_batch::RecordBatch;
use arrow_flight::{decode::FlightRecordBatchStream, FlightClient};
use assert_cmd::cargo::CommandCargoExt;
use futures::TryStreamExt;
use influxdb3_client::Precision;
use influxdb_iox_client::flightsql::FlightSqlClient;
use reqwest::header::{HeaderMap, HeaderName, HeaderValue};
use reqwest::Response;

mod auth;
mod flight;
mod limits;
mod ping;
mod query;
mod system_tables;
mod write;

/// Configuration for a [`TestServer`]
#[derive(Debug, Default)]
pub struct TestConfig {
    auth_token: Option<(String, String)>,
}

impl TestConfig {
    /// Set the auth token for this [`TestServer`]
    pub fn auth_token<S: Into<String>, R: Into<String>>(
        mut self,
        hashed_token: S,
        raw_token: R,
    ) -> Self {
        self.auth_token = Some((hashed_token.into(), raw_token.into()));
        self
    }

    /// Spawn a new [`TestServer`] with this configuration
    ///
    /// This will run the `influxdb3 serve` command, and bind its HTTP
    /// address to a random port on localhost.
    pub async fn spawn(self) -> TestServer {
        TestServer::spawn_inner(self).await
    }

    fn as_args(&self) -> Vec<&str> {
        let mut args = vec![];
        if let Some((token, _)) = &self.auth_token {
            args.append(&mut vec!["--bearer-token", token]);
        }
        args
    }
}

/// A running instance of the `influxdb3 serve` process
///
/// Logs will be emitted to stdout/stderr if the TEST_LOG environment
/// variable is set, e.g.,
/// ```
/// TEST_LOG= cargo test
/// ```
pub struct TestServer {
    config: TestConfig,
    bind_addr: SocketAddr,
    server_process: Child,
    http_client: reqwest::Client,
}

impl TestServer {
    /// Spawn a new [`TestServer`]
    ///
    /// This will run the `influxdb3 serve` command, and bind its HTTP
    /// address to a random port on localhost.
    pub async fn spawn() -> Self {
        Self::spawn_inner(Default::default()).await
    }

    /// Configure a [`TestServer`] before spawning
    pub fn configure() -> TestConfig {
        TestConfig::default()
    }

    async fn spawn_inner(config: TestConfig) -> Self {
        let bind_addr = get_local_bind_addr();
        let mut command = Command::cargo_bin("influxdb3").expect("create the influxdb3 command");
        let mut command = command
            .arg("serve")
            .args(["--http-bind", &bind_addr.to_string()])
            .args(["--object-store", "memory"])
            .args(config.as_args());

        // If TEST_LOG env var is not defined, discard stdout/stderr
        if std::env::var("TEST_LOG").is_err() {
            command = command.stdout(Stdio::null()).stderr(Stdio::null());
        }

        let server_process = command.spawn().expect("spawn the influxdb3 server process");

        let server = Self {
            config,
            bind_addr,
            server_process,
            http_client: reqwest::Client::new(),
        };

        server.wait_until_ready().await;
        server
    }

    /// Get the URL of the running service for use with an HTTP client
    pub fn client_addr(&self) -> String {
        format!("http://{addr}", addr = self.bind_addr)
    }

    /// Get a [`FlightSqlClient`] for making requests to the running service over gRPC
    pub async fn flight_sql_client(&self, database: &str) -> FlightSqlClient {
        let channel = tonic::transport::Channel::from_shared(self.client_addr())
            .expect("create tonic channel")
            .connect()
            .await
            .expect("connect to gRPC client");
        let mut client = FlightSqlClient::new(channel);
        client.add_header("database", database).unwrap();
        client
    }

    /// Get a raw [`FlightClient`] for performing Flight actions directly
    pub async fn flight_client(&self) -> FlightClient {
        let channel = tonic::transport::Channel::from_shared(self.client_addr())
            .expect("create tonic channel")
            .connect()
            .await
            .expect("connect to gRPC client");
        FlightClient::new(channel)
    }

    fn kill(&mut self) {
        self.server_process.kill().expect("kill the server process");
    }

    async fn wait_until_ready(&self) {
        while self
            .http_client
            .get(format!("{base}/health", base = self.client_addr()))
            .send()
            .await
            .is_err()
        {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }
}

impl Drop for TestServer {
    fn drop(&mut self) {
        self.kill();
    }
}

impl TestServer {
    /// Write some line protocol to the server
    pub async fn write_lp_to_db(
        &self,
        database: &str,
        lp: impl ToString,
        precision: Precision,
    ) -> Result<(), influxdb3_client::Error> {
        let mut client = influxdb3_client::Client::new(self.client_addr()).unwrap();
        if let Some((_, token)) = &self.config.auth_token {
            client = client.with_auth_token(token);
        }
        client
            .api_v3_write_lp(database)
            .body(lp.to_string())
            .precision(precision)
            .send()
            .await
    }

    pub async fn api_v3_query_sql(&self, params: &[(&str, &str)]) -> Response {
        self.http_client
            .get(format!(
                "{base}/api/v3/query_sql",
                base = self.client_addr()
            ))
            .query(params)
            .send()
            .await
            .expect("send /api/v3/query_sql request to server")
    }

    pub async fn api_v3_query_influxql(&self, params: &[(&str, &str)]) -> Response {
        self.http_client
            .get(format!(
                "{base}/api/v3/query_influxql",
                base = self.client_addr()
            ))
            .query(params)
            .send()
            .await
            .expect("send /api/v3/query_influxql request to server")
    }

    pub async fn api_v1_query(
        &self,
        params: &[(&str, &str)],
        headers: Option<&[(&str, &str)]>,
    ) -> Response {
        let default_headers = [("Accept", "application/json")];
        let headers = headers.unwrap_or(&default_headers);

        let mut header_map = HeaderMap::new();
        for (key, value) in headers {
            header_map.insert(
                HeaderName::from_bytes(key.as_bytes()).expect("Invalid header key"),
                HeaderValue::from_bytes(value.as_bytes()).expect("Invalid header value"),
            );
        }

        self.http_client
            .get(format!("{base}/query", base = self.client_addr(),))
            .headers(header_map)
            .query(params)
            .send()
            .await
            .expect("send /query request to server")
    }
}

/// Get an available bind address on localhost
///
/// This binds a [`TcpListener`] to 127.0.0.1:0, which will randomly
/// select an available port, and produces the resulting local address.
/// The [`TcpListener`] is dropped at the end of the function, thus
/// freeing the port for use by the caller.
fn get_local_bind_addr() -> SocketAddr {
    let ip = std::net::Ipv4Addr::new(127, 0, 0, 1);
    let port = 0;
    let addr = SocketAddrV4::new(ip, port);
    TcpListener::bind(addr)
        .expect("bind to a socket address")
        .local_addr()
        .expect("get local address")
}

/// Write to the server with the line protocol
pub async fn write_lp_to_db(
    server: &TestServer,
    database: &str,
    lp: &str,
    precision: Precision,
) -> Result<(), influxdb3_client::Error> {
    let client = influxdb3_client::Client::new(server.client_addr()).unwrap();
    client
        .api_v3_write_lp(database)
        .body(lp.to_string())
        .precision(precision)
        .send()
        .await
}

#[allow(dead_code)]
pub async fn collect_stream(stream: FlightRecordBatchStream) -> Vec<RecordBatch> {
    stream
        .try_collect()
        .await
        .expect("gather record batch stream")
}
