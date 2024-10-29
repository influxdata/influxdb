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
mod client;
mod configure;
mod flight;
mod limits;
mod ping;
mod pro;
mod query;
mod system_tables;
mod write;

trait ConfigProvider {
    /// Convert this to a set of command line arguments for `influxdb3 serve`
    fn as_args(&self) -> Vec<String>;

    /// Get the auth token from this config if it was set
    fn auth_token(&self) -> Option<&str>;

    /// Spawn a new [`TestServer`] with this configuration
    ///
    /// This will run the `influxdb3 serve` command and bind its HTTP address to a random port
    /// on localhost.
    async fn spawn(&self) -> TestServer
    where
        Self: Sized,
    {
        TestServer::spawn_inner(self).await
    }
}

/// Configuration for a [`TestServer`]
#[derive(Debug, Default)]
pub struct TestConfig {
    auth_token: Option<(String, String)>,
    host_id: Option<String>,
}

impl TestConfig {
    /// Set the auth token for this [`TestServer`]
    pub fn with_auth_token<S: Into<String>, R: Into<String>>(
        mut self,
        hashed_token: S,
        raw_token: R,
    ) -> Self {
        self.auth_token = Some((hashed_token.into(), raw_token.into()));
        self
    }

    /// Set a host identifier prefix on the spawned [`TestServer`]
    pub fn with_host_id<S: Into<String>>(mut self, host_id: S) -> Self {
        self.host_id = Some(host_id.into());
        self
    }
}

impl ConfigProvider for TestConfig {
    fn as_args(&self) -> Vec<String> {
        let mut args = vec![];
        if let Some((token, _)) = &self.auth_token {
            args.append(&mut vec!["--bearer-token".to_string(), token.to_owned()]);
        }
        args.push("--host-id".to_string());
        if let Some(host) = &self.host_id {
            args.push(host.to_owned());
        } else {
            args.push("test-server".to_string());
        }
        args.append(&mut vec![
            "--object-store".to_string(),
            "memory".to_string(),
        ]);
        args
    }

    fn auth_token(&self) -> Option<&str> {
        self.auth_token.as_ref().map(|(_, t)| t.as_str())
    }
}

/// A running instance of the `influxdb3 serve` process
///
/// Logs will be emitted to stdout/stderr if the TEST_LOG environment variable is set, e.g.,
/// ```
/// TEST_LOG= cargo nextest run -p influxdb3 --nocapture
/// ```
/// This will forward the value provided in `TEST_LOG` to the `LOG_FILTER` env var on the running
/// `influxdb` binary. By default, a log filter of `info` is used, which would provide similar
/// output to what is seen in production, however, per-crate filters can be provided via this
/// argument, e.g., `info,influxdb3_write=debug` would emit logs at `INFO` level for all crates
/// except for the `influxdb3_write` crate, which will emit logs at the `DEBUG` level.
pub struct TestServer {
    auth_token: Option<String>,
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
        Self::spawn_inner(&TestConfig::default()).await
    }

    /// Configure a [`TestServer`] before spawning
    pub fn configure() -> TestConfig {
        TestConfig::default()
    }

    async fn spawn_inner(config: &impl ConfigProvider) -> Self {
        let bind_addr = get_local_bind_addr();
        let mut command = Command::cargo_bin("influxdb3").expect("create the influxdb3 command");
        let mut command = command
            .arg("serve")
            .args(["--http-bind", &bind_addr.to_string()])
            .args(["--wal-flush-interval", "10ms"])
            .args(config.as_args());

        // If TEST_LOG env var is not defined, discard stdout/stderr, otherwise, pass it to the
        // inner binary in the "LOG_FILTER" env var:
        command = match std::env::var("TEST_LOG") {
            Ok(val) => command.env("LOG_FILTER", if val.is_empty() { "info" } else { &val }),
            Err(_) => command.stdout(Stdio::null()).stderr(Stdio::null()),
        };

        let server_process = command.spawn().expect("spawn the influxdb3 server process");

        let server = Self {
            auth_token: config.auth_token().map(|s| s.to_owned()),
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
        let mut count = 0;
        while self
            .http_client
            .get(format!("{base}/health", base = self.client_addr()))
            .send()
            .await
            .is_err()
        {
            if count > 500 {
                panic!("server failed to start");
            } else {
                count += 1;
            }
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
        if let Some(token) = &self.auth_token {
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

    pub async fn api_v3_configure_last_cache_create(
        &self,
        request: &serde_json::Value,
    ) -> Response {
        self.http_client
            .post(format!(
                "{base}/api/v3/configure/last_cache",
                base = self.client_addr()
            ))
            .json(request)
            .send()
            .await
            .expect("failed to send request to create last cache")
    }

    pub async fn api_v3_configure_last_cache_delete(
        &self,
        request: &serde_json::Value,
    ) -> Response {
        self.http_client
            .delete(format!(
                "{base}/api/v3/configure/last_cache",
                base = self.client_addr()
            ))
            .json(request)
            .send()
            .await
            .expect("failed to send request to delete last cache")
    }

    pub async fn api_v3_configure_file_index_create(
        &self,
        request: &serde_json::Value,
    ) -> Response {
        self.http_client
            .post(format!(
                "{base}/api/v3/pro/configure/file_index",
                base = self.client_addr()
            ))
            .json(request)
            .send()
            .await
            .expect("failed to send request to create file index")
    }

    pub async fn api_v3_configure_file_index_delete(
        &self,
        request: &serde_json::Value,
    ) -> Response {
        self.http_client
            .delete(format!(
                "{base}/api/v3/pro/configure/file_index",
                base = self.client_addr()
            ))
            .json(request)
            .send()
            .await
            .expect("failed to send request to delete file index")
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
