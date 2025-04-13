use std::{
    future::Future,
    io::{BufRead, BufReader},
    process::{Child, Command, Stdio},
    time::Duration,
};

use arrow::record_batch::RecordBatch;
use arrow_flight::{FlightClient, decode::FlightRecordBatchStream};
use assert_cmd::cargo::CommandCargoExt;
use futures::TryStreamExt;
use influxdb_iox_client::flightsql::FlightSqlClient;
use influxdb3_client::Precision;
use reqwest::header::{HeaderMap, HeaderName, HeaderValue};
use reqwest::{Certificate, Response, tls::Version};
use tonic::transport::ClientTlsConfig;

mod auth;
mod client;
mod configure;
mod flight;
mod limits;

mod packages;
mod ping;
mod query;
mod system_tables;
mod write;

pub trait ConfigProvider {
    /// Convert this to a set of command line arguments for `influxdb3 serve`
    fn as_args(&self) -> Vec<String>;

    /// Get the auth token from this config if it was set
    fn auth_token(&self) -> Option<&str>;

    /// Get if auth is enabled
    fn auth_enabled(&self) -> bool;

    /// Get if admin token needs to be generated
    fn should_generate_admin_token(&self) -> bool;

    /// Spawn a new [`TestServer`] with this configuration
    ///
    /// This will run the `influxdb3 serve` command and bind its HTTP address to a random port
    /// on localhost.
    fn spawn(&self) -> impl Future<Output = TestServer>
    where
        Self: Sized + Sync,
    {
        async { TestServer::spawn_inner(self).await }
    }
}

/// Configuration for a [`TestServer`]
#[derive(Debug, Default)]
pub struct TestConfig {
    auth_token: Option<(String, String)>,
    auth: bool,
    without_admin_token: bool,
    node_id: Option<String>,
    plugin_dir: Option<String>,
    virtual_env_dir: Option<String>,
    package_manager: Option<String>,
    // If None, use memory object store.
    object_store_dir: Option<String>,
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

    /// Set the auth (setting this will auto generate admin token)
    pub fn with_auth(mut self) -> Self {
        self.auth = true;
        self
    }

    /// Set the auth token for this [`TestServer`]
    pub fn with_no_admin_token(mut self) -> Self {
        self.without_admin_token = true;
        self
    }

    /// Set a host identifier prefix on the spawned [`TestServer`]
    pub fn with_node_id<S: Into<String>>(mut self, node_id: S) -> Self {
        self.node_id = Some(node_id.into());
        self
    }

    /// Set the plugin dir for this [`TestServer`]
    pub fn with_plugin_dir<S: Into<String>>(mut self, plugin_dir: S) -> Self {
        self.plugin_dir = Some(plugin_dir.into());
        self
    }

    /// Set the virtual env dir for this [`TestServer`]
    pub fn with_virtual_env<S: Into<String>>(mut self, virtual_env_dir: S) -> Self {
        self.virtual_env_dir = Some(virtual_env_dir.into());
        self
    }

    pub fn with_package_manager<S: Into<String>>(mut self, package_manager: S) -> Self {
        self.package_manager = Some(package_manager.into());
        self
    }

    // Set the object store dir for this [`TestServer`]
    pub fn with_object_store_dir<S: Into<String>>(mut self, object_store_dir: S) -> Self {
        self.object_store_dir = Some(object_store_dir.into());
        self
    }
}

impl ConfigProvider for TestConfig {
    fn as_args(&self) -> Vec<String> {
        let mut args = vec![];
        if !self.auth {
            args.append(&mut vec!["--without-auth".to_string()]);
        }

        if let Some(plugin_dir) = &self.plugin_dir {
            args.append(&mut vec!["--plugin-dir".to_string(), plugin_dir.to_owned()]);
        }
        if let Some(virtual_env_dir) = &self.virtual_env_dir {
            args.append(&mut vec![
                "--virtual-env-location".to_string(),
                virtual_env_dir.to_owned(),
            ]);
        }
        if let Some(package_manager) = &self.package_manager {
            args.append(&mut vec![
                "--package-manager".to_string(),
                package_manager.to_owned(),
            ]);
        }
        args.push("--node-id".to_string());
        if let Some(host) = &self.node_id {
            args.push(host.to_owned());
        } else {
            args.push("test-server".to_string());
        }
        if let Some(object_store_dir) = &self.object_store_dir {
            args.append(&mut vec![
                "--object-store".to_string(),
                "file".to_string(),
                "--data-dir".to_string(),
                object_store_dir.to_owned(),
            ]);
        } else {
            args.append(&mut vec![
                "--object-store".to_string(),
                "memory".to_string(),
            ]);
        }
        args
    }

    fn auth_token(&self) -> Option<&str> {
        self.auth_token.as_ref().map(|(_, t)| t.as_str())
    }

    fn auth_enabled(&self) -> bool {
        self.auth
    }

    fn should_generate_admin_token(&self) -> bool {
        self.without_admin_token
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
    bind_addr: String,
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
        let mut command = Command::cargo_bin("influxdb3").expect("create the influxdb3 command");
        let command = command
            .arg("serve")
            .arg("--disable-telemetry-upload")
            // bind to port 0 to get a random port assigned:
            .args(["--http-bind", "0.0.0.0:0"])
            .args(["--wal-flush-interval", "10ms"])
            .args(["--wal-snapshot-size", "1"])
            .args(["--tls-cert", "../testing-certs/localhost.pem"])
            .args(["--tls-key", "../testing-certs/localhost.key"])
            .args(config.as_args())
            .stdout(Stdio::piped());

        // Use the TEST_LOG env var to determine if logs are emitted from the spawned process
        let emit_logs = if std::env::var("TEST_LOG").is_ok() {
            // use "info" filter, as would be used in production:
            command.env("LOG_FILTER", "info");
            true
        } else {
            false
        };

        let mut server_process = command.spawn().expect("spawn the influxdb3 server process");

        // pipe stdout so we can get the randomly assigned port from the log output:
        let process_stdout = server_process
            .stdout
            .take()
            .expect("should acquire stdout from process");

        let mut lines = BufReader::new(process_stdout).lines();
        let bind_addr = loop {
            let Some(Ok(line)) = lines.next() else {
                panic!("stdout closed unexpectedly");
            };
            if emit_logs {
                println!("{line}");
            }
            if line.contains("startup time") {
                if let Some(address) = line.split("address=").last() {
                    break address.to_string();
                }
            }
        };

        tokio::task::spawn_blocking(move || {
            for line in lines {
                let line = line.expect("io error while getting line from stdout");
                if emit_logs {
                    println!("{line}");
                }
            }
        });

        let http_client = reqwest::ClientBuilder::new()
            .min_tls_version(Version::TLS_1_3)
            .use_rustls_tls()
            .add_root_certificate(
                Certificate::from_pem(&std::fs::read("../testing-certs/rootCA.pem").unwrap())
                    .unwrap(),
            )
            .build()
            .unwrap();

        let server = Self {
            auth_token: config.auth_token().map(|s| s.to_owned()),
            bind_addr,
            server_process,
            http_client,
        };

        server.wait_until_ready().await;

        let (mut server, token) = if config.auth_enabled() && !config.should_generate_admin_token()
        {
            let result = server
                .run(
                    vec!["create", "token", "--admin"],
                    &["--tls-ca", "../testing-certs/rootCA.pem"],
                )
                .unwrap();
            let token = parse_token(result);
            (server, Some(token))
        } else {
            (server, None)
        };

        server.auth_token = token;

        server
    }

    /// Get the URL of the running service for use with an HTTP client
    pub fn client_addr(&self) -> String {
        format!(
            "https://localhost:{}",
            self.bind_addr.split(':').nth(1).unwrap()
        )
    }

    /// Get the token for the server
    pub fn token(&self) -> Option<&String> {
        self.auth_token.as_ref()
    }

    /// Set the token for the server
    pub fn set_token(&mut self, token: Option<String>) {
        self.auth_token = token;
    }

    /// Get a [`FlightSqlClient`] for making requests to the running service over gRPC
    pub async fn flight_sql_client(&self, database: &str) -> FlightSqlClient {
        let cert = tonic::transport::Certificate::from_pem(
            std::fs::read("../testing-certs/rootCA.pem").unwrap(),
        );
        let channel = tonic::transport::Channel::from_shared(self.client_addr())
            .expect("create tonic channel")
            .tls_config(ClientTlsConfig::new().ca_certificate(cert))
            .unwrap()
            .connect()
            .await
            .expect("connect to gRPC client");
        let mut client = FlightSqlClient::new(channel);
        client.add_header("database", database).unwrap();
        client
    }

    /// Get a raw [`FlightClient`] for performing Flight actions directly
    pub async fn flight_client(&self) -> FlightClient {
        let cert = tonic::transport::Certificate::from_pem(
            std::fs::read("../testing-certs/rootCA.pem").unwrap(),
        );
        let channel = tonic::transport::Channel::from_shared(self.client_addr())
            .expect("create tonic channel")
            .tls_config(ClientTlsConfig::new().ca_certificate(cert))
            .unwrap()
            .connect()
            .await
            .expect("connect to gRPC client");
        FlightClient::new(channel)
    }

    pub fn http_client(&self) -> &reqwest::Client {
        &self.http_client
    }

    pub fn kill(&mut self) {
        self.server_process.kill().expect("kill the server process");
    }

    pub fn is_stopped(&mut self) -> bool {
        self.server_process
            .try_wait()
            .inspect_err(|error| println!("error when checking for stopped: {error:?}"))
            .expect("check process status")
            .is_some()
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
        let mut client = influxdb3_client::Client::new(
            self.client_addr(),
            Some("../testing-certs/rootCA.pem".into()),
        )
        .unwrap();
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

    pub async fn api_v3_query_sql_with_header(
        &self,
        params: &[(&str, &str)],
        headers: HeaderMap<HeaderValue>,
    ) -> Response {
        self.http_client
            .get(format!(
                "{base}/api/v3/query_sql",
                base = self.client_addr()
            ))
            .query(params)
            .headers(headers)
            .send()
            .await
            .expect("send /api/v3/query_sql request to server")
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
            .get(format!("{base}/query", base = self.client_addr()))
            .headers(header_map)
            .query(params)
            .send()
            .await
            .expect("send /query request to server")
    }

    pub async fn api_v3_configure_table_create(&self, request: &serde_json::Value) -> Response {
        self.http_client
            .post(format!(
                "{base}/api/v3/configure/table",
                base = self.client_addr()
            ))
            .json(request)
            .send()
            .await
            .expect("failed to send request to create table")
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

    pub async fn api_v3_configure_distinct_cache_create(
        &self,
        request: &serde_json::Value,
    ) -> Response {
        self.http_client
            .post(format!(
                "{base}/api/v3/configure/distinct_cache",
                base = self.client_addr()
            ))
            .json(request)
            .send()
            .await
            .expect("failed to send request to create distinct cache")
    }

    pub async fn api_v3_configure_distinct_cache_delete(
        &self,
        request: &serde_json::Value,
    ) -> Response {
        self.http_client
            .delete(format!(
                "{base}/api/v3/configure/distinct_cache",
                base = self.client_addr()
            ))
            .json(request)
            .send()
            .await
            .expect("failed to send request to delete distinct cache")
    }
}

/// Write to the server with the line protocol
pub async fn write_lp_to_db(
    server: &TestServer,
    database: &str,
    lp: &str,
    precision: Precision,
) -> Result<(), influxdb3_client::Error> {
    let client = influxdb3_client::Client::new(
        server.client_addr(),
        Some("../testing-certs/rootCA.pem".into()),
    )
    .unwrap();
    client
        .api_v3_write_lp(database)
        .body(lp.to_string())
        .precision(precision)
        .send()
        .await
}

pub fn parse_token(result: String) -> String {
    let all_lines: Vec<&str> = result.split('\n').collect();
    let token = all_lines
        .iter()
        .find(|line| line.starts_with("Token:"))
        .expect("token line to be present")
        .replace("Token: ", "");
    token
}

#[allow(dead_code)]
pub async fn collect_stream(stream: FlightRecordBatchStream) -> Vec<RecordBatch> {
    stream
        .try_collect()
        .await
        .expect("gather record batch stream")
}
