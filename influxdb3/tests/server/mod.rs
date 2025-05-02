use std::{
    future::Future,
    io::{BufRead, BufReader},
    process::{Child, Command, Stdio},
    time::{Duration, SystemTime},
};

use arrow::record_batch::RecordBatch;
use arrow_flight::{FlightClient, decode::FlightRecordBatchStream};
use assert_cmd::cargo::CommandCargoExt;
use futures::TryStreamExt;
use influxdb_iox_client::flightsql::FlightSqlClient;
use influxdb3_client::Precision;
use influxdb3_types::http::FieldType;
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

    /// Get if bad tls is enabled
    fn bad_tls(&self) -> bool;

    /// Get if tls 1.3 only is enabled
    fn tls_1_3(&self) -> bool;

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
    bad_tls: bool,
    tls_1_3: bool,
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

    /// Set whether to use bad tls certs or not for the [`TestServer`]
    pub fn with_bad_tls(mut self, bad_tls: bool) -> Self {
        self.bad_tls = bad_tls;
        self
    }

    /// Set whether to use tls 1.3 as the minimum or not for the [`TestServer`]
    pub fn with_tls_1_3(mut self, tls_1_3: bool) -> Self {
        self.tls_1_3 = tls_1_3;
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

    fn bad_tls(&self) -> bool {
        self.bad_tls
    }

    fn tls_1_3(&self) -> bool {
        self.tls_1_3
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

    /// Spawn a new [`TestServer`] with an expired cert
    pub async fn spawn_bad_tls() -> Self {
        Self::spawn_inner(&TestConfig::default().with_bad_tls(true)).await
    }

    /// Spawn a new [`TestServer`] using tls 1.3 as the minimum
    pub async fn spawn_tls_1_3() -> Self {
        Self::spawn_inner(&TestConfig::default().with_tls_1_3(true)).await
    }

    /// Configure a [`TestServer`] before spawning
    pub fn configure() -> TestConfig {
        TestConfig::default()
    }

    async fn spawn_inner(config: &impl ConfigProvider) -> Self {
        create_certs().await;
        let mut command = Command::cargo_bin("influxdb3").expect("create the influxdb3 command");
        let command = command
            .arg("serve")
            .arg("--disable-telemetry-upload")
            // bind to port 0 to get a random port assigned:
            .args(["--http-bind", "0.0.0.0:0"])
            .args(["--wal-flush-interval", "10ms"])
            .args(["--wal-snapshot-size", "1"])
            .args([
                "--tls-cert",
                if config.bad_tls() {
                    "../testing-certs/localhost_bad.pem"
                } else {
                    "../testing-certs/localhost.pem"
                },
            ])
            .args([
                "--tls-key",
                if config.bad_tls() {
                    "../testing-certs/localhost_bad.key"
                } else {
                    "../testing-certs/localhost.key"
                },
            ])
            .args([
                "--tls-minimum-version",
                if config.tls_1_3() {
                    "tls-1.3"
                } else {
                    "tls-1.2"
                },
            ])
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

    pub async fn api_v3_create_table(
        &self,
        database: &str,
        table: &str,
        tags: Vec<String>,
        fields: Vec<(String, FieldType)>,
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
            .api_v3_configure_table_create(database, table, tags, fields)
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

/// Generate certs for tests if they do not already exist. For the most part this
/// is needed only in CI for fresh builds. Locally it'll generate them the first
/// time and then should be valid for longer than anyone is alive or this software
/// is used.
pub async fn create_certs() {
    use rcgen::CertificateParams;
    use rcgen::IsCa;
    use rcgen::KeyPair;
    use std::fs;
    use std::fs::OpenOptions;
    use std::io::ErrorKind;

    const LOCK_FILE: &str = "../testing-certs/certs.lock";
    const ROOT_FILE: &str = "../testing-certs/rootCA.pem";
    const LOCAL_FILE: &str = "../testing-certs/localhost.pem";
    const LOCAL_KEY_FILE: &str = "../testing-certs/localhost.key";
    const LOCAL_BAD_FILE: &str = "../testing-certs/localhost_bad.pem";
    const LOCAL_BAD_KEY_FILE: &str = "../testing-certs/localhost_bad.key";

    if fs::exists(ROOT_FILE).unwrap()
        && fs::exists(LOCAL_FILE).unwrap()
        && fs::exists(LOCAL_KEY_FILE).unwrap()
        && fs::exists(LOCAL_BAD_FILE).unwrap()
        && fs::exists(LOCAL_BAD_KEY_FILE).unwrap()
    {
        return;
    }

    fs::create_dir_all("../testing-certs").unwrap();
    let lock_file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(LOCK_FILE);
    match lock_file.map_err(|e| e.kind()) {
        Ok(_) => {
            let mut ca = CertificateParams::new(Vec::new()).unwrap();
            let ca_key = KeyPair::generate().unwrap();
            let local_key = KeyPair::generate().unwrap();
            let local_bad_key = KeyPair::generate().unwrap();
            ca.is_ca = IsCa::Ca(rcgen::BasicConstraints::Unconstrained);
            let ca = ca.self_signed(&ca_key).unwrap();
            let localhost = CertificateParams::new(vec!["localhost".into()]).unwrap();
            let localhost = localhost.signed_by(&local_key, &ca, &ca_key).unwrap();
            // We make a truly expired certificate that should fail when people
            // try to connect
            let localhost_bad = {
                let mut cert = CertificateParams::new(vec!["localhost".into()]).unwrap();
                cert.not_before = SystemTime::UNIX_EPOCH.into();
                cert.not_after = SystemTime::UNIX_EPOCH.into();
                cert
            };
            let localhost_bad = localhost_bad
                .signed_by(&local_bad_key, &ca, &ca_key)
                .unwrap();

            fs::write(ROOT_FILE, ca.pem()).unwrap();
            fs::write(LOCAL_FILE, localhost.pem()).unwrap();
            fs::write(LOCAL_KEY_FILE, local_key.serialize_pem()).unwrap();
            fs::write(LOCAL_BAD_FILE, localhost_bad.pem()).unwrap();
            fs::write(LOCAL_BAD_KEY_FILE, local_bad_key.serialize_pem()).unwrap();

            fs::remove_file(LOCK_FILE).unwrap();
        }
        Err(ErrorKind::AlreadyExists) => {
            while fs::exists(LOCK_FILE).unwrap() {
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        }
        Err(_) => panic!("Failed to acquire cert lock"),
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

#[tokio::test]
#[should_panic]
async fn fail_with_invalid_certs() {
    // This will fail in the startup of TestServer as the connection should
    // fail when testing that the server is up. Note that this only holds true
    // if other tests pass.
    let _ = TestServer::spawn_bad_tls().await;
}

#[tokio::test]
async fn tls_1_3_minimum_test() {
    let server = TestServer::spawn_tls_1_3().await;

    let http_client = reqwest::ClientBuilder::new()
        .min_tls_version(Version::TLS_1_2)
        .max_tls_version(Version::TLS_1_2)
        .use_rustls_tls()
        .add_root_certificate(
            Certificate::from_pem(&std::fs::read("../testing-certs/rootCA.pem").unwrap()).unwrap(),
        )
        .build()
        .unwrap();

    // This should fail since we are using TLS 1.2
    http_client
        .execute(
            http_client
                .get(format!("{}/health", server.client_addr()))
                .build()
                .unwrap(),
        )
        .await
        .unwrap_err();

    let http_client = reqwest::ClientBuilder::new()
        .min_tls_version(Version::TLS_1_3)
        .max_tls_version(Version::TLS_1_3)
        .use_rustls_tls()
        .add_root_certificate(
            Certificate::from_pem(&std::fs::read("../testing-certs/rootCA.pem").unwrap()).unwrap(),
        )
        .build()
        .unwrap();

    // This should NOT fail since we are using TLS 1.3
    http_client
        .execute(
            http_client
                .get(format!("{}/health", server.client_addr()))
                .build()
                .unwrap(),
        )
        .await
        .unwrap();
}
