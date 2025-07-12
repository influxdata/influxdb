use std::{
    future::Future,
    process::{Child, Command},
    time::{Duration, SystemTime},
};

use arrow::record_batch::RecordBatch;
use arrow_flight::{FlightClient, decode::FlightRecordBatchStream};
use assert_cmd::cargo::CommandCargoExt;
use futures::TryStreamExt;
use influxdb_iox_client::flightsql::FlightSqlClient;
use influxdb3_client::Precision;
use influxdb3_types::http::FieldType;
use regex::Regex;
use reqwest::header::{HeaderMap, HeaderName, HeaderValue};
use reqwest::{Certificate, Response, tls::Version};
use tempfile::TempDir;
use tokio::io::AsyncReadExt;
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

pub trait ConfigProvider: Send + Sync + 'static {
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
    disable_authz: Vec<String>,
    gen1_duration: Option<String>,
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

    pub fn with_disable_authz(mut self, disabled_list: Vec<String>) -> Self {
        self.disable_authz = disabled_list;
        self
    }

    pub fn with_gen1_duration(mut self, gen1_duration: impl Into<String>) -> Self {
        self.gen1_duration = Some(gen1_duration.into());
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

        if !self.disable_authz.is_empty() {
            args.append(&mut vec![
                "--disable-authz".to_owned(),
                self.disable_authz.join(","),
            ]);
        }

        if let Some(gen1_duration) = &self.gen1_duration {
            args.append(&mut vec![
                "--gen1-duration".to_string(),
                gen1_duration.to_owned(),
            ])
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
/// Logs will be emitted to stdout/stderr if the `TEST_LOG` environment variable is set, e.g.,
/// ```
/// TEST_LOG= cargo nextest run -p influxdb3 --nocapture
/// ```
///
/// This also respects the RUST_LOG environment variable, which is typically used to set the
/// log filter for tracing/tests.
///
/// - `TEST_LOG=` (empty) will result in `INFO` logs being emitted
/// - `TEST_LOG=<filter>` will result in the provided `<filter>` being used as the `LOG_FILTER`
/// - if both `TEST_LOG` and `RUST_LOG` are set, the value provided in `RUST_LOG` will be used
///   as the `LOG_FILTER`
pub struct TestServer {
    auth_token: Option<String>,
    bind_addr: String,
    admin_token_recovery_bind_addr: String,
    server_process: Child,
    http_client: reqwest::Client,
}

impl std::fmt::Debug for TestServer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TestServer")
            .field("auth_token", &self.auth_token)
            .field("bind_addr", &self.bind_addr)
            .finish()
    }
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
        // Create a temporary file for storing the TCP Listener address. We start the server with
        // a bind address of 0.0.0.0:0, which will have the OS assign a randomly available port.
        //
        // To determine which port is selected, the server will write the listener address to the
        // file path provided in the --tcp-listener-file-path argument, which we use below to assign
        // the bind address for the TestServer harness.
        //
        // The file is deleted when it goes out of scope (the end of this method) by the TempDir type.
        let tmp_dir = TempDir::new().unwrap();
        let tmp_dir_path = tmp_dir.keep();
        let tcp_addr_file = tmp_dir_path.join("tcp-listener");

        let admin_token_recover_tmp_dir = TempDir::new().unwrap();
        let admin_token_recover_tmp_dir_path = admin_token_recover_tmp_dir.keep();
        let tcp_addr_file_2 = admin_token_recover_tmp_dir_path.join("tcp-listener");

        let mut command = Command::cargo_bin("influxdb3").expect("create the influxdb3 command");
        let command = command
            .arg("serve")
            .arg("--disable-telemetry-upload")
            .args(["--http-bind", "0.0.0.0:0"])
            .args(["--admin-token-recovery-http-bind", "0.0.0.0:0"])
            .args(["--wal-flush-interval", "10ms"])
            .args(["--wal-snapshot-size", "1"])
            .args([
                "--tcp-listener-file-path",
                tcp_addr_file
                    .to_str()
                    .expect("valid tcp listener file path"),
            ])
            .args([
                "--admin-token-recovery-tcp-listener-file-path",
                tcp_addr_file_2
                    .to_str()
                    .expect("valid tcp listener file path"),
            ])
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
            .args(config.as_args());

        // Determine the LOG_FILTER that is passed down to the process, if necessary
        match (std::env::var("TEST_LOG"), std::env::var("RUST_LOG")) {
            (Ok(t), Ok(r)) if t.is_empty() && r.is_empty() => {
                command.env("LOG_FILTER", "info");
            }
            (Ok(t), Err(_)) if t.is_empty() => {
                command.env("LOG_FILTER", "info");
            }
            (Ok(filter), Err(_)) | (Ok(_), Ok(filter)) => {
                command.env("LOG_FILTER", filter);
            }
            (Err(_), _) => (),
        }

        let server_process = command.spawn().expect("spawn the influxdb3 server process");

        let bind_addr = find_bind_addr(tcp_addr_file).await;

        let admin_token_recovery_bind_addr = find_bind_addr(tcp_addr_file_2).await;

        let http_client = reqwest::ClientBuilder::new()
            .min_tls_version(Version::TLS_1_3)
            .timeout(Duration::from_secs(60))
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
            admin_token_recovery_bind_addr,
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

    /// Get the URL of admin token recovery service for use with an HTTP client
    pub fn admin_token_recovery_client_addr(&self) -> String {
        format!(
            "https://localhost:{}",
            self.admin_token_recovery_bind_addr
                .split(':')
                .nth(1)
                .unwrap()
        )
    }

    /// Get the token for the server
    pub fn token(&self) -> Option<&String> {
        self.auth_token.as_ref()
    }

    /// Set the token for the server
    pub fn set_token(&mut self, token: Option<String>) -> &mut Self {
        self.auth_token = token;
        self
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

async fn find_bind_addr(tcp_addr_file_2: std::path::PathBuf) -> String {
    loop {
        match tokio::fs::File::open(&tcp_addr_file_2).await {
            Ok(mut file) => {
                let mut buf = String::new();
                file.read_to_string(&mut buf)
                    .await
                    .expect("read from tcp listener file");
                if buf.is_empty() {
                    tokio::time::sleep(Duration::from_millis(10)).await;
                    continue;
                } else {
                    break buf;
                }
            }
            Err(error) if matches!(error.kind(), std::io::ErrorKind::NotFound) => {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
            Err(error) => {
                panic!("unexpected error while checking for tcp listener file: {error:?}")
            }
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
    let ansi_regex = Regex::new(r"\x1b\[[0-9;]*m").unwrap();

    let token_line = result
        .lines()
        .find(|line| line.contains("Token:"))
        .expect("token line to be present");

    let clean_line = ansi_regex.replace_all(token_line, "");

    let raw_token = clean_line
        .split_once("Token: ")
        .expect("expected 'Token: ' in line")
        .1
        .trim();

    raw_token.to_string()
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
