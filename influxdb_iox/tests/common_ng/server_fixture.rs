use assert_cmd::prelude::*;
use futures::prelude::*;
use http::{header::HeaderName, HeaderValue};
use influxdb_iox_client::connection::Connection;
use sqlx::{migrate::MigrateDatabase, Postgres};
use std::{
    net::SocketAddrV4,
    path::Path,
    process::{Child, Command},
    str,
    sync::{
        atomic::{AtomicU16, Ordering},
        Arc,
    },
    time::Duration,
};
use tempfile::{NamedTempFile, TempDir};
use tokio::sync::Mutex;

// These port numbers are chosen to not collide with a development ioxd server
// running locally.
static NEXT_PORT: AtomicU16 = AtomicU16::new(8090);

/// This structure contains all the addresses a test server should use
pub struct BindAddresses {
    http_bind_addr: String,
    grpc_bind_addr: String,

    http_base: String,
    grpc_base: String,
}

impl BindAddresses {
    pub fn http_bind_addr(&self) -> &str {
        &self.http_bind_addr
    }

    pub fn grpc_bind_addr(&self) -> &str {
        &self.grpc_bind_addr
    }

    fn get_free_port() -> SocketAddrV4 {
        let ip = std::net::Ipv4Addr::new(127, 0, 0, 1);

        loop {
            let port = NEXT_PORT.fetch_add(1, Ordering::SeqCst);
            let addr = SocketAddrV4::new(ip, port);

            if std::net::TcpListener::bind(addr).is_ok() {
                return addr;
            }
        }
    }
}

impl Default for BindAddresses {
    /// return a new port assignment suitable for this test's use
    fn default() -> Self {
        let http_addr = Self::get_free_port();
        let grpc_addr = Self::get_free_port();

        let http_base = format!("http://{}", http_addr);
        let grpc_base = format!("http://{}", grpc_addr);

        Self {
            http_bind_addr: http_addr.to_string(),
            grpc_bind_addr: grpc_addr.to_string(),
            http_base,
            grpc_base,
        }
    }
}

/// Represents a server that has been started and is available for
/// testing.
pub struct ServerFixture {
    server: Arc<TestServer>,
    grpc_channel: Connection,
}

impl ServerFixture {
    /// Create a new server fixture and wait for it to be ready. This
    /// is called "create" rather than new because it is async and
    /// waits. The server is not shared with any other tests.
    pub async fn create_single_use(server_type: ServerType) -> Self {
        let test_config = TestConfig::new(server_type);
        Self::create_single_use_with_config(test_config).await
    }

    /// Create a new server fixture with the provided additional environment variables
    /// and wait for it to be ready. The server is not shared with any other tests.
    pub async fn create_single_use_with_config(test_config: TestConfig) -> Self {
        let server = TestServer::new(test_config).await;
        let server = Arc::new(server);

        // ensure the server is ready
        server.wait_until_ready().await;
        Self::create_common(server).await
    }

    async fn create_common(server: Arc<TestServer>) -> Self {
        let grpc_channel = server
            .grpc_channel()
            .await
            .expect("The server should have been up");

        ServerFixture {
            server,
            grpc_channel,
        }
    }

    /// Return a channel connected to the gRPC API. Panics if the
    /// server is not yet up
    pub fn grpc_channel(&self) -> Connection {
        self.grpc_channel.clone()
    }

    /// Return the url base of the grpc management API
    pub fn grpc_base(&self) -> &str {
        &self.server.addrs().grpc_base
    }

    /// Return the http base URL for the HTTP API
    pub fn http_base(&self) -> &str {
        &self.server.addrs().http_base
    }

    /// Return a flight client suitable for communicating with the ingester service, like the one
    /// the query service uses
    pub fn querier_flight_client(&self) -> querier::flight::Client {
        querier::flight::Client::new(self.grpc_channel())
    }

    /// Restart test server.
    ///
    /// This will break all currently connected clients!
    pub async fn restart_server(self) -> Self {
        self.server.restart().await;
        self.server.wait_until_ready().await;
        let grpc_channel = self
            .server
            .grpc_channel()
            .await
            .expect("The server should have been up");

        Self {
            server: self.server,
            grpc_channel,
        }
    }

    /// Directory used for data storage.
    pub fn dir(&self) -> &Path {
        self.server.dir.path()
    }
}

#[derive(Debug)]
/// Represents the current known state of a TestServer
enum ServerState {
    Started,
    Ready,
    Error,
}

struct TestServer {
    /// Is the server ready to accept connections?
    ready: Mutex<ServerState>,

    /// Handle to the server process being controlled
    server_process: Mutex<Process>,

    /// Which ports this server should use
    addrs: BindAddresses,

    /// The temporary directory **must** be last so that it is
    /// dropped after the database closes.
    dir: TempDir,

    /// Configuration values for starting the test server
    test_config: TestConfig,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ServerType {
    Ingester,
    Router2,
}

// Options for creating test servers
#[derive(Debug, Clone)]
pub struct TestConfig {
    /// Additional environment variables
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
    fn dsn(&self) -> &str {
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
}

struct Process {
    child: Child,
    log_path: Box<Path>,
}

impl TestServer {
    async fn new(test_config: TestConfig) -> Self {
        let addrs = BindAddresses::default();
        let ready = Mutex::new(ServerState::Started);

        let dir = test_helpers::tmp_dir().unwrap();

        let server_process = Mutex::new(
            Self::create_server_process(
                &addrs,
                &dir,
                test_config.dsn(),
                &test_config.env,
                test_config.server_type,
            )
            .await,
        );

        Self {
            ready,
            server_process,
            addrs,
            dir,
            test_config,
        }
    }

    async fn restart(&self) {
        let mut ready_guard = self.ready.lock().await;
        let mut server_process = self.server_process.lock().await;
        server_process.child.kill().unwrap();
        server_process.child.wait().unwrap();
        *server_process = Self::create_server_process(
            &self.addrs,
            &self.dir,
            self.test_config.dsn(),
            &self.test_config.env,
            self.test_config.server_type,
        )
        .await;
        *ready_guard = ServerState::Started;
    }

    async fn create_server_process(
        addrs: &BindAddresses,
        dir: &TempDir,
        dsn: &str,
        env: &[(String, String)],
        server_type: ServerType,
    ) -> Process {
        // Create a new file each time and keep it around to aid debugging
        let (log_file, log_path) = NamedTempFile::new()
            .expect("opening log file")
            .keep()
            .expect("expected to keep");

        let stdout_log_file = log_file
            .try_clone()
            .expect("cloning file handle for stdout");
        let stderr_log_file = log_file;

        println!("****************");
        println!(
            "{:?} Server {} Logging to {:?}",
            server_type,
            addrs.http_bind_addr(),
            log_path
        );
        println!("****************");

        // If set in test environment, use that value, else default to info
        let log_filter = std::env::var("LOG_FILTER").unwrap_or_else(|_| "info".to_string());

        let type_name = match server_type {
            ServerType::Ingester => "ingester",
            ServerType::Router2 => "router2",
        };

        // Create the catalog database if it doesn't exist
        if !Postgres::database_exists(dsn).await.unwrap() {
            Postgres::create_database(dsn).await.unwrap();
        }

        // Set up the catalog
        Command::cargo_bin("influxdb_iox")
            .unwrap()
            .arg("catalog")
            .arg("setup")
            .env("INFLUXDB_IOX_CATALOG_DSN", &dsn)
            .ok()
            .unwrap();

        // Create the shared Kafka topic in the catalog
        Command::cargo_bin("influxdb_iox")
            .unwrap()
            .arg("catalog")
            .arg("topic")
            .arg("update")
            .arg("iox-shared")
            .env("INFLUXDB_IOX_CATALOG_DSN", &dsn)
            .ok()
            .unwrap();

        // This will inherit environment from the test runner
        // in particular `LOG_FILTER`
        let child = Command::cargo_bin("influxdb_iox")
            .unwrap()
            .arg("run")
            .arg(type_name)
            .env("LOG_FILTER", log_filter)
            .env("INFLUXDB_IOX_CATALOG_DSN", &dsn)
            .env("INFLUXDB_IOX_OBJECT_STORE", "file")
            .env("INFLUXDB_IOX_DB_DIR", dir.path())
            .env("INFLUXDB_IOX_BIND_ADDR", addrs.http_bind_addr())
            .env("INFLUXDB_IOX_GRPC_BIND_ADDR", addrs.grpc_bind_addr())
            .envs(env.iter().map(|(a, b)| (a.as_str(), b.as_str())))
            // redirect output to log file
            .stdout(stdout_log_file)
            .stderr(stderr_log_file)
            .spawn()
            .unwrap();

        Process {
            child,
            log_path: log_path.into_boxed_path(),
        }
    }

    async fn wait_until_ready(&self) {
        let mut ready = self.ready.lock().await;
        match *ready {
            ServerState::Started => {} // first time, need to try and start it
            ServerState::Ready => {
                return;
            }
            ServerState::Error => {
                panic!("Server was previously found to be in Error, aborting");
            }
        }

        // Poll the RPC and HTTP servers separately as they listen on
        // different ports but both need to be up for the test to run
        let try_grpc_connect = self.wait_for_grpc();

        let try_http_connect = async {
            let client = reqwest::Client::new();
            let url = format!("{}/health", self.addrs().http_base);
            let mut interval = tokio::time::interval(Duration::from_millis(1000));
            loop {
                match client.get(&url).send().await {
                    Ok(resp) => {
                        println!(
                            "Successfully got a response from {:?} HTTP: {:?}",
                            self.test_config.server_type, resp
                        );
                        return;
                    }
                    Err(e) => {
                        println!(
                            "Waiting for {:?} HTTP server to be up: {}",
                            self.test_config.server_type, e
                        );
                    }
                }
                interval.tick().await;
            }
        };

        let pair = future::join(try_http_connect, try_grpc_connect);

        let capped_check = tokio::time::timeout(Duration::from_secs(30), pair);

        match capped_check.await {
            Ok(_) => {
                println!("Successfully started {}", self);
                *ready = ServerState::Ready;
            }
            Err(e) => {
                // tell others that this server had some problem
                *ready = ServerState::Error;
                std::mem::drop(ready);
                panic!(
                    "{:?} Server was not ready in required time: {}",
                    self.test_config.server_type, e
                );
            }
        }
    }

    pub async fn wait_for_grpc(&self) {
        let mut interval = tokio::time::interval(Duration::from_millis(1000));

        loop {
            match (self.grpc_channel().await, self.test_config.server_type) {
                (Ok(channel), ServerType::Router2) => {
                    let mut health = influxdb_iox_client::health::Client::new(channel);

                    match health.check("influxdata.pbdata.v1.WriteService").await {
                        Ok(true) => {
                            println!("Write service is running");
                            return;
                        }
                        Ok(false) => {
                            println!("Write service is not running");
                        }
                        Err(e) => {
                            println!(
                                "Waiting for {:?} gRPC API to be healthy: {:?}",
                                self.test_config.server_type, e
                            );
                        }
                    }
                }
                (Ok(channel), ServerType::Ingester) => {
                    let mut health = influxdb_iox_client::health::Client::new(channel);

                    match health.check_arrow().await {
                        Ok(true) => {
                            println!("Flight service is running");
                            return;
                        }
                        Ok(false) => {
                            println!("Flight service is not running");
                        }
                        Err(e) => {
                            println!(
                                "Waiting for {:?} gRPC API to be healthy: {:?}",
                                self.test_config.server_type, e
                            );
                        }
                    }
                }
                (Err(e), _) => {
                    println!(
                        "Waiting for {:?} gRPC API to be up: {}",
                        self.test_config.server_type, e
                    );
                }
            }
            interval.tick().await;
        }
    }

    /// Create a connection channel for the gRPC endpoint
    async fn grpc_channel(&self) -> influxdb_iox_client::connection::Result<Connection> {
        let builder = influxdb_iox_client::connection::Builder::default();

        self.test_config
            .client_headers
            .iter()
            .fold(builder, |builder, (header_name, header_value)| {
                builder.header(header_name, header_value)
            })
            .build(&self.addrs.grpc_base)
            .await
    }

    /// Returns the addresses to which the server has been bound
    fn addrs(&self) -> &BindAddresses {
        &self.addrs
    }
}

impl std::fmt::Display for TestServer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::result::Result<(), std::fmt::Error> {
        write!(
            f,
            "TestServer {:?} (grpc {}, http {})",
            self.test_config.server_type,
            self.addrs().grpc_base,
            self.addrs().http_base
        )
    }
}

impl Drop for TestServer {
    fn drop(&mut self) {
        use std::io::Read;

        let mut server_lock = self
            .server_process
            .try_lock()
            .expect("should be able to get a server process lock");

        server_lock
            .child
            .kill()
            .expect("Should have been able to kill the test server");

        server_lock
            .child
            .wait()
            .expect("Should have been able to wait for shutdown");

        let mut f = std::fs::File::open(&server_lock.log_path).expect("failed to open log file");
        let mut buffer = [0_u8; 8 * 1024];

        println!("****************");
        println!("Start {:?} TestServer Output", self.test_config.server_type);
        println!("****************");

        while let Ok(read) = f.read(&mut buffer) {
            if read == 0 {
                break;
            }
            if let Ok(str) = std::str::from_utf8(&buffer[..read]) {
                print!("{}", str);
            } else {
                println!(
                    "\n\n-- ERROR IN TRANSFER -- please see {:?} for raw contents ---\n\n",
                    &server_lock.log_path
                );
            }
        }

        println!("****************");
        println!("End {:?} TestServer Output", self.test_config.server_type);
        println!("****************");
    }
}
