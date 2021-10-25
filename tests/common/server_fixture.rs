use std::net::SocketAddrV4;
use std::sync::atomic::{AtomicU16, Ordering};
use std::sync::Arc;
use std::time::Duration;
use std::{
    path::Path,
    process::{Child, Command},
    str,
    sync::Weak,
    time::Instant,
};

use assert_cmd::prelude::*;
use futures::prelude::*;
use generated_types::influxdata::iox::management::v1::{
    database_status::DatabaseState, ServerStatus,
};
use http::header::HeaderName;
use http::HeaderValue;
use influxdb_iox_client::connection::Connection;
use once_cell::sync::OnceCell;
use tempfile::{NamedTempFile, TempDir};
use tokio::sync::Mutex;

// These port numbers are chosen to not collide with a development ioxd server
// running locally.
static NEXT_PORT: AtomicU16 = AtomicU16::new(8090);

pub const DEFAULT_SERVER_ID: u32 = 32;

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

const TOKEN: &str = "InfluxDB IOx doesn't have authentication yet";

/// Represents a server that has been started and is available for
/// testing.
pub struct ServerFixture {
    server: Arc<TestServer>,
    grpc_channel: Connection,
}

/// Specifieds should we configure a server initially
enum InitialConfig {
    /// Set the writer id to something so it can accept writes
    SetWriterId,

    /// leave the writer id empty so the test can set it
    None,
}

impl ServerFixture {
    /// Create a new server fixture and wait for it to be ready. This
    /// is called "create" rather than new because it is async and
    /// waits. The shared database is configured with a writer id and
    /// can be used immediately
    ///
    /// This is currently implemented as a singleton so all tests *must*
    /// use a new database and not interfere with the existing database.
    pub async fn create_shared() -> Self {
        // Try and reuse the same shared server, if there is already
        // one present
        static SHARED_SERVER: OnceCell<parking_lot::Mutex<Weak<TestServer>>> = OnceCell::new();

        let shared_server = SHARED_SERVER.get_or_init(|| parking_lot::Mutex::new(Weak::new()));

        let mut shared_server = shared_server.lock();

        // is a shared server already present?
        let server = match shared_server.upgrade() {
            Some(server) => server,
            None => {
                // if not, create one
                let server = TestServer::new(Default::default());
                let server = Arc::new(server);

                // ensure the server is ready
                server.wait_until_ready(InitialConfig::SetWriterId).await;
                // save a reference for other threads that may want to
                // use this server, but don't prevent it from being
                // destroyed when going out of scope
                *shared_server = Arc::downgrade(&server);
                server
            }
        };
        std::mem::drop(shared_server);

        Self::create_common(server).await
    }

    /// Create a new server fixture and wait for it to be ready. This
    /// is called "create" rather than new because it is async and
    /// waits. The database is left unconfigured (no writer id) and
    /// is not shared with any other tests.
    pub async fn create_single_use() -> Self {
        Self::create_single_use_with_config(Default::default()).await
    }

    /// Create a new server fixture with the provided additional environment variables
    /// and wait for it to be ready. The database is left unconfigured (no writer id)
    /// and is not shared with any other tests.
    pub async fn create_single_use_with_config(test_config: TestConfig) -> Self {
        let server = TestServer::new(test_config);
        let server = Arc::new(server);

        // ensure the server is ready
        server.wait_until_ready(InitialConfig::None).await;
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

    /// Return an a http client suitable suitable for communicating with this
    /// server
    pub fn influxdb2_client(&self) -> influxdb2_client::Client {
        influxdb2_client::Client::new(self.http_base(), TOKEN)
    }

    /// Return a management client suitable for communicating with this
    /// server
    pub fn management_client(&self) -> influxdb_iox_client::management::Client {
        influxdb_iox_client::management::Client::new(self.grpc_channel())
    }

    /// Return a operations client suitable for communicating with this
    /// server
    pub fn operations_client(&self) -> influxdb_iox_client::operations::Client {
        influxdb_iox_client::operations::Client::new(self.grpc_channel())
    }

    /// Return a write client suitable for communicating with this
    /// server
    pub fn write_client(&self) -> influxdb_iox_client::write::Client {
        influxdb_iox_client::write::Client::new(self.grpc_channel())
    }

    /// Return a flight client suitable for communicating with this
    /// server
    pub fn flight_client(&self) -> influxdb_iox_client::flight::Client {
        influxdb_iox_client::flight::Client::new(self.grpc_channel())
    }

    /// Return a storage API client suitable for communicating with this
    /// server
    pub fn storage_client(&self) -> generated_types::storage_client::StorageClient<Connection> {
        generated_types::storage_client::StorageClient::new(self.grpc_channel())
    }

    /// Restart test server.
    ///
    /// This will break all currently connected clients!
    pub async fn restart_server(self) -> Self {
        self.server.restart().await;
        self.server
            .wait_until_ready(InitialConfig::SetWriterId)
            .await;
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

    /// Wait until server is initialized and all databases have initialized or errored.
    pub async fn wait_server_initialized(&self) -> ServerStatus {
        let mut client = self.management_client();
        let t_0 = Instant::now();
        loop {
            let status = client.get_server_status().await.unwrap();
            if status.initialized
                && status.database_statuses.iter().all(|status| {
                    status.error.is_some()
                        || DatabaseState::from_i32(status.state).unwrap()
                            == DatabaseState::Initialized
                })
            {
                return status;
            }
            assert!(t_0.elapsed() < Duration::from_secs(10));
            tokio::time::sleep(Duration::from_millis(100)).await;
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

// Options for creating test servers
#[derive(Default, Debug, Clone)]
pub struct TestConfig {
    /// Additional environment variables
    env: Vec<(String, String)>,

    /// Headers to add to all client requests
    client_headers: Vec<(HeaderName, HeaderValue)>,
}

impl TestConfig {
    pub fn new() -> Self {
        Default::default()
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
    fn new(test_config: TestConfig) -> Self {
        let addrs = BindAddresses::default();
        let ready = Mutex::new(ServerState::Started);

        let dir = test_helpers::tmp_dir().unwrap();

        let server_process =
            Mutex::new(Self::create_server_process(&addrs, &dir, &test_config.env));

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
        *server_process =
            Self::create_server_process(&self.addrs, &self.dir, &self.test_config.env);
        *ready_guard = ServerState::Started;
    }

    fn create_server_process(
        addrs: &BindAddresses,
        dir: &TempDir,
        env: &[(String, String)],
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
            "Server {} Logging to {:?}",
            addrs.http_bind_addr(),
            log_path
        );
        println!("****************");

        // If set in test environment, use that value, else default to info
        let log_filter = std::env::var("LOG_FILTER").unwrap_or_else(|_| "info".to_string());

        // This will inherit environment from the test runner
        // in particular `LOG_FILTER`
        let child = Command::cargo_bin("influxdb_iox")
            .unwrap()
            .arg("run")
            .env("LOG_FILTER", log_filter)
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

    async fn wait_until_ready(&self, initial_config: InitialConfig) {
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
                        println!("Successfully got a response from HTTP: {:?}", resp);
                        return;
                    }
                    Err(e) => {
                        println!("Waiting for HTTP server to be up: {}", e);
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
                panic!("Server was not ready in required time: {}", e);
            }
        }

        let channel = self.grpc_channel().await.expect("gRPC should be running");
        let mut management_client = influxdb_iox_client::management::Client::new(channel);

        if let Ok(id) = management_client.get_server_id().await {
            // tell others that this server had some problem
            *ready = ServerState::Error;
            std::mem::drop(ready);
            panic!("Server already has an ID ({}); possibly a stray/orphan server from another test run.", id);
        }

        // Set the writer id, if requested
        match initial_config {
            InitialConfig::SetWriterId => {
                let id = DEFAULT_SERVER_ID;

                management_client
                    .update_server_id(id)
                    .await
                    .expect("set ID failed");

                println!("Set writer_id to {:?}", id);

                // if server ID was set, we can also wait until DBs are loaded
                let check_dbs_loaded = async {
                    let mut interval = tokio::time::interval(Duration::from_millis(1000));

                    while !management_client
                        .get_server_status()
                        .await
                        .unwrap()
                        .initialized
                    {
                        interval.tick().await;
                    }
                };

                let capped_check = tokio::time::timeout(Duration::from_secs(30), check_dbs_loaded);

                match capped_check.await {
                    Ok(_) => {
                        println!("Databases loaded");
                    }
                    Err(e) => {
                        // tell others that this server had some problem
                        *ready = ServerState::Error;
                        std::mem::drop(ready);
                        panic!("Server did not load databases in required time: {}", e);
                    }
                }
            }
            InitialConfig::None => {}
        };
    }

    pub async fn wait_for_grpc(&self) {
        let mut interval = tokio::time::interval(Duration::from_millis(1000));

        loop {
            match self.grpc_channel().await {
                Ok(channel) => {
                    println!("Successfully connected to server");

                    let mut health = influxdb_iox_client::health::Client::new(channel);

                    match health.check_storage().await {
                        Ok(_) => {
                            println!("Storage service is running");
                            return;
                        }
                        Err(e) => {
                            println!("Error checking storage service status: {}", e);
                        }
                    }
                }
                Err(e) => {
                    println!("Waiting for gRPC API to be up: {}", e);
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
            "TestServer (grpc {}, http {})",
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
        println!("Start TestServer Output");
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
        println!("End TestServer Output");
        println!("****************");
    }
}
