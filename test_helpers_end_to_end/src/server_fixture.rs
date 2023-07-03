use assert_cmd::cargo::CommandCargoExt;
use futures::prelude::*;
use influxdb_iox_client::connection::Connection;
use observability_deps::tracing::{info, warn};
use std::{
    fmt::Debug,
    fs::OpenOptions,
    ops::DerefMut,
    path::Path,
    process::{Child, Command},
    str,
    sync::{Arc, Weak},
    time::Duration,
};
use tempfile::NamedTempFile;
use test_helpers::timeout::FutureTimeout;
use tokio::sync::Mutex;

use crate::{database::initialize_db, dump_log_to_stdout, log_command, server_type::AddAddrEnv};

use super::{addrs::BindAddresses, ServerType, TestConfig};

/// The duration of time a [`TestServer`] is given to gracefully shutdown after
/// receiving a SIGTERM, before a SIGKILL is sent to kill it.
pub const GRACEFUL_SERVER_STOP_TIMEOUT: Duration = Duration::from_secs(5);

/// Represents a server that has been started and is available for
/// testing.
///
/// Note This structure can not be shared between tests because even
/// though a `Connection` is `Cloneable` if multiple requests are
/// issued through the same connection chaos ensues.
#[derive(Debug)]
pub struct ServerFixture {
    server: Arc<TestServer>,
    connections: Connections,
}

impl ServerFixture {
    /// Create a new server fixture and wait for it to be ready. This
    /// is called "create" rather than new because it is async and
    /// waits.
    pub async fn create(test_config: TestConfig) -> Self {
        let server = TestServer::new(test_config).await;
        Self::create_from_existing(Arc::new(server)).await
    }

    /// Create a new server fixture that shares the same TestServer,
    /// but has its own connections
    pub(crate) async fn create_from_existing(server: Arc<TestServer>) -> Self {
        // ensure the server is ready
        let connections = server.wait_until_ready().await;

        ServerFixture {
            server,
            connections,
        }
    }

    /// Restart test server, panic'ing if it is shared with some other
    /// test
    ///
    /// This will break all currently connected clients!
    pub async fn restart_server(self) -> Self {
        // get the underlying server, if possible
        let mut server = match Arc::try_unwrap(self.server) {
            Ok(s) => s,
            Err(_) => panic!("Can not restart server as it is shared"),
        };

        server.restart().await;
        let connections = server.wait_until_ready().await;

        Self {
            server: Arc::new(server),
            connections,
        }
    }

    pub fn connections(&self) -> &Connections {
        &self.connections
    }

    /// Return a channel connected to the gRPC API, panic'ing if not the correct type of server
    pub fn router_grpc_connection(&self) -> Connection {
        self.connections.router_grpc_connection()
    }

    /// Return a channel connected to the ingester gRPC API, panic'ing if not the correct type of
    /// server
    pub fn ingester_grpc_connection(&self) -> Connection {
        self.connections.ingester_grpc_connection()
    }

    /// Return a channel connected to the querier gRPC API, panic'ing if not the correct type of
    /// server
    pub fn querier_grpc_connection(&self) -> Connection {
        self.connections.querier_grpc_connection()
    }

    /// Return the http base URL for the router HTTP API
    pub fn router_http_base(&self) -> Arc<str> {
        self.server.addrs().router_http_api().client_base()
    }

    /// Return the http base URL for the router gRPC API
    pub fn router_grpc_base(&self) -> Arc<str> {
        self.server.addrs().router_grpc_api().client_base()
    }

    /// Return the http base URL for the ingester gRPC API
    pub fn ingester_grpc_base(&self) -> Arc<str> {
        self.server.addrs().ingester_grpc_api().client_base()
    }

    /// Return the http base URL for the querier gRPC API
    pub fn querier_grpc_base(&self) -> Arc<str> {
        self.server.addrs().querier_grpc_api().client_base()
    }

    /// Return log path for server process.
    pub async fn log_path(&self) -> Box<Path> {
        self.server.server_process.lock().await.log_path.clone()
    }

    /// Get a weak reference to the underlying `TestServer`
    pub(crate) fn weak(&self) -> Weak<TestServer> {
        Arc::downgrade(&self.server)
    }
}

/// Represents the current known state of a TestServer
#[derive(Debug)]
enum ServerState {
    Started,
    Starting,
    Ready,
    Error,
}

/// Mananges some number of gRPC connections
#[derive(Debug, Default)]
pub struct Connections {
    /// connection to router gRPC, if available
    router_grpc_connection: Option<Connection>,

    /// connection to ingester gRPC, if available
    ingester_grpc_connection: Option<Connection>,

    /// connection to querier gRPC, if available
    querier_grpc_connection: Option<Connection>,
}

impl Connections {
    /// Create a new set of connections, that are initially unconnected
    pub fn new() -> Self {
        Default::default()
    }

    /// Return a channel connected to the gRPC API, panic'ing if not the correct type of server
    pub fn router_grpc_connection(&self) -> Connection {
        self.router_grpc_connection
            .as_ref()
            .expect("Server type does not have router")
            .clone()
    }

    /// Return a channel connected to the ingester gRPC API, panic'ing if not the correct type of
    /// server
    pub fn ingester_grpc_connection(&self) -> Connection {
        self.ingester_grpc_connection
            .as_ref()
            .expect("Server type does not have ingester")
            .clone()
    }

    /// Return a channel connected to the querier gRPC API, panic'ing if not the correct type of
    /// server
    pub fn querier_grpc_connection(&self) -> Connection {
        self.querier_grpc_connection
            .as_ref()
            .expect("Server type does not have querier")
            .clone()
    }

    /// (re)establish channels to all gRPC services that were started with the specified test config
    async fn reconnect(&mut self, test_config: &TestConfig) -> Result<(), String> {
        let server_type = test_config.server_type();

        self.router_grpc_connection = match server_type {
            ServerType::AllInOne | ServerType::Router => {
                let client_base = test_config.addrs().router_grpc_api().client_base();
                Some(
                    grpc_channel(test_config, client_base.as_ref())
                        .await
                        .map_err(|e| format!("Cannot connect to router at {client_base}: {e}"))?,
                )
            }
            _ => None,
        };

        self.ingester_grpc_connection = match server_type {
            ServerType::AllInOne | ServerType::Ingester => {
                let client_base = test_config.addrs().ingester_grpc_api().client_base();
                Some(
                    grpc_channel(test_config, client_base.as_ref())
                        .await
                        .map_err(|e| format!("Cannot connect to ingester at {client_base}: {e}"))?,
                )
            }
            _ => None,
        };

        self.querier_grpc_connection = match server_type {
            ServerType::AllInOne | ServerType::Querier => {
                let client_base = test_config.addrs().querier_grpc_api().client_base();
                Some(
                    grpc_channel(test_config, client_base.as_ref())
                        .await
                        .map_err(|e| format!("Cannot connect to querier at {client_base}: {e}"))?,
                )
            }
            _ => None,
        };

        Ok(())
    }
}

/// Create a connection channel to the specified gRPC endpoint
async fn grpc_channel(
    test_config: &TestConfig,
    client_base: &str,
) -> influxdb_iox_client::connection::Result<Connection> {
    let builder = influxdb_iox_client::connection::Builder::default();

    info!("Creating gRPC channel to {}", client_base);
    test_config
        .client_headers()
        .iter()
        .fold(builder, |builder, (header_name, header_value)| {
            builder.header(header_name, header_value)
        })
        .build(client_base)
        .await
}

#[derive(Debug)]
pub struct TestServer {
    /// Is the server ready to accept connections?
    ready: Mutex<ServerState>,

    /// Handle to the server process being controlled
    server_process: Arc<Mutex<Process>>,

    /// Configuration values for starting the test server
    test_config: TestConfig,
}

#[derive(Debug)]
struct Process {
    child: Child,
    log_path: Box<Path>,
}

impl TestServer {
    async fn new(test_config: TestConfig) -> Self {
        let ready = Mutex::new(ServerState::Started);

        let server_process = Arc::new(Mutex::new(
            Self::create_server_process(&test_config, None).await,
        ));

        Self {
            ready,
            server_process,
            test_config,
        }
    }

    /// Returns the addresses to which the server has been bound
    fn addrs(&self) -> &BindAddresses {
        self.test_config.addrs()
    }

    /// Restarts the tests server process, but does not reconnect clients
    async fn restart(&mut self) {
        let mut ready_guard = self.ready.lock().await;
        let mut server_process = self.server_process.lock().await;
        kill_politely(&mut server_process.child, Duration::from_secs(5));
        *server_process =
            Self::create_server_process(&self.test_config, Some(server_process.log_path.clone()))
                .await;
        *ready_guard = ServerState::Started;
    }

    async fn create_server_process(
        test_config: &TestConfig,
        log_path: Option<Box<Path>>,
    ) -> Process {
        // Create a new file each time and keep it around to aid debugging
        let (log_file, log_path) = match log_path {
            Some(log_path) => (
                OpenOptions::new()
                    .read(true)
                    .append(true)
                    .open(&log_path)
                    .expect("log file should still be there"),
                log_path,
            ),
            None => {
                let (log_file, log_path) = NamedTempFile::new()
                    .expect("opening log file")
                    .keep()
                    .expect("expected to keep");
                (log_file, log_path.into_boxed_path())
            }
        };

        let stdout_log_file = log_file
            .try_clone()
            .expect("cloning file handle for stdout");
        let stderr_log_file = log_file;

        let server_type = test_config.server_type();

        info!("****************");
        info!("Server {:?} Logging to {:?}", server_type, log_path);
        info!("****************");

        // If set in test environment, use that value, else default to info
        let log_filter =
            std::env::var("LOG_FILTER").unwrap_or_else(|_| "info,sqlx=warn".to_string());

        let run_command_name = server_type.run_command();

        let mut command = Command::cargo_bin("influxdb_iox").unwrap();
        let mut command = command
            .arg("run")
            .arg(run_command_name)
            .env("LOG_FILTER", log_filter)
            // add http/grpc address information
            .add_addr_env(server_type, test_config.addrs())
            .envs(test_config.env());

        let dsn = test_config.dsn();

        // If this isn't running all-in-one in ephemeral mode, use the DSN
        if !(server_type == ServerType::AllInOne && dsn.is_none()) {
            let dsn = dsn.as_ref().expect("dsn is required for this mode");
            let schema_name = test_config.catalog_schema_name();
            initialize_db(dsn, schema_name).await;
            command = command
                .env("INFLUXDB_IOX_CATALOG_DSN", dsn)
                .env("INFLUXDB_IOX_CATALOG_POSTGRES_SCHEMA_NAME", schema_name);
        }

        command = command
            // redirect output to log file
            .stdout(stdout_log_file)
            .stderr(stderr_log_file);

        log_command(command);

        let child = command.spawn().unwrap();

        Process { child, log_path }
    }

    /// Polls the various services to ensure the server is
    /// operational, reestablishing grpc connections, and returning
    /// those active connections
    async fn wait_until_ready(&self) -> Connections {
        let mut need_wait_for_startup = false;
        {
            let mut ready = self.ready.lock().await;
            match *ready {
                ServerState::Started => {
                    // first time, need to try and start it
                    need_wait_for_startup = true;
                    *ready = ServerState::Starting;
                }
                ServerState::Starting => {
                    // someone else is starting this
                }
                ServerState::Ready => {}
                ServerState::Error => {
                    panic!("Server was previously found to be in Error, aborting");
                }
            };
        }

        // at first, attempt to reconnect all the clients
        let mut connections = Connections::new();
        async {
            let mut interval = tokio::time::interval(Duration::from_millis(100));
            loop {
                match connections.reconnect(&self.test_config).await {
                    Err(e) => info!("wait_until_ready: can not yet connect: {}", e),
                    Ok(()) => return,
                }
                interval.tick().await;
            }
        }
        .with_timeout_panic(Duration::from_secs(10))
        .await;

        if !need_wait_for_startup {
            return connections;
        }

        // Poll the RPC and HTTP servers separately as they listen on
        // different ports but both need to be up for the test to run
        let try_grpc_connect = self.wait_for_grpc(&connections);

        let server_process = Arc::clone(&self.server_process);
        let try_http_connect = async {
            let client = reqwest::Client::new();
            let url = format!("{}/health", self.addrs().router_http_api().client_base());
            let mut interval = tokio::time::interval(Duration::from_millis(1000));
            loop {
                if server_dead(server_process.as_ref()).await {
                    break;
                }
                match client.get(&url).send().await {
                    Ok(resp) => {
                        info!(
                            "Successfully got a response from {:?} HTTP: {:?}",
                            self.test_config.server_type(),
                            resp
                        );
                        return;
                    }
                    Err(e) => {
                        info!(
                            "Waiting for {:?} HTTP server to be up: {}",
                            self.test_config.server_type(),
                            e
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
                info!("Successfully started {}", self);
                let mut ready = self.ready.lock().await;
                *ready = ServerState::Ready;
            }
            Err(e) => {
                // tell others that this server had some problem
                let mut ready = self.ready.lock().await;
                *ready = ServerState::Error;
                std::mem::drop(ready);
                panic!(
                    "{:?} Server was not ready in required time: {}",
                    self.test_config.server_type(),
                    e
                );
            }
        }

        connections
    }

    pub async fn wait_for_grpc(&self, connections: &Connections) {
        let server_process = Arc::clone(&self.server_process);
        let mut interval = tokio::time::interval(Duration::from_millis(1000));

        let server_type = self.test_config.server_type();
        loop {
            if server_dead(server_process.as_ref()).await {
                break;
            }

            match server_type {
                ServerType::Compactor => {
                    unimplemented!(
                        "Don't use a long-running compactor and gRPC in e2e tests; use \
                        `influxdb_iox compactor run-once` instead"
                    );
                }
                ServerType::Router => {
                    if check_catalog_service_health(
                        server_type,
                        connections.router_grpc_connection(),
                    )
                    .await
                    {
                        return;
                    }
                }
                ServerType::Ingester => {
                    if check_arrow_service_health(
                        server_type,
                        connections.ingester_grpc_connection(),
                    )
                    .await
                    {
                        return;
                    }
                }
                ServerType::Querier => {
                    if check_arrow_service_health(
                        server_type,
                        connections.querier_grpc_connection(),
                    )
                    .await
                    {
                        return;
                    }
                }
                ServerType::AllInOne => {
                    // ensure we can write and query
                    // TODO also check ingester
                    if check_catalog_service_health(
                        server_type,
                        connections.router_grpc_connection(),
                    )
                    .await
                        && check_arrow_service_health(
                            server_type,
                            connections.ingester_grpc_connection(),
                        )
                        .await
                        && check_arrow_service_health(
                            server_type,
                            connections.querier_grpc_connection(),
                        )
                        .await
                    {
                        return;
                    }
                }
            }
            interval.tick().await;
        }
    }
}

/// checks catalog service health, as a proxy for all gRPC
/// services. Returns false if the service should be checked again
async fn check_catalog_service_health(server_type: ServerType, connection: Connection) -> bool {
    let mut health = influxdb_iox_client::health::Client::new(connection);

    match health
        .check("influxdata.iox.catalog.v1.CatalogService")
        .await
    {
        Ok(true) => {
            info!("CatalogService service {:?} is running", server_type);
            true
        }
        Ok(false) => {
            info!("CatalogService {:?} is not running", server_type);
            true
        }
        Err(e) => {
            info!("CatalogService {:?} not yet healthy: {:?}", server_type, e);
            false
        }
    }
}

/// checks the arrow service service health, returning false if the service should be checked again
async fn check_arrow_service_health(server_type: ServerType, connection: Connection) -> bool {
    let mut health = influxdb_iox_client::health::Client::new(connection);

    match health.check_arrow().await {
        Ok(true) => {
            info!("Flight service {:?} is running", server_type);
            true
        }
        Ok(false) => {
            info!("Flight service {:?} is not running", server_type);
            true
        }
        Err(e) => {
            info!("Flight service {:?} not yet healthy: {:?}", server_type, e);
            false
        }
    }
}

impl std::fmt::Display for TestServer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::result::Result<(), std::fmt::Error> {
        write!(
            f,
            "TestServer {:?} ({})",
            self.test_config.server_type(),
            self.addrs()
        )
    }
}

impl Drop for TestServer {
    fn drop(&mut self) {
        let mut server_lock = self
            .server_process
            .try_lock()
            .expect("should be able to get a server process lock");

        server_dead_inner(server_lock.deref_mut());
        kill_politely(&mut server_lock.child, GRACEFUL_SERVER_STOP_TIMEOUT);

        dump_log_to_stdout(
            &format!("{:?}", self.test_config.server_type()),
            &server_lock.log_path,
        );
    }
}

/// returns true if the server process has exited (for any reason), and
/// prints what happened to stdout
async fn server_dead(server_process: &Mutex<Process>) -> bool {
    server_dead_inner(server_process.lock().await.deref_mut())
}

fn server_dead_inner(server_process: &mut Process) -> bool {
    match server_process.child.try_wait() {
        Ok(None) => false,
        Ok(Some(status)) => {
            warn!("Server process exited: {}", status);
            true
        }
        Err(e) => {
            warn!("Error getting server process exit status: {}", e);
            true
        }
    }
}

/// Attempt to kill a child process politely.
fn kill_politely(child: &mut Child, wait: Duration) {
    use nix::{
        sys::{
            signal::{self, Signal},
            wait::waitpid,
        },
        unistd::Pid,
    };

    let pid = Pid::from_raw(child.id().try_into().unwrap());

    // try to be polite
    let wait_errored = match signal::kill(pid, Signal::SIGTERM) {
        Ok(()) => wait_timeout(pid, wait).is_err(),
        Err(e) => {
            info!("Error sending SIGTERM to child: {e}");
            true
        }
    };

    if wait_errored {
        // timeout => kill it
        info!("Cannot terminate child politely, using SIGKILL...");

        if let Err(e) = signal::kill(pid, Signal::SIGKILL) {
            info!("Error sending SIGKILL to child: {e}");
        }
        if let Err(e) = waitpid(pid, None) {
            info!("Cannot wait for child: {e}");
        }
    } else {
        info!("Killed child politely");
    }
}

/// Wait for given PID to exit with a timeout.
fn wait_timeout(pid: nix::unistd::Pid, timeout: Duration) -> Result<(), ()> {
    use nix::sys::wait::waitpid;

    // use some thread and channel trickery, see https://stackoverflow.com/a/42720480
    let (sender, receiver) = std::sync::mpsc::channel();
    std::thread::spawn(move || {
        let waitpid_res = waitpid(pid, None).map(|_| ());

        // errors if the receiver side is gone which is OK.
        sender.send(waitpid_res).ok();
    });

    match receiver.recv_timeout(timeout) {
        Ok(Ok(())) => Ok(()),
        Ok(Err(e)) => {
            info!("Cannot wait for child: {e}");
            Err(())
        }
        Err(_) => {
            info!("Timeout waiting for child");
            Err(())
        }
    }
}
