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

use crate::{
    database::initialize_db,
    dump_log_to_stdout, log_command,
    server_type::AddAddrEnv,
    service_link::{link_services, unlink_services, LinkableService, LinkableServiceImpl},
};

use super::{addrs::BindAddresses, ServerType, TestConfig};

/// The duration of time a [`TestServer`] is given to gracefully shutdown after
/// receiving a SIGTERM, before a SIGKILL is sent to kill it.
pub(crate) const GRACEFUL_SERVER_STOP_TIMEOUT: Duration = Duration::from_secs(5);

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

    /// Create multiple, potentially interdependent sever fixtures concurrently because [`create](Self::create)  only
    /// returns when health is OK.
    pub async fn create_multiple(
        test_configs: impl IntoIterator<Item = TestConfig> + Send,
    ) -> Vec<Self> {
        let test_configs = test_configs.into_iter().collect::<Vec<_>>();
        let n_configs = test_configs.len();
        futures::stream::iter(test_configs)
            .map(|cfg| async move { Self::create(cfg).await })
            .buffered(n_configs)
            .collect::<Vec<_>>()
            .await
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
        // unlink clients because we are going to drop the server
        let clients = unlink_services(Arc::clone(&self.server) as _);

        // get the underlying server, if possible
        let mut server = match Arc::try_unwrap(self.server) {
            Ok(s) => s,
            Err(_) => panic!("Can not restart server as it is shared"),
        };

        // disconnect so server doesn't wait for our client
        drop(self.connections);

        server.restart().await;
        let connections = server.wait_until_ready().await;
        let server = Arc::new(server);

        // relink clients
        for client in clients {
            link_services(Arc::clone(&server) as _, client);
        }

        Self {
            server,
            connections,
        }
    }

    /// Shutdown server in a clean way and wait for process to exit.
    pub async fn shutdown(self) {
        // unlink clients because we are going to drop the server
        unlink_services(Arc::clone(&self.server) as _);

        // get the underlying server, if possible
        let mut server = match Arc::try_unwrap(self.server) {
            Ok(s) => s,
            Err(_) => panic!("Can not restart server as it is shared"),
        };

        // disconnect so server doesn't wait for our client
        drop(self.connections);

        server.stop().await;
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

    /// Return the http base URL for the catalog HTTP API
    pub fn catalog_http_base(&self) -> Arc<str> {
        self.server.addrs().catalog_http_api().client_base()
    }

    /// Return the grpc base URL for the catalog gRPC API
    pub fn catalog_grpc_base(&self) -> Arc<str> {
        self.server.addrs().catalog_grpc_api().client_base()
    }

    /// Return log path for server process.
    pub fn log_path(&self) -> Box<Path> {
        self.server.log_path.clone()
    }

    /// Get a strong reference to the underlying `TestServer`
    pub(crate) fn strong(&self) -> Arc<TestServer> {
        Arc::clone(&self.server)
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
    Stopped,
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

    /// connection to catalog gRPC, if available
    catalog_grpc_connection: Option<Connection>,
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

    /// Return a channel connected to the gRPC API, panic'ing if not the correct type of server
    pub fn catalog_grpc_connection(&self) -> Connection {
        self.catalog_grpc_connection
            .as_ref()
            .expect("Server type does not have router")
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

        self.catalog_grpc_connection = match server_type {
            ServerType::Catalog => {
                let client_base = test_config.addrs().catalog_grpc_api().client_base();
                Some(
                    grpc_channel(test_config, client_base.as_ref())
                        .await
                        .map_err(|e| format!("Cannot connect to catalog at {client_base}: {e}"))?,
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

    /// Path to log file.
    log_path: Box<Path>,

    /// Handle to the server process being controlled
    server_process: Arc<Mutex<Option<Child>>>,

    /// Configuration values for starting the test server
    test_config: TestConfig,

    /// Service links.
    links: LinkableServiceImpl,
}

impl TestServer {
    async fn new(test_config: TestConfig) -> Self {
        let ready = Mutex::new(ServerState::Started);

        let (_log_file, log_path) = NamedTempFile::new()
            .expect("opening log file")
            .keep()
            .expect("expected to keep");

        let server_process = Arc::new(Mutex::new(Some(
            Self::create_server_process(&test_config, &log_path).await,
        )));

        Self {
            ready,
            log_path: log_path.into_boxed_path(),
            server_process,
            test_config,
            links: Default::default(),
        }
    }

    /// Returns the addresses to which the server has been bound
    fn addrs(&self) -> &BindAddresses {
        self.test_config.addrs()
    }

    /// Stop server.
    async fn stop(&mut self) {
        let mut ready_guard = self.ready.lock().await;
        let mut server_lock = self.server_process.lock().await;

        Self::stop_inner(
            &mut ready_guard,
            &mut server_lock,
            self.test_config.server_type(),
        )
        .await;
    }

    async fn stop_inner(
        ready: &mut ServerState,
        server_process: &mut Option<Child>,
        t: ServerType,
    ) {
        let server_process = server_process.take().expect("server process exists");
        tokio::task::spawn_blocking(move || {
            kill_politely(server_process, Duration::from_secs(5), t);
        })
        .await
        .expect("kill politely worked");

        *ready = ServerState::Stopped;
    }

    /// Restarts the tests server process, but does not reconnect clients
    async fn restart(&mut self) {
        let mut ready_guard = self.ready.lock().await;
        let mut server_lock = self.server_process.lock().await;

        Self::stop_inner(
            &mut ready_guard,
            &mut server_lock,
            self.test_config.server_type(),
        )
        .await;

        *server_lock = Some(Self::create_server_process(&self.test_config, &self.log_path).await);
        *ready_guard = ServerState::Started;
    }

    async fn create_server_process(test_config: &TestConfig, log_path: &Path) -> Child {
        // Create a new file each time and keep it around to aid debugging
        let log_file = OpenOptions::new()
            .read(true)
            .append(true)
            .open(log_path)
            .expect("log file should still be there");

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

        command.spawn().unwrap()
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
                ServerState::Stopped => {
                    panic!("Server was stopped");
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
            let url = format!("{}/health", self.test_config.http_base());
            let mut interval = tokio::time::interval(Duration::from_millis(100));
            loop {
                if server_dead(server_process.as_ref()).await {
                    break;
                }
                match client.get(&url).send().await {
                    Ok(resp)
                        if resp.status().is_success() || !self.test_config.wait_for_ready() =>
                    {
                        info!(
                            "Successfully got a response from {:?} HTTP: {:?}",
                            self.test_config.server_type(),
                            resp
                        );
                        return;
                    }
                    Ok(resp) => {
                        info!(
                            "Waiting for {:?} HTTP server to be up: {:?}",
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
        let mut interval = tokio::time::interval(Duration::from_millis(100));

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
                ServerType::Catalog => {
                    if check_catalog_v2_service_health(
                        server_type,
                        connections.catalog_grpc_connection(),
                        self.test_config.wait_for_ready(),
                    )
                    .await
                    {
                        return;
                    }
                }
                ServerType::ParquetCache => {
                    unimplemented!("ParquetCache server should not use grpc, only http");
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

impl LinkableService for TestServer {
    fn add_link_client(&self, client: Weak<dyn LinkableService>) {
        self.links.add_link_client(client)
    }

    fn remove_link_clients(&self) -> Vec<Arc<dyn LinkableService>> {
        self.links.remove_link_clients()
    }

    fn add_link_server(&self, server: Arc<dyn LinkableService>) {
        self.links.add_link_server(server)
    }

    fn remove_link_server(&self, server: Arc<dyn LinkableService>) {
        self.links.remove_link_server(server)
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

/// checks catalog service V2 health, as a proxy for all gRPC
/// services. Returns false if the service should be checked again
async fn check_catalog_v2_service_health(
    server_type: ServerType,
    connection: Connection,
    wait_for_ready: bool,
) -> bool {
    let mut health = influxdb_iox_client::health::Client::new(connection);

    match health
        .check("influxdata.iox.catalog.v2.CatalogService")
        .await
    {
        Ok(ready) => {
            if ready || !wait_for_ready {
                info!("CatalogService service {:?} is running", server_type);
                true
            } else {
                info!("CatalogService {:?} is not running", server_type);
                false
            }
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

        if let Some(server_process) = server_lock.take() {
            let test_config = self.test_config.clone();
            let log_path = self.log_path.clone();
            let links = self.links.clone();

            let kill_and_dump = move || {
                kill_politely(
                    server_process,
                    GRACEFUL_SERVER_STOP_TIMEOUT,
                    test_config.server_type(),
                );

                dump_log_to_stdout(&format!("{:?}", test_config.server_type()), &log_path);

                // keep links til server is actually gone
                drop(links);

                // keep test config til the very last because it contains the WAL dir
                drop(test_config);
            };

            // if there's still a tokio runtime around, use that to help the shut down process, because our client
            // connections need to interact with the HTTP/2 shutdown and we shall not block the runtime during that
            match tokio::runtime::Handle::try_current() {
                Ok(handle) => {
                    // tokio might decide to not schedule our future, in which case we still want to kill the child, so
                    // we wrap the kill method into a helper that is either executed within a tokio context or is
                    // executed when tokio drops it.
                    let mut kill_and_dump = ExecOnDrop(Some(Box::new(kill_and_dump)));
                    handle.spawn_blocking(move || {
                        kill_and_dump.maybe_exec();
                    });
                }
                Err(_) => {
                    kill_and_dump();
                }
            }
        }
    }
}

struct ExecOnDrop(Option<Box<dyn FnOnce() + Send>>);

impl ExecOnDrop {
    fn maybe_exec(&mut self) {
        if let Some(f) = self.0.take() {
            f();
        }
    }
}

impl Drop for ExecOnDrop {
    fn drop(&mut self) {
        self.maybe_exec();
    }
}

/// returns true if the server process has exited (for any reason), and
/// prints what happened to stdout
async fn server_dead(server_process: &Mutex<Option<Child>>) -> bool {
    match server_process.lock().await.deref_mut() {
        Some(server_process) => server_dead_inner(server_process),
        None => true,
    }
}

fn server_dead_inner(server_process: &mut Child) -> bool {
    match server_process.try_wait() {
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
fn kill_politely(mut child: Child, wait: Duration, t: ServerType) {
    if server_dead_inner(&mut child) {
        // fast path
        return;
    }

    kill_politely_inner(&mut child, wait, t);
}

fn kill_politely_inner(child: &mut Child, wait: Duration, t: ServerType) {
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
            info!("Error sending SIGTERM to child ({t:?}): {e}");
            true
        }
    };

    if wait_errored {
        // timeout => kill it
        warn!("Cannot terminate child ({t:?}) politely, using SIGKILL...");

        if let Err(e) = signal::kill(pid, Signal::SIGKILL) {
            info!("Error sending SIGKILL to child ({t:?}): {e}");
        }
        if let Err(e) = waitpid(pid, None) {
            info!("Cannot wait for child ({t:?}): {e}");
        }
    } else {
        info!("Killed child ({t:?}) politely");
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
