use assert_cmd::prelude::*;
use futures::prelude::*;
use influxdb_iox_client::connection::Connection;
use std::{
    path::Path,
    process::{Child, Command},
    str,
    sync::Arc,
    time::Duration,
};
use tempfile::{NamedTempFile, TempDir};
use tokio::sync::Mutex;

use crate::{database::initialize_db, server_type::AddAddrEnv};

use super::{addrs::BindAddresses, ServerType, TestConfig};

/// Represents a server that has been started and is available for
/// testing.
pub struct ServerFixture {
    server: Arc<TestServer>,
}

impl ServerFixture {
    /// Create a new server fixture and wait for it to be ready. This
    /// is called "create" rather than new because it is async and
    /// waits. The server is not shared with any other tests.
    pub async fn create(test_config: TestConfig) -> Self {
        let mut server = TestServer::new(test_config).await;

        // ensure the server is ready
        server.wait_until_ready().await;

        let server = Arc::new(server);

        ServerFixture { server }
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
        server.wait_until_ready().await;

        Self {
            server: Arc::new(server),
        }
    }

    /// Directory used for data storage.
    pub fn dir(&self) -> &Path {
        self.server.dir.path()
    }

    /// Get a reference to the underlying server.
    #[must_use]
    pub fn server(&self) -> &TestServer {
        self.server.as_ref()
    }
}

#[derive(Debug)]
/// Represents the current known state of a TestServer
enum ServerState {
    Started,
    Starting,
    Ready,
    Error,
}

pub struct TestServer {
    /// Is the server ready to accept connections?
    ready: Mutex<ServerState>,

    /// Handle to the server process being controlled
    server_process: Arc<Mutex<Process>>,

    /// Which ports this server should use
    addrs: BindAddresses,

    /// The temporary directory **must** be last so that it is
    /// dropped after the database closes.
    dir: TempDir,

    /// Configuration values for starting the test server
    test_config: TestConfig,

    /// connection to router gRPC, if available
    router_grpc_connection: Option<Connection>,

    /// connection to ingester gRPC, if available
    ingester_grpc_connection: Option<Connection>,

    /// connection to querier gRPC, if available
    querier_grpc_connection: Option<Connection>,
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

        let server_process = Arc::new(Mutex::new(
            Self::create_server_process(&addrs, &dir, &test_config).await,
        ));

        Self {
            ready,
            server_process,
            addrs,
            dir,
            test_config,
            router_grpc_connection: None,
            ingester_grpc_connection: None,
            querier_grpc_connection: None,
        }
    }

    /// Return a channel connected to the gRPC API, panic'ing if not the correct type of server
    pub fn router_grpc_connection(&self) -> Connection {
        self.router_grpc_connection
            .as_ref()
            .expect("Server type does not have router")
            .clone()
    }

    /// Return a channel connected to the ingester gRPC API, panic'ing if not the correct type of server
    pub fn ingester_grpc_connection(&self) -> Connection {
        self.ingester_grpc_connection
            .as_ref()
            .expect("Server type does not have ingester")
            .clone()
    }

    /// Return a channel connected to the querier gRPC API, panic'ing if not the correct type of server
    pub fn querier_grpc_connection(&self) -> Connection {
        self.querier_grpc_connection
            .as_ref()
            .expect("Server type does not have querier")
            .clone()
    }

    /// Return the http base URL for the router HTTP API
    pub fn router_http_base(&self) -> Arc<str> {
        self.addrs.router_http_api().client_base()
    }

    /// Create a connection channel to the specified gRPC endpoint
    async fn grpc_channel(
        &self,
        client_base: &str,
    ) -> influxdb_iox_client::connection::Result<Connection> {
        let builder = influxdb_iox_client::connection::Builder::default();

        println!("Creating gRPC channel to {}", client_base);
        self.test_config
            .client_headers()
            .iter()
            .fold(builder, |builder, (header_name, header_value)| {
                builder.header(header_name, header_value)
            })
            .build(client_base)
            .await
    }

    /// (re)establish channels to all gRPC services that are running
    /// for this particular server type
    async fn reconnect(&mut self) -> Result<(), String> {
        let server_type = self.test_config.server_type();

        self.router_grpc_connection =
            match server_type {
                ServerType::AllInOne | ServerType::Router2 => {
                    let client_base = self.addrs.router_grpc_api().client_base();
                    Some(self.grpc_channel(client_base.as_ref()).await.map_err(|e| {
                        format!("Can not connect to router at {}: {}", client_base, e)
                    })?)
                }
                _ => None,
            };

        self.ingester_grpc_connection = match server_type {
            ServerType::AllInOne | ServerType::Ingester => {
                let client_base = self.addrs.ingester_grpc_api().client_base();
                Some(self.grpc_channel(client_base.as_ref()).await.map_err(|e| {
                    format!("Can not connect to ingester at {}: {}", client_base, e)
                })?)
            }
            _ => None,
        };

        self.querier_grpc_connection =
            match server_type {
                ServerType::AllInOne => {
                    let client_base = self.addrs.querier_grpc_api().client_base();
                    Some(self.grpc_channel(client_base.as_ref()).await.map_err(|e| {
                        format!("Can not connect to querier at {}: {}", client_base, e)
                    })?)
                }
                _ => None,
            };

        Ok(())
    }

    /// Returns the addresses to which the server has been bound
    fn addrs(&self) -> &BindAddresses {
        &self.addrs
    }

    /// Restarts the tests server process, but does not reconnect clients
    async fn restart(&self) {
        let mut ready_guard = self.ready.lock().await;
        let mut server_process = self.server_process.lock().await;
        server_process.child.kill().unwrap();
        server_process.child.wait().unwrap();
        *server_process =
            Self::create_server_process(&self.addrs, &self.dir, &self.test_config).await;
        *ready_guard = ServerState::Started;
    }

    async fn create_server_process(
        addrs: &BindAddresses,
        object_store_dir: &TempDir,
        test_config: &TestConfig,
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

        let server_type = test_config.server_type();

        println!("****************");
        println!("Server {:?} Logging to {:?}", server_type, log_path);
        println!("****************");

        // If set in test environment, use that value, else default to info
        let log_filter =
            std::env::var("LOG_FILTER").unwrap_or_else(|_| "info,sqlx=warn".to_string());

        let run_command = server_type.run_command();

        let dsn = test_config.dsn();
        initialize_db(dsn).await;

        // This will inherit environment from the test runner
        // in particular `LOG_FILTER`
        let child = Command::cargo_bin("influxdb_iox")
            .unwrap()
            .arg("run")
            .arg(run_command)
            .env("LOG_FILTER", log_filter)
            .env("INFLUXDB_IOX_CATALOG_DSN", &dsn)
            .env("INFLUXDB_IOX_OBJECT_STORE", "file")
            .env("INFLUXDB_IOX_DB_DIR", object_store_dir.path())
            // add http/grpc address information
            .add_addr_env(server_type, addrs)
            .envs(test_config.env())
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

    /// Polls the various services to ensure the server is
    /// operational, reestablishing grpc connections
    async fn wait_until_ready(&mut self) {
        {
            let mut ready = self.ready.lock().await;
            match *ready {
                ServerState::Started => {} // first time, need to try and start it
                ServerState::Starting => {
                    // someone else is starting this
                    return;
                }
                ServerState::Ready => {
                    return;
                }
                ServerState::Error => {
                    panic!("Server was previously found to be in Error, aborting");
                }
            };
            *ready = ServerState::Starting;
        }

        // at first, attempt to reconnect all the clients
        tokio::time::timeout(Duration::from_secs(10), async {
            let mut interval = tokio::time::interval(Duration::from_millis(100));
            loop {
                match self.reconnect().await {
                    Err(e) => println!("wait_until_ready: can not yet connect: {}", e),
                    Ok(()) => return,
                }
                interval.tick().await;
            }
        })
        .await
        .expect("Timed out waiting to connect clients");

        // Poll the RPC and HTTP servers separately as they listen on
        // different ports but both need to be up for the test to run
        let try_grpc_connect = self.wait_for_grpc();

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
                        println!(
                            "Successfully got a response from {:?} HTTP: {:?}",
                            self.test_config.server_type(),
                            resp
                        );
                        return;
                    }
                    Err(e) => {
                        println!(
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
                println!("Successfully started {}", self);
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
    }

    pub async fn wait_for_grpc(&self) {
        let server_process = Arc::clone(&self.server_process);
        let mut interval = tokio::time::interval(Duration::from_millis(1000));

        let server_type = self.test_config.server_type();
        loop {
            if server_dead(server_process.as_ref()).await {
                break;
            }

            match server_type {
                ServerType::Router2 => {
                    if check_write_service_health(server_type, self.router_grpc_connection()).await
                    {
                        return;
                    }
                }
                ServerType::Ingester => {
                    if check_arrow_service_health(server_type, self.ingester_grpc_connection())
                        .await
                    {
                        return;
                    }
                }
                ServerType::AllInOne => {
                    // ensure we can write and query
                    // TODO also check compactor and ingester
                    if check_write_service_health(server_type, self.router_grpc_connection()).await
                        && check_arrow_service_health(server_type, self.ingester_grpc_connection())
                            .await
                        && check_arrow_service_health(server_type, self.querier_grpc_connection())
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

/// checks write service health, returning false if the service should be checked again
async fn check_write_service_health(server_type: ServerType, connection: Connection) -> bool {
    let mut health = influxdb_iox_client::health::Client::new(connection);

    match health.check("influxdata.pbdata.v1.WriteService").await {
        Ok(true) => {
            println!("Write service {:?} is running", server_type);
            true
        }
        Ok(false) => {
            println!("Write service {:?} is not running", server_type);
            true
        }
        Err(e) => {
            println!("Write service {:?} not yet healthy: {:?}", server_type, e);
            false
        }
    }
}

/// checks the arrow service service health, returning false if the service should be checked again
async fn check_arrow_service_health(server_type: ServerType, connection: Connection) -> bool {
    let mut health = influxdb_iox_client::health::Client::new(connection);

    match health.check_arrow().await {
        Ok(true) => {
            println!("Flight service {:?} is running", server_type);
            true
        }
        Ok(false) => {
            println!("Flight service {:?} is not running", server_type);
            true
        }
        Err(e) => {
            println!("Flight service {:?} not yet healthy: {:?}", server_type, e);
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
            self.addrs
        )
    }
}

impl Drop for TestServer {
    fn drop(&mut self) {
        let mut server_lock = self
            .server_process
            .try_lock()
            .expect("should be able to get a server process lock");

        if let Err(e) = server_lock.child.kill() {
            println!("Error killing child: {}", e);
        }

        if let Err(e) = server_lock.child.wait() {
            println!("Error waiting on child exit: {}", e);
        }

        dump_log_to_stdout(self.test_config.server_type(), &server_lock.log_path);
    }
}

/// returns true if the server process has exited (for any reason), and
/// prints what happened to stdout
async fn server_dead(server_process: &Mutex<Process>) -> bool {
    match server_process.lock().await.child.try_wait() {
        Ok(None) => false,
        Ok(Some(status)) => {
            println!("Server process exited: {}", status);
            true
        }
        Err(e) => {
            println!("Error getting server process exit status: {}", e);
            true
        }
    }
}

/// Dumps the content of the log file to stdout
fn dump_log_to_stdout(server_type: ServerType, log_path: &Path) {
    use std::io::Read;

    let mut f = std::fs::File::open(log_path).expect("failed to open log file");
    let mut buffer = [0_u8; 8 * 1024];

    println!("****************");
    println!("Start {:?} TestServer Output", server_type);
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
                log_path
            );
        }
    }

    println!("****************");
    println!("End {:?} TestServer Output", server_type);
    println!("****************");
}
