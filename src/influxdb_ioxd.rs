use tracing::{error, info, warn};

use std::fs;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;

pub mod http_routes;
pub mod rpc;

use server::{ConnectionManagerImpl as ConnectionManager, Server as AppServer};

use hyper::Server;
use object_store::{self, gcp::GoogleCloudStorage, ObjectStore};

use snafu::{ResultExt, Snafu};

use panic_logging::SendPanicsToTracing;

use crate::commands::{
    config::{load_config, Config},
    logging::LoggingLevel,
};

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Unable to create database directory {:?}: {}", path, source))]
    CreatingDatabaseDirectory {
        path: PathBuf,
        source: std::io::Error,
    },

    #[snafu(display("Unable to initialize database in directory {:?}:  {}", db_dir, source))]
    InitializingMutableBuffer {
        db_dir: PathBuf,
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    #[snafu(display("Unable to restore WAL from directory {:?}:  {}", dir, source))]
    RestoringMutableBuffer {
        dir: PathBuf,
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    #[snafu(display(
        "Unable to bind to listen for HTTP requests on {}: {}",
        bind_addr,
        source
    ))]
    StartListeningHttp {
        bind_addr: SocketAddr,
        source: hyper::Error,
    },

    #[snafu(display(
        "Unable to bind to listen for gRPC requests on {}: {}",
        grpc_bind_addr,
        source
    ))]
    StartListeningGrpc {
        grpc_bind_addr: SocketAddr,
        source: std::io::Error,
    },

    #[snafu(display("Error serving HTTP: {}", source))]
    ServingHttp { source: hyper::Error },

    #[snafu(display("Error serving RPC: {}", source))]
    ServingRPC { source: self::rpc::service::Error },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

/// This is the entry point for the IOx server. `config` represents
/// command line arguments, if any
///
/// The logging_level passed in is the global setting (e.g. if -v or
/// -vv was passed in before 'server')
pub async fn main(logging_level: LoggingLevel, config: Option<Config>) -> Result<()> {
    // load config from environment if no command line
    let config = config.unwrap_or_else(load_config);

    // Handle the case if -v/-vv is specified both before and after the server
    // command
    let logging_level = logging_level.combine(LoggingLevel::new(config.verbose_count));

    let _drop_handle = logging_level.setup_logging(&config);

    // Install custom panic handler and forget about it.
    //
    // This leaks the handler and prevents it from ever being dropped during the
    // lifetime of the program - this is actually a good thing, as it prevents
    // the panic handler from being removed while unwinding a panic (which in
    // turn, causes a panic - see #548)
    let f = SendPanicsToTracing::new();
    std::mem::forget(f);

    let db_dir = &config.database_directory;

    let object_store = if let Some(bucket_name) = &config.gcp_bucket {
        info!("Using GCP bucket {} for storage", bucket_name);
        ObjectStore::new_google_cloud_storage(GoogleCloudStorage::new(bucket_name))
    } else if let Some(db_dir) = db_dir {
        info!("Using local dir {:?} for storage", db_dir);
        fs::create_dir_all(db_dir).context(CreatingDatabaseDirectory { path: db_dir })?;
        ObjectStore::new_file(object_store::disk::File::new(&db_dir))
    } else {
        warn!("NO PERSISTENCE: using memory for object storage");
        ObjectStore::new_in_memory(object_store::memory::InMemory::new())
    };
    let object_storage = Arc::new(object_store);

    let connection_manager = ConnectionManager {};
    let app_server = Arc::new(AppServer::new(connection_manager, object_storage));

    // if this ID isn't set the server won't be usable until this is set via an API
    // call
    if let Some(id) = config.writer_id {
        app_server.set_id(id);
        if let Err(e) = app_server.load_database_configs().await {
            error!(
                "unable to load database configurations from object storage: {}",
                e
            )
        }
    } else {
        warn!("server ID not set. ID must be set via the INFLUXDB_IOX_ID config or API before writing or querying data.");
    }

    // Construct and start up gRPC server

    let grpc_bind_addr = config.grpc_bind_address;
    let socket = tokio::net::TcpListener::bind(grpc_bind_addr)
        .await
        .context(StartListeningGrpc { grpc_bind_addr })?;

    let grpc_server = self::rpc::service::make_server(socket, app_server.clone());

    info!(bind_address=?grpc_bind_addr, "gRPC server listening");

    // Construct and start up HTTP server

    let router_service = http_routes::router_service(app_server.clone());

    let bind_addr = config.http_bind_address;
    let http_server = Server::try_bind(&bind_addr)
        .context(StartListeningHttp { bind_addr })?
        .serve(router_service);
    info!(bind_address=?bind_addr, "HTTP server listening");

    println!("InfluxDB IOx server ready");

    // Wait for both the servers to complete
    let (grpc_server, server) = futures::future::join(grpc_server, http_server).await;

    grpc_server.context(ServingRPC)?;
    server.context(ServingHttp)?;

    Ok(())
}
