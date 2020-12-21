use tracing::{info, warn};

use std::fs;
use std::net::SocketAddr;
use std::sync::Arc;
use std::{env::VarError, path::PathBuf};

use crate::server::http_routes;
use crate::server::rpc::service;
use server::server::{ConnectionManagerImpl as ConnectionManager, Server as AppServer};

use hyper::service::{make_service_fn, service_fn};
use hyper::Server;
use object_store::{self, GoogleCloudStorage, ObjectStore};
use query::exec::Executor as QueryExecutor;

use snafu::{ResultExt, Snafu};

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Unable to create database directory {:?}: {}", path, source))]
    CreatingDatabaseDirectory {
        path: PathBuf,
        source: std::io::Error,
    },

    #[snafu(display("Unable to initialize database in directory {:?}:  {}", db_dir, source))]
    InitializingWriteBuffer {
        db_dir: PathBuf,
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    #[snafu(display("Unable to restore WAL from directory {:?}:  {}", dir, source))]
    RestoringWriteBuffer {
        dir: PathBuf,
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    #[snafu(display(
        "Unable to bind to listen for HTTP requests on {}: {}",
        bind_addr,
        source
    ))]
    StartListening {
        bind_addr: SocketAddr,
        source: hyper::error::Error,
    },

    #[snafu(display("Error serving HTTP: {}", source))]
    ServingHttp { source: hyper::error::Error },

    #[snafu(display("Error serving RPC: {}", source))]
    ServingRPC {
        source: crate::server::rpc::service::Error,
    },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

pub async fn main() -> Result<()> {
    dotenv::dotenv().ok();

    let db_dir = match std::env::var("INFLUXDB_IOX_DB_DIR") {
        Ok(val) => val,
        Err(_) => {
            // default database path is $HOME/.influxdb_iox
            let mut path = dirs::home_dir().unwrap();
            path.push(".influxdb_iox/");
            path.into_os_string().into_string().unwrap()
        }
    };

    fs::create_dir_all(&db_dir).context(CreatingDatabaseDirectory { path: &db_dir })?;

    let object_store = if let Ok(bucket) = std::env::var("INFLUXDB_IOX_GCP_BUCKET") {
        info!("Using GCP bucket {} for storage", &bucket);
        ObjectStore::new_google_cloud_storage(GoogleCloudStorage::new(bucket))
    } else {
        info!("Using local dir {} for storage", &db_dir);
        ObjectStore::new_file(object_store::File::new(&db_dir))
    };
    let object_storage = Arc::new(object_store);

    let connection_manager = ConnectionManager {};
    let app_server = Arc::new(AppServer::new(connection_manager, object_storage));

    // if this ID isn't set the server won't be usable until this is set via an API
    // call
    if let Ok(id) = std::env::var("INFLUXDB_IOX_ID") {
        let id = id
            .parse::<u32>()
            .expect("INFLUXDB_IOX_ID must be a u32 integer");
        info!("setting server ID to {}", id);
        app_server.set_id(id).await;
    } else {
        warn!("server ID not set. ID must be set via the INFLUXDB_IOX_ID environment variable or via API before writing or querying data.");
    }

    // Fire up the query executor
    let executor = Arc::new(QueryExecutor::default());

    // Construct and start up gRPC server

    let grpc_bind_addr: SocketAddr = match std::env::var("INFLUXDB_IOX_GRPC_BIND_ADDR") {
        Ok(addr) => addr
            .parse()
            .expect("INFLUXDB_IOX_GRPC_BIND_ADDR environment variable not a valid SocketAddr"),
        Err(VarError::NotPresent) => "127.0.0.1:8082".parse().unwrap(),
        Err(VarError::NotUnicode(_)) => {
            panic!("INFLUXDB_IOX_GRPC_BIND_ADDR environment variable not a valid unicode string")
        }
    };

    let socket = tokio::net::TcpListener::bind(grpc_bind_addr)
        .await
        .expect("failed to bind server");

    let grpc_server = service::make_server(socket, app_server.clone(), executor);

    info!(bind_address=?grpc_bind_addr, "gRPC server listening");

    // Construct and start up HTTP server

    let bind_addr: SocketAddr = match std::env::var("INFLUXDB_IOX_BIND_ADDR") {
        Ok(addr) => addr
            .parse()
            .expect("INFLUXDB_IOX_BIND_ADDR environment variable not a valid SocketAddr"),
        Err(VarError::NotPresent) => "127.0.0.1:8080".parse().unwrap(),
        Err(VarError::NotUnicode(_)) => {
            panic!("INFLUXDB_IOX_BIND_ADDR environment variable not a valid unicode string")
        }
    };

    let make_svc = make_service_fn(move |_conn| {
        let app_server = app_server.clone();
        async move {
            Ok::<_, http::Error>(service_fn(move |req| {
                http_routes::service(req, app_server.clone())
            }))
        }
    });

    let http_server = Server::try_bind(&bind_addr)
        .context(StartListening { bind_addr })?
        .serve(make_svc);
    info!(bind_address=?bind_addr, "HTTP server listening");

    println!("InfluxDB IOx server ready");

    // Wait for both the servers to complete
    let (grpc_server, server) = futures::future::join(grpc_server, http_server).await;

    grpc_server.context(ServingRPC)?;
    server.context(ServingHttp)?;

    Ok(())
}
