use tracing::{debug, info};

use std::fs;
use std::net::SocketAddr;
use std::sync::Arc;
use std::{env::VarError, path::PathBuf};

use crate::server::http_routes;
use crate::server::rpc::service;

use hyper::service::{make_service_fn, service_fn};
use hyper::Server;
use query::exec::Executor as QueryExecutor;
use write_buffer::{Db, WriteBufferDatabases};

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

    debug!("InfluxDB IOx Server using database directory: {:?}", db_dir);

    let storage = Arc::new(WriteBufferDatabases::new(&db_dir));
    let dirs = storage
        .wal_dirs()
        .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)
        .context(InitializingWriteBuffer { db_dir })?;

    // TODO: make recovery of multiple databases multi-threaded
    for dir in dirs {
        let db = Db::restore_from_wal(&dir)
            .await
            .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)
            .context(RestoringWriteBuffer { dir })?;
        storage.add_db(db).await;
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

    let grpc_server = service::make_server(socket, storage.clone(), executor);

    info!("gRPC server listening on http://{}", grpc_bind_addr);

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
        let storage = storage.clone();
        async move {
            Ok::<_, http::Error>(service_fn(move |req| {
                let state = storage.clone();
                http_routes::service(req, state)
            }))
        }
    });

    let server = Server::try_bind(&bind_addr)
        .context(StartListening { bind_addr })?
        .serve(make_svc);
    info!("Listening on http://{}", bind_addr);

    println!("InfluxDB IOx server ready");

    // Wait for both the servers to complete
    let (grpc_server, server) = futures::future::join(grpc_server, server).await;

    grpc_server.context(ServingRPC)?;
    server.context(ServingHttp)?;

    Ok(())
}
