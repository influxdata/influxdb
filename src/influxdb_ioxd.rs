use std::fs;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;

use hyper::Server;
use snafu::{ResultExt, Snafu};
use tracing::{error, info, warn};

use object_store::{self, aws::AmazonS3, gcp::GoogleCloudStorage, ObjectStore};
use panic_logging::SendPanicsToTracing;
use server::{ConnectionManagerImpl as ConnectionManager, Server as AppServer};

use crate::commands::{
    logging::LoggingLevel,
    server::{load_config, Config, ObjectStore as ObjStoreOpt},
};

mod http;
mod rpc;

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
    ServingRPC { source: self::rpc::Error },

    #[snafu(display("Specifed {} for the object store, but not a bucket", object_store))]
    InvalidCloudObjectStoreConfiguration { object_store: ObjStoreOpt },

    #[snafu(display("Specified file for the object store, but not a database directory"))]
    InvalidFileObjectStoreConfiguration,
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

    let object_store = match (
        config.object_store,
        config.bucket,
        config.database_directory,
    ) {
        (Some(ObjStoreOpt::Google), Some(bucket), _) => {
            info!("Using GCP bucket {} for storage", bucket);
            ObjectStore::new_google_cloud_storage(GoogleCloudStorage::new(bucket))
        }
        (Some(ObjStoreOpt::Google), None, _) => {
            return InvalidCloudObjectStoreConfiguration {
                object_store: ObjStoreOpt::Google,
            }
            .fail();
        }
        (Some(ObjStoreOpt::S3), Some(bucket), _) => {
            info!("Using S3 bucket {} for storage", bucket);
            // rusoto::Region's default takes the value from the AWS_DEFAULT_REGION env var.
            ObjectStore::new_amazon_s3(AmazonS3::new(Default::default(), bucket))
        }
        (Some(ObjStoreOpt::S3), None, _) => {
            return InvalidCloudObjectStoreConfiguration {
                object_store: ObjStoreOpt::S3,
            }
            .fail();
        }
        (Some(ObjStoreOpt::File), _, Some(ref db_dir)) => {
            info!("Using local dir {:?} for storage", db_dir);
            fs::create_dir_all(db_dir).context(CreatingDatabaseDirectory { path: db_dir })?;
            ObjectStore::new_file(object_store::disk::File::new(&db_dir))
        }
        (Some(ObjStoreOpt::File), _, None) => {
            return InvalidFileObjectStoreConfiguration.fail();
        }
        (Some(ObjStoreOpt::Azure), Some(_bucket), _) => {
            unimplemented!();
        }
        (Some(ObjStoreOpt::Azure), None, _) => {
            return InvalidCloudObjectStoreConfiguration {
                object_store: ObjStoreOpt::Azure,
            }
            .fail();
        }
        (Some(ObjStoreOpt::Memory), _, _) | (None, _, _) => {
            warn!("NO PERSISTENCE: using memory for object storage");
            ObjectStore::new_in_memory(object_store::memory::InMemory::new())
        }
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

    let grpc_server = self::rpc::make_server(socket, Arc::clone(&app_server));

    info!(bind_address=?grpc_bind_addr, "gRPC server listening");

    // Construct and start up HTTP server

    let router_service = http::router_service(Arc::clone(&app_server));

    let bind_addr = config.http_bind_address;
    let http_server = Server::try_bind(&bind_addr)
        .context(StartListeningHttp { bind_addr })?
        .serve(router_service);
    info!(bind_address=?bind_addr, "HTTP server listening");

    let git_hash = option_env!("GIT_HASH").unwrap_or("UNKNOWN");
    info!(git_hash, "InfluxDB IOx server ready");

    // Wait for both the servers to complete
    let (grpc_server, server) = futures::future::join(grpc_server, http_server).await;

    grpc_server.context(ServingRPC)?;
    server.context(ServingHttp)?;

    Ok(())
}
