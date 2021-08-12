use crate::commands::run::{Config, ObjectStore as ObjStoreOpt};
use futures::{future::FusedFuture, pin_mut, FutureExt, TryStreamExt};
use hyper::server::conn::AddrIncoming;
use object_store::{self, path::ObjectStorePath, ObjectStore, ObjectStoreApi, ThrottleConfig};
use observability_deps::tracing::{error, info, warn};
use panic_logging::SendPanicsToTracing;
use server::{
    ApplicationState, ConnectionManagerImpl as ConnectionManager, RemoteTemplate,
    Server as AppServer, ServerConfig,
};
use snafu::{ResultExt, Snafu};
use std::{convert::TryFrom, fs, net::SocketAddr, path::PathBuf, sync::Arc};
use tokio::time::Duration;
use uuid::Uuid;

mod http;
mod planner;
mod rpc;
pub(crate) mod serving_readiness;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Unable to create database directory {:?}: {}", path, source))]
    CreatingDatabaseDirectory {
        path: PathBuf,
        source: std::io::Error,
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
    ServingRpc { source: rpc::Error },

    #[snafu(display(
        "Specified {} for the object store, required configuration missing for {}",
        object_store,
        missing
    ))]
    MissingObjectStoreConfig {
        object_store: ObjStoreOpt,
        missing: String,
    },

    // Creating a new S3 object store can fail if the region is *specified* but
    // not *parseable* as a rusoto `Region`. The other object store constructors
    // don't return `Result`.
    #[snafu(display("Error configuring Amazon S3: {}", source))]
    InvalidS3Config { source: object_store::Error },

    #[snafu(display("Error configuring GCS: {}", source))]
    InvalidGCSConfig { source: object_store::Error },

    #[snafu(display("Error configuring Microsoft Azure: {}", source))]
    InvalidAzureConfig { source: object_store::Error },

    #[snafu(display("Cannot read from object store: {}", source))]
    CannotReadObjectStore { source: object_store::Error },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

/// On unix platforms we want to intercept SIGINT and SIGTERM
/// This method returns if either are signalled
#[cfg(unix)]
async fn wait_for_signal() {
    use tokio::signal::unix::{signal, SignalKind};
    let mut term = signal(SignalKind::terminate()).expect("failed to register signal handler");
    let mut int = signal(SignalKind::interrupt()).expect("failed to register signal handler");

    tokio::select! {
        _ = term.recv() => info!("Received SIGTERM"),
        _ = int.recv() => info!("Received SIGINT"),
    }
}

#[cfg(windows)]
/// ctrl_c is the cross-platform way to intercept the equivalent of SIGINT
/// This method returns if this occurs
async fn wait_for_signal() {
    let _ = tokio::signal::ctrl_c().await;
}

async fn make_application(config: &Config) -> Result<Arc<ApplicationState>> {
    match config.object_store {
        Some(ObjStoreOpt::Memory) | None => {
            warn!("NO PERSISTENCE: using Memory for object storage");
        }
        Some(store) => {
            info!("Using {} for object storage", store);
        }
    }

    let object_store = ObjectStore::try_from(config)?;
    check_object_store(&object_store).await?;
    let object_storage = Arc::new(object_store);

    Ok(Arc::new(ApplicationState::new(
        object_storage,
        config.num_worker_threads,
    )))
}

fn make_server(
    application: Arc<ApplicationState>,
    config: &Config,
) -> Arc<AppServer<ConnectionManager>> {
    let server_config = ServerConfig {
        remote_template: config.remote_template.clone().map(RemoteTemplate::new),
        wipe_catalog_on_error: config.wipe_catalog_on_error.into(),
        skip_replay_and_seek_instead: config.skip_replay_and_seek_instead.into(),
    };

    if config.grpc_bind_address == config.http_bind_address && config.grpc_bind_address.port() != 0
    {
        error!(
            %config.grpc_bind_address,
            %config.http_bind_address,
            "grpc and http bind addresses must differ",
        );
        std::process::exit(1);
    }

    let connection_manager = ConnectionManager::new();
    let app_server = Arc::new(AppServer::new(
        connection_manager,
        application,
        server_config,
    ));

    // if this ID isn't set the server won't be usable until this is set via an API
    // call
    if let Some(id) = config.server_id {
        app_server.set_id(id).expect("server id already set");
    } else {
        warn!("server ID not set. ID must be set via the INFLUXDB_IOX_ID config or API before writing or querying data.");
    }

    app_server
}

/// This is the entry point for the IOx server. `config` represents
/// command line arguments, if any.
pub async fn main(config: Config) -> Result<()> {
    let git_hash = option_env!("GIT_HASH").unwrap_or("UNKNOWN");
    let num_cpus = num_cpus::get();
    let build_malloc_conf = tikv_jemalloc_ctl::config::malloc_conf::mib()
        .unwrap()
        .read()
        .unwrap();
    info!(
        git_hash,
        num_cpus, build_malloc_conf, "InfluxDB IOx server starting"
    );

    // Install custom panic handler and forget about it.
    //
    // This leaks the handler and prevents it from ever being dropped during the
    // lifetime of the program - this is actually a good thing, as it prevents
    // the panic handler from being removed while unwinding a panic (which in
    // turn, causes a panic - see #548)
    let f = SendPanicsToTracing::new();
    std::mem::forget(f);

    let application = make_application(&config).await?;
    let app_server = make_server(Arc::clone(&application), &config);

    serve(config, application, app_server).await
}

/// Instantiates the gRPC and HTTP listeners and returns a Future that completes when
/// these listeners, the Server, Databases, etc... have all exited.
///
/// This is effectively the "main loop" for influxdb_iox
async fn serve(
    config: Config,
    application: Arc<ApplicationState>,
    app_server: Arc<AppServer<ConnectionManager>>,
) -> Result<()> {
    // Construct a token to trigger shutdown of API services
    let frontend_shutdown = tokio_util::sync::CancellationToken::new();

    // Construct and start up gRPC server
    let grpc_bind_addr = config.grpc_bind_address;
    let socket = tokio::net::TcpListener::bind(grpc_bind_addr)
        .await
        .context(StartListeningGrpc { grpc_bind_addr })?;

    let grpc_server = rpc::serve(
        socket,
        Arc::clone(&application),
        Arc::clone(&app_server),
        frontend_shutdown.clone(),
        config.initial_serving_state.into(),
    )
    .fuse();

    info!(bind_address=?grpc_bind_addr, "gRPC server listening");

    let bind_addr = config.http_bind_address;
    let addr = AddrIncoming::bind(&bind_addr).context(StartListeningHttp { bind_addr })?;

    let max_http_request_size = config.max_http_request_size;

    let http_server = http::serve(
        addr,
        Arc::clone(&application),
        Arc::clone(&app_server),
        frontend_shutdown.clone(),
        max_http_request_size,
    )
    .fuse();
    info!(bind_address=?bind_addr, "HTTP server listening");

    info!("InfluxDB IOx server ready");

    // Get IOx background worker task
    let server_worker = app_server.join().fuse();

    // Shutdown signal
    let signal = wait_for_signal().fuse();

    // There are two different select macros - tokio::select and futures::select
    //
    // tokio::select takes ownership of the passed future "moving" it into the
    // select block. This works well when not running select inside a loop, or
    // when using a future that can be dropped and recreated, often the case
    // with tokio's futures e.g. `channel.recv()`
    //
    // futures::select is more flexible as it doesn't take ownership of the provided
    // future. However, to safely provide this it imposes some additional
    // requirements
    //
    // All passed futures must implement FusedFuture - it is IB to poll a future
    // that has returned Poll::Ready(_). A FusedFuture has an is_terminated()
    // method that indicates if it is safe to poll - e.g. false if it has
    // returned Poll::Ready(_). futures::select uses this to implement its
    // functionality. futures::FutureExt adds a fuse() method that
    // wraps an arbitrary future and makes it a FusedFuture
    //
    // The additional requirement of futures::select is that if the future passed
    // outlives the select block, it must be Unpin or already Pinned

    // pin_mut constructs a Pin<&mut T> from a T by preventing moving the T
    // from the current stack frame and constructing a Pin<&mut T> to it
    pin_mut!(signal);
    pin_mut!(server_worker);
    pin_mut!(grpc_server);
    pin_mut!(http_server);

    // Return the first error encountered
    let mut res = Ok(());

    // Graceful shutdown can be triggered by sending SIGINT or SIGTERM to the
    // process, or by a background task exiting - most likely with an error
    //
    // Graceful shutdown should then proceed in the following order
    // 1. Stop accepting new HTTP and gRPC requests and drain existing connections
    // 2. Trigger shutdown of internal background workers loops
    //
    // This is important to ensure background tasks, such as polling the tracker
    // registry, don't exit before HTTP and gRPC requests dependent on them
    while !grpc_server.is_terminated() && !http_server.is_terminated() {
        futures::select! {
            _ = signal => info!("Shutdown requested"),
            _ = server_worker => {
                info!("server worker shutdown prematurely");
            },
            result = grpc_server => match result {
                Ok(_) => info!("gRPC server shutdown"),
                Err(error) => {
                    error!(%error, "gRPC server error");
                    res = res.and(Err(Error::ServingRpc{source: error}))
                }
            },
            result = http_server => match result {
                Ok(_) => info!("HTTP server shutdown"),
                Err(error) => {
                    error!(%error, "HTTP server error");
                    res = res.and(Err(Error::ServingHttp{source: error}))
                }
            },
        }

        frontend_shutdown.cancel()
    }

    info!("frontend shutdown completed");
    app_server.shutdown();

    if !server_worker.is_terminated() {
        match server_worker.await {
            Ok(_) => info!("server worker shutdown"),
            Err(error) => error!(%error, "server worker error"),
        }
    }

    info!("server completed shutting down");

    res
}

/// Check if object store is properly configured and accepts writes and reads.
///
/// Note: This does NOT test if the object store is writable!
async fn check_object_store(object_store: &ObjectStore) -> Result<()> {
    // Use some prefix that will very likely end in an empty result, so we don't pull too much actual data here.
    let uuid = Uuid::new_v4().to_string();
    let mut prefix = object_store.new_path();
    prefix.push_dir(&uuid);

    // create stream (this might fail if the store is not readable)
    let mut stream = object_store
        .list(Some(&prefix))
        .await
        .context(CannotReadObjectStore)?;

    // ... but sometimes it fails only if we use the resulting stream, so try that once
    stream.try_next().await.context(CannotReadObjectStore)?;

    // store seems to be readable
    Ok(())
}

impl TryFrom<&Config> for ObjectStore {
    type Error = Error;

    fn try_from(config: &Config) -> Result<Self, Self::Error> {
        match config.object_store {
            Some(ObjStoreOpt::Memory) | None => Ok(Self::new_in_memory()),
            Some(ObjStoreOpt::MemoryThrottled) => {
                let config = ThrottleConfig {
                    // for every call: assume a 100ms latency
                    wait_delete_per_call: Duration::from_millis(100),
                    wait_get_per_call: Duration::from_millis(100),
                    wait_list_per_call: Duration::from_millis(100),
                    wait_list_with_delimiter_per_call: Duration::from_millis(100),
                    wait_put_per_call: Duration::from_millis(100),

                    // for list operations: assume we need 1 call per 1k entries at 100ms
                    wait_list_per_entry: Duration::from_millis(100) / 1_000,
                    wait_list_with_delimiter_per_entry: Duration::from_millis(100) / 1_000,

                    // for upload/download: assume 1GByte/s
                    wait_get_per_byte: Duration::from_secs(1) / 1_000_000_000,
                    wait_put_per_byte: Duration::from_secs(1) / 1_000_000_000,
                };

                Ok(Self::new_in_memory_throttled(config))
            }

            Some(ObjStoreOpt::Google) => {
                match (
                    config.bucket.as_ref(),
                    config.google_service_account.as_ref(),
                ) {
                    (Some(bucket), Some(service_account)) => {
                        Self::new_google_cloud_storage(service_account, bucket)
                            .context(InvalidGCSConfig)
                    }
                    (bucket, service_account) => {
                        let mut missing_args = vec![];

                        if bucket.is_none() {
                            missing_args.push("bucket");
                        }
                        if service_account.is_none() {
                            missing_args.push("google-service-account");
                        }
                        MissingObjectStoreConfig {
                            object_store: ObjStoreOpt::Google,
                            missing: missing_args.join(", "),
                        }
                        .fail()
                    }
                }
            }

            Some(ObjStoreOpt::S3) => {
                match (
                    config.bucket.as_ref(),
                    config.aws_access_key_id.as_ref(),
                    config.aws_secret_access_key.as_ref(),
                    config.aws_default_region.as_str(),
                    config.aws_endpoint.as_ref(),
                    config.aws_session_token.as_ref(),
                ) {
                    (Some(bucket), key_id, secret_key, region, endpoint, session_token) => {
                        Self::new_amazon_s3(
                            key_id,
                            secret_key,
                            region,
                            bucket,
                            endpoint,
                            session_token,
                        )
                        .context(InvalidS3Config)
                    }
                    (bucket, _, _, _, _, _) => {
                        let mut missing_args = vec![];

                        if bucket.is_none() {
                            missing_args.push("bucket");
                        }
                        MissingObjectStoreConfig {
                            object_store: ObjStoreOpt::S3,
                            missing: missing_args.join(", "),
                        }
                        .fail()
                    }
                }
            }

            Some(ObjStoreOpt::Azure) => {
                match (
                    config.bucket.as_ref(),
                    config.azure_storage_account.as_ref(),
                    config.azure_storage_access_key.as_ref(),
                ) {
                    (Some(bucket), Some(storage_account), Some(access_key)) => {
                        Self::new_microsoft_azure(storage_account, access_key, bucket)
                            .context(InvalidAzureConfig)
                    }
                    (bucket, storage_account, access_key) => {
                        let mut missing_args = vec![];

                        if bucket.is_none() {
                            missing_args.push("bucket");
                        }
                        if storage_account.is_none() {
                            missing_args.push("azure-storage-account");
                        }
                        if access_key.is_none() {
                            missing_args.push("azure-storage-access-key");
                        }

                        MissingObjectStoreConfig {
                            object_store: ObjStoreOpt::Azure,
                            missing: missing_args.join(", "),
                        }
                        .fail()
                    }
                }
            }

            Some(ObjStoreOpt::File) => match config.database_directory.as_ref() {
                Some(db_dir) => {
                    fs::create_dir_all(db_dir)
                        .context(CreatingDatabaseDirectory { path: db_dir })?;
                    Ok(Self::new_file(&db_dir))
                }
                None => MissingObjectStoreConfig {
                    object_store: ObjStoreOpt::File,
                    missing: "data-dir",
                }
                .fail(),
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use data_types::{database_rules::DatabaseRules, DatabaseName};
    use object_store::ObjectStoreIntegration;
    use structopt::StructOpt;
    use tempfile::TempDir;

    #[test]
    fn default_object_store_is_memory() {
        let config = Config::from_iter_safe(&["server"]).unwrap();

        let object_store = ObjectStore::try_from(&config).unwrap();
        let ObjectStore { integration, .. } = object_store;

        assert!(matches!(integration, ObjectStoreIntegration::InMemory(_)));
    }

    #[test]
    fn explicitly_set_object_store_to_memory() {
        let config = Config::from_iter_safe(&["server", "--object-store", "memory"]).unwrap();

        let object_store = ObjectStore::try_from(&config).unwrap();
        let ObjectStore { integration, .. } = object_store;

        assert!(matches!(integration, ObjectStoreIntegration::InMemory(_)));
    }

    #[test]
    #[cfg(feature = "aws")]
    fn valid_s3_config() {
        let config = Config::from_iter_safe(&[
            "server",
            "--object-store",
            "s3",
            "--bucket",
            "mybucket",
            "--aws-access-key-id",
            "NotARealAWSAccessKey",
            "--aws-secret-access-key",
            "NotARealAWSSecretAccessKey",
        ])
        .unwrap();

        let object_store = ObjectStore::try_from(&config).unwrap();
        let ObjectStore { integration, .. } = object_store;

        assert!(matches!(integration, ObjectStoreIntegration::AmazonS3(_)));
    }

    #[test]
    fn s3_config_missing_params() {
        let config = Config::from_iter_safe(&["server", "--object-store", "s3"]).unwrap();

        let err = ObjectStore::try_from(&config).unwrap_err().to_string();

        assert_eq!(
            err,
            "Specified S3 for the object store, required configuration missing for bucket"
        );
    }

    #[test]
    #[cfg(feature = "gcp")]
    fn valid_google_config() {
        let config = Config::from_iter_safe(&[
            "server",
            "--object-store",
            "google",
            "--bucket",
            "mybucket",
            "--google-service-account",
            "~/Not/A/Real/path.json",
        ])
        .unwrap();

        let object_store = ObjectStore::try_from(&config).unwrap();
        let ObjectStore { integration, .. } = object_store;

        assert!(matches!(
            integration,
            ObjectStoreIntegration::GoogleCloudStorage(_)
        ));
    }

    #[test]
    fn google_config_missing_params() {
        let config = Config::from_iter_safe(&["server", "--object-store", "google"]).unwrap();

        let err = ObjectStore::try_from(&config).unwrap_err().to_string();

        assert_eq!(
            err,
            "Specified Google for the object store, required configuration missing for \
            bucket, google-service-account"
        );
    }

    #[test]
    #[cfg(feature = "azure")]
    fn valid_azure_config() {
        let config = Config::from_iter_safe(&[
            "server",
            "--object-store",
            "azure",
            "--bucket",
            "mybucket",
            "--azure-storage-account",
            "NotARealStorageAccount",
            "--azure-storage-access-key",
            "NotARealKey",
        ])
        .unwrap();

        let object_store = ObjectStore::try_from(&config).unwrap();
        let ObjectStore { integration, .. } = object_store;

        assert!(matches!(
            integration,
            ObjectStoreIntegration::MicrosoftAzure(_)
        ));
    }

    #[test]
    fn azure_config_missing_params() {
        let config = Config::from_iter_safe(&["server", "--object-store", "azure"]).unwrap();

        let err = ObjectStore::try_from(&config).unwrap_err().to_string();

        assert_eq!(
            err,
            "Specified Azure for the object store, required configuration missing for \
            bucket, azure-storage-account, azure-storage-access-key"
        );
    }

    #[test]
    fn valid_file_config() {
        let root = TempDir::new().unwrap();

        let config = Config::from_iter_safe(&[
            "server",
            "--object-store",
            "file",
            "--data-dir",
            root.path().to_str().unwrap(),
        ])
        .unwrap();

        let object_store = ObjectStore::try_from(&config).unwrap();
        let ObjectStore { integration, .. } = object_store;

        assert!(matches!(integration, ObjectStoreIntegration::File(_)));
    }

    #[test]
    fn file_config_missing_params() {
        let config = Config::from_iter_safe(&["server", "--object-store", "file"]).unwrap();

        let err = ObjectStore::try_from(&config).unwrap_err().to_string();

        assert_eq!(
            err,
            "Specified File for the object store, required configuration missing for \
            data-dir"
        );
    }

    #[tokio::test]
    async fn test_server_shutdown() {
        test_helpers::maybe_start_logging();

        // Create a server and wait for it to initialize
        let config = Config::from_iter(&["run", "--server-id", "23"]);
        let application = make_application(&config).await.unwrap();
        let server = make_server(Arc::clone(&application), &config);
        server.wait_for_init().await.unwrap();

        // Start serving
        let serve_fut = serve(config, application, Arc::clone(&server)).fuse();
        pin_mut!(serve_fut);

        // Nothing to trigger termination, so serve future should continue running
        futures::select! {
            _ = serve_fut => panic!("serve shouldn't finish"),
            _ = tokio::time::sleep(tokio::time::Duration::from_millis(10)).fuse() => {}
        }

        // Trigger shutdown
        server.shutdown();

        // The serve future should now complete
        futures::select! {
            _ = serve_fut => {},
            _ = tokio::time::sleep(tokio::time::Duration::from_secs(5)).fuse() => panic!("timeout shouldn't expire")
        }
    }

    #[tokio::test]
    async fn test_server_shutdown_uninit() {
        // Create a server but don't set a server id
        let config = Config::from_iter(&[
            "run",
            "--api-bind",
            "127.0.0.1:0",
            "--grpc-bind",
            "127.0.0.1:0",
        ]);
        let application = make_application(&config).await.unwrap();
        let server = make_server(Arc::clone(&application), &config);

        let serve_fut = serve(config, application, Arc::clone(&server)).fuse();
        pin_mut!(serve_fut);

        // Nothing should have triggered shutdown so serve shouldn't finish
        futures::select! {
            _ = serve_fut => panic!("serve shouldn't finish"),
            _ = tokio::time::sleep(tokio::time::Duration::from_millis(10)).fuse() => {}
        }

        // We never set the server ID and so the server should not initialize
        assert!(
            server.wait_for_init().now_or_never().is_none(),
            "shouldn't have initialized"
        );

        // But it should still be possible to shut it down
        server.shutdown();

        futures::select! {
            _ = serve_fut => {},
            _ = tokio::time::sleep(tokio::time::Duration::from_secs(5)).fuse() => panic!("timeout shouldn't expire")
        }
    }

    #[tokio::test]
    async fn test_server_panic() {
        // Create a server and wait for it to initialize
        let config = Config::from_iter(&[
            "run",
            "--server-id",
            "999999999",
            "--api-bind",
            "127.0.0.1:0",
            "--grpc-bind",
            "127.0.0.1:0",
        ]);
        let application = make_application(&config).await.unwrap();
        let server = make_server(Arc::clone(&application), &config);
        server.wait_for_init().await.unwrap();

        let serve_fut = serve(config, application, Arc::clone(&server)).fuse();
        pin_mut!(serve_fut);

        // Nothing should have triggered shutdown so serve shouldn't finish
        futures::select! {
            _ = serve_fut => panic!("serve shouldn't finish"),
            _ = tokio::time::sleep(tokio::time::Duration::from_millis(10)).fuse() => {}
        }

        // Trigger a panic in the Server background worker
        server::utils::register_panic_key("server background worker: 999999999");

        // This should trigger shutdown
        futures::select! {
            _ = serve_fut => {},
            _ = tokio::time::sleep(tokio::time::Duration::from_secs(5)).fuse() => panic!("timeout shouldn't expire")
        }
    }

    #[tokio::test]
    async fn test_database_panic() {
        // Create a server and wait for it to initialize
        let config = Config::from_iter(&[
            "run",
            "--server-id",
            "23",
            "--api-bind",
            "127.0.0.1:0",
            "--grpc-bind",
            "127.0.0.1:0",
        ]);
        let application = make_application(&config).await.unwrap();
        let server = make_server(Arc::clone(&application), &config);
        server.wait_for_init().await.unwrap();

        // Create a database that won't panic
        let other_db_name = DatabaseName::new("other").unwrap();
        server
            .create_database(DatabaseRules::new(other_db_name.clone()))
            .await
            .unwrap();

        let other_db = server.database(&other_db_name).unwrap();

        let serve_fut = serve(config, Arc::clone(&application), Arc::clone(&server)).fuse();
        pin_mut!(serve_fut);

        // Nothing should have triggered shutdown so serve shouldn't finish
        futures::select! {
            _ = serve_fut => panic!("serve shouldn't finish"),
            _ = tokio::time::sleep(tokio::time::Duration::from_millis(10)).fuse() => {}
        }

        // Configure a panic in the worker of the database we're about to create
        server::utils::register_panic_key("database background worker: panic_test");

        // Spawn a dummy job that will delay shutdown as it runs to completion
        let task = application
            .job_registry()
            .spawn_dummy_job(vec![1_000_000_000], None);

        // Create database that will panic in its worker loop
        server
            .create_database(DatabaseRules::new(DatabaseName::new("panic_test").unwrap()))
            .await
            .unwrap();

        // The serve future shouldn't resolve until the dummy job finishes
        futures::select! {
            _ = serve_fut => panic!("should wait for jobs to finish"),
            _ = tokio::time::sleep(tokio::time::Duration::from_millis(10)).fuse() => {}
        }

        assert!(!task.is_complete(), "task should still be running");

        // But the databases should have been shutdown
        assert!(
            other_db.join().now_or_never().is_some(),
            "database should have been terminated and have finished"
        );

        // Once the dummy job completes - the serve future should resolve
        futures::select! {
            _ = serve_fut => {},
            _ = tokio::time::sleep(tokio::time::Duration::from_secs(5)).fuse() => panic!("timeout shouldn't expire")
        }

        assert_eq!(
            task.get_status().result().unwrap(),
            tracker::TaskResult::Success
        )
    }
}
