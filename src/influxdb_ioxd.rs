use crate::{
    commands::run::Config,
    object_store::{check_object_store, warn_about_inmem_store},
};
use futures::{future::FusedFuture, pin_mut, FutureExt};
use hyper::server::conn::AddrIncoming;
use object_store::{self, ObjectStore};
use observability_deps::tracing::{error, info, warn};
use panic_logging::SendPanicsToTracing;
use server::{
    ApplicationState, ConnectionManagerImpl as ConnectionManager, RemoteTemplate,
    Server as AppServer, ServerConfig,
};
use snafu::{ResultExt, Snafu};
use std::{convert::TryFrom, net::SocketAddr, sync::Arc};
use trace::{LogTraceCollector, TraceCollector};

mod http;
mod planner;
mod rpc;
pub(crate) mod serving_readiness;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Unable to bind to listen for HTTP requests on {}: {}", addr, source))]
    StartListeningHttp {
        addr: SocketAddr,
        source: hyper::Error,
    },

    #[snafu(display("Unable to bind to listen for gRPC requests on {}: {}", addr, source))]
    StartListeningGrpc {
        addr: SocketAddr,
        source: std::io::Error,
    },

    #[snafu(display("Error serving HTTP: {}", source))]
    ServingHttp { source: hyper::Error },

    #[snafu(display("Error serving RPC: {}", source))]
    ServingRpc { source: rpc::Error },

    #[snafu(display("Cannot parse object store config: {}", source))]
    ObjectStoreParsing {
        source: crate::object_store::ParseError,
    },

    #[snafu(display("Cannot check object store config: {}", source))]
    ObjectStoreCheck {
        source: crate::object_store::CheckError,
    },
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
    warn_about_inmem_store(&config.object_store_config);
    let object_store =
        ObjectStore::try_from(&config.object_store_config).context(ObjectStoreParsing)?;
    check_object_store(&object_store)
        .await
        .context(ObjectStoreCheck)?;
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

    let grpc_listener = grpc_listener(config.grpc_bind_address).await?;
    let http_listener = http_listener(config.http_bind_address).await?;
    let trace_collector = Arc::new(LogTraceCollector::new());

    serve(
        config,
        application,
        grpc_listener,
        http_listener,
        trace_collector,
        app_server,
    )
    .await
}

async fn grpc_listener(addr: SocketAddr) -> Result<tokio::net::TcpListener> {
    let listener = tokio::net::TcpListener::bind(addr)
        .await
        .context(StartListeningGrpc { addr })?;

    match listener.local_addr() {
        Ok(local_addr) => info!(%local_addr, "bound gRPC listener"),
        Err(_) => info!(%addr, "bound gRPC listener"),
    }

    Ok(listener)
}

async fn http_listener(addr: SocketAddr) -> Result<AddrIncoming> {
    let listener = AddrIncoming::bind(&addr).context(StartListeningHttp { addr })?;
    info!(bind_addr=%listener.local_addr(), "bound HTTP listener");

    Ok(listener)
}

/// Instantiates the gRPC and HTTP listeners and returns a Future that completes when
/// these listeners, the Server, Databases, etc... have all exited.
///
/// This is effectively the "main loop" for influxdb_iox
async fn serve(
    config: Config,
    application: Arc<ApplicationState>,
    grpc_listener: tokio::net::TcpListener,
    http_listener: AddrIncoming,
    trace_collector: Arc<dyn TraceCollector>,
    app_server: Arc<AppServer<ConnectionManager>>,
) -> Result<()> {
    // Construct a token to trigger shutdown of API services
    let frontend_shutdown = tokio_util::sync::CancellationToken::new();

    // Construct and start up gRPC server

    let grpc_server = rpc::serve(
        grpc_listener,
        Arc::clone(&application),
        Arc::clone(&app_server),
        trace_collector,
        frontend_shutdown.clone(),
        config.initial_serving_state.into(),
    )
    .fuse();

    info!("gRPC server listening");

    let max_http_request_size = config.max_http_request_size;

    let http_server = http::serve(
        http_listener,
        Arc::clone(&application),
        Arc::clone(&app_server),
        frontend_shutdown.clone(),
        max_http_request_size,
    )
    .fuse();
    info!("HTTP server listening");

    // Purposefully use log not tokio-tracing to ensure correctly hooked up
    log::info!("InfluxDB IOx server ready");

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

#[cfg(test)]
mod tests {
    use super::*;
    use ::http::{header::HeaderName, HeaderValue};
    use data_types::{database_rules::DatabaseRules, DatabaseName};
    use std::convert::TryInto;
    use structopt::StructOpt;
    use trace::RingBufferTraceCollector;

    fn test_config(server_id: Option<u32>) -> Config {
        let mut config = Config::from_iter(&[
            "run",
            "--api-bind",
            "127.0.0.1:0",
            "--grpc-bind",
            "127.0.0.1:0",
        ]);
        config.server_id = server_id.map(|x| x.try_into().unwrap());
        config
    }

    async fn test_serve(
        config: Config,
        application: Arc<ApplicationState>,
        server: Arc<AppServer<ConnectionManager>>,
    ) {
        let grpc_listener = grpc_listener(config.grpc_bind_address).await.unwrap();
        let http_listener = http_listener(config.grpc_bind_address).await.unwrap();

        serve(
            config,
            application,
            grpc_listener,
            http_listener,
            Arc::new(LogTraceCollector::new()),
            server,
        )
        .await
        .unwrap()
    }

    #[tokio::test]
    async fn test_server_shutdown() {
        test_helpers::maybe_start_logging();

        // Create a server and wait for it to initialize
        let config = test_config(Some(23));
        let application = make_application(&config).await.unwrap();
        let server = make_server(Arc::clone(&application), &config);
        server.wait_for_init().await.unwrap();

        // Start serving
        let serve_fut = test_serve(config, application, Arc::clone(&server)).fuse();
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
        let config = test_config(None);
        let application = make_application(&config).await.unwrap();
        let server = make_server(Arc::clone(&application), &config);

        let serve_fut = test_serve(config, application, Arc::clone(&server)).fuse();
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
        let config = test_config(Some(999999999));
        let application = make_application(&config).await.unwrap();
        let server = make_server(Arc::clone(&application), &config);
        server.wait_for_init().await.unwrap();

        let serve_fut = test_serve(config, application, Arc::clone(&server)).fuse();
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
        let config = test_config(Some(23));
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

        let serve_fut = test_serve(config, Arc::clone(&application), Arc::clone(&server)).fuse();
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

    #[tokio::test]
    async fn test_tracing() {
        let config = test_config(Some(23));
        let application = make_application(&config).await.unwrap();
        let server = make_server(Arc::clone(&application), &config);
        server.wait_for_init().await.unwrap();

        let trace_collector = Arc::new(RingBufferTraceCollector::new(20));

        let grpc_listener = grpc_listener(config.grpc_bind_address).await.unwrap();
        let http_listener = http_listener(config.grpc_bind_address).await.unwrap();

        let addr = grpc_listener.local_addr().unwrap();

        let fut = serve(
            config,
            application,
            grpc_listener,
            http_listener,
            Arc::<RingBufferTraceCollector>::clone(&trace_collector),
            Arc::clone(&server),
        );

        let join = tokio::spawn(fut);

        let client = influxdb_iox_client::connection::Builder::default()
            .build(format!("http://{}", addr))
            .await
            .unwrap();

        let mut client = influxdb_iox_client::management::Client::new(client);

        client.list_databases().await.unwrap();

        assert_eq!(trace_collector.spans().len(), 0);

        let b3_tracing_client = influxdb_iox_client::connection::Builder::default()
            .header(
                HeaderName::from_static("x-b3-sampled"),
                HeaderValue::from_static("1"),
            )
            .header(
                HeaderName::from_static("x-b3-traceid"),
                HeaderValue::from_static("fea24902"),
            )
            .header(
                HeaderName::from_static("x-b3-spanid"),
                HeaderValue::from_static("ab3409"),
            )
            .build(format!("http://{}", addr))
            .await
            .unwrap();

        let mut b3_tracing_client = influxdb_iox_client::management::Client::new(b3_tracing_client);

        b3_tracing_client.list_databases().await.unwrap();
        b3_tracing_client.get_server_status().await.unwrap();

        let jaeger_tracing_client = influxdb_iox_client::connection::Builder::default()
            .header(
                HeaderName::from_static("uber-trace-id"),
                HeaderValue::from_static("34f9495:30e34:0:1"),
            )
            .build(format!("http://{}", addr))
            .await
            .unwrap();

        influxdb_iox_client::management::Client::new(jaeger_tracing_client)
            .list_databases()
            .await
            .unwrap();

        let spans = trace_collector.spans();
        assert_eq!(spans.len(), 3);

        let spans: Vec<trace::span::Span<'_>> = spans
            .iter()
            .map(|x| serde_json::from_str(x.as_str()).unwrap())
            .collect();

        assert_eq!(spans[0].name, "IOx");
        assert_eq!(spans[0].ctx.parent_span_id.unwrap().0.get(), 0xab3409);
        assert_eq!(spans[0].ctx.trace_id.0.get(), 0xfea24902);
        assert!(spans[0].start.is_some());
        assert!(spans[0].end.is_some());

        assert_eq!(spans[1].name, "IOx");
        assert_eq!(spans[1].ctx.parent_span_id.unwrap().0.get(), 0xab3409);
        assert_eq!(spans[1].ctx.trace_id.0.get(), 0xfea24902);
        assert!(spans[1].start.is_some());
        assert!(spans[1].end.is_some());

        assert_eq!(spans[2].name, "IOx");
        assert_eq!(spans[2].ctx.parent_span_id.unwrap().0.get(), 0x30e34);
        assert_eq!(spans[2].ctx.trace_id.0.get(), 0x34f9495);
        assert!(spans[2].start.is_some());
        assert!(spans[2].end.is_some());

        assert_ne!(spans[0].ctx.span_id, spans[1].ctx.span_id);
        server.shutdown();
        join.await.unwrap().unwrap();
    }
}
