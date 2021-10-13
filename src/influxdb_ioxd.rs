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
    connection::ConnectionManagerImpl as ConnectionManager, ApplicationState, RemoteTemplate,
    Server as AppServer, ServerConfig,
};
use snafu::{ResultExt, Snafu};
use std::{convert::TryFrom, net::SocketAddr, sync::Arc};
use trace::TraceCollector;
use trace_http::ctx::TraceHeaderParser;

mod http;
mod jemalloc;
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

    #[snafu(display("Cannot create tracing pipeline: {}", source))]
    Tracing { source: trace_exporters::Error },
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
    if let Some(id) = config.server_id_config.server_id {
        app_server.set_id(id).expect("server id already set");
    } else {
        warn!("server ID not set. ID must be set via the INFLUXDB_IOX_ID config or API before writing or querying data.");
    }

    app_server
}

#[cfg(all(not(feature = "heappy"), not(feature = "jemalloc_replacing_malloc")))]
fn build_malloc_conf() -> String {
    "system".to_string()
}

#[cfg(all(feature = "heappy", not(feature = "jemalloc_replacing_malloc")))]
fn build_malloc_conf() -> String {
    "heappy".to_string()
}

#[cfg(all(not(feature = "heappy"), feature = "jemalloc_replacing_malloc"))]
fn build_malloc_conf() -> String {
    tikv_jemalloc_ctl::config::malloc_conf::mib()
        .unwrap()
        .read()
        .unwrap()
        .to_string()
}

#[cfg(all(feature = "heappy", feature = "jemalloc_replacing_malloc"))]
fn build_malloc_conf() -> String {
    compile_error!("must use exactly one memory allocator")
}

/// This is the entry point for the IOx server. `config` represents
/// command line arguments, if any.
pub async fn main(config: Config) -> Result<()> {
    let git_hash = option_env!("GIT_HASH").unwrap_or("UNKNOWN");
    let num_cpus = num_cpus::get();
    let build_malloc_conf = build_malloc_conf();
    info!(
        git_hash,
        num_cpus,
        %build_malloc_conf,
        "InfluxDB IOx server starting",
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

    // Register jemalloc metrics
    application
        .metric_registry()
        .register_instrument("jemalloc_metrics", jemalloc::JemallocMetrics::new);

    let app_server = make_server(Arc::clone(&application), &config);

    let grpc_listener = grpc_listener(config.grpc_bind_address).await?;
    let http_listener = http_listener(config.http_bind_address).await?;
    let async_exporter = config.tracing_config.build().context(Tracing)?;
    let trace_collector = async_exporter
        .clone()
        .map(|x| -> Arc<dyn TraceCollector> { x });

    let r = serve(
        config,
        application,
        grpc_listener,
        http_listener,
        trace_collector,
        app_server,
    )
    .await;

    if let Some(async_exporter) = async_exporter {
        if let Err(e) = async_exporter.drain().await {
            error!(%e, "error draining trace exporter");
        }
    }
    r
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
    trace_collector: Option<Arc<dyn TraceCollector>>,
    app_server: Arc<AppServer<ConnectionManager>>,
) -> Result<()> {
    // Construct a token to trigger shutdown of API services
    let frontend_shutdown = tokio_util::sync::CancellationToken::new();

    let trace_header_parser = TraceHeaderParser::new()
        .with_jaeger_trace_context_header_name(
            config
                .tracing_config
                .traces_jaeger_trace_context_header_name,
        )
        .with_jaeger_debug_name(config.tracing_config.traces_jaeger_debug_name);

    // Construct and start up gRPC server

    let grpc_server = rpc::serve(
        grpc_listener,
        Arc::clone(&application),
        Arc::clone(&app_server),
        trace_header_parser.clone(),
        trace_collector.clone(),
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
        trace_header_parser,
        trace_collector,
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

    application.join();
    info!("shared application state completed shutting down");

    res
}

#[cfg(test)]
mod tests {
    use super::*;
    use ::http::{header::HeaderName, HeaderValue};
    use data_types::{database_rules::DatabaseRules, DatabaseName};
    use influxdb_iox_client::connection::Connection;
    use server::rules::ProvidedDatabaseRules;
    use std::{convert::TryInto, num::NonZeroU64};
    use structopt::StructOpt;
    use tokio::task::JoinHandle;
    use trace::{
        span::{Span, SpanStatus},
        RingBufferTraceCollector,
    };
    use trace_exporters::export::{AsyncExporter, TestAsyncExporter};

    fn test_config(server_id: Option<u32>) -> Config {
        let mut config = Config::from_iter(&[
            "run",
            "--api-bind",
            "127.0.0.1:0",
            "--grpc-bind",
            "127.0.0.1:0",
        ]);
        config.server_id_config.server_id = server_id.map(|x| x.try_into().unwrap());
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
            None,
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
            .create_database(make_rules(&other_db_name))
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
            .create_database(make_rules("panic_test"))
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

    async fn jaeger_client(addr: SocketAddr, trace: &'static str) -> Connection {
        influxdb_iox_client::connection::Builder::default()
            .header(
                HeaderName::from_static("uber-trace-id"),
                HeaderValue::from_static(trace),
            )
            .build(format!("http://{}", addr))
            .await
            .unwrap()
    }

    async fn tracing_server<T: TraceCollector + 'static>(
        collector: &Arc<T>,
    ) -> (
        SocketAddr,
        Arc<AppServer<ConnectionManager>>,
        JoinHandle<Result<()>>,
    ) {
        let config = test_config(Some(23));
        let application = make_application(&config).await.unwrap();
        let server = make_server(Arc::clone(&application), &config);
        server.wait_for_init().await.unwrap();

        let grpc_listener = grpc_listener(config.grpc_bind_address).await.unwrap();
        let http_listener = http_listener(config.grpc_bind_address).await.unwrap();

        let addr = grpc_listener.local_addr().unwrap();

        let fut = serve(
            config,
            application,
            grpc_listener,
            http_listener,
            Some(Arc::<T>::clone(collector)),
            Arc::clone(&server),
        );

        let join = tokio::spawn(fut);
        (addr, server, join)
    }

    #[tokio::test]
    async fn test_tracing() {
        let trace_collector = Arc::new(RingBufferTraceCollector::new(20));
        let (addr, server, join) = tracing_server(&trace_collector).await;

        let client = influxdb_iox_client::connection::Builder::default()
            .build(format!("http://{}", addr))
            .await
            .unwrap();

        let mut client = influxdb_iox_client::management::Client::new(client);

        client.list_database_names().await.unwrap();

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

        b3_tracing_client.list_database_names().await.unwrap();
        b3_tracing_client.get_server_status().await.unwrap();

        let conn = jaeger_client(addr, "34f9495:30e34:0:1").await;
        influxdb_iox_client::management::Client::new(conn)
            .list_database_names()
            .await
            .unwrap();

        let spans = trace_collector.spans();
        assert_eq!(spans.len(), 3);

        assert_eq!(spans[0].name, "IOx");
        assert_eq!(spans[0].ctx.parent_span_id.unwrap().0.get(), 0xab3409);
        assert_eq!(spans[0].ctx.trace_id.0.get(), 0xfea24902);
        assert!(spans[0].start.is_some());
        assert!(spans[0].end.is_some());
        assert_eq!(spans[0].status, SpanStatus::Ok);

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

    #[tokio::test]
    async fn test_query_tracing() {
        let collector = Arc::new(RingBufferTraceCollector::new(100));
        let (addr, server, join) = tracing_server(&collector).await;
        let conn = jaeger_client(addr, "34f8495:35e32:0:1").await;

        let db_info = influxdb_storage_client::OrgAndBucket::new(
            NonZeroU64::new(12).unwrap(),
            NonZeroU64::new(344).unwrap(),
        );

        // Perform a number of different requests to generate traces

        let mut management = influxdb_iox_client::management::Client::new(conn.clone());
        management
            .create_database(
                influxdb_iox_client::management::generated_types::DatabaseRules {
                    name: db_info.db_name().to_string(),
                    ..Default::default()
                },
            )
            .await
            .unwrap();

        let mut write = influxdb_iox_client::write::Client::new(conn.clone());
        write
            .write(db_info.db_name(), "cpu,tag0=foo val=1 100\n")
            .await
            .unwrap();

        let mut flight = influxdb_iox_client::flight::Client::new(conn.clone());
        flight
            .perform_query(db_info.db_name(), "select * from cpu;")
            .await
            .unwrap();

        flight
            .perform_query("nonexistent", "select * from cpu;")
            .await
            .unwrap_err();

        flight
            .perform_query(db_info.db_name(), "select * from nonexistent;")
            .await
            .unwrap_err();

        let mut storage = influxdb_storage_client::Client::new(conn);
        storage
            .tag_values(influxdb_storage_client::generated_types::TagValuesRequest {
                tags_source: Some(influxdb_storage_client::Client::read_source(&db_info, 1)),
                range: None,
                predicate: None,
                tag_key: "tag0".into(),
            })
            .await
            .unwrap();

        server.shutdown();
        join.await.unwrap().unwrap();

        // Check generated traces

        let spans = collector.spans();

        let root_spans: Vec<_> = spans.iter().filter(|span| &span.name == "IOx").collect();
        // Made 6 requests
        assert_eq!(root_spans.len(), 6);

        let child = |parent: &Span, name: &'static str| -> Option<&Span> {
            spans.iter().find(|span| {
                span.ctx.parent_span_id == Some(parent.ctx.span_id) && span.name == name
            })
        };

        // Test SQL
        let sql_query_span = root_spans[2];
        assert_eq!(sql_query_span.status, SpanStatus::Ok);

        let ctx_span = child(sql_query_span, "Query Execution").unwrap();
        let planner_span = child(ctx_span, "Planner").unwrap();
        let sql_span = child(planner_span, "sql").unwrap();
        let prepare_sql_span = child(sql_span, "prepare_sql").unwrap();
        child(prepare_sql_span, "prepare_plan").unwrap();

        let collect_span = child(ctx_span, "collect").unwrap();
        let execute_span = child(collect_span, "execute_stream_partitioned").unwrap();
        let coalesce_span = child(execute_span, "CoalescePartitionsEx").unwrap();

        // validate spans from DataFusion ExecutionPlan are present
        child(coalesce_span, "ProjectionExec: expr").unwrap();

        let database_not_found = root_spans[3];
        assert_eq!(database_not_found.status, SpanStatus::Err);
        assert!(database_not_found
            .events
            .iter()
            .any(|event| event.msg.as_ref() == "not found"));

        let table_not_found = root_spans[4];
        assert_eq!(table_not_found.status, SpanStatus::Err);
        assert!(table_not_found
            .events
            .iter()
            .any(|event| event.msg.as_ref() == "invalid argument"));

        // Test tag_values
        let storage_span = root_spans[5];
        let ctx_span = child(storage_span, "Query Execution").unwrap();
        child(ctx_span, "Planner").unwrap();

        let to_string_set = child(ctx_span, "to_string_set").unwrap();
        child(to_string_set, "run_logical_plans").unwrap();
    }

    #[tokio::test]
    async fn test_async_exporter() {
        let (sender, mut receiver) = tokio::sync::mpsc::channel(20);
        let collector = Arc::new(AsyncExporter::new(TestAsyncExporter::new(sender)));

        let (addr, server, join) = tracing_server(&collector).await;
        let conn = jaeger_client(addr, "34f8495:30e34:0:1").await;
        influxdb_iox_client::management::Client::new(conn)
            .list_database_names()
            .await
            .unwrap();

        collector.drain().await.unwrap();

        server.shutdown();
        join.await.unwrap().unwrap();

        let span = receiver.recv().await.unwrap();
        assert_eq!(span.ctx.trace_id.get(), 0x34f8495);
        assert_eq!(span.ctx.parent_span_id.unwrap().get(), 0x30e34);
    }

    fn make_rules(db_name: impl Into<String>) -> ProvidedDatabaseRules {
        let db_name = DatabaseName::new(db_name.into()).unwrap();
        ProvidedDatabaseRules::new_rules(DatabaseRules::new(db_name).into())
            .expect("Tests should create valid DatabaseRules")
    }
}
