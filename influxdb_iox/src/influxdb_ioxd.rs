use crate::influxdb_ioxd::server_type::{common_state::CommonServerState, ServerType};
use futures::{future::FusedFuture, pin_mut, FutureExt};
use hyper::server::conn::AddrIncoming;
use observability_deps::tracing::{error, info};
use panic_logging::SendPanicsToTracing;
use snafu::{ResultExt, Snafu};
use std::{net::SocketAddr, sync::Arc};
use trace_http::ctx::TraceHeaderParser;

mod http;
mod jemalloc;
mod planner;
pub(crate) mod rpc;
pub(crate) mod server_type;
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
    ServingRpc { source: server_type::RpcError },

    #[snafu(display("Early Http shutdown"))]
    LostHttp,

    #[snafu(display("Early RPC shutdown"))]
    LostRpc,

    #[snafu(display("Early server shutdown"))]
    LostServer,
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

#[cfg(all(
    feature = "heappy",
    feature = "jemalloc_replacing_malloc",
    not(feature = "clippy")
))]
fn build_malloc_conf() -> String {
    compile_error!("must use exactly one memory allocator")
}

#[cfg(feature = "clippy")]
fn build_malloc_conf() -> String {
    "clippy".to_string()
}

/// This is the entry point for the IOx server.
///
/// The precise server type depends on `T`. This entry point ensures that the given `server_type` is started using best
/// practice, e.g. that we print the GIT-hash and malloc-configs, that a panic handler is installed, etc.
///
/// Due to the invasive nature of the setup routine, this should not be used during unit tests.
pub async fn main<T>(common_state: CommonServerState, server_type: Arc<T>) -> Result<()>
where
    T: ServerType,
{
    let git_hash = option_env!("GIT_HASH").unwrap_or("UNKNOWN");
    let num_cpus = num_cpus::get();
    let build_malloc_conf = build_malloc_conf();
    info!(
        git_hash,
        num_cpus,
        %build_malloc_conf,
        "InfluxDB IOx server starting",
    );

    if (common_state.run_config().grpc_bind_address == common_state.run_config().http_bind_address)
        && (common_state.run_config().grpc_bind_address.port() != 0)
    {
        error!(
            grpc_bind_address=%common_state.run_config().grpc_bind_address,
            http_bind_address=%common_state.run_config().http_bind_address,
            "grpc and http bind addresses must differ",
        );
        std::process::exit(1);
    }

    // Install custom panic handler and forget about it.
    //
    // This leaks the handler and prevents it from ever being dropped during the
    // lifetime of the program - this is actually a good thing, as it prevents
    // the panic handler from being removed while unwinding a panic (which in
    // turn, causes a panic - see #548)
    let f = SendPanicsToTracing::new();
    std::mem::forget(f);

    // Register jemalloc metrics
    server_type
        .metric_registry()
        .register_instrument("jemalloc_metrics", jemalloc::JemallocMetrics::new);

    let grpc_listener = grpc_listener(common_state.run_config().grpc_bind_address.into()).await?;
    let http_listener = http_listener(common_state.run_config().http_bind_address.into()).await?;

    let trace_exporter = common_state.trace_exporter();
    let r = serve(common_state, grpc_listener, http_listener, server_type).await;

    if let Some(trace_exporter) = trace_exporter {
        if let Err(e) = trace_exporter.drain().await {
            error!(%e, "error draining trace exporter");
        }
    }
    r
}

pub async fn grpc_listener(addr: SocketAddr) -> Result<tokio::net::TcpListener> {
    let listener = tokio::net::TcpListener::bind(addr)
        .await
        .context(StartListeningGrpcSnafu { addr })?;

    match listener.local_addr() {
        Ok(local_addr) => info!(%local_addr, "bound gRPC listener"),
        Err(_) => info!(%addr, "bound gRPC listener"),
    }

    Ok(listener)
}

pub async fn http_listener(addr: SocketAddr) -> Result<AddrIncoming> {
    let listener = AddrIncoming::bind(&addr).context(StartListeningHttpSnafu { addr })?;
    info!(bind_addr=%listener.local_addr(), "bound HTTP listener");

    Ok(listener)
}

/// Instantiates the gRPC and HTTP listeners and returns a Future that completes when
/// these listeners, the Server, Databases, etc... have all exited.
///
/// This is effectively the "main loop" for influxdb_iox
async fn serve<T>(
    common_state: CommonServerState,
    grpc_listener: tokio::net::TcpListener,
    http_listener: AddrIncoming,
    server_type: Arc<T>,
) -> Result<()>
where
    T: ServerType,
{
    // Construct a token to trigger shutdown of API services
    let frontend_shutdown = tokio_util::sync::CancellationToken::new();

    let trace_header_parser = TraceHeaderParser::new()
        .with_jaeger_trace_context_header_name(
            &common_state
                .run_config()
                .tracing_config
                .traces_jaeger_trace_context_header_name,
        )
        .with_jaeger_debug_name(
            &common_state
                .run_config()
                .tracing_config
                .traces_jaeger_debug_name,
        );

    // Construct and start up gRPC server

    let grpc_server = rpc::serve(
        grpc_listener,
        Arc::clone(&server_type),
        trace_header_parser.clone(),
        frontend_shutdown.clone(),
        common_state.serving_readiness().clone(),
    )
    .fuse();

    info!("gRPC server listening");

    let http_server = http::serve(
        http_listener,
        Arc::clone(&server_type),
        frontend_shutdown.clone(),
        trace_header_parser,
    )
    .fuse();
    info!("HTTP server listening");

    // Purposefully use log not tokio-tracing to ensure correctly hooked up
    log::info!("InfluxDB IOx server ready");

    // Get IOx background worker join handle
    let server_handle = Arc::clone(&server_type).join().fuse();

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
    pin_mut!(server_handle);
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
            _ = server_handle => {
                error!("server worker shutdown prematurely");
                res = res.and(Err(Error::LostServer));
            },
            result = grpc_server => match result {
                Ok(_) if frontend_shutdown.is_cancelled() => info!("gRPC server shutdown"),
                Ok(_) => {
                    error!("Early gRPC server exit");
                    res = res.and(Err(Error::LostRpc));
                }
                Err(error) => {
                    error!(%error, "gRPC server error");
                    res = res.and(Err(Error::ServingRpc{source: error}));
                }
            },
            result = http_server => match result {
                Ok(_) if frontend_shutdown.is_cancelled() => info!("HTTP server shutdown"),
                Ok(_) => {
                    error!("Early HTTP server exit");
                    res = res.and(Err(Error::LostHttp));
                }
                Err(error) => {
                    error!(%error, "HTTP server error");
                    res = res.and(Err(Error::ServingHttp{source: error}));
                }
            },
        }

        frontend_shutdown.cancel()
    }
    info!("frontend shutdown completed");

    server_type.shutdown();
    if !server_handle.is_terminated() {
        server_handle.await;
    }
    info!("backend shutdown completed");

    res
}
