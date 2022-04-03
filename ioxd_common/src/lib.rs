pub mod http;
pub mod rpc;
pub mod server_type;
mod service;

pub use service::Service;

use crate::server_type::{CommonServerState, ServerType};
use futures::{future::FusedFuture, pin_mut, FutureExt};
use hyper::server::conn::AddrIncoming;
use observability_deps::tracing::{error, info};
use snafu::{ResultExt, Snafu};
use std::{net::SocketAddr, sync::Arc};
use tokio_util::sync::CancellationToken;
use trace_http::ctx::TraceHeaderParser;

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
pub async fn wait_for_signal() {
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
pub async fn wait_for_signal() {
    let _ = tokio::signal::ctrl_c().await;
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

/// Instantiates the gRPC and optional HTTP listeners and returns a
/// Future that completes when these listeners, the Server, Databases,
/// etc... have all exited or the frontend_shutdown token is called.
pub async fn serve(
    common_state: CommonServerState,
    frontend_shutdown: CancellationToken,
    grpc_listener: tokio::net::TcpListener,
    http_listener: Option<AddrIncoming>,
    server_type: Arc<dyn ServerType>,
) -> Result<()> {
    let trace_header_parser = TraceHeaderParser::new()
        .with_jaeger_trace_context_header_name(
            &common_state
                .run_config()
                .tracing_config()
                .traces_jaeger_trace_context_header_name,
        )
        .with_jaeger_debug_name(
            &common_state
                .run_config()
                .tracing_config()
                .traces_jaeger_debug_name,
        );

    // Construct and start up gRPC server
    let grpc_server = rpc::serve(
        grpc_listener,
        Arc::clone(&server_type),
        trace_header_parser.clone(),
        frontend_shutdown.clone(),
    )
    .fuse();
    info!("gRPC server listening");

    let captured_server_type = Arc::clone(&server_type);
    let captured_shutdown = frontend_shutdown.clone();
    let http_server = async move {
        if let Some(http_listener) = http_listener {
            http::serve(
                http_listener,
                captured_server_type,
                captured_shutdown,
                trace_header_parser,
            )
            .await?
        } else {
            // don't resolve otherwise will cause server to shutdown
            captured_shutdown.cancelled().await
        }
        Ok(())
    }
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
