//! InfluxDB 3 Core server implementation
//!
//! The server is responsible for handling the HTTP API
#![deny(rustdoc::broken_intra_doc_links, rustdoc::bare_urls, rust_2018_idioms)]
#![warn(
missing_debug_implementations,
clippy::explicit_iter_loop,
clippy::use_self,
clippy::clone_on_ref_ptr,
// See https://github.com/influxdata/influxdb_iox/pull/1671
clippy::future_not_send
)]

pub mod all_paths;
mod grpc;
pub mod http;
mod unified_service;

use crate::grpc::make_flight_server;
use crate::http::HttpApi;
use crate::http::RecoveryHttpApi;
use authz::Authorizer;
use http::route_admin_token_recovery_request;
use hyper::Request;
use hyper::body::Incoming;
use hyper_util::rt::{TokioExecutor, TokioIo};
use hyper_util::server::conn::auto::Builder as ConnectionBuilder;
use hyper_util::server::graceful::GracefulShutdown;
use hyper_util::service::TowerToHyperService;
use influxdb3_authz::AuthProvider;
use influxdb3_catalog::catalog::Catalog;
use influxdb3_process::build_version_string;
use influxdb3_telemetry::store::TelemetryStore;
use observability_deps::tracing::{error, info, trace, warn};
use std::fmt::Debug;
use std::path::PathBuf;
use std::sync::{Arc, LazyLock};
use thiserror::Error;
use tokio::io::AsyncWriteExt;
use tokio::net::TcpListener;
use tokio::time::Instant;
use tokio_rustls::TlsAcceptor;
use tokio_rustls::rustls;
use tokio_rustls::rustls::pki_types::pem::PemObject;
use tokio_rustls::rustls::pki_types::{CertificateDer, PrivateKeyDer};
use tokio_rustls::rustls::{ServerConfig, SupportedProtocolVersion};
use tokio_util::sync::CancellationToken;
use trace::TraceCollector;
use trace_http::ctx::TraceHeaderParser;
use trace_http::metrics::{MetricFamily, RequestMetrics};
use trace_http::tower::{ServiceProtocol, TraceLayer};
use unified_service::{RemoteAddrLayer, UnifiedService};

pub const PRODUCT_NAME: &str = "InfluxDB 3 Core";
pub const INFLUXDB3_BUILD: &str = "Core";

/// Version string combining the product name, [`influxdb3_process::INFLUXDB3_VERSION`], and [`influxdb3_process::INFLUXDB3_GIT_HASH`].
pub static VERSION_STRING: LazyLock<String> = LazyLock::new(|| build_version_string(PRODUCT_NAME));

#[derive(Debug, Error)]
pub enum Error {
    #[error("hyper error: {0}")]
    Hyper(#[from] hyper::Error),

    #[error("http error: {0}")]
    Http(#[from] Box<http::Error>),

    #[error("database not found {db_name}")]
    DatabaseNotFound { db_name: String },

    #[error("datafusion error: {0}")]
    DataFusion(#[from] datafusion::error::DataFusionError),

    #[error("influxdb3_write error: {0}")]
    InfluxDB3Write(#[from] influxdb3_write::Error),

    #[error("from hex error: {0}")]
    FromHex(#[from] hex::FromHexError),

    #[error("io error: {0}")]
    Io(#[from] std::io::Error),

    #[error("tls config error: {0}")]
    TlsConfig(String),

    #[error("rustls error: {0}")]
    Rustls(#[from] rustls::Error),
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

#[derive(Debug, Clone)]
pub struct CommonServerState {
    catalog: Arc<Catalog>,
    metrics: Arc<metric::Registry>,
    trace_exporter: Option<Arc<trace_exporters::export::AsyncExporter>>,
    trace_header_parser: TraceHeaderParser,
    telemetry_store: Arc<TelemetryStore>,
}

impl CommonServerState {
    pub fn new(
        catalog: Arc<Catalog>,
        metrics: Arc<metric::Registry>,
        trace_exporter: Option<Arc<trace_exporters::export::AsyncExporter>>,
        trace_header_parser: TraceHeaderParser,
        telemetry_store: Arc<TelemetryStore>,
    ) -> Self {
        Self {
            catalog,
            metrics,
            trace_exporter,
            trace_header_parser,
            telemetry_store,
        }
    }

    pub fn trace_exporter(&self) -> Option<Arc<trace_exporters::export::AsyncExporter>> {
        self.trace_exporter.clone()
    }

    pub fn trace_collector(&self) -> Option<Arc<dyn TraceCollector>> {
        self.trace_exporter
            .clone()
            .map(|x| -> Arc<dyn TraceCollector> { x })
    }

    pub fn trace_header_parser(&self) -> TraceHeaderParser {
        self.trace_header_parser.clone()
    }

    pub fn metric_registry(&self) -> Arc<metric::Registry> {
        Arc::<metric::Registry>::clone(&self.metrics)
    }
}

#[derive(Debug)]
pub struct CreateServerArgs<'a> {
    pub common_state: CommonServerState,
    pub http: Arc<HttpApi>,
    pub authorizer: Arc<dyn AuthProvider>,
    pub listener: TcpListener,
    pub cert_file: Option<PathBuf>,
    pub key_file: Option<PathBuf>,
    pub tls_minimum_version: &'a [&'static SupportedProtocolVersion],
}

#[derive(Debug)]
pub struct Server<'a> {
    common_state: CommonServerState,
    http: Arc<HttpApi>,
    authorizer: Arc<dyn AuthProvider>,
    listener: TcpListener,
    key_file: Option<PathBuf>,
    cert_file: Option<PathBuf>,
    tls_minimum_version: &'a [&'static SupportedProtocolVersion],
}

impl<'a> Server<'a> {
    pub fn new(
        CreateServerArgs {
            common_state,
            http,
            authorizer,
            listener,
            cert_file,
            key_file,
            tls_minimum_version,
        }: CreateServerArgs<'a>,
    ) -> Self {
        Self {
            common_state,
            http,
            authorizer,
            listener,
            key_file,
            cert_file,
            tls_minimum_version,
        }
    }

    pub fn authorizer(&self) -> Arc<dyn Authorizer> {
        Arc::clone(&self.authorizer.upcast())
    }

    /// Create an HTTP trace layer for request monitoring and metrics
    fn create_http_trace_layer(&self) -> TraceLayer {
        TraceLayer::new(
            self.common_state.trace_header_parser(),
            Arc::new(RequestMetrics::new(
                self.common_state.metric_registry(),
                MetricFamily::HttpServer,
            )),
            self.common_state.trace_collector(),
            "influxdb3_server_http",
            ServiceProtocol::Http,
        )
    }

    /// Create a gRPC trace layer for request monitoring and metrics
    fn create_grpc_trace_layer(&self) -> TraceLayer {
        TraceLayer::new(
            self.common_state.trace_header_parser(),
            Arc::new(RequestMetrics::new(
                self.common_state.metric_registry(),
                MetricFamily::GrpcServer,
            )),
            self.common_state.trace_collector(),
            "influxdb3_server_grpc",
            ServiceProtocol::Grpc,
        )
    }
}

pub async fn serve_admin_token_recovery_endpoint(
    server: Server<'_>,
    shutdown: CancellationToken,
    tcp_listener_file_path: Option<PathBuf>,
) -> Result<()> {
    // Create HTTP trace layer for monitoring and metrics
    let http_trace_layer = server.create_http_trace_layer();

    // Create a dedicated shutdown token for the recovery endpoint
    // This allows us to shut down just the recovery endpoint after token regeneration
    let recovery_shutdown = CancellationToken::new();

    // Create the recovery API wrapper with the shutdown token
    let recovery_api = Arc::new(RecoveryHttpApi::new(
        Arc::clone(&server.http),
        recovery_shutdown.clone(),
    ));

    if let (Some(key_file), Some(cert_file)) = (&server.key_file, &server.cert_file) {
        let listener = server.listener;
        let tls_min = server.tls_minimum_version;

        let (tcp_listener, certs, key) = setup_tls(listener, key_file, cert_file)?;
        let addr = tcp_listener.local_addr()?;
        info!(
            address = %addr,
            "starting admin token recovery endpoint with TLS on",
        );

        write_address_to_file(tcp_listener_file_path, &addr).await?;

        // Configure TLS
        let mut tls_config = ServerConfig::builder_with_protocol_versions(tls_min)
            .with_no_client_auth()
            .with_single_cert(certs, key)?;
        tls_config.alpn_protocols = vec![b"h2".to_vec(), b"http/1.1".to_vec()];
        let tls_acceptor = tokio_rustls::TlsAcceptor::from(Arc::new(tls_config));

        // Connection handling loop
        loop {
            tokio::select! {
                res = tcp_listener.accept() => {
                    let (stream, _) = res?;
                    if let Err(e) = stream.set_nodelay(true) {
                        warn!(err = %e, "cannot set TCP_NODELAY on the incoming socket");
                    }
                    let tls_acceptor = tls_acceptor.clone();
                    let recovery_api = Arc::clone(&recovery_api);
                    let http_trace_layer = http_trace_layer.clone();

                    tokio::spawn(async move {
                        let tls_stream = match tls_acceptor.accept(stream).await {
                            Ok(stream) => stream,
                            Err(e) => {
                                error!("TLS handshake failed: {}", e);
                                return;
                            }
                        };

                        let io = TokioIo::new(tls_stream);

                        // Create service with trace layer
                        let service_fn = tower::service_fn(move |req| {
                            let recovery_api = Arc::clone(&recovery_api);
                            async move {
                                route_admin_token_recovery_request(recovery_api, req).await
                            }
                        });
                        let service = tower::ServiceBuilder::new()
                            .layer(http_trace_layer.clone())
                            .service(service_fn);
                        let service = TowerToHyperService::new(service);

                        if let Err(err) = ConnectionBuilder::new(TokioExecutor::new())
                            .serve_connection(io, service)
                            .await
                        {
                            error!("Error serving connection: {:?}", err);
                        }
                    });
                }
                _ = shutdown.cancelled() => break,
                _ = recovery_shutdown.cancelled() => {
                    info!("Admin token recovery endpoint shutting down after token regeneration");
                    break;
                },
            }
        }
    } else {
        let tcp_listener = server.listener;
        let addr = tcp_listener.local_addr()?;
        info!(
            address = %addr,
            "starting admin token recovery endpoint on",
        );

        write_address_to_file(tcp_listener_file_path, &addr).await?;

        // Connection handling loop
        loop {
            tokio::select! {
                res = tcp_listener.accept() => {
                    let (stream, _) = res?;
                    if let Err(e) = stream.set_nodelay(true) {
                        warn!(err = %e, "cannot set TCP_NODELAY on the incoming socket");
                    }
                    let io = TokioIo::new(stream);
                    let recovery_api = Arc::clone(&recovery_api);
                    let http_trace_layer = http_trace_layer.clone();

                    tokio::spawn(async move {
                        // Create service with trace layer
                        let service_fn = tower::service_fn(move |req| {
                            let recovery_api = Arc::clone(&recovery_api);
                            async move {
                                route_admin_token_recovery_request(recovery_api, req).await
                            }
                        });
                        let service = tower::ServiceBuilder::new()
                            .layer(http_trace_layer.clone())
                            .service(service_fn);
                        let service = TowerToHyperService::new(service);

                        if let Err(err) = ConnectionBuilder::new(TokioExecutor::new())
                            .serve_connection(io, service)
                            .await
                        {
                            error!("Error serving connection: {:?}", err);
                        }
                    });
                }
                _ = shutdown.cancelled() => break,
                _ = recovery_shutdown.cancelled() => {
                    info!("Admin token recovery endpoint shutting down after token regeneration");
                    break;
                },
            }
        }
    }

    Ok(())
}

/// Determines if an HTTP request is a gRPC request based on version and content-type
pub(crate) fn is_grpc_request(req: &Request<Incoming>) -> bool {
    req.version() == hyper::Version::HTTP_2
        && req
            .headers()
            .get(hyper::header::CONTENT_TYPE)
            .and_then(|ct| ct.to_str().ok())
            .map(|ct| ct.starts_with("application/grpc"))
            .unwrap_or(false)
}

pub async fn serve(
    server: Server<'_>,
    shutdown: CancellationToken,
    startup_timer: Instant,
    without_auth: bool,
    paths_without_authz: &'static Vec<&'static str>,
    tcp_listener_file_path: Option<PathBuf>,
) -> Result<()> {
    // Create trace layers for HTTP and gRPC
    let http_trace_layer = server.create_http_trace_layer();
    let grpc_trace_layer = server.create_grpc_trace_layer();

    // Create gRPC service with trace layer
    let grpc_service = make_flight_server(
        Arc::clone(&server.http.query_executor),
        Some(server.authorizer()),
    );

    let key_file = server.key_file.clone();
    let cert_file = server.cert_file.clone();
    let http_api = Arc::clone(&server.http);

    // Create graceful shutdown handler
    let graceful = GracefulShutdown::new();

    // Create unified service once and wrap in Arc for sharing across connections
    let unified_service = Arc::new(UnifiedService::new(
        Arc::clone(&http_api),
        grpc_service,
        without_auth,
        paths_without_authz,
    ));

    if let (Some(key_file), Some(cert_file)) = (key_file.as_ref(), cert_file.as_ref()) {
        let listener = server.listener;
        let tls_min = server.tls_minimum_version;

        let (tcp_listener, certs, key) = setup_tls(listener, key_file, cert_file)?;
        let addr = tcp_listener.local_addr()?;

        let timer_end = Instant::now();
        let startup_time = timer_end.duration_since(startup_timer);
        info!(
            address = %addr,
            "startup time: {}ms",
            startup_time.as_millis()
        );

        write_address_to_file(tcp_listener_file_path, &addr).await?;

        // Configure TLS
        let mut tls_config = ServerConfig::builder_with_protocol_versions(tls_min)
            .with_no_client_auth()
            .with_single_cert(certs, key)?;
        tls_config.alpn_protocols = vec![b"h2".to_vec(), b"http/1.1".to_vec()];
        let tls_acceptor = TlsAcceptor::from(Arc::new(tls_config));

        // Connection handling loop
        loop {
            tokio::select! {
                _ = shutdown.cancelled() => break,
                res = tcp_listener.accept() => {
                    let (stream, remote_addr) = res?;
                    if let Err(e) = stream.set_nodelay(true) {
                        warn!(err = %e, "cannot set TCP_NODELAY on the incoming socket");
                    }
                    let tls_acceptor = tls_acceptor.clone();
                    let unified_service = Arc::clone(&unified_service);
                    let http_trace_layer = http_trace_layer.clone();
                    let grpc_trace_layer = grpc_trace_layer.clone();
                    let graceful_watcher = graceful.watcher();

                    tokio::spawn(async move {
                        // Perform TLS handshake
                        let tls_stream = match tls_acceptor.accept(stream).await {
                            Ok(stream) => stream,
                            Err(e) => {
                                error!("TLS handshake failed: {}", e);
                                return;
                            }
                        };

                        let io = TokioIo::new(tls_stream);

                        // Build the service stack with both trace layers
                        let service = tower::ServiceBuilder::new()
                            .layer(RemoteAddrLayer::new(remote_addr))
                            .layer(http_trace_layer)
                            .layer(grpc_trace_layer)
                            .service(unified_service.as_ref().clone());

                        let service = TowerToHyperService::new(service);

                        // Create connection
                        let conn = ConnectionBuilder::new(TokioExecutor::new())
                            .serve_connection(io, service)
                            .into_owned();

                        // Watch with graceful
                        let conn = graceful_watcher.watch(conn);

                        // Handle connection
                        if let Err(e) = conn.await {
                            error!("Error serving TLS connection: {:?}", e);
                        }
                    });
                }
            }
        }
    } else {
        let tcp_listener = server.listener;
        let addr = tcp_listener.local_addr()?;

        let timer_end = Instant::now();
        let startup_time = timer_end.duration_since(startup_timer);
        info!(
            address = %addr,
            "startup time: {}ms",
            startup_time.as_millis()
        );

        write_address_to_file(tcp_listener_file_path, &addr).await?;

        // Connection handling loop
        loop {
            tokio::select! {
                _ = shutdown.cancelled() => break,
                res = tcp_listener.accept() => {
                    let (stream, remote_addr) = res?;
                    if let Err(e) = stream.set_nodelay(true) {
                        warn!(err = %e, "cannot set TCP_NODELAY on the incoming socket");
                    }
                    let unified_service = Arc::clone(&unified_service);
                    let http_trace_layer = http_trace_layer.clone();
                    let grpc_trace_layer = grpc_trace_layer.clone();
                    let graceful_watcher = graceful.watcher();

                    tokio::spawn(async move {
                        let io = TokioIo::new(stream);

                        // Build the service stack with both trace layers
                        let service = tower::ServiceBuilder::new()
                            .layer(RemoteAddrLayer::new(remote_addr))
                            .layer(http_trace_layer)
                            .layer(grpc_trace_layer)
                            .service(unified_service.as_ref().clone());

                        let service = TowerToHyperService::new(service);

                        // Create connection
                        let conn = ConnectionBuilder::new(TokioExecutor::new())
                            .serve_connection(io, service)
                            .into_owned();

                        // Watch with graceful
                        let conn = graceful_watcher.watch(conn);

                        // Handle connection
                        if let Err(e) = conn.await {
                            error!("Error serving connection: {:?}", e);
                        }
                    });
                }
            }
        }
    }

    // This explicit select! is needed for graceful shutdown
    trace!("Starting graceful shutdown, waiting for connections to close");
    tokio::select! {
        _ = graceful.shutdown() => {
            info!("All connections closed gracefully");
        }
        _ = tokio::time::sleep(tokio::time::Duration::from_secs(30)) => {
            info!("Graceful shutdown timed out after 30 seconds");
        }
    }

    Ok(())
}

// This function is only called when running tests to get hold of the server port details as the
// tests start on arbitrary port by passing in 0 as port. This is also called when setting up TLS
// as the tests seem to use TLS by default.
async fn write_address_to_file(
    tcp_listener_file_path: Option<PathBuf>,
    addr: &std::net::SocketAddr,
) -> Result<(), Error> {
    if let Some(path) = tcp_listener_file_path {
        let mut f = tokio::fs::File::create_new(path).await?;
        let _ = f.write(addr.to_string().as_bytes()).await?;
        f.flush().await?;
    };
    Ok(())
}

fn setup_tls(
    tcp_listener: TcpListener,
    key_file: &PathBuf,
    cert_file: &PathBuf,
) -> Result<
    (
        TcpListener,
        Vec<CertificateDer<'static>>,
        PrivateKeyDer<'static>,
    ),
    Error,
> {
    let certs = CertificateDer::pem_file_iter(cert_file)
        .map_err(|e| Error::TlsConfig(format!("Error reading certs: {e}")))?
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| Error::TlsConfig(format!("Error reading certs: {e}")))?;
    let key = PrivateKeyDer::from_pem_file(key_file)
        .map_err(|e| Error::TlsConfig(format!("Error reading private key: {e}")))?;
    Ok((tcp_listener, certs, key))
}

#[cfg(test)]
mod tests;
