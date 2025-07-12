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
pub mod query_executor;
mod query_planner;
mod service;
mod system_tables;

use crate::grpc::make_flight_server;
use crate::http::HttpApi;
use crate::http::route_request;
use authz::Authorizer;
use http::route_admin_token_recovery_request;
use hyper::server::conn::AddrIncoming;
use hyper::server::conn::Http;
use hyper::service::service_fn;
use influxdb3_authz::AuthProvider;
use influxdb3_telemetry::store::TelemetryStore;
use observability_deps::tracing::error;
use observability_deps::tracing::info;
use rustls::ServerConfig;
use rustls::SupportedProtocolVersion;
use service::hybrid;
use std::convert::Infallible;
use std::fmt::Debug;
use std::fs::File;
use std::io::BufReader;
use std::path::PathBuf;
use std::sync::Arc;
use thiserror::Error;
use tokio::io::AsyncWriteExt;
use tokio::net::TcpListener;
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;
use tower::Layer;
use trace::TraceCollector;
use trace_http::ctx::TraceHeaderParser;
use trace_http::metrics::MetricFamily;
use trace_http::metrics::RequestMetrics;
use trace_http::tower::TraceLayer;

const TRACE_HTTP_SERVER_NAME: &str = "influxdb3_http";
const ADMIN_TOKEN_RECOVERY_TRACE_HTTP_SERVER_NAME: &str = "influxdb3_token_recovery_http";
const TRACE_GRPC_SERVER_NAME: &str = "influxdb3_grpc";

#[derive(Debug, Error)]
pub enum Error {
    #[error("hyper error: {0}")]
    Hyper(#[from] hyper::Error),

    #[error("http error: {0}")]
    Http(#[from] http::Error),

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
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

#[derive(Debug, Clone)]
pub struct CommonServerState {
    metrics: Arc<metric::Registry>,
    trace_exporter: Option<Arc<trace_exporters::export::AsyncExporter>>,
    trace_header_parser: TraceHeaderParser,
    telemetry_store: Arc<TelemetryStore>,
}

impl CommonServerState {
    pub fn new(
        metrics: Arc<metric::Registry>,
        trace_exporter: Option<Arc<trace_exporters::export::AsyncExporter>>,
        trace_header_parser: TraceHeaderParser,
        telemetry_store: Arc<TelemetryStore>,
    ) -> Self {
        Self {
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
}

/// Creates HTTP trace layer
fn create_http_trace_layer(common_state: &CommonServerState, server_name: &str) -> TraceLayer {
    let http_metrics =
        RequestMetrics::new(Arc::clone(&common_state.metrics), MetricFamily::HttpServer);
    TraceLayer::new(
        common_state.trace_header_parser.clone(),
        Arc::new(http_metrics),
        common_state.trace_collector().clone(),
        server_name,
        trace_http::tower::ServiceProtocol::Http,
    )
}

/// Creates gRPC trace layer
fn create_grpc_trace_layer(common_state: &CommonServerState) -> TraceLayer {
    let grpc_metrics =
        RequestMetrics::new(Arc::clone(&common_state.metrics), MetricFamily::GrpcServer);
    TraceLayer::new(
        common_state.trace_header_parser.clone(),
        Arc::new(grpc_metrics),
        common_state.trace_collector().clone(),
        TRACE_GRPC_SERVER_NAME,
        trace_http::tower::ServiceProtocol::Grpc,
    )
}

pub async fn serve_admin_token_recovery_endpoint(
    server: Server<'_>,
    shutdown: CancellationToken,
    tcp_listener_file_path: Option<PathBuf>,
) -> Result<()> {
    let http_trace_layer = create_http_trace_layer(
        &server.common_state,
        ADMIN_TOKEN_RECOVERY_TRACE_HTTP_SERVER_NAME,
    );

    if let (Some(key_file), Some(cert_file)) = (&server.key_file, &server.cert_file) {
        let listener = server.listener;
        let tls_min = server.tls_minimum_version;
        let http = Arc::clone(&server.http);

        let (addr, certs, key) = setup_tls(listener, key_file, cert_file)?;
        info!(
            address = %addr.local_addr(),
            "starting admin token recovery endpoint with TLS on",
        );

        let http_server = Arc::clone(&http);
        let rest_service = hyper::service::make_service_fn(move |_| {
            let http_server = Arc::clone(&http_server);
            let service = service_fn(move |req: hyper::Request<hyper::Body>| {
                route_admin_token_recovery_request(Arc::clone(&http_server), req)
            });
            let service = http_trace_layer.layer(service);
            futures::future::ready(Ok::<_, Infallible>(service))
        });

        write_address_to_file(tcp_listener_file_path, &addr).await?;

        let acceptor = hyper_rustls::TlsAcceptor::builder()
            .with_tls_config(
                ServerConfig::builder_with_protocol_versions(tls_min)
                    .with_no_client_auth()
                    .with_single_cert(certs, key)
                    .unwrap(),
            )
            .with_all_versions_alpn()
            .with_incoming(addr);

        hyper::server::Server::builder(acceptor)
            .serve(rest_service)
            .with_graceful_shutdown(shutdown.cancelled())
            .await?;
    } else {
        let rest_service = hyper::service::make_service_fn(|_| {
            let http_server = Arc::clone(&server.http);
            let service = service_fn(move |req: hyper::Request<hyper::Body>| {
                route_admin_token_recovery_request(Arc::clone(&http_server), req)
            });
            let service = http_trace_layer.layer(service);
            futures::future::ready(Ok::<_, Infallible>(service))
        });

        let addr = AddrIncoming::from_listener(server.listener)?;
        info!(
            address = %addr.local_addr(),
            "starting admin token recovery endpoint on",
        );
        hyper::server::Builder::new(addr, Http::new())
            .tcp_nodelay(true)
            .serve(rest_service)
            .with_graceful_shutdown(shutdown.cancelled())
            .await?;
    }

    Ok(())
}

pub async fn serve(
    server: Server<'_>,
    shutdown: CancellationToken,
    startup_timer: Instant,
    without_auth: bool,
    paths_without_authz: &'static Vec<&'static str>,
    tcp_listener_file_path: Option<PathBuf>,
) -> Result<()> {
    let grpc_trace_layer = create_grpc_trace_layer(&server.common_state);
    let grpc_service = grpc_trace_layer.layer(make_flight_server(
        Arc::clone(&server.http.query_executor),
        Some(server.authorizer()),
    ));

    let http_trace_layer = create_http_trace_layer(&server.common_state, TRACE_HTTP_SERVER_NAME);

    let key_file = server.key_file.clone();
    let cert_file = server.cert_file.clone();

    if let (Some(key_file), Some(cert_file)) = (key_file.as_ref(), cert_file.as_ref()) {
        let listener = server.listener;
        let tls_min = server.tls_minimum_version;
        let http = Arc::clone(&server.http);
        let rest_service = hyper::service::make_service_fn(|_| {
            let http_server = Arc::clone(&http);
            let service = service_fn(move |req: hyper::Request<hyper::Body>| {
                route_request(
                    Arc::clone(&http_server),
                    req,
                    without_auth,
                    paths_without_authz,
                )
            });
            let service = http_trace_layer.layer(service);
            futures::future::ready(Ok::<_, Infallible>(service))
        });

        let hybrid_make_service = hybrid(rest_service, grpc_service);
        let (addr, certs, key) = setup_tls(listener, key_file, cert_file)?;

        let timer_end = Instant::now();
        let startup_time = timer_end.duration_since(startup_timer);
        info!(
            address = %addr.local_addr(),
            "startup time: {}ms",
            startup_time.as_millis()
        );

        write_address_to_file(tcp_listener_file_path, &addr).await?;

        let acceptor = hyper_rustls::TlsAcceptor::builder()
            .with_tls_config(
                ServerConfig::builder_with_protocol_versions(tls_min)
                    .with_no_client_auth()
                    .with_single_cert(certs, key)
                    .unwrap(),
            )
            .with_all_versions_alpn()
            .with_incoming(addr);

        hyper::server::Server::builder(acceptor)
            .serve(hybrid_make_service)
            .with_graceful_shutdown(shutdown.cancelled())
            .await?;
    } else {
        let rest_service = hyper::service::make_service_fn(|_| {
            let http_server = Arc::clone(&server.http);
            let service = service_fn(move |req: hyper::Request<hyper::Body>| {
                route_request(
                    Arc::clone(&http_server),
                    req,
                    without_auth,
                    paths_without_authz,
                )
            });
            let service = http_trace_layer.layer(service);
            futures::future::ready(Ok::<_, Infallible>(service))
        });

        let hybrid_make_service = hybrid(rest_service, grpc_service);
        let addr = AddrIncoming::from_listener(server.listener)?;

        let timer_end = Instant::now();
        let startup_time = timer_end.duration_since(startup_timer);
        info!(
            address = %addr.local_addr(),
            "startup time: {}ms",
            startup_time.as_millis()
        );

        hyper::server::Builder::new(addr, Http::new())
            .tcp_nodelay(true)
            .serve(hybrid_make_service)
            .with_graceful_shutdown(shutdown.cancelled())
            .await?;
    }

    Ok(())
}

// This function is only called when running tests to get hold of the server port details as the
// tests start on arbitrary port by passing in 0 as port. This is also called when setting up TLS
// as the tests seem to use TLS by default.
async fn write_address_to_file(
    tcp_listener_file_path: Option<PathBuf>,
    addr: &AddrIncoming,
) -> Result<(), Error> {
    if let Some(path) = tcp_listener_file_path {
        let mut f = tokio::fs::File::create_new(path).await?;
        let _ = f.write(addr.local_addr().to_string().as_bytes()).await?;
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
        AddrIncoming,
        Vec<rustls::pki_types::CertificateDer<'static>>,
        rustls::pki_types::PrivateKeyDer<'static>,
    ),
    Error,
> {
    let mut addr = AddrIncoming::from_listener(tcp_listener)?;
    addr.set_nodelay(true);
    let certs = {
        let cert_file = File::open(cert_file).unwrap();
        let mut buf_reader = BufReader::new(cert_file);
        rustls_pemfile::certs(&mut buf_reader)
            .collect::<Result<Vec<_>, _>>()
            .unwrap()
    };
    let key = {
        let key_file = File::open(key_file).unwrap();
        let mut buf_reader = BufReader::new(key_file);
        rustls_pemfile::private_key(&mut buf_reader)
            .unwrap()
            .unwrap()
    };
    Ok((addr, certs, key))
}

#[cfg(test)]
mod tests {
    use crate::query_executor::{CreateQueryExecutorArgs, QueryExecutorImpl};
    use crate::{CreateServerArgs, serve};
    use crate::{Server, http::HttpApi};
    use chrono::DateTime;
    use datafusion::parquet::data_type::AsBytes;
    use hyper::{Client, StatusCode};
    use influxdb3_authz::NoAuthAuthenticator;
    use influxdb3_cache::distinct_cache::DistinctCacheProvider;
    use influxdb3_cache::last_cache::LastCacheProvider;
    use influxdb3_cache::parquet_cache::test_cached_obj_store_and_oracle;
    use influxdb3_catalog::catalog::Catalog;
    use influxdb3_processing_engine::ProcessingEngineManagerImpl;
    use influxdb3_processing_engine::environment::DisabledManager;
    use influxdb3_processing_engine::plugins::ProcessingEngineEnvironmentManager;
    use influxdb3_shutdown::ShutdownManager;
    use influxdb3_sys_events::SysEventStore;
    use influxdb3_telemetry::store::TelemetryStore;
    use influxdb3_wal::WalConfig;
    use influxdb3_write::persister::Persister;
    use influxdb3_write::write_buffer::N_SNAPSHOTS_TO_LOAD_ON_START;
    use influxdb3_write::write_buffer::persisted_files::PersistedFiles;
    use influxdb3_write::{Bufferer, WriteBuffer};
    use iox_http_util::{
        RequestBuilder, Response, bytes_to_request_body, empty_request_body,
        read_body_bytes_for_tests,
    };
    use iox_query::exec::{DedicatedExecutor, Executor, ExecutorConfig, PerQueryMemoryPoolConfig};
    use iox_time::{MockProvider, Time};
    use object_store::DynObjectStore;
    use parquet_file::storage::{ParquetStorage, StorageId};
    use pretty_assertions::assert_eq;
    use std::net::{IpAddr, Ipv4Addr, SocketAddr};
    use std::num::NonZeroUsize;
    use std::sync::{Arc, OnceLock};
    use tokio::net::TcpListener;
    use tokio_util::sync::CancellationToken;

    static EMPTY_PATHS: OnceLock<Vec<&'static str>> = OnceLock::new();

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn write_and_query() {
        let start_time = 0;
        let (server, shutdown, _) = setup_server(start_time).await;

        write_lp(
            &server,
            "foo",
            "cpu,host=a val=1i 123",
            None,
            false,
            "nanosecond",
        )
        .await;

        // Test that we can query the output with a pretty output
        let res = query(
            &server,
            "foo",
            "select host, time, val from cpu",
            "pretty",
            None,
        )
        .await;
        let body = read_body_bytes_for_tests(res.into_body()).await;
        let body = String::from_utf8(body.as_bytes().to_vec()).unwrap();
        let expected = vec![
            "+------+-------------------------------+-----+",
            "| host | time                          | val |",
            "+------+-------------------------------+-----+",
            "| a    | 1970-01-01T00:00:00.000000123 | 1   |",
            "+------+-------------------------------+-----+",
        ];
        let actual: Vec<_> = body.split('\n').collect();
        assert_eq!(
            expected, actual,
            "\n\nexpected:\n\n{:#?}\nactual:\n\n{:#?}\n\n",
            expected, actual
        );
        // Test that we can query the output with a json output
        let res = query(
            &server,
            "foo",
            "select host, time, val from cpu",
            "json",
            None,
        )
        .await;
        let body = read_body_bytes_for_tests(res.into_body()).await;
        let actual = std::str::from_utf8(body.as_bytes()).unwrap();
        let expected = r#"[{"host":"a","time":"1970-01-01T00:00:00.000000123","val":1}]"#;
        assert_eq!(actual, expected);
        // Test that we can query the output with a csv output
        let res = query(
            &server,
            "foo",
            "select host, time, val from cpu",
            "csv",
            None,
        )
        .await;
        let body = read_body_bytes_for_tests(res.into_body()).await;
        let actual = std::str::from_utf8(body.as_bytes()).unwrap();
        let expected = "host,time,val\na,1970-01-01T00:00:00.000000123,1\n";
        assert_eq!(actual, expected);

        // Test that we can query the output with a parquet
        use arrow::buffer::Buffer;
        use parquet::arrow::arrow_reader;
        let res = query(&server, "foo", "select * from cpu", "parquet", None).await;
        let body = read_body_bytes_for_tests(res.into_body()).await;
        let batches = arrow_reader::ParquetRecordBatchReaderBuilder::try_new(body)
            .unwrap()
            .build()
            .unwrap();
        let batches = batches.into_iter().collect::<Result<Vec<_>, _>>().unwrap();
        assert_eq!(batches.len(), 1);

        // Check that we only have the columns we expect
        assert_eq!(batches[0].num_columns(), 3);
        assert!(batches[0].schema().column_with_name("host").is_some());
        assert!(batches[0].schema().column_with_name("time").is_some());
        assert!(batches[0].schema().column_with_name("val").is_some());
        assert!(
            batches[0]
                .schema()
                .column_with_name("random_name")
                .is_none()
        );

        assert_eq!(
            batches[0]["host"].to_data().child_data()[0].buffers()[1],
            Buffer::from([b'a'])
        );

        assert_eq!(
            batches[0]["time"].to_data().buffers(),
            &[Buffer::from([123, 0, 0, 0, 0, 0, 0, 0])]
        );
        assert_eq!(
            batches[0]["val"].to_data().buffers(),
            &[Buffer::from(1_u64.to_le_bytes())]
        );

        shutdown.cancel();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn write_lp_tests() {
        let start_time = 0;
        let (server, shutdown, _) = setup_server(start_time).await;

        let resp = write_lp(
            &server,
            "foo",
            "cpu,host=a val= 123\ncpu,host=b val=5 124\ncpu,host=b val= 124",
            None,
            false,
            "nanosecond",
        )
        .await;

        let status = resp.status();
        let body =
            String::from_utf8(read_body_bytes_for_tests(resp.into_body()).await.to_vec()).unwrap();

        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(
            body,
            "{\
                \"error\":\"parsing failed for write_lp endpoint\",\
                \"data\":{\
                    \"original_line\":\"cpu,host=a val= 123\",\
                    \"line_number\":1,\
                    \"error_message\":\"No fields were provided\"\
                }\
            }"
        );

        let resp = write_lp(
            &server,
            "foo",
            "cpu,host=b val=2 155\ncpu,host=a val= 123\ncpu,host=b val=5 199",
            None,
            true,
            "nanosecond",
        )
        .await;

        let status = resp.status();
        let body =
            String::from_utf8(read_body_bytes_for_tests(resp.into_body()).await.to_vec()).unwrap();

        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(
            body,
            "{\
                \"error\":\"partial write of line protocol occurred\",\
                \"data\":[{\
                    \"original_line\":\"cpu,host=a val= 123\",\
                    \"line_number\":2,\
                    \"error_message\":\"No fields were provided\"\
                }]\
            }"
        );

        // Check that the first write did not partially write any data. We
        // should only see 2 values from the above write.
        let res = query(
            &server,
            "foo",
            "select host, time, val from cpu",
            "csv",
            None,
        )
        .await;
        let body = read_body_bytes_for_tests(res.into_body()).await;
        let actual = std::str::from_utf8(body.as_bytes()).unwrap();
        let expected = "host,time,val\n\
                        b,1970-01-01T00:00:00.000000155,2.0\n\
                        b,1970-01-01T00:00:00.000000199,5.0\n";
        assert_eq!(actual, expected);

        // Check that invalid database names are rejected
        let resp = write_lp(
            &server,
            "this/_is_fine",
            "cpu,host=b val=2 155\n",
            None,
            true,
            "nanosecond",
        )
        .await;

        let status = resp.status();
        let body =
            String::from_utf8(read_body_bytes_for_tests(resp.into_body()).await.to_vec()).unwrap();

        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(
            body,
            "{\
                \"error\":\"invalid character in database name: must be ASCII, containing only letters, numbers, underscores, or hyphens\",\
                \"data\":null\
            }"
        );

        let resp = write_lp(
            &server,
            "?this_is_fine",
            "cpu,host=b val=2 155\n",
            None,
            true,
            "nanosecond",
        )
        .await;

        let status = resp.status();
        let body =
            String::from_utf8(read_body_bytes_for_tests(resp.into_body()).await.to_vec()).unwrap();

        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(
            body,
            "{\
                \"error\":\"db name did not start with a number or letter\",\
                \"data\":null\
            }"
        );

        let resp = write_lp(
            &server,
            "",
            "cpu,host=b val=2 155\n",
            None,
            true,
            "nanosecond",
        )
        .await;

        let status = resp.status();
        let body =
            String::from_utf8(read_body_bytes_for_tests(resp.into_body()).await.to_vec()).unwrap();

        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(
            body,
            "{\
                \"error\":\"db name cannot be empty\",\
                \"data\":null\
            }"
        );

        shutdown.cancel();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn write_lp_precision_tests() {
        let start_time = 1708473607000000000;
        let (server, shutdown, _) = setup_server(start_time).await;

        let resp = write_lp(
            &server,
            "foo",
            "cpu,host=b val=5 1708473600",
            None,
            false,
            "auto",
        )
        .await;
        assert_eq!(resp.status(), StatusCode::NO_CONTENT);
        let resp = write_lp(
            &server,
            "foo",
            "cpu,host=b val=5 1708473601000",
            None,
            false,
            "auto",
        )
        .await;
        assert_eq!(resp.status(), StatusCode::NO_CONTENT);
        let resp = write_lp(
            &server,
            "foo",
            "cpu,host=b val=5 1708473602000000",
            None,
            false,
            "auto",
        )
        .await;
        assert_eq!(resp.status(), StatusCode::NO_CONTENT);
        let resp = write_lp(
            &server,
            "foo",
            "cpu,host=b val=5 1708473603000000000",
            None,
            false,
            "auto",
        )
        .await;
        assert_eq!(resp.status(), StatusCode::NO_CONTENT);
        let resp = write_lp(
            &server,
            "foo",
            "cpu,host=b val=6 1708473604",
            None,
            false,
            "second",
        )
        .await;
        assert_eq!(resp.status(), StatusCode::NO_CONTENT);
        let resp = write_lp(
            &server,
            "foo",
            "cpu,host=b val=6 1708473605000",
            None,
            false,
            "millisecond",
        )
        .await;
        assert_eq!(resp.status(), StatusCode::NO_CONTENT);
        let resp = write_lp(
            &server,
            "foo",
            "cpu,host=b val=6 1708473606000000",
            None,
            false,
            "microsecond",
        )
        .await;
        assert_eq!(resp.status(), StatusCode::NO_CONTENT);
        let resp = write_lp(
            &server,
            "foo",
            "cpu,host=b val=6 1708473607000000000",
            None,
            false,
            "nanosecond",
        )
        .await;
        assert_eq!(resp.status(), StatusCode::NO_CONTENT);

        let res = query(
            &server,
            "foo",
            "select host, time, val from cpu",
            "csv",
            None,
        )
        .await;
        let body = read_body_bytes_for_tests(res.into_body()).await;
        // Since a query can come back with data in any order we need to sort it
        // here before we do any assertions
        let mut unsorted = String::from_utf8(body.as_bytes().to_vec())
            .unwrap()
            .lines()
            .skip(1)
            .map(|s| s.to_string())
            .collect::<Vec<String>>();
        unsorted.sort();
        let actual = unsorted.join("\n");
        let expected = "b,2024-02-21T00:00:00,5.0\n\
                        b,2024-02-21T00:00:01,5.0\n\
                        b,2024-02-21T00:00:02,5.0\n\
                        b,2024-02-21T00:00:03,5.0\n\
                        b,2024-02-21T00:00:04,6.0\n\
                        b,2024-02-21T00:00:05,6.0\n\
                        b,2024-02-21T00:00:06,6.0\n\
                        b,2024-02-21T00:00:07,6.0";
        assert_eq!(actual, expected);

        shutdown.cancel();
    }

    #[tokio::test]
    async fn delete_table_defaults_to_hard_delete_default() {
        let start_time = 0;
        let (server, shutdown, write_buffer) = setup_server(start_time).await;

        let db_name = "test_db";
        let table_name = "test_table";

        // Write some data to create the table
        write_lp(
            &server,
            db_name,
            &format!("{table_name},host=a val=1i 123"),
            None,
            false,
            "nanosecond",
        )
        .await;

        // Make a DELETE request to delete the table without hard_delete_at parameter
        let client = Client::new();
        let url = format!("{server}/api/v3/configure/table?db={db_name}&table={table_name}");

        let request = RequestBuilder::new()
            .uri(url)
            .method("DELETE")
            .body(empty_request_body())
            .expect("failed to construct HTTP request");

        let response = client.request(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        // Access the catalog to verify the table's hard_delete_time is None (which represents Never)
        let catalog = write_buffer.catalog();
        let db_schema = catalog.db_schema(db_name).expect("database should exist");

        // After soft deletion, the table name is changed, so we need to find it by iterating through all tables
        let deleted_table = db_schema
            .tables()
            .find(|table| table.deleted && table.table_name.starts_with(table_name))
            .expect("deleted table should exist");

        // Verify the table is marked as deleted and hard_delete_time is set to default duration
        assert!(deleted_table.deleted, "table should be marked as deleted");
        assert_eq!(
            deleted_table.hard_delete_time.unwrap().timestamp_nanos(),
            start_time + Catalog::DEFAULT_HARD_DELETE_DURATION.as_nanos() as i64,
            "hard_delete_time should be set to default duration when hard_delete_at is omitted"
        );

        shutdown.cancel();
    }

    #[tokio::test]
    async fn delete_table_with_explicit_hard_delete_never() {
        let start_time = 0;
        let (server, shutdown, write_buffer) = setup_server(start_time).await;

        let db_name = "test_db";
        let table_name = "test_table";

        // Write some data to create the table
        write_lp(
            &server,
            db_name,
            &format!("{table_name},host=a val=1i 123"),
            None,
            false,
            "nanosecond",
        )
        .await;

        // Make a DELETE request to delete the table with explicit hard_delete_at=never parameter
        let client = Client::new();
        let url = format!(
            "{server}/api/v3/configure/table?db={db_name}&table={table_name}&hard_delete_at=never"
        );

        let request = RequestBuilder::new()
            .uri(url)
            .method("DELETE")
            .body(empty_request_body())
            .expect("failed to construct HTTP request");

        let response = client.request(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        // Access the catalog to verify the table's hard_delete_time is None (which represents Never)
        let catalog = write_buffer.catalog();
        let db_schema = catalog.db_schema(db_name).expect("database should exist");

        // After soft deletion, the table name is changed, so we need to find it by iterating through all tables
        let deleted_table = db_schema
            .tables()
            .find(|table| table.deleted && table.table_name.starts_with(table_name))
            .expect("deleted table should exist");

        // Verify the table is marked as deleted and hard_delete_time is None (Never)
        assert!(deleted_table.deleted, "table should be marked as deleted");
        assert!(
            deleted_table.hard_delete_time.is_none(),
            "hard_delete_time should be None (Never) when hard_delete_at=never is explicitly provided"
        );

        shutdown.cancel();
    }

    #[tokio::test]
    async fn delete_table_with_explicit_hard_delete_now() {
        let start_time = 1000;
        let (server, shutdown, write_buffer) = setup_server(start_time).await;

        let db_name = "test_db";
        let table_name = "test_table";

        // Write some data to create the table
        write_lp(
            &server,
            db_name,
            &format!("{table_name},host=a val=1i 123"),
            None,
            false,
            "nanosecond",
        )
        .await;

        // Make a DELETE request to delete the table with explicit hard_delete_at=now parameter
        let client = Client::new();
        let url = format!(
            "{server}/api/v3/configure/table?db={db_name}&table={table_name}&hard_delete_at=now"
        );

        let request = RequestBuilder::new()
            .uri(url)
            .method("DELETE")
            .body(empty_request_body())
            .expect("failed to construct HTTP request");

        let response = client.request(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        // Access the catalog to verify the table's hard_delete_time is set to a time value (not None)
        let catalog = write_buffer.catalog();
        let db_schema = catalog.db_schema(db_name).expect("database should exist");

        // After soft deletion, the table name is changed, so we need to find it by iterating through all tables
        let deleted_table = db_schema
            .tables()
            .find(|table| table.deleted && table.table_name.starts_with(table_name))
            .expect("deleted table should exist");

        // Verify the table is marked as deleted and hard_delete_time is Some (indicating it will be hard deleted)
        assert!(deleted_table.deleted, "table should be marked as deleted");
        assert_eq!(
            deleted_table.hard_delete_time.unwrap().timestamp_nanos(),
            start_time,
        );

        shutdown.cancel();
    }

    #[tokio::test]
    async fn delete_table_with_explicit_hard_delete_timestamp() {
        let start_time = 0;
        let (server, shutdown, write_buffer) = setup_server(start_time).await;

        let db_name = "test_db";
        let table_name = "test_table";

        // Write some data to create the table
        write_lp(
            &server,
            db_name,
            &format!("{table_name},host=a val=1i 123"),
            None,
            false,
            "nanosecond",
        )
        .await;

        // Use a future timestamp (year 2025)
        let future_timestamp = "2025-12-31T23:59:59Z";
        let expected_time = Time::from_datetime(
            DateTime::parse_from_rfc3339(future_timestamp)
                .unwrap()
                .to_utc(),
        );

        // Make a DELETE request to delete the table with explicit hard_delete_at timestamp
        let client = Client::new();
        let url = format!(
            "{server}/api/v3/configure/table?db={db_name}&table={table_name}&hard_delete_at={future_timestamp}"
        );

        let request = RequestBuilder::new()
            .uri(url)
            .method("DELETE")
            .body(empty_request_body())
            .expect("failed to construct HTTP request");

        let response = client.request(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        // Access the catalog to verify the table's hard_delete_time matches the expected timestamp
        let catalog = write_buffer.catalog();
        let db_schema = catalog.db_schema(db_name).expect("database should exist");

        // After soft deletion, the table name is changed, so we need to find it by iterating through all tables
        let deleted_table = db_schema
            .tables()
            .find(|table| table.deleted && table.table_name.starts_with(table_name))
            .expect("deleted table should exist");

        // Verify the table is marked as deleted and hard_delete_time matches the expected timestamp
        assert!(deleted_table.deleted, "table should be marked as deleted");
        assert_eq!(
            deleted_table.hard_delete_time.unwrap().timestamp_nanos(),
            expected_time.timestamp_nanos(),
            "hard_delete_time should match the explicitly provided timestamp"
        );

        shutdown.cancel();
    }

    #[tokio::test]
    async fn delete_table_with_explicit_hard_delete_default() {
        let start_time = 0;
        let (server, shutdown, write_buffer) = setup_server(start_time).await;

        let db_name = "test_db";
        let table_name = "test_table";

        // Write some data to create the table
        write_lp(
            &server,
            db_name,
            &format!("{table_name},host=a val=1i 123"),
            None,
            false,
            "nanosecond",
        )
        .await;

        // Make a DELETE request to delete the table with explicit hard_delete_at=default parameter
        let client = Client::new();
        let url = format!(
            "{server}/api/v3/configure/table?db={db_name}&table={table_name}&hard_delete_at=default"
        );

        let request = RequestBuilder::new()
            .uri(url)
            .method("DELETE")
            .body(empty_request_body())
            .expect("failed to construct HTTP request");

        let response = client.request(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        // Access the catalog to verify the table's hard_delete_time is set to Some (indicating it will be hard deleted after default duration)
        let catalog = write_buffer.catalog();
        let db_schema = catalog.db_schema(db_name).expect("database should exist");

        // After soft deletion, the table name is changed, so we need to find it by iterating through all tables
        let deleted_table = db_schema
            .tables()
            .find(|table| table.deleted && table.table_name.starts_with(table_name))
            .expect("deleted table should exist");

        // Verify the table is marked as deleted and hard_delete_time is Some (indicating it will be hard deleted after default duration)
        assert!(deleted_table.deleted, "table should be marked as deleted");
        assert_eq!(
            deleted_table.hard_delete_time.unwrap().timestamp_nanos(),
            start_time + Catalog::DEFAULT_HARD_DELETE_DURATION.as_nanos() as i64,
        );

        shutdown.cancel();
    }

    #[tokio::test]
    async fn delete_database_with_explicit_hard_delete_never() {
        let start_time = 0;
        let (server, shutdown, write_buffer) = setup_server(start_time).await;

        let db_name = "test_db";

        // Write some data to create the database
        write_lp(
            &server,
            db_name,
            "cpu,host=a val=1i 123",
            None,
            false,
            "nanosecond",
        )
        .await;

        // Make a DELETE request to delete the database with explicit hard_delete_at=never parameter
        let client = Client::new();
        let url = format!("{server}/api/v3/configure/database?db={db_name}&hard_delete_at=never");

        let request = RequestBuilder::new()
            .uri(url)
            .method("DELETE")
            .body(empty_request_body())
            .expect("failed to construct HTTP request");

        let response = client.request(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        // After soft deletion, the database name is changed, so we need to find it by iterating through all databases
        let all_databases = write_buffer.catalog().list_db_schema();
        let deleted_db = all_databases
            .iter()
            .find(|db| db.deleted && db.name.starts_with(db_name))
            .expect("deleted database should exist");

        // Verify the database is marked as deleted and hard_delete_time is None (indicating it will never be hard deleted)
        assert!(deleted_db.deleted, "database should be marked as deleted");
        assert!(
            deleted_db.hard_delete_time.is_none(),
            "hard_delete_time should be None for never hard delete"
        );

        shutdown.cancel();
    }

    #[tokio::test]
    async fn delete_database_defaults_to_hard_delete_default() {
        let start_time = 0;
        let (server, shutdown, write_buffer) = setup_server(start_time).await;

        let db_name = "test_db";

        // Write some data to create the database
        write_lp(
            &server,
            db_name,
            "cpu,host=a val=1i 123",
            None,
            false,
            "nanosecond",
        )
        .await;

        // Make a DELETE request to delete the database without hard_delete_at parameter
        let client = Client::new();
        let url = format!("{server}/api/v3/configure/database?db={db_name}");

        let request = RequestBuilder::new()
            .uri(url)
            .method("DELETE")
            .body(empty_request_body())
            .expect("failed to construct HTTP request");

        let response = client.request(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        // After soft deletion, the database name is changed, so we need to find it by iterating through all databases
        let all_databases = write_buffer.catalog().list_db_schema();
        let deleted_db = all_databases
            .iter()
            .find(|db| db.deleted && db.name.starts_with(db_name))
            .expect("deleted database should exist");

        // Verify the database is marked as deleted and hard_delete_time is set to default duration
        assert!(deleted_db.deleted, "database should be marked as deleted");
        assert_eq!(
            deleted_db.hard_delete_time.unwrap().timestamp_nanos(),
            start_time + Catalog::DEFAULT_HARD_DELETE_DURATION.as_nanos() as i64,
            "hard_delete_time should be set to default duration when hard_delete_at is omitted"
        );

        shutdown.cancel();
    }

    #[tokio::test]
    async fn delete_database_with_explicit_hard_delete_now() {
        let start_time = 1000;
        let (server, shutdown, write_buffer) = setup_server(start_time).await;

        let db_name = "test_db";

        // Write some data to create the database
        write_lp(
            &server,
            db_name,
            "cpu,host=a val=1i 123",
            None,
            false,
            "nanosecond",
        )
        .await;

        // Make a DELETE request to delete the database with explicit hard_delete_at=now parameter
        let client = Client::new();
        let url = format!("{server}/api/v3/configure/database?db={db_name}&hard_delete_at=now");

        let request = RequestBuilder::new()
            .uri(url)
            .method("DELETE")
            .body(empty_request_body())
            .expect("failed to construct HTTP request");

        let response = client.request(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        // After soft deletion, the database name is changed, so we need to find it by iterating through all databases
        let all_databases = write_buffer.catalog().list_db_schema();
        let deleted_db = all_databases
            .iter()
            .find(|db| db.deleted && db.name.starts_with(db_name))
            .expect("deleted database should exist");

        // Verify the database is marked as deleted and hard_delete_time is Some (indicating it will be hard deleted)
        assert!(deleted_db.deleted, "database should be marked as deleted");
        assert_eq!(
            deleted_db.hard_delete_time.unwrap().timestamp_nanos(),
            start_time,
        );

        shutdown.cancel();
    }

    #[tokio::test]
    async fn delete_database_with_explicit_hard_delete_default() {
        let start_time = 0;
        let (server, shutdown, write_buffer) = setup_server(start_time).await;

        let db_name = "test_db";

        // Write some data to create the database
        write_lp(
            &server,
            db_name,
            "cpu,host=a val=1i 123",
            None,
            false,
            "nanosecond",
        )
        .await;

        // Make a DELETE request to delete the database with explicit hard_delete_at=default parameter
        let client = Client::new();
        let url = format!("{server}/api/v3/configure/database?db={db_name}&hard_delete_at=default");

        let request = RequestBuilder::new()
            .uri(url)
            .method("DELETE")
            .body(empty_request_body())
            .expect("failed to construct HTTP request");

        let response = client.request(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        // After soft deletion, the database name is changed, so we need to find it by iterating through all databases
        let all_databases = write_buffer.catalog().list_db_schema();
        let deleted_db = all_databases
            .iter()
            .find(|db| db.deleted && db.name.starts_with(db_name))
            .expect("deleted database should exist");

        // Verify the database is marked as deleted and hard_delete_time is Some (indicating it will be hard deleted after default duration)
        assert!(deleted_db.deleted, "database should be marked as deleted");
        assert_eq!(
            deleted_db.hard_delete_time.unwrap().timestamp_nanos(),
            start_time + Catalog::DEFAULT_HARD_DELETE_DURATION.as_nanos() as i64,
        );

        shutdown.cancel();
    }

    #[tokio::test]
    async fn delete_database_with_explicit_hard_delete_timestamp() {
        let start_time = 0;
        let (server, shutdown, write_buffer) = setup_server(start_time).await;

        let db_name = "test_db";

        // Write some data to create the database
        write_lp(
            &server,
            db_name,
            "cpu,host=a val=1i 123",
            None,
            false,
            "nanosecond",
        )
        .await;

        // Use a future timestamp (year 2025)
        let future_timestamp = "2025-12-31T23:59:59Z";
        let expected_time = Time::from_datetime(
            DateTime::parse_from_rfc3339(future_timestamp)
                .unwrap()
                .to_utc(),
        );

        // Make a DELETE request to delete the database with explicit hard_delete_at timestamp
        let client = Client::new();
        let url = format!(
            "{server}/api/v3/configure/database?db={db_name}&hard_delete_at={future_timestamp}"
        );

        let request = RequestBuilder::new()
            .uri(url)
            .method("DELETE")
            .body(empty_request_body())
            .expect("failed to construct HTTP request");

        let response = client.request(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        // After soft deletion, the database name is changed, so we need to find it by iterating through all databases
        let all_databases = write_buffer.catalog().list_db_schema();
        let deleted_db = all_databases
            .iter()
            .find(|db| db.deleted && db.name.starts_with(db_name))
            .expect("deleted database should exist");

        // Verify the database is marked as deleted and hard_delete_time matches the expected timestamp
        assert!(deleted_db.deleted, "database should be marked as deleted");
        assert_eq!(
            deleted_db.hard_delete_time.unwrap().timestamp_nanos(),
            expected_time.timestamp_nanos(),
            "hard_delete_time should match the explicitly provided timestamp"
        );

        shutdown.cancel();
    }

    #[tokio::test]
    async fn query_from_last_cache() {
        let start_time = 0;
        let (url, shutdown, wbuf) = setup_server(start_time).await;
        let db_name = "foo";
        let tbl_name = "cpu";

        // Write to generate a db/table in the catalog:
        let resp = write_lp(
            &url,
            db_name,
            format!("{tbl_name},region=us,host=a usage=50 500"),
            None,
            false,
            "second",
        )
        .await;
        assert_eq!(resp.status(), StatusCode::NO_CONTENT);

        // Create the last cache:
        wbuf.catalog()
            .create_last_cache(
                db_name,
                tbl_name,
                None,
                None as Option<&[&str]>,
                None as Option<&[&str]>,
                Default::default(),
                Default::default(),
            )
            .await
            .unwrap();

        // Write to put something in the last cache:
        let resp = write_lp(
            &url,
            db_name,
            format!(
                "\
                {tbl_name},region=us,host=a usage=11 1000\n\
                {tbl_name},region=us,host=b usage=22 1000\n\
                {tbl_name},region=us,host=c usage=33 1000\n\
                {tbl_name},region=ca,host=d usage=44 1000\n\
                {tbl_name},region=ca,host=e usage=55 1000\n\
                {tbl_name},region=eu,host=f usage=66 1000\n\
                "
            ),
            None,
            false,
            "second",
        )
        .await;
        assert_eq!(resp.status(), StatusCode::NO_CONTENT);

        struct TestCase {
            query: &'static str,
            expected: &'static str,
        }

        let test_cases = [
            TestCase {
                query: "SELECT * FROM last_cache('cpu') ORDER BY host",
                expected: "\
                    +--------+------+---------------------+-------+\n\
                    | region | host | time                | usage |\n\
                    +--------+------+---------------------+-------+\n\
                    | us     | a    | 1970-01-01T00:16:40 | 11.0  |\n\
                    | us     | b    | 1970-01-01T00:16:40 | 22.0  |\n\
                    | us     | c    | 1970-01-01T00:16:40 | 33.0  |\n\
                    | ca     | d    | 1970-01-01T00:16:40 | 44.0  |\n\
                    | ca     | e    | 1970-01-01T00:16:40 | 55.0  |\n\
                    | eu     | f    | 1970-01-01T00:16:40 | 66.0  |\n\
                    +--------+------+---------------------+-------+",
            },
            TestCase {
                query: "SELECT * FROM last_cache('cpu') WHERE region = 'us' ORDER BY host",
                expected: "\
                    +--------+------+---------------------+-------+\n\
                    | region | host | time                | usage |\n\
                    +--------+------+---------------------+-------+\n\
                    | us     | a    | 1970-01-01T00:16:40 | 11.0  |\n\
                    | us     | b    | 1970-01-01T00:16:40 | 22.0  |\n\
                    | us     | c    | 1970-01-01T00:16:40 | 33.0  |\n\
                    +--------+------+---------------------+-------+",
            },
            TestCase {
                query: "SELECT * FROM last_cache('cpu') WHERE region != 'us' ORDER BY host",
                expected: "\
                    +--------+------+---------------------+-------+\n\
                    | region | host | time                | usage |\n\
                    +--------+------+---------------------+-------+\n\
                    | ca     | d    | 1970-01-01T00:16:40 | 44.0  |\n\
                    | ca     | e    | 1970-01-01T00:16:40 | 55.0  |\n\
                    | eu     | f    | 1970-01-01T00:16:40 | 66.0  |\n\
                    +--------+------+---------------------+-------+",
            },
            TestCase {
                query: "SELECT * FROM last_cache('cpu') WHERE host IN ('a', 'b') ORDER BY host",
                expected: "\
                    +--------+------+---------------------+-------+\n\
                    | region | host | time                | usage |\n\
                    +--------+------+---------------------+-------+\n\
                    | us     | a    | 1970-01-01T00:16:40 | 11.0  |\n\
                    | us     | b    | 1970-01-01T00:16:40 | 22.0  |\n\
                    +--------+------+---------------------+-------+",
            },
            TestCase {
                query: "SELECT * FROM last_cache('cpu') WHERE host NOT IN ('a', 'b') ORDER BY host",
                expected: "\
                    +--------+------+---------------------+-------+\n\
                    | region | host | time                | usage |\n\
                    +--------+------+---------------------+-------+\n\
                    | us     | c    | 1970-01-01T00:16:40 | 33.0  |\n\
                    | ca     | d    | 1970-01-01T00:16:40 | 44.0  |\n\
                    | ca     | e    | 1970-01-01T00:16:40 | 55.0  |\n\
                    | eu     | f    | 1970-01-01T00:16:40 | 66.0  |\n\
                    +--------+------+---------------------+-------+",
            },
        ];

        for t in test_cases {
            let res = query(&url, db_name, t.query, "pretty", None).await;
            let body = read_body_bytes_for_tests(res.into_body()).await;
            let body = String::from_utf8(body.as_bytes().to_vec()).unwrap();
            assert_eq!(t.expected, body, "query failed: {}", t.query);
        }
        // Query from the last cache:

        shutdown.cancel();
    }

    async fn setup_server(start_time: i64) -> (String, CancellationToken, Arc<dyn WriteBuffer>) {
        let server_start_time = tokio::time::Instant::now();
        let trace_header_parser = trace_http::ctx::TraceHeaderParser::new();
        let metrics = Arc::new(metric::Registry::new());
        let object_store: Arc<DynObjectStore> = Arc::new(object_store::memory::InMemory::new());
        let time_provider = Arc::new(MockProvider::new(Time::from_timestamp_nanos(start_time)));
        let (object_store, parquet_cache) = test_cached_obj_store_and_oracle(
            object_store,
            Arc::clone(&time_provider) as _,
            Default::default(),
        );
        let parquet_store =
            ParquetStorage::new(Arc::clone(&object_store), StorageId::from("influxdb3"));
        let exec = Arc::new(Executor::new_with_config_and_executor(
            ExecutorConfig {
                target_query_partitions: NonZeroUsize::new(1).unwrap(),
                object_stores: [&parquet_store]
                    .into_iter()
                    .map(|store| (store.id(), Arc::clone(store.object_store())))
                    .collect(),
                metric_registry: Arc::clone(&metrics),
                mem_pool_size: usize::MAX,
                per_query_mem_pool_config: PerQueryMemoryPoolConfig::Disabled,
                heap_memory_limit: None,
            },
            DedicatedExecutor::new_testing(),
        ));
        let node_identifier_prefix = "test_host";
        let persister = Arc::new(Persister::new(
            Arc::clone(&object_store),
            node_identifier_prefix,
            Arc::clone(&time_provider) as _,
        ));
        let sample_node_id = Arc::from("sample-host-id");
        let catalog = Arc::new(
            Catalog::new(
                sample_node_id,
                Arc::clone(&object_store),
                Arc::clone(&time_provider) as _,
                Default::default(),
            )
            .await
            .unwrap(),
        );
        let frontend_shutdown = CancellationToken::new();
        let shutdown_manager = ShutdownManager::new(frontend_shutdown.clone());
        let write_buffer_impl = influxdb3_write::write_buffer::WriteBufferImpl::new(
            influxdb3_write::write_buffer::WriteBufferImplArgs {
                persister: Arc::clone(&persister),
                catalog: Arc::clone(&catalog),
                last_cache: LastCacheProvider::new_from_catalog(Arc::clone(&catalog))
                    .await
                    .unwrap(),
                distinct_cache: DistinctCacheProvider::new_from_catalog(
                    Arc::clone(&time_provider) as _,
                    Arc::clone(&catalog),
                )
                .await
                .unwrap(),
                time_provider: Arc::clone(&time_provider) as _,
                executor: Arc::clone(&exec),
                wal_config: WalConfig::test_config(),
                parquet_cache: Some(parquet_cache),
                metric_registry: Arc::clone(&metrics),
                snapshotted_wal_files_to_keep: 100,
                query_file_limit: None,
                n_snapshots_to_load_on_start: N_SNAPSHOTS_TO_LOAD_ON_START,
                shutdown: shutdown_manager.register(),
                wal_replay_concurrency_limit: Some(1),
            },
        )
        .await
        .unwrap();

        let sys_events_store = Arc::new(SysEventStore::new(Arc::clone(&time_provider) as _));
        let parquet_metrics_provider: Arc<PersistedFiles> =
            Arc::clone(&write_buffer_impl.persisted_files());
        let processing_engine_metrics_provider: Arc<Catalog> =
            Arc::clone(&write_buffer_impl.catalog());

        let sample_telem_store = TelemetryStore::new_without_background_runners(
            Some(parquet_metrics_provider),
            processing_engine_metrics_provider,
        );
        let write_buffer: Arc<dyn WriteBuffer> = write_buffer_impl;
        let common_state = crate::CommonServerState::new(
            Arc::clone(&metrics),
            None,
            trace_header_parser,
            Arc::clone(&sample_telem_store),
        );
        let query_executor = Arc::new(QueryExecutorImpl::new(CreateQueryExecutorArgs {
            catalog: write_buffer.catalog(),
            write_buffer: Arc::clone(&write_buffer),
            exec: Arc::clone(&exec),
            metrics: Arc::clone(&metrics),
            datafusion_config: Default::default(),
            query_log_size: 10,
            telemetry_store: Arc::clone(&sample_telem_store),
            sys_events_store: Arc::clone(&sys_events_store),
            started_with_auth: false,
            time_provider: Arc::clone(&time_provider) as _,
        }));

        // bind to port 0 will assign a random available port:
        let socket_addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 0);
        let listener = TcpListener::bind(socket_addr)
            .await
            .expect("bind tcp address");
        let addr = listener.local_addr().unwrap();

        let processing_engine = ProcessingEngineManagerImpl::new(
            ProcessingEngineEnvironmentManager {
                plugin_dir: None,
                virtual_env_location: None,
                package_manager: Arc::new(DisabledManager),
            },
            write_buffer.catalog(),
            node_identifier_prefix,
            Arc::clone(&write_buffer),
            Arc::clone(&query_executor) as _,
            Arc::clone(&time_provider) as _,
            sys_events_store,
        )
        .await;

        // We declare this as a static so that the lifetimes workout here and that
        // it lives long enough.
        static TLS_MIN_VERSION: &[&rustls::SupportedProtocolVersion] =
            &[&rustls::version::TLS12, &rustls::version::TLS13];

        // Start processing engine triggers
        Arc::clone(&processing_engine)
            .start_triggers()
            .await
            .expect("failed to start processing engine triggers");

        write_buffer
            .wal()
            .add_file_notifier(Arc::clone(&processing_engine) as _);

        let authorizer = Arc::new(NoAuthAuthenticator);
        let http = Arc::new(HttpApi::new(
            common_state.clone(),
            Arc::clone(&time_provider) as _,
            Arc::clone(&write_buffer),
            Arc::clone(&query_executor) as _,
            Arc::clone(&processing_engine),
            usize::MAX,
            Arc::clone(&authorizer) as _,
        ));

        let server = Server::new(CreateServerArgs {
            common_state,
            http,
            authorizer: authorizer as _,
            listener,
            cert_file: None,
            key_file: None,
            tls_minimum_version: TLS_MIN_VERSION,
        });
        let shutdown = frontend_shutdown.clone();
        let paths = EMPTY_PATHS.get_or_init(std::vec::Vec::new);
        tokio::spawn(async move {
            serve(
                server,
                frontend_shutdown,
                server_start_time,
                false,
                paths,
                None,
            )
            .await
        });

        (format!("http://{addr}"), shutdown, write_buffer)
    }

    pub(crate) async fn write_lp(
        server: impl Into<String> + Send,
        database: impl Into<String> + Send,
        lp: impl Into<String> + Send,
        authorization: Option<&str>,
        accept_partial: bool,
        precision: impl Into<String> + Send,
    ) -> Response {
        let server = server.into();
        let client = Client::new();
        let url = format!(
            "{}/api/v3/write_lp?db={}&accept_partial={accept_partial}&precision={}",
            server,
            database.into(),
            precision.into(),
        );
        println!("{url}");

        let mut builder = RequestBuilder::new().uri(url).method("POST");
        if let Some(authorization) = authorization {
            builder = builder.header(hyper::header::AUTHORIZATION, authorization);
        };
        let request = builder
            .body(bytes_to_request_body(lp.into()))
            .expect("failed to construct HTTP request");

        client
            .request(request)
            .await
            .expect("http error sending write")
    }

    pub(crate) async fn query(
        server: impl Into<String> + Send,
        database: impl Into<String> + Send,
        query: impl Into<String> + Send,
        format: impl Into<String> + Send,
        authorization: Option<&str>,
    ) -> Response {
        let client = Client::new();
        // query escaped for uri
        let query = urlencoding::encode(&query.into());
        let url = format!(
            "{}/api/v3/query_sql?db={}&q={}&format={}",
            server.into(),
            database.into(),
            query,
            format.into()
        );

        println!("query url: {url}");
        let mut builder = RequestBuilder::new().uri(url).method("GET");
        if let Some(authorization) = authorization {
            builder = builder.header(hyper::header::AUTHORIZATION, authorization);
        };
        let request = builder
            .body(empty_request_body())
            .expect("failed to construct HTTP request");

        client
            .request(request)
            .await
            .expect("http error sending query")
    }
}
