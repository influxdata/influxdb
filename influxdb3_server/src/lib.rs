//! InfluxDB 3.0 Edge server implementation
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

mod grpc;
mod http;
pub mod query_executor;
mod service;

use crate::grpc::make_flight_server;
use crate::http::route_request;
use crate::http::HttpApi;
use async_trait::async_trait;
use datafusion::execution::SendableRecordBatchStream;
use hyper::service::service_fn;
use influxdb3_write::{Persister, WriteBuffer};
use iox_query::QueryNamespaceProvider;
use observability_deps::tracing::{error, info};
use service::hybrid;
use std::convert::Infallible;
use std::fmt::Debug;
use std::net::SocketAddr;
use std::sync::Arc;
use thiserror::Error;
use tokio_util::sync::CancellationToken;
use tower::Layer;
use trace::ctx::SpanContext;
use trace::TraceCollector;
use trace_http::ctx::RequestLogContext;
use trace_http::ctx::TraceHeaderParser;
use trace_http::metrics::MetricFamily;
use trace_http::metrics::RequestMetrics;
use trace_http::tower::TraceLayer;

const TRACE_SERVER_NAME: &str = "influxdb3_http";

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
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

#[derive(Debug, Clone)]
pub struct CommonServerState {
    metrics: Arc<metric::Registry>,
    trace_exporter: Option<Arc<trace_exporters::export::AsyncExporter>>,
    trace_header_parser: TraceHeaderParser,
    http_addr: SocketAddr,
    bearer_token: Option<Vec<u8>>,
}

impl CommonServerState {
    pub fn new(
        metrics: Arc<metric::Registry>,
        trace_exporter: Option<Arc<trace_exporters::export::AsyncExporter>>,
        trace_header_parser: TraceHeaderParser,
        http_addr: SocketAddr,
        bearer_token: Option<String>,
    ) -> Result<Self> {
        Ok(Self {
            metrics,
            trace_exporter,
            trace_header_parser,
            http_addr,
            bearer_token: bearer_token.map(hex::decode).transpose()?,
        })
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

    pub fn bearer_token(&self) -> Option<&[u8]> {
        self.bearer_token.as_deref()
    }
}

#[derive(Debug)]
pub struct Server<W, Q> {
    common_state: CommonServerState,
    http: Arc<HttpApi<W, Q>>,
}

#[async_trait]
pub trait QueryExecutor: QueryNamespaceProvider + Debug + Send + Sync + 'static {
    type Error;

    async fn query(
        &self,
        database: &str,
        q: &str,
        kind: QueryKind,
        span_ctx: Option<SpanContext>,
        external_span_ctx: Option<RequestLogContext>,
    ) -> Result<SendableRecordBatchStream, Self::Error>;
}

#[derive(Debug)]
pub enum QueryKind {
    Sql,
    InfluxQl,
}

impl<W, Q> Server<W, Q>
where
    Q: QueryExecutor,
{
    pub fn new(
        common_state: CommonServerState,
        _persister: Arc<dyn Persister>,
        write_buffer: Arc<W>,
        query_executor: Arc<Q>,
        max_http_request_size: usize,
    ) -> Self {
        let http = Arc::new(HttpApi::new(
            common_state.clone(),
            Arc::clone(&write_buffer),
            Arc::clone(&query_executor),
            max_http_request_size,
        ));

        Self { common_state, http }
    }
}

pub async fn serve<W, Q>(server: Server<W, Q>, shutdown: CancellationToken) -> Result<()>
where
    W: WriteBuffer,
    Q: QueryExecutor,
    http::Error: From<<Q as QueryExecutor>::Error>,
{
    // TODO:
    //  1. load the persisted catalog and segments from the persister
    //  2. load semgments into the buffer
    //  3. persist any segments from the buffer that are closed and haven't yet been persisted
    //  4. start serving

    let req_metrics = RequestMetrics::new(
        Arc::clone(&server.common_state.metrics),
        MetricFamily::HttpServer,
    );
    let trace_layer = TraceLayer::new(
        server.common_state.trace_header_parser.clone(),
        Arc::new(req_metrics),
        server.common_state.trace_collector().clone(),
        TRACE_SERVER_NAME,
    );

    let grpc_service = trace_layer.clone().layer(make_flight_server(
        Arc::clone(&server.http.query_executor),
        // TODO - need to configure authz here:
        None,
    ));
    let rest_service = hyper::service::make_service_fn(|_| {
        let http_server = Arc::clone(&server.http);
        let service = service_fn(move |req: hyper::Request<hyper::Body>| {
            route_request(Arc::clone(&http_server), req)
        });
        let service = trace_layer.layer(service);
        futures::future::ready(Ok::<_, Infallible>(service))
    });

    let hybrid_make_service = hybrid(rest_service, grpc_service);

    hyper::Server::bind(&server.common_state.http_addr)
        .serve(hybrid_make_service)
        .with_graceful_shutdown(shutdown.cancelled())
        .await?;

    Ok(())
}

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

#[cfg(test)]
mod tests {
    use crate::serve;
    use datafusion::parquet::data_type::AsBytes;
    use hyper::{body, Body, Client, Request, Response};
    use influxdb3_write::persister::PersisterImpl;
    use influxdb3_write::SegmentId;
    use iox_query::exec::{Executor, ExecutorConfig};
    use object_store::DynObjectStore;
    use parquet_file::storage::{ParquetStorage, StorageId};
    use std::collections::HashMap;
    use std::net::{SocketAddr, SocketAddrV4};
    use std::num::NonZeroUsize;
    use std::sync::atomic::{AtomicU16, Ordering};
    use std::sync::Arc;
    use tokio_util::sync::CancellationToken;

    static NEXT_PORT: AtomicU16 = AtomicU16::new(8090);

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn write_and_query() {
        let addr = get_free_port();
        let trace_header_parser = trace_http::ctx::TraceHeaderParser::new();
        let metrics = Arc::new(metric::Registry::new());
        let common_state = crate::CommonServerState::new(
            Arc::clone(&metrics),
            None,
            trace_header_parser,
            addr,
            None,
        )
        .unwrap();
        let catalog = Arc::new(influxdb3_write::catalog::Catalog::new());
        let object_store: Arc<DynObjectStore> = Arc::new(object_store::memory::InMemory::new());
        let parquet_store =
            ParquetStorage::new(Arc::clone(&object_store), StorageId::from("influxdb3"));
        let num_threads = NonZeroUsize::new(2).unwrap();
        let exec = Arc::new(Executor::new_with_config(ExecutorConfig {
            num_threads,
            target_query_partitions: NonZeroUsize::new(1).unwrap(),
            object_stores: [&parquet_store]
                .into_iter()
                .map(|store| (store.id(), Arc::clone(store.object_store())))
                .collect(),
            metric_registry: Arc::clone(&metrics),
            mem_pool_size: usize::MAX,
        }));

        let write_buffer = Arc::new(
            influxdb3_write::write_buffer::WriteBufferImpl::new(
                Arc::clone(&catalog),
                None::<Arc<influxdb3_write::wal::WalImpl>>,
                SegmentId::new(0),
            )
            .unwrap(),
        );
        let query_executor = crate::query_executor::QueryExecutorImpl::new(
            catalog,
            Arc::clone(&write_buffer),
            Arc::clone(&exec),
            Arc::clone(&metrics),
            Arc::new(HashMap::new()),
            10,
        );
        let persister = Arc::new(PersisterImpl::new(Arc::clone(&object_store)));

        let server = crate::Server::new(
            common_state,
            persister,
            Arc::clone(&write_buffer),
            Arc::new(query_executor),
            usize::MAX,
        );
        let frontend_shutdown = CancellationToken::new();
        let shutdown = frontend_shutdown.clone();

        tokio::spawn(async move { serve(server, frontend_shutdown).await });

        let server = format!("http://{}", addr);
        write_lp(&server, "foo", "cpu,host=a val=1i 123", None).await;

        // Test that we can query the output with a pretty output
        let res = query(&server, "foo", "select * from cpu", "pretty", None).await;
        let body = body::to_bytes(res.into_body()).await.unwrap();
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
        let res = query(&server, "foo", "select * from cpu", "json", None).await;
        let body = body::to_bytes(res.into_body()).await.unwrap();
        let actual = std::str::from_utf8(body.as_bytes()).unwrap();
        let expected = r#"[{"host":"a","time":"1970-01-01T00:00:00.000000123","val":1}]"#;
        assert_eq!(actual, expected);
        // Test that we can query the output with a csv output
        let res = query(&server, "foo", "select * from cpu", "csv", None).await;
        let body = body::to_bytes(res.into_body()).await.unwrap();
        let actual = std::str::from_utf8(body.as_bytes()).unwrap();
        let expected = "host,time,val\na,1970-01-01T00:00:00.000000123,1\n";
        assert_eq!(actual, expected);

        // Test that we can query the output with a parquet
        use arrow::buffer::Buffer;
        use parquet::arrow::arrow_reader;
        let res = query(&server, "foo", "select * from cpu", "parquet", None).await;
        let body = body::to_bytes(res.into_body()).await.unwrap();
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
        assert!(batches[0]
            .schema()
            .column_with_name("random_name")
            .is_none());

        assert_eq!(
            batches[0]["host"].to_data().child_data()[0].buffers()[1],
            Buffer::from([b'a'].to_vec())
        );

        assert_eq!(
            batches[0]["time"].to_data().buffers(),
            &[Buffer::from(vec![123, 0, 0, 0, 0, 0, 0, 0])]
        );
        assert_eq!(
            batches[0]["val"].to_data().buffers(),
            &[Buffer::from(1_u64.to_le_bytes().to_vec())]
        );

        shutdown.cancel();
    }

    pub(crate) async fn write_lp(
        server: impl Into<String> + Send,
        database: impl Into<String> + Send,
        lp: impl Into<String> + Send,
        authorization: Option<&str>,
    ) -> Response<Body> {
        let server = server.into();
        let client = Client::new();
        let url = format!("{}/api/v3/write_lp?db={}", server, database.into());
        println!("{}", url);

        let mut builder = Request::builder().uri(url).method("POST");
        if let Some(authorization) = authorization {
            builder = builder.header(hyper::header::AUTHORIZATION, authorization);
        };
        let request = builder
            .body(Body::from(lp.into()))
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
    ) -> Response<Body> {
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

        println!("query url: {}", url);
        let mut builder = Request::builder().uri(url).method("GET");
        if let Some(authorization) = authorization {
            builder = builder.header(hyper::header::AUTHORIZATION, authorization);
        };
        let request = builder
            .body(Body::empty())
            .expect("failed to construct HTTP request");

        client
            .request(request)
            .await
            .expect("http error sending query")
    }

    pub(crate) fn get_free_port() -> SocketAddr {
        let ip = std::net::Ipv4Addr::new(127, 0, 0, 1);

        loop {
            let port = NEXT_PORT.fetch_add(1, Ordering::SeqCst);
            let addr = SocketAddrV4::new(ip, port);

            if std::net::TcpListener::bind(addr).is_ok() {
                return addr.into();
            }
        }
    }
}
