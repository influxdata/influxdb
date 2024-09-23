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

pub mod auth;
pub mod builder;
mod grpc;
mod http;
pub mod query_executor;
mod service;
mod system_tables;

use crate::grpc::make_flight_server;
use crate::http::route_request;
use crate::http::HttpApi;
use async_trait::async_trait;
use authz::Authorizer;
use datafusion::execution::SendableRecordBatchStream;
use hyper::server::conn::AddrIncoming;
use hyper::server::conn::Http;
use hyper::service::service_fn;
use influxdb3_write::persister::Persister;
use iox_query::QueryDatabase;
use iox_query_params::StatementParams;
use iox_time::TimeProvider;
use observability_deps::tracing::error;
use service::hybrid;
use std::convert::Infallible;
use std::fmt::Debug;
use std::sync::Arc;
use thiserror::Error;
use tokio::net::TcpListener;
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
}

impl CommonServerState {
    pub fn new(
        metrics: Arc<metric::Registry>,
        trace_exporter: Option<Arc<trace_exporters::export::AsyncExporter>>,
        trace_header_parser: TraceHeaderParser,
    ) -> Result<Self> {
        Ok(Self {
            metrics,
            trace_exporter,
            trace_header_parser,
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
}

#[allow(dead_code)]
#[derive(Debug)]
pub struct Server<Q, T> {
    common_state: CommonServerState,
    http: Arc<HttpApi<Q, T>>,
    persister: Arc<Persister>,
    authorizer: Arc<dyn Authorizer>,
    listener: TcpListener,
}

#[async_trait]
pub trait QueryExecutor: QueryDatabase + Debug + Send + Sync + 'static {
    type Error;

    async fn query(
        &self,
        database: &str,
        q: &str,
        params: Option<StatementParams>,
        kind: QueryKind,
        span_ctx: Option<SpanContext>,
        external_span_ctx: Option<RequestLogContext>,
    ) -> Result<SendableRecordBatchStream, Self::Error>;

    fn show_databases(&self) -> Result<SendableRecordBatchStream, Self::Error>;

    async fn show_retention_policies(
        &self,
        database: Option<&str>,
        span_ctx: Option<SpanContext>,
    ) -> Result<SendableRecordBatchStream, Self::Error>;
}

#[derive(Debug)]
pub enum QueryKind {
    Sql,
    InfluxQl,
}
impl<Q, T> Server<Q, T> {
    pub fn authorizer(&self) -> Arc<dyn Authorizer> {
        Arc::clone(&self.authorizer)
    }
}

pub async fn serve<Q, T>(server: Server<Q, T>, shutdown: CancellationToken) -> Result<()>
where
    Q: QueryExecutor,
    http::Error: From<<Q as QueryExecutor>::Error>,
    T: TimeProvider,
{
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
        Some(server.authorizer()),
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

    let addr = AddrIncoming::from_listener(server.listener)?;
    hyper::server::Builder::new(addr, Http::new())
        .serve(hybrid_make_service)
        .with_graceful_shutdown(shutdown.cancelled())
        .await?;

    Ok(())
}

/// On unix platforms we want to intercept SIGINT and SIGTERM
/// This method returns if either are signalled
#[cfg(unix)]
pub async fn wait_for_signal() {
    use observability_deps::tracing::info;
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
    use crate::auth::DefaultAuthorizer;
    use crate::builder::ServerBuilder;
    use crate::serve;
    use datafusion::parquet::data_type::AsBytes;
    use hyper::{body, Body, Client, Request, Response, StatusCode};
    use influxdb3_catalog::catalog::Catalog;
    use influxdb3_wal::WalConfig;
    use influxdb3_write::last_cache::LastCacheProvider;
    use influxdb3_write::parquet_cache::test_cached_obj_store_and_oracle;
    use influxdb3_write::persister::Persister;
    use influxdb3_write::WriteBuffer;
    use iox_query::exec::{DedicatedExecutor, Executor, ExecutorConfig};
    use iox_time::{MockProvider, Time};
    use object_store::DynObjectStore;
    use parquet_file::storage::{ParquetStorage, StorageId};
    use pretty_assertions::assert_eq;
    use std::collections::HashMap;
    use std::net::{IpAddr, Ipv4Addr, SocketAddr};
    use std::num::NonZeroUsize;
    use std::sync::Arc;
    use tokio::net::TcpListener;
    use tokio_util::sync::CancellationToken;

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
        let res = query(
            &server,
            "foo",
            "select host, time, val from cpu",
            "json",
            None,
        )
        .await;
        let body = body::to_bytes(res.into_body()).await.unwrap();
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
            String::from_utf8(body::to_bytes(resp.into_body()).await.unwrap().to_vec()).unwrap();

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
            String::from_utf8(body::to_bytes(resp.into_body()).await.unwrap().to_vec()).unwrap();

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
        let body = body::to_bytes(res.into_body()).await.unwrap();
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
            String::from_utf8(body::to_bytes(resp.into_body()).await.unwrap().to_vec()).unwrap();

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
            String::from_utf8(body::to_bytes(resp.into_body()).await.unwrap().to_vec()).unwrap();

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
            String::from_utf8(body::to_bytes(resp.into_body()).await.unwrap().to_vec()).unwrap();

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
        assert_eq!(resp.status(), StatusCode::OK);
        let resp = write_lp(
            &server,
            "foo",
            "cpu,host=b val=5 1708473601000",
            None,
            false,
            "auto",
        )
        .await;
        assert_eq!(resp.status(), StatusCode::OK);
        let resp = write_lp(
            &server,
            "foo",
            "cpu,host=b val=5 1708473602000000",
            None,
            false,
            "auto",
        )
        .await;
        assert_eq!(resp.status(), StatusCode::OK);
        let resp = write_lp(
            &server,
            "foo",
            "cpu,host=b val=5 1708473603000000000",
            None,
            false,
            "auto",
        )
        .await;
        assert_eq!(resp.status(), StatusCode::OK);
        let resp = write_lp(
            &server,
            "foo",
            "cpu,host=b val=6 1708473604",
            None,
            false,
            "second",
        )
        .await;
        assert_eq!(resp.status(), StatusCode::OK);
        let resp = write_lp(
            &server,
            "foo",
            "cpu,host=b val=6 1708473605000",
            None,
            false,
            "millisecond",
        )
        .await;
        assert_eq!(resp.status(), StatusCode::OK);
        let resp = write_lp(
            &server,
            "foo",
            "cpu,host=b val=6 1708473606000000",
            None,
            false,
            "microsecond",
        )
        .await;
        assert_eq!(resp.status(), StatusCode::OK);
        let resp = write_lp(
            &server,
            "foo",
            "cpu,host=b val=6 1708473607000000000",
            None,
            false,
            "nanosecond",
        )
        .await;
        assert_eq!(resp.status(), StatusCode::OK);

        let res = query(
            &server,
            "foo",
            "select host, time, val from cpu",
            "csv",
            None,
        )
        .await;
        let body = body::to_bytes(res.into_body()).await.unwrap();
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
        assert_eq!(resp.status(), StatusCode::OK);

        // Create the last cache:
        wbuf.create_last_cache(db_name, tbl_name, None, None, None, None, None)
            .await
            .expect("create last cache");

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
        assert_eq!(resp.status(), StatusCode::OK);

        struct TestCase {
            query: &'static str,
            expected: &'static str,
        }

        let test_cases = [
            TestCase {
                query: "SELECT * FROM last_cache('cpu') ORDER BY host",
                expected: "\
                    +------+--------+---------------------+-------+\n\
                    | host | region | time                | usage |\n\
                    +------+--------+---------------------+-------+\n\
                    | a    | us     | 1970-01-01T00:16:40 | 11.0  |\n\
                    | b    | us     | 1970-01-01T00:16:40 | 22.0  |\n\
                    | c    | us     | 1970-01-01T00:16:40 | 33.0  |\n\
                    | d    | ca     | 1970-01-01T00:16:40 | 44.0  |\n\
                    | e    | ca     | 1970-01-01T00:16:40 | 55.0  |\n\
                    | f    | eu     | 1970-01-01T00:16:40 | 66.0  |\n\
                    +------+--------+---------------------+-------+",
            },
            TestCase {
                query: "SELECT * FROM last_cache('cpu') WHERE region = 'us' ORDER BY host",
                expected: "\
                    +------+--------+---------------------+-------+\n\
                    | host | region | time                | usage |\n\
                    +------+--------+---------------------+-------+\n\
                    | a    | us     | 1970-01-01T00:16:40 | 11.0  |\n\
                    | b    | us     | 1970-01-01T00:16:40 | 22.0  |\n\
                    | c    | us     | 1970-01-01T00:16:40 | 33.0  |\n\
                    +------+--------+---------------------+-------+",
            },
            TestCase {
                query: "SELECT * FROM last_cache('cpu') WHERE region != 'us' ORDER BY host",
                expected: "\
                    +------+--------+---------------------+-------+\n\
                    | host | region | time                | usage |\n\
                    +------+--------+---------------------+-------+\n\
                    | d    | ca     | 1970-01-01T00:16:40 | 44.0  |\n\
                    | e    | ca     | 1970-01-01T00:16:40 | 55.0  |\n\
                    | f    | eu     | 1970-01-01T00:16:40 | 66.0  |\n\
                    +------+--------+---------------------+-------+",
            },
            TestCase {
                query: "SELECT * FROM last_cache('cpu') WHERE host IN ('a', 'b') ORDER BY host",
                expected: "\
                    +------+--------+---------------------+-------+\n\
                    | host | region | time                | usage |\n\
                    +------+--------+---------------------+-------+\n\
                    | a    | us     | 1970-01-01T00:16:40 | 11.0  |\n\
                    | b    | us     | 1970-01-01T00:16:40 | 22.0  |\n\
                    +------+--------+---------------------+-------+",
            },
            TestCase {
                query: "SELECT * FROM last_cache('cpu') WHERE host NOT IN ('a', 'b') ORDER BY host",
                expected: "\
                    +------+--------+---------------------+-------+\n\
                    | host | region | time                | usage |\n\
                    +------+--------+---------------------+-------+\n\
                    | c    | us     | 1970-01-01T00:16:40 | 33.0  |\n\
                    | d    | ca     | 1970-01-01T00:16:40 | 44.0  |\n\
                    | e    | ca     | 1970-01-01T00:16:40 | 55.0  |\n\
                    | f    | eu     | 1970-01-01T00:16:40 | 66.0  |\n\
                    +------+--------+---------------------+-------+",
            },
        ];

        for t in test_cases {
            let res = query(&url, db_name, t.query, "pretty", None).await;
            let body = body::to_bytes(res.into_body()).await.unwrap();
            let body = String::from_utf8(body.as_bytes().to_vec()).unwrap();
            assert_eq!(t.expected, body, "query failed: {}", t.query);
        }
        // Query from the last cache:

        shutdown.cancel();
    }

    async fn setup_server(start_time: i64) -> (String, CancellationToken, Arc<dyn WriteBuffer>) {
        let trace_header_parser = trace_http::ctx::TraceHeaderParser::new();
        let metrics = Arc::new(metric::Registry::new());
        let common_state =
            crate::CommonServerState::new(Arc::clone(&metrics), None, trace_header_parser).unwrap();
        let object_store: Arc<DynObjectStore> = Arc::new(object_store::memory::InMemory::new());
        let (object_store, parquet_cache) = test_cached_obj_store_and_oracle(object_store);
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
            },
            DedicatedExecutor::new_testing(),
        ));
        let persister = Arc::new(Persister::new(Arc::clone(&object_store), "test_host"));
        let time_provider = Arc::new(MockProvider::new(Time::from_timestamp_nanos(start_time)));
        let dummy_host_id = Arc::from("dummy-host-id");
        let instance_id = Arc::from("dummy-instance-id");

        let write_buffer: Arc<dyn WriteBuffer> = Arc::new(
            influxdb3_write::write_buffer::WriteBufferImpl::new(
                Arc::clone(&persister),
                Arc::new(Catalog::new(dummy_host_id, instance_id)),
                Arc::new(LastCacheProvider::new()),
                Arc::<MockProvider>::clone(&time_provider),
                Arc::clone(&exec),
                WalConfig::test_config(),
                parquet_cache,
            )
            .await
            .unwrap(),
        );
        let query_executor = crate::query_executor::QueryExecutorImpl::new(
            write_buffer.catalog(),
            Arc::clone(&write_buffer),
            Arc::clone(&exec),
            Arc::clone(&metrics),
            Arc::new(HashMap::new()),
            10,
            10,
        );

        // bind to port 0 will assign a random available port:
        let socket_addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 0);
        let listener = TcpListener::bind(socket_addr)
            .await
            .expect("bind tcp address");
        let addr = listener.local_addr().unwrap();

        let server = ServerBuilder::new(common_state)
            .write_buffer(Arc::clone(&write_buffer))
            .query_executor(Arc::new(query_executor))
            .persister(persister)
            .authorizer(Arc::new(DefaultAuthorizer))
            .time_provider(Arc::clone(&time_provider))
            .tcp_listener(listener)
            .build();
        let frontend_shutdown = CancellationToken::new();
        let shutdown = frontend_shutdown.clone();

        tokio::spawn(async move { serve(server, frontend_shutdown).await });

        (format!("http://{addr}"), shutdown, write_buffer)
    }

    pub(crate) async fn write_lp(
        server: impl Into<String> + Send,
        database: impl Into<String> + Send,
        lp: impl Into<String> + Send,
        authorization: Option<&str>,
        accept_partial: bool,
        precision: impl Into<String> + Send,
    ) -> Response<Body> {
        let server = server.into();
        let client = Client::new();
        let url = format!(
            "{}/api/v3/write_lp?db={}&accept_partial={accept_partial}&precision={}",
            server,
            database.into(),
            precision.into(),
        );
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
}
