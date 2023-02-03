use std::{
    fmt::Debug,
    net::{IpAddr, Ipv4Addr, SocketAddr},
    sync::Arc,
};

use http::header::CONTENT_TYPE;
use hyper::{server::conn::AddrIncoming, StatusCode};
use reqwest::Client;
use serde::de::DeserializeOwned;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use trace::RingBufferTraceCollector;

use crate::{http::serve, server_type::ServerType};

/// checks a http response against expected results
pub async fn check_response(
    description: &str,
    response: Result<reqwest::Response, reqwest::Error>,
    expected_status: StatusCode,
    expected_body: Option<&str>,
) {
    // Print the response so if the test fails, we have a log of
    // what went wrong
    println!("{description} response: {response:?}");

    if let Ok(response) = response {
        let status = response.status();
        let body = response
            .text()
            .await
            .expect("Converting request body to string");

        assert_eq!(status, expected_status);
        if let Some(expected_body) = expected_body {
            assert!(
                body.contains(expected_body),
                "Could not find expected in body.\n\nExpected:\n{expected_body}\n\nBody:\n{body}"
            );
        }
    } else {
        panic!("Unexpected error response: {response:?}");
    }
}

#[allow(dead_code)]
pub async fn check_json_response<T: DeserializeOwned + Eq + Debug>(
    client: &reqwest::Client,
    url: &str,
    expected_status: StatusCode,
) -> T {
    let response = client.get(url).send().await;

    // Print the response so if the test fails, we have a log of
    // what went wrong
    println!("{url} response: {response:?}");

    if let Ok(response) = response {
        let status = response.status();
        let body: T = response
            .json()
            .await
            .expect("Converting request body to string");

        assert_eq!(status, expected_status);
        body
    } else {
        panic!("Unexpected error response: {response:?}");
    }
}

pub fn get_content_type(response: &Result<reqwest::Response, reqwest::Error>) -> String {
    if let Ok(response) = response {
        response
            .headers()
            .get(CONTENT_TYPE)
            .map(|v| v.to_str().unwrap())
            .unwrap_or("")
            .to_string()
    } else {
        "".to_string()
    }
}

pub struct TestServer<M>
where
    M: ServerType,
{
    join_handle: JoinHandle<()>,
    url: String,
    server_type: Arc<M>,
}

impl<M> TestServer<M>
where
    M: ServerType,
{
    pub fn new(server_type: Arc<M>) -> Self {
        // NB: specify port 0 to let the OS pick the port.
        let bind_addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 0);
        let addr = AddrIncoming::bind(&bind_addr).expect("failed to bind server");
        let url = format!("http://{}", addr.local_addr());

        let trace_header_parser = trace_http::ctx::TraceHeaderParser::new()
            .with_jaeger_trace_context_header_name("uber-trace-id");

        let server_type_captured = Arc::clone(&server_type);
        let join_handle = tokio::task::spawn(async {
            serve(
                addr,
                server_type_captured,
                CancellationToken::new(),
                trace_header_parser,
            )
            .await
            .unwrap();
        });
        println!("Started server at {url}");

        Self {
            join_handle,
            url,
            server_type,
        }
    }

    pub fn url(&self) -> &str {
        &self.url
    }

    pub fn server_type(&self) -> &Arc<M> {
        &self.server_type
    }
}

impl<M> Drop for TestServer<M>
where
    M: ServerType,
{
    fn drop(&mut self) {
        self.join_handle.abort();
    }
}

pub const TEST_MAX_REQUEST_SIZE: usize = 1024 * 1024;

/// Assert that health route is working.
pub async fn assert_health<T>(test_server: TestServer<T>)
where
    T: ServerType,
{
    let client = Client::new();
    let response = client
        .get(&format!("{}/health", test_server.url()))
        .send()
        .await;

    // Print the response so if the test fails, we have a log of what went wrong
    check_response("health", response, StatusCode::OK, Some("OK")).await;
}

/// Assert that metrics exposure is working.
pub async fn assert_metrics<T>(test_server: TestServer<T>)
where
    T: ServerType,
{
    use metric::{Metric, U64Counter};

    let metric: Metric<U64Counter> = test_server
        .server_type()
        .metric_registry()
        .register_metric("my_metric", "description");

    metric.recorder(&[("tag", "value")]).inc(20);

    let client = Client::new();
    let response = client
        .get(&format!("{}/metrics", test_server.url()))
        .send()
        .await
        .unwrap();

    let data = response.text().await.unwrap();

    assert!(data.contains("\nmy_metric_total{tag=\"value\"} 20\n"));

    let response = client
        .get(&format!("{}/nonexistent", test_server.url()))
        .send()
        .await
        .unwrap();

    assert_eq!(response.status().as_u16(), 404);

    let response = client
        .get(&format!("{}/metrics", test_server.url()))
        .send()
        .await
        .unwrap();

    let data = response.text().await.unwrap();

    // Should include previous metrics scrape but not the current one
    assert!(data.contains("\nhttp_requests_total{path=\"/metrics\",status=\"ok\"} 1\n"));
    // Should include 404 but not encode the path
    assert!(!data.contains("nonexistent"));
    assert!(data.contains("\nhttp_requests_total{status=\"client_error\"} 1\n"));
}

/// Assert that tracing works.
///
/// For this to work the used trace collector must be a [`RingBufferTraceCollector`].
pub async fn assert_tracing<T>(test_server: TestServer<T>)
where
    T: ServerType,
{
    let trace_collector = test_server.server_type().trace_collector().unwrap();
    let trace_collector = trace_collector
        .as_any()
        .downcast_ref::<RingBufferTraceCollector>()
        .unwrap();

    let client = Client::new();
    let response = client
        .get(&format!("{}/health", test_server.url()))
        .header("uber-trace-id", "34f3495:36e34:0:1")
        .send()
        .await;

    // Print the response so if the test fails, we have a log of what went wrong
    check_response("health", response, StatusCode::OK, Some("OK")).await;

    let mut spans = trace_collector.spans();
    assert_eq!(spans.len(), 1);

    let span = spans.pop().unwrap();
    assert_eq!(span.ctx.trace_id.get(), 0x34f3495);
    assert_eq!(span.ctx.parent_span_id.unwrap().get(), 0x36e34);
}
