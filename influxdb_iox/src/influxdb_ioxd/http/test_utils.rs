use std::{
    fmt::Debug,
    net::{IpAddr, Ipv4Addr, SocketAddr},
    sync::Arc,
};

use http::header::CONTENT_TYPE;
use hyper::{server::conn::AddrIncoming, StatusCode};
use serde::de::DeserializeOwned;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use crate::influxdb_ioxd::{http::serve, server_type::ServerType};

/// checks a http response against expected results
pub async fn check_response(
    description: &str,
    response: Result<reqwest::Response, reqwest::Error>,
    expected_status: StatusCode,
    expected_body: Option<&str>,
) {
    // Print the response so if the test fails, we have a log of
    // what went wrong
    println!("{} response: {:?}", description, response);

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
                "Could not find expected in body.\n\nExpected:\n{}\n\nBody:\n{}",
                expected_body,
                body
            );
        }
    } else {
        panic!("Unexpected error response: {:?}", response);
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
    println!("{} response: {:?}", url, response);

    if let Ok(response) = response {
        let status = response.status();
        let body: T = response
            .json()
            .await
            .expect("Converting request body to string");

        assert_eq!(status, expected_status);
        body
    } else {
        panic!("Unexpected error response: {:?}", response);
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
        println!("Started server at {}", url);

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
