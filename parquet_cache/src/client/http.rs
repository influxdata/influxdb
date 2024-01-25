use std::{pin::Pin, sync::Arc, task::Poll};

use futures::Future;
use hyper::{client::HttpConnector, Body, Client, Request, Response, StatusCode};
use tower::Service;

use super::request::{PinnedFuture, RawRequest};

#[derive(Debug, Clone)]
pub struct HttpService {
    /// Pool of connections.
    client: Arc<Client<HttpConnector, Body>>,
}

impl HttpService {
    pub fn new() -> Self {
        let client = Client::builder()
            .http2_keep_alive_while_idle(true)
            .http2_only(true)
            .retry_canceled_requests(true)
            .build_http::<Body>();

        Self {
            client: Arc::new(client),
        }
    }
}

impl Default for HttpService {
    fn default() -> Self {
        Self::new()
    }
}

impl Service<RawRequest> for HttpService {
    type Response = Response<Body>;
    type Error = hyper::Error;
    type Future = PinnedFuture<Self::Response, Self::Error>;

    fn poll_ready(&mut self, _cx: &mut std::task::Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, req: RawRequest) -> Self::Future {
        match Request::<Body>::try_from(req) {
            Ok(req) => Box::pin(self.client.request(req)),
            Err(e) => invalid_request(e),
        }
    }
}

fn invalid_request(
    error: impl std::error::Error,
) -> Pin<Box<dyn Future<Output = Result<Response<Body>, hyper::Error>> + Send>> {
    let (mut parts, _) = Response::new("invalid request").into_parts();
    parts.status = StatusCode::BAD_REQUEST;

    let body = Body::from(
        serde_json::json!({"status": 400, "description": error.to_string()}).to_string(),
    );
    Box::pin(futures::future::ok(Response::from_parts(parts, body)))
}
