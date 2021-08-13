use http::header::HeaderName;
use http::{HeaderValue, Request, Response};
use std::sync::Arc;
use std::task::{Context, Poll};
use tower::{Layer, Service};

/// `SetRequestHeadersLayer` sets the provided headers on all requests flowing through it
/// unless they're already set
#[derive(Debug, Clone)]
pub(crate) struct SetRequestHeadersLayer {
    headers: Arc<Vec<(HeaderName, HeaderValue)>>,
}

impl SetRequestHeadersLayer {
    pub(crate) fn new(headers: Vec<(HeaderName, HeaderValue)>) -> Self {
        Self {
            headers: Arc::new(headers),
        }
    }
}

impl<S> Layer<S> for SetRequestHeadersLayer {
    type Service = SetRequestHeadersService<S>;

    fn layer(&self, service: S) -> Self::Service {
        SetRequestHeadersService {
            service,
            headers: Arc::clone(&self.headers),
        }
    }
}

/// SetRequestHeadersService wraps an inner tower::Service and sets the provided
/// headers on requests flowing through it
#[derive(Debug, Clone)]
pub struct SetRequestHeadersService<S> {
    service: S,
    headers: Arc<Vec<(HeaderName, HeaderValue)>>,
}

impl<S, ReqBody, ResBody> Service<Request<ReqBody>> for SetRequestHeadersService<S>
where
    S: Service<Request<ReqBody>, Response = Response<ResBody>>,
{
    type Response = Response<ResBody>;
    type Error = S::Error;
    type Future = S::Future;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.service.poll_ready(cx)
    }

    fn call(&mut self, mut request: Request<ReqBody>) -> Self::Future {
        let headers = request.headers_mut();
        for (name, value) in self.headers.iter() {
            headers.insert(name, value.clone());
        }
        self.service.call(request)
    }
}
