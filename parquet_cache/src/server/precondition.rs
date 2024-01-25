use std::task::Poll;

use http::{HeaderMap, Request};
use hyper::Body;
use object_store::ObjectMeta;
use tower::{Layer, Service};

use super::error::Error;
use super::response::PinnedFuture;

/// Service that applies the preconditions per request.
///
/// Refer to GetOptions:
/// <https://github.com/apache/arrow-rs/blob/481652a4f8d972b633063158903dbdb0adcf094d/object_store/src/lib.rs#L871>
#[derive(Debug, Clone)]
pub struct PreconditionService<S: Clone + Send + Sync + 'static> {
    inner: S,
}

impl<S: Clone + Send + Sync + 'static> PreconditionService<S> {
    pub fn new(inner: S) -> Self {
        Self { inner }
    }

    fn passes(&self, _preconditions: HeaderMap, _metadata: ObjectMeta) -> bool {
        unimplemented!("TODO: precondition applied for any request, per HTTP header contract")
    }
}

impl<S> Service<Request<Body>> for PreconditionService<S>
where
    S: Service<Request<Body>, Future = PinnedFuture, Error = Error> + Clone + Send + Sync + 'static,
{
    type Response = super::response::Response;
    type Error = Error;
    type Future = super::response::PinnedFuture;

    fn poll_ready(&mut self, cx: &mut std::task::Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, req: Request<Body>) -> Self::Future {
        let clone = self.inner.clone();
        let mut inner = std::mem::replace(&mut self.inner, clone);
        Box::pin(async move { inner.call(req).await })
    }
}

pub struct BuildPreconditionService;

impl<S: Clone + Send + Sync + 'static> Layer<S> for BuildPreconditionService {
    type Service = PreconditionService<S>;

    fn layer(&self, service: S) -> Self::Service {
        PreconditionService::new(service)
    }
}
