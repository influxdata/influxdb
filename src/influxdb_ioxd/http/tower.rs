use std::convert::Infallible;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use futures::future::BoxFuture;
use futures::{ready, FutureExt};
use hyper::server::conn::AddrStream;
use hyper::Body;
use routerify::{RequestService, Router, RouterService};

use trace::TraceCollector;
use trace_http::tower::{TraceLayer, TraceService};

use super::ApplicationError;
use tower::Layer;

/// `MakeService` can be thought of as a hyper-compatible connection factory
///
/// Specifically it implements the necessary trait to be used with `hyper::server::Builder::serve`
pub struct MakeService {
    inner: RouterService<Body, ApplicationError>,
    trace_layer: trace_http::tower::TraceLayer,
}

impl MakeService {
    pub fn new(
        router: Router<Body, ApplicationError>,
        collector: Option<Arc<dyn TraceCollector>>,
    ) -> Self {
        Self {
            inner: RouterService::new(router).unwrap(),
            trace_layer: TraceLayer::new(collector),
        }
    }
}

impl tower::Service<&AddrStream> for MakeService {
    type Response = Service;
    type Error = Infallible;
    type Future = MakeServiceFuture;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, conn: &AddrStream) -> Self::Future {
        MakeServiceFuture {
            inner: self.inner.call(conn),
            trace_layer: self.trace_layer.clone(),
        }
    }
}

/// A future produced by `MakeService` that resolves to a `Service`
pub struct MakeServiceFuture {
    inner: BoxFuture<'static, Result<RequestService<Body, ApplicationError>, Infallible>>,
    trace_layer: trace_http::tower::TraceLayer,
}

impl Future for MakeServiceFuture {
    type Output = Result<Service, Infallible>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let maybe_service = ready!(self.inner.poll_unpin(cx));
        Poll::Ready(maybe_service.map(|service| self.trace_layer.layer(service)))
    }
}

pub type Service = TraceService<RequestService<Body, ApplicationError>>;
