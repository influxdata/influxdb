//!
//! Tower plumbing for adding tracing instrumentation to an HTTP service stack
//!
//! This is loosely based on tower-http's trace crate but with the tokio-tracing
//! specific bits removed and less generics.
//!
//! For those not familiar with tower:
//!
//! - A Layer produces a Service
//! - A Service can then be called with a request which returns a Future  
//! - This Future returns a response which contains a Body
//! - This Body contains the data payload (potentially streamed)
//!

use crate::{ctx::SpanContext, span::EnteredSpan, TraceCollector};
use futures::ready;
use http::{Request, Response};
use http_body::SizeHint;
use observability_deps::tracing::error;
use pin_project::pin_project;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use tower::{Layer, Service};

/// `TraceLayer` implements `tower::Layer` and can be used to decorate a
/// `tower::Service` to collect information about requests flowing through it
#[derive(Debug, Clone)]
pub struct TraceLayer {
    collector: Option<Arc<dyn TraceCollector>>,
}

impl TraceLayer {
    pub fn new(collector: Option<Arc<dyn TraceCollector>>) -> Self {
        Self { collector }
    }
}

impl<S> Layer<S> for TraceLayer {
    type Service = TraceService<S>;

    fn layer(&self, service: S) -> Self::Service {
        TraceService {
            service,
            collector: self.collector.clone(),
        }
    }
}

/// TraceService wraps an inner tower::Service and instruments its returned futures
#[derive(Debug, Clone)]
pub struct TraceService<S> {
    service: S,
    collector: Option<Arc<dyn TraceCollector>>,
}

impl<S, ReqBody, ResBody> Service<Request<ReqBody>> for TraceService<S>
where
    S: Service<Request<ReqBody>, Response = Response<ResBody>>,
    ResBody: http_body::Body,
{
    type Response = Response<TracedBody<ResBody>>;
    type Error = S::Error;
    type Future = TracedFuture<S::Future>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.service.poll_ready(cx)
    }

    fn call(&mut self, mut request: Request<ReqBody>) -> Self::Future {
        let collector = match self.collector.as_ref() {
            Some(collector) => collector,
            None => {
                return TracedFuture {
                    span: None,
                    inner: self.service.call(request),
                }
            }
        };

        let span = match SpanContext::from_headers(collector, request.headers()) {
            Ok(Some(ctx)) => {
                let span = ctx.child("IOx");

                // Add context to request for use by service handlers
                request.extensions_mut().insert(span.ctx.clone());

                // Create Span to use to instrument request
                Some(EnteredSpan::new(span))
            }
            Ok(None) => None,
            Err(e) => {
                error!(%e, "error extracting trace context from request");
                None
            }
        };

        TracedFuture {
            span,
            inner: self.service.call(request),
        }
    }
}

/// `TracedFuture` wraps a future returned by a `tower::Service` and
/// instruments the returned body if any
#[pin_project]
#[derive(Debug)]
pub struct TracedFuture<F> {
    span: Option<EnteredSpan>,
    #[pin]
    inner: F,
}

impl<F, ResBody, Error> Future for TracedFuture<F>
where
    F: Future<Output = Result<Response<ResBody>, Error>>,
    ResBody: http_body::Body,
{
    type Output = Result<Response<TracedBody<ResBody>>, Error>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let result = ready!(self.as_mut().project().inner.poll(cx));
        if let Some(span) = self.as_mut().project().span.as_mut() {
            match &result {
                Ok(_) => span.event("request processed"),
                Err(_) => span.error("error processing request"),
            }
        }

        match result {
            Ok(response) => Poll::Ready(Ok(response.map(|body| TracedBody {
                span: self.as_mut().project().span.take(),
                inner: body,
            }))),
            Err(e) => Poll::Ready(Err(e)),
        }
    }
}

/// `TracedBody` wraps a `http_body::Body` and instruments it
#[pin_project]
#[derive(Debug)]
pub struct TracedBody<B> {
    span: Option<EnteredSpan>,
    #[pin]
    inner: B,
}

impl<B: http_body::Body> http_body::Body for TracedBody<B> {
    type Data = B::Data;
    type Error = B::Error;

    fn poll_data(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Self::Data, Self::Error>>> {
        let maybe_result = ready!(self.as_mut().project().inner.poll_data(cx));
        let result = match maybe_result {
            Some(result) => result,
            None => return Poll::Ready(None),
        };

        if let Some(span) = self.as_mut().project().span.as_mut() {
            match &result {
                Ok(_) => span.event("returned body data"),
                Err(_) => span.error("eos getting body"),
            }
        }
        Poll::Ready(Some(result))
    }

    fn poll_trailers(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Result<Option<http::header::HeaderMap>, Self::Error>> {
        // TODO: Classify response status and set SpanStatus

        let result = ready!(self.as_mut().project().inner.poll_trailers(cx));
        if let Some(span) = self.as_mut().project().span.as_mut() {
            match &result {
                Ok(_) => span.event("returned trailers"),
                Err(_) => span.error("eos getting trailers"),
            }
        }
        Poll::Ready(result)
    }

    fn is_end_stream(&self) -> bool {
        self.inner.is_end_stream()
    }

    fn size_hint(&self) -> SizeHint {
        self.inner.size_hint()
    }
}
