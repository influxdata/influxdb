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

use crate::ctx::parse_span_ctx;
use futures::ready;
use http::{Request, Response};
use http_body::SizeHint;
use observability_deps::tracing::error;
use pin_project::pin_project;
use std::borrow::Cow;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use tower::{Layer, Service};
use trace::{span::SpanRecorder, TraceCollector};

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
                    recorder: SpanRecorder::new(None),
                    inner: self.service.call(request),
                }
            }
        };

        let span = match parse_span_ctx(collector, request.headers()) {
            Ok(Some(ctx)) => {
                let span = ctx.child("IOx");

                // Add context to request for use by service handlers
                request.extensions_mut().insert(span.ctx.clone());

                // Create Span to use to instrument request
                Some(span)
            }
            Ok(None) => None,
            Err(e) => {
                error!(%e, "error extracting trace context from request");
                None
            }
        };

        TracedFuture {
            recorder: SpanRecorder::new(span),
            inner: self.service.call(request),
        }
    }
}

/// `TracedFuture` wraps a future returned by a `tower::Service` and
/// instruments the returned body if any
#[pin_project]
#[derive(Debug)]
pub struct TracedFuture<F> {
    recorder: SpanRecorder,
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
        let result: Result<Response<ResBody>, Error> =
            ready!(self.as_mut().project().inner.poll(cx));

        let recorder = self.as_mut().project().recorder;
        match &result {
            Ok(response) => match classify_response(response) {
                Ok(_) => recorder.event("request processed"),
                Err(e) => recorder.error(e),
            },
            Err(_) => recorder.error("error processing request"),
        }

        match result {
            Ok(response) => Poll::Ready(Ok(response.map(|body| TracedBody {
                recorder: self.as_mut().project().recorder.take(),
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
    recorder: SpanRecorder,
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

        let recorder = self.as_mut().project().recorder;
        match &result {
            Ok(_) => recorder.event("returned body data"),
            Err(_) => recorder.error("eos getting body"),
        }
        Poll::Ready(Some(result))
    }

    fn poll_trailers(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Result<Option<http::header::HeaderMap>, Self::Error>> {
        let result: Result<Option<http::header::HeaderMap>, Self::Error> =
            ready!(self.as_mut().project().inner.poll_trailers(cx));

        let recorder = self.as_mut().project().recorder;
        match &result {
            Ok(headers) => match classify_headers(headers.as_ref()) {
                Ok(_) => recorder.ok("returned trailers"),
                Err(error) => recorder.error(error),
            },
            Err(_) => recorder.error("eos getting trailers"),
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

fn classify_response<B>(response: &http::Response<B>) -> Result<(), Cow<'static, str>> {
    let status = response.status();
    match status {
        http::StatusCode::OK | http::StatusCode::CREATED | http::StatusCode::NO_CONTENT => {
            classify_headers(Some(response.headers()))
        }
        http::StatusCode::BAD_REQUEST => Err("bad request".into()),
        http::StatusCode::NOT_FOUND => Err("not found".into()),
        http::StatusCode::INTERNAL_SERVER_ERROR => Err("internal server error".into()),
        _ => Err(format!("unexpected status code: {}", status).into()),
    }
}

/// gRPC indicates failure via a [special][1] header allowing it to signal an error
/// at the end of an HTTP chunked stream as part of the [response trailer][2]
///
/// [1]: https://grpc.github.io/grpc/core/md_doc_statuscodes.html
/// [2]: https://developer.mozilla.org/en-US/docs/Web/HTTP/Headers/Trailer
fn classify_headers(headers: Option<&http::header::HeaderMap>) -> Result<(), Cow<'static, str>> {
    match headers.and_then(|headers| headers.get("grpc-status")) {
        Some(header) => {
            let value = header.to_str().map_err(|_| "grpc status not string")?;
            let value: i32 = value.parse().map_err(|_| "grpc status not integer")?;
            match value {
                0 => Ok(()),
                1 => Err("cancelled".into()),
                2 => Err("unknown".into()),
                3 => Err("invalid argument".into()),
                4 => Err("deadline exceeded".into()),
                5 => Err("not found".into()),
                6 => Err("already exists".into()),
                7 => Err("permission denied".into()),
                8 => Err("resource exhausted".into()),
                9 => Err("failed precondition".into()),
                10 => Err("aborted".into()),
                11 => Err("out of range".into()),
                12 => Err("unimplemented".into()),
                13 => Err("internal".into()),
                14 => Err("unavailable".into()),
                15 => Err("data loss".into()),
                16 => Err("unauthenticated".into()),
                _ => Err(format!("unrecognised status code: {}", value).into()),
            }
        }
        None => Ok(()),
    }
}
