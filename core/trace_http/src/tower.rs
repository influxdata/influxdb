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

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::task::{Context, Poll};

use bytes::Buf;
use futures::ready;
use http::{HeaderValue, Request, Response};
use http_body::{Frame, SizeHint};
use pin_project::{pin_project, pinned_drop};
use tower::{Layer, Service};

use trace::span::{SpanEvent, SpanStatus};
use trace::{TraceCollector, span::SpanRecorder};
use tracing::{debug, error};

use crate::classify::{Classification, classify_headers, classify_response};
use crate::ctx::{RequestLogContext, RequestLogContextExt, TraceHeaderParser};
use crate::metrics::{MetricsRecorder, RequestMetrics};
use crate::query_variant::QueryVariantExt;

/// ServiceProtocol is used to denote what protocol is being handled by the `Service`.
/// This is used as part of the algorithm for determining when
/// a request has fully completed rather than been aborted by the client.
#[derive(Debug, Clone, Copy)]
pub enum ServiceProtocol {
    Http,
    Grpc,
}

/// `TraceLayer` implements `tower::Layer` and can be used to decorate a
/// `tower::Service` to collect information about requests flowing through it
///
/// Including:
///
/// - Extracting distributed trace context and attaching span context
/// - Collecting count and duration metrics - [RED metrics][1]
///
/// [1]: https://www.weave.works/blog/the-red-method-key-metrics-for-microservices-architecture/
#[derive(Debug, Clone)]
pub struct TraceLayer {
    trace_header_parser: TraceHeaderParser,
    metrics: Arc<RequestMetrics>,
    collector: Option<Arc<dyn TraceCollector>>,
    name: Arc<str>,
    service_protocol: ServiceProtocol,
}

impl TraceLayer {
    /// Create a new tower [`Layer`] for tracing
    pub fn new(
        trace_header_parser: TraceHeaderParser,
        metrics: Arc<RequestMetrics>,
        collector: Option<Arc<dyn TraceCollector>>,
        name: &str,
        service_protocol: ServiceProtocol,
    ) -> Self {
        Self {
            trace_header_parser,
            metrics,
            collector,
            name: name.into(),
            service_protocol,
        }
    }
}

impl<S> Layer<S> for TraceLayer {
    type Service = TraceService<S>;

    fn layer(&self, service: S) -> Self::Service {
        TraceService {
            service,
            collector: self.collector.clone(),
            metrics: Arc::clone(&self.metrics),
            trace_header_parser: Some(self.trace_header_parser.clone()),
            name: Arc::clone(&self.name),
            service_protocol: self.service_protocol,
        }
    }
}

/// TraceService wraps an inner tower::Service and instruments its returned futures
#[derive(Debug, Clone)]
pub struct TraceService<S> {
    service: S,
    trace_header_parser: Option<TraceHeaderParser>,
    collector: Option<Arc<dyn TraceCollector>>,
    metrics: Arc<RequestMetrics>,
    name: Arc<str>,
    service_protocol: ServiceProtocol,
}

impl<S> TraceService<S> {
    /// Create a new [`TraceService`] for instrumenting a client
    pub fn new_client(
        service: S,
        metrics: Arc<RequestMetrics>,
        collector: Option<Arc<dyn TraceCollector>>,
        name: &str,
        service_protocol: ServiceProtocol,
    ) -> Self {
        Self {
            service,
            trace_header_parser: None,
            metrics,
            collector,
            name: name.into(),
            service_protocol,
        }
    }
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
        let query_variant = QueryVariantExt::default();
        let metrics_recorder = Some(self.metrics.recorder(&request, query_variant.clone()));
        request.extensions_mut().insert(query_variant);

        let request_ctx = self.trace_header_parser.as_ref().and_then(|parser| {
            match parser.parse(self.collector.as_ref(), request.headers()) {
                Ok(Some(ctx)) => {
                    let ctx = RequestLogContext::new(ctx);

                    request.extensions_mut().insert(ctx.clone());

                    Some(ctx)
                }
                Ok(None) => None,
                Err(e) => {
                    error!(%e, "error extracting trace context from request");
                    None
                }
            }
        });

        let span = request_ctx.as_ref().and_then(|ctx| {
            let ctx = ctx.ctx();

            (ctx.sampled && ctx.collector.is_some()).then(|| {
                let span = ctx.child(format!("IOx {}", self.name));

                // Add context to request for use by service handlers
                request.extensions_mut().insert(span.ctx.clone());

                span
            })
        });

        TracedFuture {
            request_ctx,
            metrics_recorder,
            span_recorder: SpanRecorder::new(span),
            was_ready: false,
            protocol: self.service_protocol,
            inner: self.service.call(request),
        }
    }
}

/// `TracedFuture` wraps a future returned by a `tower::Service` and
/// instruments the returned body if any
#[pin_project(PinnedDrop)]
#[derive(Debug)]
pub struct TracedFuture<F> {
    request_ctx: Option<RequestLogContext>,
    span_recorder: SpanRecorder,
    metrics_recorder: Option<MetricsRecorder>,
    was_ready: bool,
    protocol: ServiceProtocol,
    #[pin]
    inner: F,
}

#[pinned_drop]
impl<F> PinnedDrop for TracedFuture<F> {
    fn drop(self: Pin<&mut Self>) {
        if !self.was_ready {
            let trace = self.request_ctx.format_jaeger();
            debug!(
                %trace,
                when="before returning headers",
                "request cancelled",
            );
        }
    }
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

        let projected = self.as_mut().project();
        *projected.was_ready = true;
        let span_recorder = projected.span_recorder;
        let mut metrics_recorder = projected.metrics_recorder.take().unwrap();
        match &result {
            Ok(response) => match classify_response(response) {
                (_, Classification::Ok) => match response.body().is_end_stream() {
                    true => {
                        metrics_recorder.set_classification(Classification::Ok);
                        span_recorder.ok("request processed with empty response")
                    }
                    false => span_recorder.event(SpanEvent::new("request processed")),
                },
                (error, c) => {
                    metrics_recorder.set_classification(c);
                    span_recorder.error(error);
                }
            },
            Err(_) => {
                metrics_recorder.set_classification(Classification::ServerErr);
                span_recorder.error("error processing request")
            }
        }

        match result {
            Ok(mut response) => {
                // add trace-id header to the response, if we have one
                let projected = self.as_mut().project();
                let request_ctx = projected.request_ctx.take();
                let span_recorder = projected.span_recorder.take();
                if let Some(trace_id) = span_recorder.span().map(|span| span.ctx.trace_id) {
                    // format as hex
                    let trace_id = HeaderValue::from_str(&format!("{:x}", trace_id.get())).unwrap();
                    response.headers_mut().insert("trace-id", trace_id);
                }

                Poll::Ready(Ok(response.map(|body| TracedBody {
                    request_ctx,
                    span_recorder,
                    was_done_data: AtomicBool::new(false),
                    was_ready_trailers: AtomicBool::new(false),
                    protocol: *projected.protocol,
                    inner: body,
                    metrics_recorder,
                })))
            }
            Err(e) => Poll::Ready(Err(e)),
        }
    }
}

/// `TracedBody` wraps a `http_body::Body` and instruments it
#[pin_project(PinnedDrop)]
#[derive(Debug)]
pub struct TracedBody<B> {
    request_ctx: Option<RequestLogContext>,
    span_recorder: SpanRecorder,
    metrics_recorder: MetricsRecorder,
    was_done_data: AtomicBool,
    was_ready_trailers: AtomicBool,
    protocol: ServiceProtocol,
    #[pin]
    inner: B,
}

#[pinned_drop]
impl<B> PinnedDrop for TracedBody<B> {
    fn drop(self: Pin<&mut Self>) {
        if !self.was_done_data.load(Ordering::SeqCst) {
            let trace = self.request_ctx.format_jaeger();
            debug!(
                %trace,
                when="before fully returning body data",
                "request cancelled",
            );
        } else if !self.was_ready_trailers.load(Ordering::SeqCst) {
            let trace = self.request_ctx.format_jaeger();
            debug!(
                %trace,
                when="before returning trailers",
                "request cancelled",
            );
        }
    }
}

impl<B: http_body::Body> http_body::Body for TracedBody<B> {
    type Data = B::Data;
    type Error = B::Error;

    fn poll_frame(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Self::Data>, Self::Error>>> {
        let maybe_result = ready!(self.as_mut().project().inner.poll_frame(cx));

        match maybe_result {
            Some(Ok(result)) => {
                if result.is_trailers() {
                    self.handle_trailers(result.trailers_ref());
                } else if let Some(data) = result.data_ref() {
                    self.handle_data(data);
                }
                Poll::Ready(Some(Ok(result)))
            }
            Some(Err(e)) => {
                self.handle_error();
                Poll::Ready(Some(Err(e)))
            }
            None => {
                let projected = self.as_mut().project();
                match projected.protocol {
                    ServiceProtocol::Http => {
                        // Hyper v0.14.31 does not ever poll the trailers for HTTP 1 connections.
                        // As a result, we need to record an `ok` metric here to prevent all
                        // HTTP 1 requests from being considered as `aborted`.
                        projected
                            .metrics_recorder
                            .set_classification(Classification::Ok);

                        projected.was_ready_trailers.store(true, Ordering::SeqCst);
                    }
                    ServiceProtocol::Grpc => {
                        // Do nothing for Grpc, we need trailers to be polled
                        // before we can be certain the response has been fully consumed.
                    }
                }

                projected.was_done_data.store(true, Ordering::SeqCst);

                Poll::Ready(None)
            }
        }
    }

    fn is_end_stream(&self) -> bool {
        let res = self.inner.is_end_stream();
        if res {
            self.was_done_data.store(true, Ordering::SeqCst);
            self.was_ready_trailers.store(true, Ordering::SeqCst);
        }
        res
    }

    fn size_hint(&self) -> SizeHint {
        self.inner.size_hint()
    }
}

impl<B: http_body::Body> TracedBody<B> {
    fn handle_data(mut self: Pin<&mut Self>, body: &B::Data) {
        let projected = self.as_mut().project();
        let span_recorder = projected.span_recorder;
        let metrics_recorder = projected.metrics_recorder;

        let size = body.remaining() as i64;
        metrics_recorder.add_response_body_size(size as u64);

        match projected.inner.is_end_stream() {
            true => {
                metrics_recorder.set_classification(Classification::Ok);

                let mut evt = SpanEvent::new("returned body data and no trailers");
                evt.set_metadata("size", size);
                span_recorder.event(evt);
                span_recorder.status(SpanStatus::Ok);

                projected.was_done_data.store(true, Ordering::SeqCst);
                projected.was_ready_trailers.store(true, Ordering::SeqCst);
            }
            false => {
                let mut evt = SpanEvent::new("returned body data");
                evt.set_metadata("size", size);
                span_recorder.event(evt);
            }
        }
    }

    fn handle_trailers(mut self: Pin<&mut Self>, headers: Option<&http::header::HeaderMap>) {
        let projected = self.as_mut().project();

        projected.was_done_data.store(true, Ordering::SeqCst);
        projected.was_ready_trailers.store(true, Ordering::SeqCst);

        let span_recorder = projected.span_recorder;
        let metrics_recorder = projected.metrics_recorder;

        match classify_headers(headers) {
            (_, Classification::Ok) => {
                metrics_recorder.set_classification(Classification::Ok);
                span_recorder.ok("returned trailers")
            }
            (error, c) => {
                metrics_recorder.set_classification(c);
                span_recorder.error(error)
            }
        }
    }

    fn handle_error(mut self: Pin<&mut Self>) {
        let projected = self.as_mut().project();
        let span_recorder = projected.span_recorder;
        let metrics_recorder = projected.metrics_recorder;

        metrics_recorder.set_classification(Classification::ServerErr);
        span_recorder.error("error getting frame");
        projected.was_done_data.store(true, Ordering::SeqCst);
        projected.was_ready_trailers.store(true, Ordering::SeqCst);
    }
}
