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
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ServiceProtocol {
    Http,
    Grpc,
}

/// Determine whether a request is a gRPC request (HTTP/2 with a gRPC content-type).
pub fn request_protocol<B>(request: &Request<B>) -> ServiceProtocol {
    let is_grpc = request.version() == http::Version::HTTP_2
        && request
            .headers()
            .get(http::header::CONTENT_TYPE)
            .and_then(|ct| ct.to_str().ok())
            .is_some_and(is_grpc_content_type);
    if is_grpc {
        ServiceProtocol::Grpc
    } else {
        ServiceProtocol::Http
    }
}

/// Whether a `content-type` header value denotes gRPC.
///
/// The gRPC wire spec defines the type as `application/grpc` optionally
/// followed by a `+subtype` (e.g. `+proto`, `+json`) or media-type parameters.
/// Matching is case-insensitive (RFC 9110 §8.3.1) and tolerant of leading
/// whitespace. The base must be terminated by end-of-string, `+`, `;`, or
/// whitespace, so unrelated types that merely share the prefix -- notably
/// `application/grpc-web`, a distinct framing tonic cannot decode -- are not
/// treated as gRPC.
fn is_grpc_content_type(value: &str) -> bool {
    const GRPC_CONTENT_TYPE: &str = "application/grpc";
    let value = value.trim_start();
    let Some(prefix) = value.get(..GRPC_CONTENT_TYPE.len()) else {
        return false;
    };
    prefix.eq_ignore_ascii_case(GRPC_CONTENT_TYPE)
        && match value[GRPC_CONTENT_TYPE.len()..].chars().next() {
            None => true,
            Some(c) => c == '+' || c == ';' || c.is_whitespace(),
        }
}

/// Whether a request should be routed and instrumented as gRPC.
///
/// Routing and metrics must agree on this predicate: if a router uses a
/// different notion of "is gRPC" than the trace layers, requests can be
/// dispatched to one protocol's handler while being recorded (or dropped)
/// by the other protocol's metrics.
pub fn is_grpc_request<B>(request: &Request<B>) -> bool {
    request_protocol(request) == ServiceProtocol::Grpc
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
        // A unified listener stacks one TraceLayer per protocol; only the
        // layer whose protocol matches the request may record it, otherwise
        // every request produces series in both metric families and the
        // layers clobber each other's request extensions.
        if request_protocol(&request) != self.service_protocol {
            return TracedFuture {
                request_ctx: None,
                span_recorder: SpanRecorder::new(None),
                metrics_recorder: None,
                was_ready: false,
                instrumented: false,
                protocol: self.service_protocol,
                inner: self.service.call(request),
            };
        }

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
            instrumented: true,
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
    instrumented: bool,
    protocol: ServiceProtocol,
    #[pin]
    inner: F,
}

#[pinned_drop]
impl<F> PinnedDrop for TracedFuture<F> {
    fn drop(self: Pin<&mut Self>) {
        if self.instrumented && !self.was_ready {
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
        let mut metrics_recorder = projected.metrics_recorder.take();
        // Skip response classification and span bookkeeping for protocol-mismatch
        // pass-throughs: `metrics_recorder` is `None` and the span recorder is
        // empty, so `classify_response` would be avoidable work on the hot path.
        if let Some(mr) = metrics_recorder.as_mut() {
            let span_recorder = projected.span_recorder;
            match &result {
                Ok(response) => match classify_response(response) {
                    (_, Classification::Ok) => match response.body().is_end_stream() {
                        true => {
                            mr.set_classification(Classification::Ok);
                            span_recorder.ok("request processed with empty response")
                        }
                        false => span_recorder.event(SpanEvent::new("request processed")),
                    },
                    (error, c) => {
                        mr.set_classification(c);
                        span_recorder.error(error);
                    }
                },
                Err(_) => {
                    mr.set_classification(Classification::ServerErr);
                    span_recorder.error("error processing request")
                }
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
    metrics_recorder: Option<MetricsRecorder>,
    was_done_data: AtomicBool,
    was_ready_trailers: AtomicBool,
    protocol: ServiceProtocol,
    #[pin]
    inner: B,
}

#[pinned_drop]
impl<B> PinnedDrop for TracedBody<B> {
    fn drop(self: Pin<&mut Self>) {
        // Pass-through bodies (protocol mismatch) carry no recorder; their
        // completion bookkeeping is meaningless and must not log.
        if self.metrics_recorder.is_none() {
            return;
        }
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
                        if let Some(metrics_recorder) = projected.metrics_recorder.as_mut() {
                            metrics_recorder.set_classification(Classification::Ok);
                        }

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
        if let Some(metrics_recorder) = metrics_recorder.as_mut() {
            metrics_recorder.add_response_body_size(size as u64);
        }

        match projected.inner.is_end_stream() {
            true => {
                if let Some(metrics_recorder) = metrics_recorder.as_mut() {
                    metrics_recorder.set_classification(Classification::Ok);
                }

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
                if let Some(metrics_recorder) = metrics_recorder.as_mut() {
                    metrics_recorder.set_classification(Classification::Ok);
                }
                span_recorder.ok("returned trailers")
            }
            (error, c) => {
                if let Some(metrics_recorder) = metrics_recorder.as_mut() {
                    metrics_recorder.set_classification(c);
                }
                span_recorder.error(error)
            }
        }
    }

    fn handle_error(mut self: Pin<&mut Self>) {
        let projected = self.as_mut().project();
        let span_recorder = projected.span_recorder;
        let metrics_recorder = projected.metrics_recorder;

        if let Some(metrics_recorder) = metrics_recorder.as_mut() {
            metrics_recorder.set_classification(Classification::ServerErr);
        }
        span_recorder.error("error getting frame");
        projected.was_done_data.store(true, Ordering::SeqCst);
        projected.was_ready_trailers.store(true, Ordering::SeqCst);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::metrics::MetricFamily;
    use http::Method;
    use http_body::Body as _;
    use metric::{Observation, RawReporter};
    use std::convert::Infallible;

    #[derive(Debug)]
    struct EmptyBody;

    impl http_body::Body for EmptyBody {
        type Data = bytes::Bytes;
        type Error = Infallible;

        fn poll_frame(
            self: Pin<&mut Self>,
            _cx: &mut Context<'_>,
        ) -> Poll<Option<Result<Frame<Self::Data>, Self::Error>>> {
            Poll::Ready(None)
        }

        fn is_end_stream(&self) -> bool {
            true
        }
    }

    #[derive(Debug, Clone)]
    struct OkService;

    impl Service<Request<EmptyBody>> for OkService {
        type Response = Response<EmptyBody>;
        type Error = Infallible;
        type Future = std::future::Ready<Result<Self::Response, Self::Error>>;

        fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
            Poll::Ready(Ok(()))
        }

        fn call(&mut self, _req: Request<EmptyBody>) -> Self::Future {
            std::future::ready(Ok(Response::new(EmptyBody)))
        }
    }

    fn drive(
        service_protocol: ServiceProtocol,
        family: MetricFamily,
        request: Request<EmptyBody>,
    ) -> Arc<metric::Registry> {
        let registry = Arc::new(metric::Registry::new());
        let metrics = Arc::new(RequestMetrics::new(Arc::clone(&registry), family));
        let layer = TraceLayer::new(
            TraceHeaderParser::new(),
            metrics,
            None,
            "test",
            service_protocol,
        );
        let mut service = layer.layer(OkService);
        let response = futures::executor::block_on(service.call(request)).unwrap();
        drop(response);
        registry
    }

    fn http_request() -> Request<EmptyBody> {
        Request::builder()
            .method(Method::GET)
            .uri("/health")
            .body(EmptyBody)
            .unwrap()
    }

    fn grpc_request() -> Request<EmptyBody> {
        Request::builder()
            .method(Method::POST)
            .uri("/pkg.Service/Method")
            .version(http::Version::HTTP_2)
            .header(http::header::CONTENT_TYPE, "application/grpc")
            .body(EmptyBody)
            .unwrap()
    }

    fn has_metric(registry: &metric::Registry, name: &str) -> bool {
        let mut reporter = RawReporter::default();
        registry.report(&mut reporter);
        reporter.metric(name).is_some()
    }

    #[test]
    fn grpc_layer_ignores_http_requests() {
        let registry = drive(
            ServiceProtocol::Grpc,
            MetricFamily::GrpcServer,
            http_request(),
        );
        assert!(
            !has_metric(&registry, "grpc_requests"),
            "gRPC-family layer must not record HTTP requests"
        );
    }

    #[test]
    fn http_layer_ignores_grpc_requests() {
        let registry = drive(
            ServiceProtocol::Http,
            MetricFamily::HttpServer,
            grpc_request(),
        );
        assert!(
            !has_metric(&registry, "http_requests"),
            "HTTP-family layer must not record gRPC requests"
        );
    }

    #[test]
    fn http_layer_records_http_requests() {
        let registry = drive(
            ServiceProtocol::Http,
            MetricFamily::HttpServer,
            http_request(),
        );
        assert!(has_metric(&registry, "http_requests"));
    }

    #[test]
    fn grpc_layer_records_grpc_requests() {
        let registry = drive(
            ServiceProtocol::Grpc,
            MetricFamily::GrpcServer,
            grpc_request(),
        );
        assert!(has_metric(&registry, "grpc_requests"));
    }

    #[test]
    fn request_protocol_detection() {
        assert_eq!(request_protocol(&http_request()), ServiceProtocol::Http);
        assert_eq!(request_protocol(&grpc_request()), ServiceProtocol::Grpc);
        // grpc content-type without HTTP/2 is not gRPC
        let req = Request::builder()
            .header(http::header::CONTENT_TYPE, "application/grpc")
            .body(EmptyBody)
            .unwrap();
        assert_eq!(request_protocol(&req), ServiceProtocol::Http);

        // Content-Type is case-insensitive; a gRPC subtype (+proto) or
        // media-type parameter (;) still counts.
        for content_type in [
            "APPLICATION/GRPC",
            "Application/gRPC+proto",
            "application/grpc; charset=utf-8",
        ] {
            let req = Request::builder()
                .method(Method::POST)
                .version(http::Version::HTTP_2)
                .header(http::header::CONTENT_TYPE, content_type)
                .body(EmptyBody)
                .unwrap();
            assert_eq!(
                request_protocol(&req),
                ServiceProtocol::Grpc,
                "content-type {content_type:?} should be detected as gRPC"
            );
        }

        // Types that merely share the `application/grpc` prefix are not gRPC:
        // grpc-web is a distinct framing tonic cannot decode.
        for content_type in ["application/grpc-web", "application/grpc-web+proto"] {
            let req = Request::builder()
                .method(Method::POST)
                .version(http::Version::HTTP_2)
                .header(http::header::CONTENT_TYPE, content_type)
                .body(EmptyBody)
                .unwrap();
            assert_eq!(
                request_protocol(&req),
                ServiceProtocol::Http,
                "content-type {content_type:?} must not be detected as gRPC"
            );
        }
    }

    /// Yields `frames` data frames, then end-of-stream.
    #[derive(Debug)]
    struct DataBody {
        frames: usize,
    }

    impl http_body::Body for DataBody {
        type Data = bytes::Bytes;
        type Error = Infallible;

        fn poll_frame(
            mut self: Pin<&mut Self>,
            _cx: &mut Context<'_>,
        ) -> Poll<Option<Result<Frame<Self::Data>, Self::Error>>> {
            if self.frames == 0 {
                return Poll::Ready(None);
            }
            self.frames -= 1;
            Poll::Ready(Some(Ok(Frame::data(bytes::Bytes::from_static(b"abcd")))))
        }

        fn is_end_stream(&self) -> bool {
            self.frames == 0
        }
    }

    #[derive(Debug, Clone)]
    struct DataService;

    impl Service<Request<EmptyBody>> for DataService {
        type Response = Response<DataBody>;
        type Error = Infallible;
        type Future = std::future::Ready<Result<Self::Response, Self::Error>>;

        fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
            Poll::Ready(Ok(()))
        }

        fn call(&mut self, _req: Request<EmptyBody>) -> Self::Future {
            std::future::ready(Ok(Response::new(DataBody { frames: 2 })))
        }
    }

    /// Drive a two-frame response body to completion, returning the registry.
    fn drive_data_body() -> Arc<metric::Registry> {
        let registry = Arc::new(metric::Registry::new());
        let metrics = Arc::new(RequestMetrics::new(
            Arc::clone(&registry),
            MetricFamily::HttpServer,
        ));
        let layer = TraceLayer::new(
            TraceHeaderParser::new(),
            metrics,
            None,
            "test",
            ServiceProtocol::Http,
        );
        let mut service = layer.layer(DataService);

        let response = futures::executor::block_on(service.call(http_request())).unwrap();
        let mut body = Box::pin(response.into_body());
        futures::executor::block_on(futures::future::poll_fn(|cx| {
            loop {
                match body.as_mut().poll_frame(cx) {
                    Poll::Ready(Some(Ok(_))) => continue,
                    Poll::Ready(Some(Err(_))) => unreachable!("DataBody is infallible"),
                    Poll::Ready(None) => return Poll::Ready(()),
                    Poll::Pending => return Poll::Pending,
                }
            }
        }));
        drop(body);

        registry
    }

    /// `handle_data` is reached only by a response body that yields data
    /// frames, which the `EmptyBody` harness never does, leaving
    /// `add_response_body_size` and its accumulation across frames uncovered.
    #[test]
    fn body_data_frames_are_recorded() {
        let registry = drive_data_body();
        let mut reporter = RawReporter::default();
        registry.report(&mut reporter);

        let requests = reporter.metric("http_requests").unwrap();
        assert_eq!(
            requests.observation(&[
                ("method", "GET"),
                ("method_path", "GET /health"),
                ("path", "/health"),
                ("status", "ok"),
            ]),
            Some(&Observation::U64Counter(1)),
            "end-of-stream should classify the request ok: {:?}",
            requests.observations
        );

        let sizes = reporter.metric("http_response_body_size_bytes").unwrap();
        let observation = sizes
            .observation(&[
                ("method", "GET"),
                ("method_path", "GET /health"),
                ("path", "/health"),
                ("status", "ok"),
            ])
            .expect("body size should be recorded against the ok series");
        let Observation::U64Histogram(histogram) = observation else {
            panic!("expected a histogram, got {observation:?}");
        };
        assert_eq!(histogram.total, 8, "two 4-byte frames should accumulate");
        assert_eq!(histogram.sample_count(), 1, "one observation per request");
    }
}
