use base64::prelude::BASE64_STANDARD;
use base64::Engine;
use bytes::Bytes;
use http::header::AsHeaderName;
use http::uri::Authority;
use http::HeaderMap;
use http_body::Body as HttpBody;
use hyper::Body;
use pin_project::pin_project;
use std::marker::PhantomData;
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::{Duration, SystemTime};
use tonic::Code;
use tonic::{body::BoxBody, Status};
use tower::{Layer, Service};

use crate::sink::ErrorLogger;
use crate::{proto, sink::NopErrorLogger, NoReflection, Predicate, Sink};

/// Intercepts all gRPC frames, builds gRPC log entries and sends them to a [`Sink`].
#[derive(Debug, Default, Clone)]
pub struct BinaryLoggerLayer<K, P = NoReflection, L = NopErrorLogger>
where
    K: Sink + Send + Sync,
    L: ErrorLogger<K::Error>,
{
    sink: Arc<K>,
    predicate: P,
    error_logger: L,
}

impl<K> BinaryLoggerLayer<K, NoReflection, NopErrorLogger>
where
    K: Sink + Send + Sync,
{
    /// Creates a new binary logger layer with the default predicate that
    /// logs everything except gRPC reflection requests
    pub fn new(sink: K) -> Self {
        Self {
            sink: Arc::new(sink),
            predicate: Default::default(),
            error_logger: NopErrorLogger,
        }
    }
}

impl<K, P, L> BinaryLoggerLayer<K, P, L>
where
    K: Sink + Send + Sync,
    P: Predicate,
    L: ErrorLogger<K::Error>,
{
    /// Builds a new binary logger layer with the provided predicate.
    pub fn with_predicate<P2: Predicate>(self, predicate: P2) -> BinaryLoggerLayer<K, P2, L> {
        BinaryLoggerLayer {
            sink: self.sink,
            predicate,
            error_logger: self.error_logger,
        }
    }

    /// Builds a new binary logger layer with the provided error logger.
    pub fn with_error_logger<L2: ErrorLogger<K::Error>>(
        self,
        error_logger: L2,
    ) -> BinaryLoggerLayer<K, P, L2> {
        BinaryLoggerLayer {
            sink: self.sink,
            predicate: self.predicate,
            error_logger,
        }
    }
}

impl<S, K, P, L> Layer<S> for BinaryLoggerLayer<K, P, L>
where
    P: Predicate + Send,
    K: Sink + Send + Sync + 'static,
    L: ErrorLogger<K::Error> + 'static,
{
    type Service = BinaryLoggerMiddleware<S, K, P, L>;

    fn layer(&self, service: S) -> Self::Service {
        BinaryLoggerMiddleware::new(
            service,
            Arc::clone(&self.sink),
            self.predicate.clone(),
            self.error_logger.clone(),
        )
    }
}

#[derive(Debug, Clone)]
pub struct BinaryLoggerMiddleware<S, K, P, L>
where
    K: Sink + Send + Sync,
    L: ErrorLogger<K::Error>,
{
    sink: Arc<K>,
    inner: S,
    predicate: P,
    error_logger: L,
    next_call_id: Arc<AtomicU64>,
}

impl<S, K, P, L> BinaryLoggerMiddleware<S, K, P, L>
where
    K: Sink + Send + Sync,
    P: Predicate + Send,
    L: ErrorLogger<K::Error>,
{
    fn new(inner: S, sink: Arc<K>, predicate: P, error_logger: L) -> Self {
        Self {
            sink,
            inner,
            predicate,
            error_logger,
            next_call_id: Arc::new(AtomicU64::new(1)),
        }
    }

    fn next_call_id(&self) -> u64 {
        self.next_call_id.fetch_add(1, Ordering::SeqCst)
    }
}

impl<S, K, P, L> Service<hyper::Request<Body>> for BinaryLoggerMiddleware<S, K, P, L>
where
    S: Service<hyper::Request<Body>, Response = hyper::Response<BoxBody>> + Clone + Send + 'static,
    S::Future: Send + 'static,
    K: Sink + Send + Sync + 'static,
    P: Predicate + Send,
    L: ErrorLogger<K::Error> + 'static,
{
    type Response = S::Response;
    type Error = S::Error;
    type Future = futures::future::BoxFuture<'static, Result<Self::Response, Self::Error>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, request: hyper::Request<Body>) -> Self::Future {
        // This is necessary because tonic internally uses `tower::buffer::Buffer`.
        // See https://github.com/tower-rs/tower/issues/547#issuecomment-767629149
        // for details on why this is necessary
        let clone = self.inner.clone();
        let mut inner = std::mem::replace(&mut self.inner, clone);

        if !self.predicate.should_log(&request) {
            Box::pin(async move { inner.call(request).await })
        } else {
            let call = CallLogger::new(
                self.next_call_id(),
                Arc::clone(&self.sink),
                self.error_logger.clone(),
            );
            Box::pin(async move {
                let uri = request.uri();
                call.log(LogEntry::ClientHeaders {
                    method: uri.path(),
                    authority: uri.authority(),
                    headers: request.headers(),
                });

                // wrap our logger around the request stream.
                let request = Self::logged_request(request, call.clone());

                // Perform the actual request.
                // When a handler returns an error, we get an `Ok(Response(...))` here.
                // TODO(mkm): figure out what's the right way to log an error when we get `Err` here
                let response = inner.call(request).await?;

                // wrap our logger around the response stream.
                Ok(Self::logged_response(response, call))
            })
        }
    }
}

impl<S, K, P, L> BinaryLoggerMiddleware<S, K, P, L>
where
    K: Sink + Send + Sync + 'static,
    L: ErrorLogger<K::Error> + 'static,
{
    fn logged_request(req: hyper::Request<Body>, call: CallLogger<K, L>) -> hyper::Request<Body> {
        let (req_parts, mut req_body) = req.into_parts();

        // We *must* return a Request<Body> because that's what `inner` requires.
        // `Body` is not a trait though, so we cannot wrap it and passively log as
        // tonic consumes bytes from the client connection. Instead we have to construct
        // a `Body` with one of its public constructors. We can create a body that
        // produces its data as obtained asynchronously from a channel.
        // We spawn a task that reads from the request body and forwards bytes to the
        // inner request. While we're streaming data we determine gRPC message boundaries
        // and log messages.
        let (mut sender, client_body) = hyper::Body::channel();
        tokio::spawn(async move {
            while let Some(buf) = req_body.data().await {
                match buf {
                    Ok(buf) => {
                        // `data` returns an actual gRPC frames, even if it has been sent
                        // over multiple tcp segments and over multiple HTTP/2 DATA frames.

                        // TODO(mkm): figure out why the client produces a zero length chunk here.
                        // Ignoring it seems to be the right thing to do.
                        if !buf.is_empty() {
                            call.log(LogEntry::ClientMessage(&buf));
                        }
                        if sender.send_data(buf).await.is_err() {
                            // TODO(mkm): figure out how to log this kind of error, if any.
                            // The Go gRPC framework seems to either log a frame if successful
                            // or just not log it.
                            sender.abort();
                            return;
                        }
                    }
                    Err(_err) => {
                        // TODO(mkm): figure out how to log this kind of error, if any.
                        // The Go gRPC framework seems to either log a frame if successful
                        // or just not log it.
                        sender.abort();
                        return;
                    }
                }
            }
            // gRPC doesn't use client trailers, but let's forward them nevertheless for completeness.
            match req_body.trailers().await {
                Ok(Some(trailers)) => {
                    if sender.send_trailers(trailers).await.is_err() {
                        // TODO(mkm): figure out how to log this kind of error, if any.
                        // The Go gRPC framework seems to either log a frame if successful
                        // or just not log it.
                        sender.abort();
                    }
                }
                Err(_err) => {
                    // TODO(mkm): figure out how to log this kind of error, if any.
                    // The Go gRPC framework seems to either log a frame if successful
                    // or just not log it.
                    sender.abort();
                }
                _ => {}
            };
        });
        hyper::Request::from_parts(req_parts, client_body)
    }

    fn logged_response(
        response: hyper::Response<BoxBody>,
        call: CallLogger<K, L>,
    ) -> hyper::Response<BoxBody> {
        let (parts, inner) = response.into_parts();
        let body = BoxBody::new(BinaryLoggingBody {
            inner,
            headers: parts.headers.clone(),
            call: call.clone(),
            _phantom_error_logger: PhantomData::default(),
        });
        call.log(LogEntry::ServerHeaders(&parts.headers));
        if body.is_end_stream() {
            // When an grpc call doesn't produce any results (either because the result type
            // is empty or because it returns an error immediately), the BinaryLoggingBody
            // won't be able to log anything. We have to log the event here.
            call.log(LogEntry::ServerTrailers(&parts.headers));
        }
        hyper::Response::from_parts(parts, body)
    }
}

#[pin_project]
struct BinaryLoggingBody<K, L>
where
    K: Sink + Send + Sync,
    L: ErrorLogger<K::Error>,
{
    #[pin]
    inner: BoxBody,
    headers: HeaderMap,
    call: CallLogger<K, L>,
    _phantom_error_logger: PhantomData<L>,
}

impl<K, L> HttpBody for BinaryLoggingBody<K, L>
where
    K: Sink + Send + Sync,
    L: ErrorLogger<K::Error>,
{
    type Data = bytes::Bytes;

    type Error = Status;

    fn poll_data(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Self::Data, Self::Error>>> {
        let call = self.call.clone();
        let data = self.project().inner.poll_data(cx);
        if let Poll::Ready(Some(Ok(ref body))) = data {
            call.log(LogEntry::ServerMessage(body));
        }
        data
    }

    fn poll_trailers(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Result<Option<http::HeaderMap>, Self::Error>> {
        let call = self.call.clone();
        let trailers = self.project().inner.poll_trailers(cx);
        if let Poll::Ready(Ok(Some(ref headers))) = trailers {
            call.log(LogEntry::ServerTrailers(headers));
        }
        trailers
    }

    fn size_hint(&self) -> http_body::SizeHint {
        self.inner.size_hint()
    }

    fn is_end_stream(&self) -> bool {
        self.inner.is_end_stream()
    }
}

enum LogEntry<'a> {
    ClientHeaders {
        method: &'a str,
        authority: Option<&'a Authority>,
        headers: &'a HeaderMap,
    },
    ClientMessage(&'a Bytes),
    ServerHeaders(&'a HeaderMap),
    ServerMessage(&'a Bytes),
    ServerTrailers(&'a HeaderMap),
}

#[derive(Clone)]
struct CallLogger<K, L>
where
    K: Sink + Send + Sync,
    L: ErrorLogger<K::Error>,
{
    call_id: u64,
    sequence: Arc<AtomicU64>,
    sink: Arc<K>,
    error_logger: L,
}

impl<K, L> CallLogger<K, L>
where
    K: Sink + Send + Sync,
    L: ErrorLogger<K::Error>,
{
    fn new(call_id: u64, sink: Arc<K>, error_logger: L) -> Self {
        Self {
            call_id,
            sequence: Arc::new(AtomicU64::new(1)),
            sink,
            error_logger,
        }
    }
    fn log(&self, entry: LogEntry<'_>) {
        let sequence_id_within_call = self.sequence.fetch_add(1, Ordering::SeqCst);

        let common_entry = proto::GrpcLogEntry {
            timestamp: Some(SystemTime::now().into()),
            call_id: self.call_id,
            sequence_id_within_call,
            logger: proto::grpc_log_entry::Logger::Server as i32,
            ..Default::default()
        };

        let log_entry = match entry {
            LogEntry::ClientHeaders {
                method,
                authority,
                headers,
            } => {
                let timeout = headers.grpc_timeout().map(|t| t.try_into().unwrap());
                proto::GrpcLogEntry {
                    r#type: proto::grpc_log_entry::EventType::ClientHeader as i32,
                    payload: Some(proto::grpc_log_entry::Payload::ClientHeader(
                        proto::ClientHeader {
                            method_name: method.to_string(),
                            metadata: Some(Self::metadata(headers)),
                            authority: authority
                                .map(|a| a.as_str())
                                .unwrap_or_default()
                                .to_string(),
                            timeout,
                        },
                    )),
                    ..common_entry
                }
            }
            LogEntry::ClientMessage(body) => proto::GrpcLogEntry {
                r#type: proto::grpc_log_entry::EventType::ClientMessage as i32,
                payload: Some(Self::message(body)),
                ..common_entry
            },
            LogEntry::ServerHeaders(headers) => proto::GrpcLogEntry {
                r#type: proto::grpc_log_entry::EventType::ServerHeader as i32,
                payload: Some(proto::grpc_log_entry::Payload::ServerHeader(
                    proto::ServerHeader {
                        metadata: Some(Self::metadata(headers)),
                    },
                )),
                ..common_entry
            },
            LogEntry::ServerMessage(body) => proto::GrpcLogEntry {
                r#type: proto::grpc_log_entry::EventType::ServerMessage as i32,
                payload: Some(Self::message(body)),
                ..common_entry
            },
            LogEntry::ServerTrailers(headers) => proto::GrpcLogEntry {
                r#type: proto::grpc_log_entry::EventType::ServerTrailer as i32,
                payload: Some(proto::grpc_log_entry::Payload::Trailer(proto::Trailer {
                    status_code: headers.grpc_status() as u32,
                    status_message: headers.grpc_message().to_string(),
                    metadata: Some(Self::metadata(headers)),
                    status_details: headers.grpc_status_details(),
                })),
                ..common_entry
            },
        };
        self.sink.write(log_entry, self.error_logger.clone());
    }

    fn message(bytes: &Bytes) -> proto::grpc_log_entry::Payload {
        let compressed = bytes[0] == 1;
        if compressed {
            unimplemented!("grpc compressed messages");
        }

        const COMPRESSED_FLAG_FIELD_LEN: usize = 1;
        const MESSAGE_LENGTH_FIELD_LEN: usize = 4;
        let data = bytes
            .clone() // cheap
            .into_iter()
            .skip(COMPRESSED_FLAG_FIELD_LEN + MESSAGE_LENGTH_FIELD_LEN)
            .collect::<Vec<_>>();

        proto::grpc_log_entry::Payload::Message(proto::Message {
            length: data.len() as u32,
            data,
        })
    }

    fn metadata(headers: &HeaderMap) -> proto::Metadata {
        proto::Metadata {
            entry: headers
                .iter()
                .filter(|&(key, _)| !is_reserved_header(key))
                .map(|(key, value)| proto::MetadataEntry {
                    key: key.to_string(),
                    value: value.as_bytes().to_vec(),
                })
                .collect(),
        }
    }
}

/// As defined in [binarylog.proto](https://github.com/grpc/grpc-proto/blob/master/grpc/binlog/v1/binarylog.proto)
fn is_reserved_header<K>(key: K) -> bool
where
    K: AsHeaderName,
{
    let key = key.as_str();
    match key {
        "grpc-trace-bin" => false, // this is the only "grpc-" prefixed header that is not ignored.
        "te" | "content-type" | "content-length" | "content-encoding" | "user-agent" => true,
        _ if key.starts_with("grpc-") => true,
        _ => false,
    }
}

trait GrpcHeaderExt {
    fn grpc_message(&self) -> &str;
    fn grpc_status(&self) -> Code;
    fn grpc_status_details(&self) -> Vec<u8>;
    fn grpc_timeout(&self) -> Option<Duration>;

    fn get_grpc_header<K>(&self, key: K) -> Option<&str>
    where
        K: AsHeaderName;
}

impl GrpcHeaderExt for HeaderMap {
    fn grpc_message(&self) -> &str {
        self.get_grpc_header("grpc-message").unwrap_or_default()
    }

    fn grpc_status(&self) -> Code {
        self.get_grpc_header("grpc-status")
            .map(|s| s.as_bytes())
            .map(Code::from_bytes)
            .unwrap_or(Code::Unknown)
    }

    fn grpc_status_details(&self) -> Vec<u8> {
        self.get_grpc_header("grpc-status-details-bin")
            .and_then(|v| BASE64_STANDARD.decode(v).ok())
            .unwrap_or_default()
    }

    /// Extract a gRPC timeout from the `grpc-timeout` header.
    /// Returns None if the header is not present or not valid.
    fn grpc_timeout(&self) -> Option<Duration> {
        self.get_grpc_header("grpc-timeout")
            .and_then(parse_grpc_timeout)
    }

    fn get_grpc_header<K>(&self, key: K) -> Option<&str>
    where
        K: AsHeaderName,
    {
        self.get(key).and_then(|s| s.to_str().ok())
    }
}

/// Parse a gRPC "Timeout" format (see [gRPC over HTTP2]).
/// Returns None if it cannot parse the format.
///
/// [gRPC over HTTP2]: https://github.com/grpc/grpc/blob/master/doc/PROTOCOL-HTTP2.md
fn parse_grpc_timeout(header_value: &str) -> Option<Duration> {
    if header_value.is_empty() {
        return None;
    }
    let (digits, unit) = header_value.split_at(header_value.len() - 1);
    let timeout: u64 = digits.parse().ok()?;
    match unit {
        "H" => Some(Duration::from_secs(timeout * 60 * 60)),
        "M" => Some(Duration::from_secs(timeout * 60)),
        "S" => Some(Duration::from_secs(timeout)),
        "m" => Some(Duration::from_millis(timeout)),
        "u" => Some(Duration::from_micros(timeout)),
        "n" => Some(Duration::from_nanos(timeout)),
        _ => None,
    }
}
