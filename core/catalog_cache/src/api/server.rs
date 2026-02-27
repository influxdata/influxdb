//! Server for the cache HTTP API

use crate::CacheValue;
use crate::api::list::{ListEntry, v2};
use crate::api::{GENERATION, GENERATION_NOT_MATCH, LIST_PROTOCOL_V2, NO_VALUE, RequestPath};
use crate::local::CatalogCache;
use bytes::{Bytes, BytesMut};
use futures::stream::FusedStream;
use futures::{Stream, ready};
use hyper::header::{ACCEPT, CONTENT_TYPE, ETAG, HeaderValue, IF_NONE_MATCH, ToStrError};
use hyper::http::request::Parts;
use hyper::service::Service;
use hyper::{HeaderMap, Method, StatusCode};
use iox_http_util::{
    Body, BoxError, Request, RequestBody, Response, ResponseBuilder, bytes_to_response_body,
    empty_response_body, stream_bytes_to_response_body,
};
use snafu::{OptionExt, ResultExt, Snafu};
use std::convert::Infallible;
use std::fmt;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

#[derive(Debug, Snafu)]
enum Error {
    #[snafu(display("Http error: {source}"), context(false))]
    Http { source: hyper::http::Error },

    #[snafu(display("Hyper error: {source}"))]
    Hyper { source: BoxError },

    #[snafu(display("Local cache error: {source}"), context(false))]
    Local { source: crate::local::Error },

    #[snafu(display("Non UTF-8 Header: {source}"))]
    BadHeader { source: ToStrError },

    #[snafu(display("Request missing generation header"))]
    MissingGeneration,

    #[snafu(display("Invalid generation header: {source}"))]
    InvalidGeneration { source: std::num::ParseIntError },

    #[snafu(display("Invalid etag header: {source}"))]
    InvalidEtag { source: ToStrError },

    #[snafu(display("List query missing size"))]
    MissingSize,

    #[snafu(display("List query invalid size: {source}"))]
    InvalidSize { source: std::num::ParseIntError },

    #[snafu(display("Client indicates unsupported LIST protocol version: {version}"))]
    UnsupportedListProtocol { version: String },
}

impl Error {
    /// Convert an error into a [`Response`]
    fn response(self) -> Response {
        let mut response = Response::new(bytes_to_response_body(self.to_string()));
        *response.status_mut() = match &self {
            Self::Http { .. } | Self::Hyper { .. } | Self::Local { .. } => {
                StatusCode::INTERNAL_SERVER_ERROR
            }
            Self::InvalidGeneration { .. }
            | Self::MissingGeneration
            | Self::InvalidSize { .. }
            | Self::InvalidEtag { .. }
            | Self::MissingSize
            | Self::BadHeader { .. } => StatusCode::BAD_REQUEST,
            Self::UnsupportedListProtocol { .. } => StatusCode::NOT_IMPLEMENTED,
        };
        response
    }
}

/// A [`Service`] that wraps a [`CatalogCache`]
#[derive(Debug, Clone)]
pub struct CatalogCacheService(Arc<ServiceState>);

/// Shared state for [`CatalogCacheService`]
#[derive(Debug)]
struct ServiceState {
    cache: Arc<CatalogCache>,
}

impl Service<Request> for CatalogCacheService {
    type Response = Response;

    type Error = Infallible; // errors should already be converted to Response
    type Future = CatalogRequestFuture;

    fn call(&self, req: Request) -> Self::Future {
        let (parts, body) = req.into_parts();
        CatalogRequestFuture {
            parts,
            body,
            buffer: vec![],
            state: Arc::clone(&self.0),
        }
    }
}

/// The future for [`CatalogCacheService`]
#[derive(Debug)]
pub struct CatalogRequestFuture {
    /// The request body
    body: RequestBody,
    /// The request parts
    parts: Parts,
    /// The in-progress body
    ///
    /// We use Vec not Bytes to ensure the cache isn't storing slices of large allocations
    buffer: Vec<u8>,
    /// The cache to service requests
    state: Arc<ServiceState>,
}

impl Future for CatalogRequestFuture {
    type Output = Result<Response, Infallible>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let r = loop {
            match ready!(Pin::new(&mut self.body).poll_frame(cx)) {
                Some(Ok(b)) => self
                    .buffer
                    .extend_from_slice(b.data_ref().unwrap_or(&Bytes::default())),
                Some(Err(e)) => break Err(e),
                None => break Ok(()),
            }
        };
        Poll::Ready(match r.context(HyperSnafu).and_then(|_| self.call()) {
            Ok(resp) => Ok(resp),
            Err(e) => Ok(e.response()),
        })
    }
}

impl CatalogRequestFuture {
    fn call(&mut self) -> Result<Response, Error> {
        let body = std::mem::take(&mut self.buffer);

        let status = match RequestPath::parse(self.parts.uri.path()) {
            Some(RequestPath::List) => match self.parts.method {
                Method::GET => {
                    let query = self.parts.uri.query().context(MissingSizeSnafu)?;
                    let mut parts = url::form_urlencoded::parse(query.as_bytes());
                    let (_, size) = parts.find(|(k, _)| k == "size").context(MissingSizeSnafu)?;
                    let size = size.parse().context(InvalidSizeSnafu)?;

                    let iter = self.state.cache.list();
                    let entries = iter.map(|(k, v)| ListEntry::new(k, v)).collect();

                    match self.parts.headers.get(ACCEPT) {
                        Some(x) if x == LIST_PROTOCOL_V2 => {
                            let encoder = v2::ListEncoder::new(entries).with_max_value_size(size);
                            let stream = futures::stream::iter(encoder);
                            let stream = BatchedBytesStream::new(stream, size);
                            let response = ResponseBuilder::new()
                                .header(CONTENT_TYPE, &LIST_PROTOCOL_V2)
                                .body(stream_bytes_to_response_body(stream))?;
                            return Ok(response);
                        }
                        other => {
                            return Err(Error::UnsupportedListProtocol {
                                version: other
                                    .map(|h| String::from_utf8_lossy(h.as_bytes()).into_owned())
                                    .unwrap_or_else(|| "<none>".to_owned()),
                            });
                        }
                    }
                }
                _ => StatusCode::METHOD_NOT_ALLOWED,
            },
            Some(RequestPath::Resource(key)) => match self.parts.method {
                Method::GET => match self.state.cache.get(key) {
                    Some(value) => {
                        let mut builder =
                            ResponseBuilder::new().header(&GENERATION, value.generation);
                        if let Some(x) = &value.etag {
                            builder = builder.header(ETAG, x.as_ref())
                        }

                        return Ok(match check_preconditions(&value, &self.parts.headers)? {
                            Some(s) => builder.status(s).body(empty_response_body())?,
                            None => match value.data {
                                Some(bytes) => builder.body(bytes_to_response_body(bytes))?,
                                None => builder
                                    .header(&NO_VALUE, "true")
                                    .body(empty_response_body())?,
                            },
                        });
                    }
                    None => StatusCode::NOT_FOUND,
                },
                Method::PUT => {
                    let headers = &self.parts.headers;
                    let generation = parse_generation(
                        headers.get(&GENERATION).context(MissingGenerationSnafu)?,
                    )?;
                    let no_value = headers
                        .get(&NO_VALUE)
                        .map(|v| v.to_str().unwrap_or("false") == "true")
                        .unwrap_or(false);

                    let mut value = if no_value {
                        CacheValue::new_empty(generation)
                    } else {
                        CacheValue::new(body.into(), generation)
                    };

                    if let Some(x) = headers.get(ETAG) {
                        let etag = x.to_str().context(InvalidEtagSnafu)?.to_string();
                        value = value.with_etag(etag);
                    }

                    match self.state.cache.insert(key, value)? {
                        true => StatusCode::OK,
                        false => StatusCode::NOT_MODIFIED,
                    }
                }
                _ => StatusCode::METHOD_NOT_ALLOWED,
            },
            None => StatusCode::NOT_FOUND,
        };

        let mut response = Response::new(empty_response_body());
        *response.status_mut() = status;
        Ok(response)
    }
}

fn check_preconditions(
    value: &CacheValue,
    headers: &HeaderMap,
) -> Result<Option<StatusCode>, Error> {
    if let Some(v) = headers.get(&GENERATION_NOT_MATCH)
        && value.generation == parse_generation(v)?
    {
        return Ok(Some(StatusCode::NOT_MODIFIED));
    }
    if let Some(etag) = &value.etag
        && let Some(v) = headers.get(&IF_NONE_MATCH)
        && etag.as_bytes() == v.as_bytes()
    {
        return Ok(Some(StatusCode::NOT_MODIFIED));
    }

    Ok(None)
}

fn parse_generation(value: &HeaderValue) -> Result<u64, Error> {
    let generation = value.to_str().context(BadHeaderSnafu)?;
    generation.parse().context(InvalidGenerationSnafu)
}

/// Runs a [`CatalogCacheService`] in a background task
///
/// Will abort the background task on drop
#[derive(Debug)]
pub struct CatalogCacheServer {
    state: Arc<ServiceState>,
}

impl CatalogCacheServer {
    /// Create a new [`CatalogCacheServer`].
    ///
    /// Note that the HTTP interface needs to be wired up in some higher-level structure. Use [`service`](Self::service)
    /// for that.
    pub fn new(cache: Arc<CatalogCache>) -> Self {
        let state = Arc::new(ServiceState { cache });

        Self { state }
    }

    /// Returns HTTP service.
    pub fn service(&self) -> CatalogCacheService {
        CatalogCacheService(Arc::clone(&self.state))
    }

    /// Returns a reference to the [`CatalogCache`] of this server
    pub fn cache(&self) -> &Arc<CatalogCache> {
        &self.state.cache
    }
}

/// Stream that batches a number of small response elements into a
/// single, larger, buffer. The input values are never split and are
/// batched until whilst the combined size is below the given batch
/// size. This stream will only produce a value that is larger than the
/// batch size if one was returned from the inner stream.
#[derive(Debug)]
struct BatchedBytesStream<S: fmt::Debug> {
    inner: S,
    batch_size: usize,
    buf: Option<BytesMut>,
}

impl<S: fmt::Debug> BatchedBytesStream<S> {
    /// Create a new BatchedBytesStream wrapping inner.
    fn new(inner: S, batch_size: usize) -> Self {
        Self {
            inner,
            batch_size,
            buf: Some(BytesMut::with_capacity(batch_size)),
        }
    }
}

impl<S, B> Stream for BatchedBytesStream<S>
where
    B: AsRef<[u8]>,
    S: Stream<Item = B> + fmt::Debug + Send + Sync + Unpin,
{
    type Item = BytesMut;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        if this.buf.is_none() {
            return Poll::Ready(None);
        }
        loop {
            let s = Pin::new(&mut this.inner);
            match ready!(s.poll_next(cx)) {
                Some(b) => {
                    let b = b.as_ref();
                    let buf = this.buf.as_mut().unwrap();
                    if buf.len() + b.len() > this.batch_size {
                        let mut nbuf = BytesMut::with_capacity(this.batch_size);
                        nbuf.extend_from_slice(b);
                        return Poll::Ready(this.buf.replace(nbuf));
                    } else {
                        this.buf.as_mut().unwrap().extend_from_slice(b);
                    }
                }
                None => {
                    return Poll::Ready(
                        this.buf
                            .take()
                            .and_then(|buf| (!buf.is_empty()).then_some(buf)),
                    );
                }
            }
        }
    }
}

impl<S, B> FusedStream for BatchedBytesStream<S>
where
    B: AsRef<[u8]>,
    S: Stream<Item = B> + fmt::Debug + Send + Sync + Unpin,
{
    fn is_terminated(&self) -> bool {
        self.buf.is_none()
    }
}

/// Test utilities.
pub mod test_util {
    use std::{fmt::Debug, net::SocketAddr, ops::Deref};

    use hyper_util::{
        rt::{TokioExecutor, TokioIo},
        server::conn::auto::Builder,
        server::graceful::GracefulShutdown,
    };
    use iox_http_util::box_request;
    use tokio::{net::TcpListener, select, task::JoinHandle};
    use tokio_util::sync::CancellationToken;

    use crate::api::client::CatalogCacheClient;

    use super::*;

    /// Test runner for a [`CatalogCacheServer`].
    #[derive(Debug)]
    pub struct TestCacheServer {
        addr: SocketAddr,
        server: CatalogCacheServer,
        shutdown: CancellationToken,
        handle: Option<JoinHandle<()>>,
        metric_registry: Arc<metric::Registry>,
    }

    impl TestCacheServer {
        /// Create a new [`TestCacheServer`] bound to an ephemeral port
        pub async fn bind_ephemeral(metric_registry: &Arc<metric::Registry>) -> Self {
            Self::bind(&SocketAddr::from(([127, 0, 0, 1], 0)), metric_registry).await
        }

        /// Create a new [`CatalogCacheServer`] bound to the provided [`SocketAddr`]
        pub async fn bind(addr: &SocketAddr, metric_registry: &Arc<metric::Registry>) -> Self {
            let server = CatalogCacheServer::new(Arc::new(CatalogCache::default()));
            let service = server.service();

            // convert hyper::Incoming to the more generic BoxBody
            let service = hyper::service::service_fn::<_, hyper::body::Incoming, _>(
                move |request: hyper::Request<_>| service.call(box_request(request)),
            );

            let listener = TcpListener::bind(addr).await.unwrap();
            let addr = listener.local_addr().unwrap();

            // graceful shutdown guide: https://hyper.rs/guides/1/server/graceful-shutdown/
            let shutdown = CancellationToken::new();
            let signal = shutdown.clone().cancelled_owned();
            let graceful = GracefulShutdown::new();

            let handle = tokio::task::spawn(async move {
                tokio::pin!(signal);
                loop {
                    select! {
                        _ = signal.as_mut() => break,
                        res = listener.accept() => {
                            // server guide: https://hyper.rs/guides/1/server/hello-world/
                            // we use hyper_util's Builder to handle both http1 and http2

                            let (stream, _) = res.unwrap();
                            let service = service.clone();

                            let conn = Builder::new(TokioExecutor::new())
                                .serve_connection_with_upgrades(TokioIo::new(stream), service)
                                .into_owned();
                            let conn = graceful.watch(conn);

                            tokio::task::spawn(async move {
                                if let Err(err) = conn
                                    .await {
                                        println!("Error serving connection: {err:?}");
                                    };
                            });
                        },
                    }
                }
                graceful.shutdown().await;
            });

            Self {
                addr,
                server,
                shutdown,
                handle: Some(handle),
                metric_registry: Arc::clone(metric_registry),
            }
        }

        /// Returns a [`CatalogCacheClient`] for communicating with this server
        pub fn client(&self) -> CatalogCacheClient {
            // Use localhost to test DNS resolution
            let addr = format!("http://localhost:{}", self.addr.port());
            CatalogCacheClient::builder(addr.parse().unwrap(), Arc::clone(&self.metric_registry))
                .build()
                .unwrap()
        }

        /// Triggers and waits for graceful shutdown
        pub async fn shutdown(mut self) {
            self.shutdown.cancel();
            if let Some(x) = self.handle.take() {
                x.await.unwrap()
            }
        }
    }

    impl Deref for TestCacheServer {
        type Target = CatalogCacheServer;

        fn deref(&self) -> &Self::Target {
            &self.server
        }
    }

    impl Drop for TestCacheServer {
        fn drop(&mut self) {
            if let Some(x) = &self.handle {
                x.abort()
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use futures::stream::StreamExt;

    #[tokio::test]
    async fn test_batched_bytes_stream() {
        let bufs = [
            "1234567890",
            "1234567890",
            "1234567890",
            "1234567890",
            "1234567890",
            "1234567890",
        ];
        let mut stream = BatchedBytesStream::new(futures::stream::iter(&bufs), 55);

        assert_eq!(
            stream.next().await.unwrap().as_ref(),
            b"12345678901234567890123456789012345678901234567890"
        );
        assert_eq!(stream.next().await.unwrap().as_ref(), b"1234567890");
        assert!(stream.next().await.is_none());

        let bufs = [
            "1234567890",
            "123456789012345678901234567890",
            "1234567890",
            "1234567890",
            "12345",
        ];
        let mut stream = BatchedBytesStream::new(futures::stream::iter(&bufs), 25);

        assert_eq!(stream.next().await.unwrap().as_ref(), b"1234567890");
        assert_eq!(
            stream.next().await.unwrap().as_ref(),
            b"123456789012345678901234567890"
        );
        assert_eq!(
            stream.next().await.unwrap().as_ref(),
            b"1234567890123456789012345"
        );
        assert!(stream.next().await.is_none());
    }
}
