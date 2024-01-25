//! Server for the cache HTTP API

use crate::api::list::{ListEncoder, ListEntry};
use crate::api::{RequestPath, GENERATION};
use crate::local::CatalogCache;
use crate::CacheValue;
use futures::ready;
use hyper::body::HttpBody;
use hyper::header::ToStrError;
use hyper::http::request::Parts;
use hyper::service::Service;
use hyper::{Body, Method, Request, Response, StatusCode};
use snafu::{OptionExt, ResultExt, Snafu};
use std::convert::Infallible;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

#[derive(Debug, Snafu)]
#[allow(missing_docs)]
enum Error {
    #[snafu(display("Http error: {source}"), context(false))]
    Http { source: hyper::http::Error },

    #[snafu(display("Hyper error: {source}"), context(false))]
    Hyper { source: hyper::Error },

    #[snafu(display("Local cache error: {source}"), context(false))]
    Local { source: crate::local::Error },

    #[snafu(display("Non UTF-8 Header: {source}"))]
    BadHeader { source: ToStrError },

    #[snafu(display("Request missing generation header"))]
    MissingGeneration,

    #[snafu(display("Invalid generation header: {source}"))]
    InvalidGeneration { source: std::num::ParseIntError },

    #[snafu(display("List query missing size"))]
    MissingSize,

    #[snafu(display("List query invalid size: {source}"))]
    InvalidSize { source: std::num::ParseIntError },
}

impl Error {
    /// Convert an error into a [`Response`]
    fn response(self) -> Response<Body> {
        let mut response = Response::new(Body::from(self.to_string()));
        *response.status_mut() = match &self {
            Self::Http { .. } | Self::Hyper { .. } | Self::Local { .. } => {
                StatusCode::INTERNAL_SERVER_ERROR
            }
            Self::InvalidGeneration { .. }
            | Self::MissingGeneration
            | Self::InvalidSize { .. }
            | Self::MissingSize
            | Self::BadHeader { .. } => StatusCode::BAD_REQUEST,
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

impl Service<Request<Body>> for CatalogCacheService {
    type Response = Response<Body>;

    type Error = Infallible;
    type Future = CatalogRequestFuture;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Infallible>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, req: Request<Body>) -> Self::Future {
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
    body: Body,
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
    type Output = Result<Response<Body>, Infallible>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let r = loop {
            match ready!(Pin::new(&mut self.body).poll_data(cx)) {
                Some(Ok(b)) => self.buffer.extend_from_slice(&b),
                Some(Err(e)) => break Err(e.into()),
                None => break Ok(()),
            }
        };
        Poll::Ready(Ok(match r.and_then(|_| self.call()) {
            Ok(resp) => resp,
            Err(e) => e.response(),
        }))
    }
}

impl CatalogRequestFuture {
    fn call(&mut self) -> Result<Response<Body>, Error> {
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
                    let encoder = ListEncoder::new(entries).with_max_value_size(size);

                    let stream = futures::stream::iter(encoder.map(Ok::<_, Error>));
                    let response = Response::builder().body(Body::wrap_stream(stream))?;
                    return Ok(response);
                }
                _ => StatusCode::METHOD_NOT_ALLOWED,
            },
            Some(RequestPath::Resource(key)) => match self.parts.method {
                Method::GET => match self.state.cache.get(key) {
                    Some(value) => {
                        let response = Response::builder()
                            .header(&GENERATION, value.generation)
                            .body(value.data.into())?;
                        return Ok(response);
                    }
                    None => StatusCode::NOT_FOUND,
                },
                Method::PUT => {
                    let headers = &self.parts.headers;
                    let generation = headers.get(&GENERATION).context(MissingGenerationSnafu)?;
                    let generation = generation.to_str().context(BadHeaderSnafu)?;
                    let generation = generation.parse().context(InvalidGenerationSnafu)?;
                    let value = CacheValue::new(body.into(), generation);

                    match self.state.cache.insert(key, value)? {
                        true => StatusCode::OK,
                        false => StatusCode::NOT_MODIFIED,
                    }
                }
                Method::DELETE => {
                    self.state.cache.delete(key);
                    StatusCode::OK
                }
                _ => StatusCode::METHOD_NOT_ALLOWED,
            },
            None => StatusCode::NOT_FOUND,
        };

        let mut response = Response::new(Body::empty());
        *response.status_mut() = status;
        Ok(response)
    }
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

/// Test utilities.
pub mod test_util {
    use std::{net::SocketAddr, ops::Deref};

    use hyper::{service::make_service_fn, Server};
    use tokio::task::JoinHandle;
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
    }

    impl TestCacheServer {
        /// Create a new [`TestCacheServer`] bound to an ephemeral port
        pub fn bind_ephemeral() -> Self {
            Self::bind(&SocketAddr::from(([127, 0, 0, 1], 0)))
        }

        /// Create a new [`CatalogCacheServer`] bound to the provided [`SocketAddr`]
        pub fn bind(addr: &SocketAddr) -> Self {
            let server = CatalogCacheServer::new(Arc::new(CatalogCache::default()));
            let service = server.service();
            let make_service = make_service_fn(move |_conn| {
                futures::future::ready(Ok::<_, Infallible>(service.clone()))
            });

            let hyper_server = Server::bind(addr).serve(make_service);
            let addr = hyper_server.local_addr();

            let shutdown = CancellationToken::new();
            let signal = shutdown.clone().cancelled_owned();
            let graceful = hyper_server.with_graceful_shutdown(signal);
            let handle = Some(tokio::spawn(async move { graceful.await.unwrap() }));

            Self {
                addr,
                server,
                shutdown,
                handle,
            }
        }

        /// Returns a [`CatalogCacheClient`] for communicating with this server
        pub fn client(&self) -> CatalogCacheClient {
            let addr = format!("http://{}", self.addr);
            CatalogCacheClient::try_new(addr.parse().unwrap()).unwrap()
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
