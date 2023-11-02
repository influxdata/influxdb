//! HTTP API service implementations for `server`

use std::convert::Infallible;
use std::fmt::Debug;
use std::net::SocketAddr;
use std::num::NonZeroI32;
use std::str::Utf8Error;
use std::sync::Arc;
use thiserror::Error;
use hyper::{header::CONTENT_ENCODING, Body, Method, Request, Response, StatusCode};
use hyper::http::HeaderValue;
use hyper::server::conn::{AddrIncoming, AddrStream};
use tokio_util::sync::CancellationToken;
use observability_deps::tracing::{debug, error, info};
use serde::Deserialize;
use tonic::async_trait;
use authz::http::AuthorizationHeaderExtension;
use tower::Layer;
use trace::TraceCollector;
use trace_http::ctx::TraceHeaderParser;
use trace_http::tower::TraceLayer;
use crate::{CommonServerState, QueryExecutor, WriteBuffer};

#[derive(Debug, Error)]
pub enum Error {
    /// The requested path has no registered handler.
    #[error("not found")]
    NoHandler,

    /// The request body content is not valid utf8.
    #[error("body content is not valid utf8: {0}")]
    NonUtf8Body(Utf8Error),

    /// The `Content-Encoding` header is invalid and cannot be read.
    #[error("invalid content-encoding header: {0}")]
    NonUtf8ContentHeader(hyper::header::ToStrError),

    /// The specified `Content-Encoding` is not acceptable.
    #[error("unacceptable content-encoding: {0}")]
    InvalidContentEncoding(String),

    /// The client disconnected.
    #[error("client disconnected")]
    ClientHangup(hyper::Error),

    /// The client sent a request body that exceeds the configured maximum.
    #[error("max request size ({0} bytes) exceeded")]
    RequestSizeExceeded(usize),

    /// Decoding a gzip-compressed stream of data failed.
    #[error("error decoding gzip stream: {0}")]
    InvalidGzip(std::io::Error),

    /// Failure to decode the provided line protocol.
    #[error("failed to parse line protocol: {0}")]
    ParseLineProtocol(influxdb_line_protocol::Error),

    /// The router is currently servicing the maximum permitted number of
    /// simultaneous requests.
    #[error("this service is overloaded, please try again later")]
    RequestLimit,

    /// The request has no authentication, but authorization is configured.
    #[error("authentication required")]
    Unauthenticated,

    /// The provided authorization is not sufficient to perform the request.
    #[error("access denied")]
    Forbidden,

    /// PProf support is not compiled
    #[error("pprof support is not compiled")]
    PProfIsNotCompiled,

    /// Heappy support is not compiled
    #[error("heappy support is not compiled")]
    HeappyIsNotCompiled,

    #[cfg(feature = "heappy")]
    #[error("heappy error: {0}")]
    Heappy(heappy::Error),

    /// Hyper serving error
    #[error("error serving http: {0}")]
    ServingHttp(#[from] hyper::Error),
}

impl Error {
    fn response(&self) -> Response<Body> {
        let body = Body::from(self.to_string());
        Response::builder()
            .status(StatusCode::INTERNAL_SERVER_ERROR)
            .body(body)
            .unwrap()
    }
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

const TRACE_SERVER_NAME: &str = "http_api";

#[derive(Debug)]
pub(crate) struct HttpApi<W, Q> {
    common_state: CommonServerState,
    write_buffer: Arc<W>,
    query_executor: Arc<Q>,
}

impl<W, Q> HttpApi<W, Q> {
    pub(crate) fn new(common_state: CommonServerState, write_buffer: Arc<W>, query_executor: Arc<Q>) -> Self {
        Self {
            common_state,
            write_buffer,
            query_executor,
        }
    }
}

impl<W, Q> HttpApi<W, Q>
where
    W: WriteBuffer,
    Q: QueryExecutor,
{

    async fn write_lp(&self, req: Request<Body>) -> Result<Response<Body>> {
        let response_body = "write ok";
        Ok(Response::new(Body::from(response_body.to_string())))
    }

    async fn query_sql(&self, req: Request<Body>) -> Result<Response<Body>> {
        let response_body = "query ok";
        Ok(Response::new(Body::from(response_body.to_string())))
    }

    fn health(&self) -> Result<Response<Body>> {
        let response_body = "OK";
        Ok(Response::new(Body::from(response_body.to_string())))
    }

    fn handle_metrics(&self) -> Result<Response<Body>> {
        let mut body: Vec<u8> = Default::default();
        let mut reporter = metric_exporters::PrometheusTextEncoder::new(&mut body);
        self.common_state.metric_registry().report(&mut reporter);

        Ok(Response::new(Body::from(body)))
    }
}

pub(crate) async fn serve<W: WriteBuffer, Q: QueryExecutor>(http_server: Arc<HttpApi<W, Q>>, shutdown: CancellationToken) -> Result<()> {
    let listener = AddrIncoming::bind(&http_server.common_state.http_addr)?;
    info!(bind_addr=%listener.local_addr(), "bound HTTP listener");

    let trace_layer = TraceLayer::new(
        http_server.common_state.trace_header_parser.clone(),
        http_server.common_state.metrics.clone(),
        http_server.common_state.trace_collector().clone(),
        false,
        TRACE_SERVER_NAME,
    );

    hyper::Server::builder(listener)
        .serve(hyper::service::make_service_fn(|_conn: &AddrStream| {
            let http_server = Arc::clone(&http_server);
            let service = hyper::service::service_fn(move |request: Request<_>| {
                route_request(Arc::clone(&http_server), request)
            });

            let service = trace_layer.layer(service);
            futures::future::ready(Ok::<_, Infallible>(service))
        }))
        .with_graceful_shutdown(shutdown.cancelled())
        .await?;

    Ok(())
}

async fn route_request<W: WriteBuffer, Q: QueryExecutor>(http_server: Arc<HttpApi<W, Q>>, mut req: Request<Body>) -> Result<Response<Body>, Infallible> {
    let auth = { req.headers().get(hyper::header::AUTHORIZATION).cloned() };
    req.extensions_mut()
        .insert(AuthorizationHeaderExtension::new(auth));

    // we don't need the authorization header anymore and we don't want to accidentally log it.
    req.headers_mut().remove(hyper::header::AUTHORIZATION);
    debug!(request = ?req,"Processing request");

    let method = req.method().clone();
    let uri = req.uri().clone();
    let content_length = req.headers().get("content-length").cloned();

    let response = match (method.clone(), uri.path()) {
        (Method::POST, "/api/v3/write_lp") => http_server.write_lp(req).await,
        (Method::GET|Method::POST, "/api/v3/query_sql") => http_server.query_sql(req).await,
        (Method::GET, "/health") => http_server.health(),
        (Method::GET, "/metrics") => http_server.handle_metrics(),
        (Method::GET, "/debug/pprof") => pprof_home(req).await,
        (Method::GET, "/debug/pprof/profile") => pprof_profile(req).await,
        (Method::GET, "/debug/pprof/allocs") => pprof_heappy_profile(req).await,
        _ => {
            let body = Body::from("not found");
            Ok(Response::builder()
                .status(StatusCode::NOT_FOUND)
                .body(body)
                .unwrap())
        },
    };

    // TODO: Move logging to TraceLayer
    match response {
        Ok(response) => {
            debug!(?response, "Successfully processed request");
            Ok(response)
        }
        Err(error) => {
            error!(%error, %method, %uri, ?content_length, "Error while handling request");
            Ok(error.response())
        }
    }
}

async fn pprof_home(req: Request<Body>) -> Result<Response<Body>> {
    let default_host = HeaderValue::from_static("localhost");
    let host = req
        .headers()
        .get("host")
        .unwrap_or(&default_host)
        .to_str()
        .unwrap_or_default();
    let profile_cmd = format!(
        "/debug/pprof/profile?seconds={}",
        PProfArgs::default_seconds()
    );
    let allocs_cmd = format!(
        "/debug/pprof/allocs?seconds={}",
        PProfAllocsArgs::default_seconds()
    );
    Ok(Response::new(Body::from(format!(
        r#"<a href="{profile_cmd}">http://{host}{profile_cmd}</a><br><a href="{allocs_cmd}">http://{host}{allocs_cmd}</a>"#,
    ))))
}

#[derive(Debug, Deserialize)]
struct PProfArgs {
    #[serde(default = "PProfArgs::default_seconds")]
    #[allow(dead_code)]
    seconds: u64,
    #[serde(default = "PProfArgs::default_frequency")]
    #[allow(dead_code)]
    frequency: NonZeroI32,
}

impl PProfArgs {
    fn default_seconds() -> u64 {
        30
    }

    // 99Hz to avoid coinciding with special periods
    fn default_frequency() -> NonZeroI32 {
        NonZeroI32::new(99).unwrap()
    }
}

#[derive(Debug, Deserialize)]
struct PProfAllocsArgs {
    #[serde(default = "PProfAllocsArgs::default_seconds")]
    #[allow(dead_code)]
    seconds: u64,
    // The sampling interval is a number of bytes that have to cumulatively allocated for a sample to be taken.
    //
    // For example if the sampling interval is 99, and you're doing a million of 40 bytes allocations,
    // the allocations profile will account for 16MB instead of 40MB.
    // Heappy will adjust the estimate for sampled recordings, but now that feature is not yet implemented.
    #[serde(default = "PProfAllocsArgs::default_interval")]
    #[allow(dead_code)]
    interval: NonZeroI32,
}

impl PProfAllocsArgs {
    fn default_seconds() -> u64 {
        30
    }

    // 1 means: sample every allocation.
    fn default_interval() -> NonZeroI32 {
        NonZeroI32::new(1).unwrap()
    }
}

#[cfg(feature = "pprof")]
async fn pprof_profile(req: Request<Body>) -> Result<Response<Body>, ApplicationError> {
    use ::pprof::protos::Message;
    use snafu::ResultExt;

    let query_string = req.uri().query().unwrap_or_default();
    let query: PProfArgs = serde_urlencoded::from_str(query_string)
        .context(InvalidQueryStringSnafu { query_string })?;

    let report = self::pprof::dump_rsprof(query.seconds, query.frequency.get())
        .await
        .map_err(|e| Box::new(e) as _)
        .context(PProfSnafu)?;

    let mut body: Vec<u8> = Vec::new();

    // render flamegraph when opening in the browser
    // otherwise render as protobuf; works great with: go tool pprof http://..../debug/pprof/profile
    if req
        .headers()
        .get_all("Accept")
        .iter()
        .flat_map(|i| i.to_str().unwrap_or_default().split(','))
        .any(|i| i == "text/html" || i == "image/svg+xml")
    {
        report
            .flamegraph(&mut body)
            .map_err(|e| Box::new(e) as _)
            .context(PProfSnafu)?;
        if body.is_empty() {
            return EmptyFlamegraphSnafu.fail();
        }
    } else {
        let profile = report
            .pprof()
            .map_err(|e| Box::new(e) as _)
            .context(PProfSnafu)?;
        profile
            .encode(&mut body)
            .map_err(|e| Box::new(e) as _)
            .context(ProstSnafu)?;
    }

    Ok(Response::new(Body::from(body)))
}

#[cfg(not(feature = "pprof"))]
async fn pprof_profile(_req: Request<Body>) -> Result<Response<Body>> {
    Err(Error::PProfIsNotCompiled)
}

// If heappy support is enabled, call it
#[cfg(feature = "heappy")]
async fn pprof_heappy_profile(req: Request<Body>) -> Result<Response<Body>> {
    let query_string = req.uri().query().unwrap_or_default();
    let query: PProfAllocsArgs = serde_urlencoded::from_str(query_string)?;

    let report = self::heappy::dump_heappy_rsprof(query.seconds, query.interval.get())
        .await?;

    let mut body: Vec<u8> = Vec::new();

    // render flamegraph when opening in the browser
    // otherwise render as protobuf;
    // works great with: go tool pprof http://..../debug/pprof/allocs
    if req
        .headers()
        .get_all("Accept")
        .iter()
        .flat_map(|i| i.to_str().unwrap_or_default().split(','))
        .any(|i| i == "text/html" || i == "image/svg+xml")
    {
        report.flamegraph(&mut body);
        if body.is_empty() {
            return EmptyFlamegraphSnafu.fail();
        }
    } else {
        report.write_pprof(&mut body)?
    }

    Ok(Response::new(Body::from(body)))
}

//  Return error if heappy not enabled
#[cfg(not(feature = "heappy"))]
async fn pprof_heappy_profile(_req: Request<Body>) -> Result<Response<Body>> {
    Err(Error::HeappyIsNotCompiled)
}
