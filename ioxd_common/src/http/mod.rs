use std::{convert::Infallible, num::NonZeroI32, sync::Arc};

use authz::http::AuthorizationHeaderExtension;
use hyper::{
    http::HeaderValue,
    server::conn::{AddrIncoming, AddrStream},
    Body, Method, Request, Response,
};
use observability_deps::tracing::{debug, error};
use serde::Deserialize;
use snafu::Snafu;
use tokio_util::sync::CancellationToken;
use tower::Layer;
use trace_http::{ctx::TraceHeaderParser, tower::TraceLayer};

use crate::{
    http::error::{HttpApiError, HttpApiErrorExt, HttpApiErrorSource},
    server_type::ServerType,
};

#[cfg(feature = "heappy")]
mod heappy;

#[cfg(feature = "pprof")]
mod pprof;

pub mod error;
pub mod metrics;
pub mod utils;

pub mod test_utils;

#[allow(clippy::large_enum_variant)]
#[derive(Debug, Snafu)]
pub enum ApplicationError {
    /// Error for when we could not parse the http query uri (e.g.
    /// `?foo=bar&bar=baz)`
    #[snafu(display("Invalid query string in HTTP URI '{}': {}", query_string, source))]
    InvalidQueryString {
        query_string: String,
        source: serde_urlencoded::de::Error,
    },

    #[snafu(display("PProf error: {}", source))]
    PProf {
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    #[cfg(feature = "heappy")]
    #[snafu(display("Heappy error: {}", source))]
    HeappyError { source: heappy::Error },

    #[snafu(display("Protobuf error: {}", source))]
    Prost {
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    #[snafu(display("Protobuf error: {}", source))]
    ProstIO { source: std::io::Error },

    #[snafu(display("Empty flamegraph"))]
    EmptyFlamegraph,

    #[snafu(display("heappy support is not compiled"))]
    HeappyIsNotCompiled,

    #[snafu(display("pprof support is not compiled"))]
    PProfIsNotCompiled,

    #[snafu(display("Route error from run mode: {}", e))]
    RunModeRouteError { e: Box<dyn HttpApiErrorSource> },
}

impl HttpApiErrorSource for ApplicationError {
    fn to_http_api_error(&self) -> HttpApiError {
        match self {
            e @ Self::InvalidQueryString { .. } => e.invalid(),
            e @ Self::PProf { .. } => e.internal_error(),
            e @ Self::Prost { .. } => e.internal_error(),
            e @ Self::ProstIO { .. } => e.internal_error(),
            e @ Self::EmptyFlamegraph => e.empty_value(),
            e @ Self::HeappyIsNotCompiled => e.internal_error(),
            e @ Self::PProfIsNotCompiled => e.internal_error(),
            #[cfg(feature = "heappy")]
            e @ Self::HeappyError { .. } => e.internal_error(),
            Self::RunModeRouteError { e } => e.to_http_api_error(),
        }
    }
}

pub async fn serve(
    addr: AddrIncoming,
    server_type: Arc<dyn ServerType>,
    shutdown: CancellationToken,
    trace_header_parser: TraceHeaderParser,
) -> Result<(), hyper::Error> {
    let metric_registry = server_type.metric_registry();
    let trace_collector = server_type.trace_collector();

    let trace_layer = TraceLayer::new(
        trace_header_parser,
        metric_registry,
        trace_collector,
        false,
        server_type.name(),
    );

    hyper::Server::builder(addr)
        .serve(hyper::service::make_service_fn(|_conn: &AddrStream| {
            let server_type = Arc::clone(&server_type);
            let service = hyper::service::service_fn(move |request: Request<_>| {
                route_request(Arc::clone(&server_type), request)
            });

            let service = trace_layer.layer(service);
            futures::future::ready(Ok::<_, Infallible>(service))
        }))
        .with_graceful_shutdown(shutdown.cancelled())
        .await
}

async fn route_request(
    server_type: Arc<dyn ServerType>,
    mut req: Request<Body>,
) -> Result<Response<Body>, Infallible> {
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
        (Method::GET, "/health") => health(),
        (Method::GET, "/metrics") => handle_metrics(server_type.as_ref()),
        (Method::GET, "/debug/pprof") => pprof_home(req).await,
        (Method::GET, "/debug/pprof/profile") => pprof_profile(req).await,
        (Method::GET, "/debug/pprof/allocs") => pprof_heappy_profile(req).await,
        _ => server_type
            .route_http_request(req)
            .await
            .map_err(|e| ApplicationError::RunModeRouteError { e }),
    };

    // TODO: Move logging to TraceLayer
    match response {
        Ok(response) => {
            debug!(?response, "Successfully processed request");
            Ok(response)
        }
        Err(error) => {
            let error: HttpApiError = error.to_http_api_error();
            if error.is_internal() {
                error!(%error, %method, %uri, ?content_length, "Error while handling request");
            } else {
                debug!(%error, %method, %uri, ?content_length, "Error while handling request");
            }
            Ok(error.response())
        }
    }
}

fn health() -> Result<Response<Body>, ApplicationError> {
    let response_body = "OK";
    Ok(Response::new(Body::from(response_body.to_string())))
}

fn handle_metrics(server_type: &dyn ServerType) -> Result<Response<Body>, ApplicationError> {
    let mut body: Vec<u8> = Default::default();
    let mut reporter = metric_exporters::PrometheusTextEncoder::new(&mut body);
    server_type.metric_registry().report(&mut reporter);

    Ok(Response::new(Body::from(body)))
}

async fn pprof_home(req: Request<Body>) -> Result<Response<Body>, ApplicationError> {
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
async fn pprof_profile(_req: Request<Body>) -> Result<Response<Body>, ApplicationError> {
    PProfIsNotCompiledSnafu {}.fail()
}

// If heappy support is enabled, call it
#[cfg(feature = "heappy")]
async fn pprof_heappy_profile(req: Request<Body>) -> Result<Response<Body>, ApplicationError> {
    use snafu::ResultExt;

    let query_string = req.uri().query().unwrap_or_default();
    let query: PProfAllocsArgs = serde_urlencoded::from_str(query_string)
        .context(InvalidQueryStringSnafu { query_string })?;

    let report = self::heappy::dump_heappy_rsprof(query.seconds, query.interval.get())
        .await
        .context(HeappySnafu)?;

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
        report.write_pprof(&mut body).context(ProstIOSnafu)?
    }

    Ok(Response::new(Body::from(body)))
}

//  Return error if heappy not enabled
#[cfg(not(feature = "heappy"))]
async fn pprof_heappy_profile(_req: Request<Body>) -> Result<Response<Body>, ApplicationError> {
    HeappyIsNotCompiledSnafu {}.fail()
}
