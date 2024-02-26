//! HTTP API service implementations for `server`

use crate::{CommonServerState, QueryExecutor};
use arrow::record_batch::RecordBatch;
use arrow::util::pretty;
use authz::http::AuthorizationHeaderExtension;
use bytes::{Bytes, BytesMut};
use data_types::NamespaceName;
use datafusion::execution::memory_pool::UnboundedMemoryPool;
use futures::StreamExt;
use hyper::header::ACCEPT;
use hyper::header::AUTHORIZATION;
use hyper::header::CONTENT_ENCODING;
use hyper::http::HeaderValue;
use hyper::server::conn::{AddrIncoming, AddrStream};
use hyper::{Body, Method, Request, Response, StatusCode};
use influxdb3_write::persister::TrackedMemoryArrowWriter;
use influxdb3_write::write_buffer::Error as WriteBufferError;
use influxdb3_write::BufferedWriteRequest;
use influxdb3_write::Precision;
use influxdb3_write::WriteBuffer;
use iox_time::{SystemProvider, TimeProvider};
use observability_deps::tracing::{debug, error, info};
use serde::Deserialize;
use serde::Serialize;
use sha2::Digest;
use sha2::Sha256;
use std::convert::Infallible;
use std::fmt::Debug;
use std::num::NonZeroI32;
use std::str::Utf8Error;
use std::sync::Arc;
use thiserror::Error;
use tokio_util::sync::CancellationToken;
use tower::Layer;
use trace_http::metrics::MetricFamily;
use trace_http::metrics::RequestMetrics;
use trace_http::tower::TraceLayer;
use unicode_segmentation::UnicodeSegmentation;

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

    /// NamespaceName validation error.
    #[error("error validating namespace name: {0}")]
    InvalidNamespaceName(#[from] data_types::NamespaceNameError),

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

    /// Missing parameters for query
    #[error("missing query paramters 'db' and 'q'")]
    MissingQueryParams,

    /// MIssing parameters for write
    #[error("missing query paramter 'db'")]
    MissingWriteParams,

    /// Serde decode error
    #[error("serde error: {0}")]
    Serde(#[from] serde_urlencoded::de::Error),

    /// Arrow error
    #[error("arrow error: {0}")]
    Arrow(#[from] arrow::error::ArrowError),

    /// Hyper error
    #[error("hyper http error: {0}")]
    Hyper(#[from] hyper::http::Error),

    /// WriteBuffer error
    #[error("write buffer error: {0}")]
    WriteBuffer(#[from] influxdb3_write::write_buffer::Error),

    // ToStrError
    #[error("to str error: {0}")]
    ToStr(#[from] hyper::header::ToStrError),

    // SerdeJsonError
    #[error("serde json error: {0}")]
    SerdeJson(#[from] serde_json::Error),

    // Influxdb3 Write
    #[error("serde json error: {0}")]
    Influxdb3Write(#[from] influxdb3_write::Error),

    // Invalid Start Character for a Database Name
    #[error("db name did not start with a number or letter")]
    DbNameInvalidStartChar,

    // Invalid Character for a Database Name
    #[error("db name must use ASCII letters, numbers, underscores and hyphens only")]
    DbNameInvalidChar,

    #[error("partial write of line protocol ocurred")]
    PartialLpWrite(BufferedWriteRequest),
}

#[derive(Debug, Error)]
pub enum AuthorizationError {
    #[error("the request was not authorized")]
    Unauthorized,
    #[error("the request was not in the form of 'Authorization: Bearer <token>'")]
    MalformedRequest,
    #[error("to str error: {0}")]
    ToStr(#[from] hyper::header::ToStrError),
}

impl Error {
    fn response(self) -> Response<Body> {
        #[derive(Debug, Serialize)]
        struct ErrorMessage<T: Serialize> {
            error: String,
            data: Option<T>,
        }
        match self {
            Self::WriteBuffer(WriteBufferError::ParseError(err)) => {
                let err = ErrorMessage {
                    error: "parsing failed for write_lp endpoint".into(),
                    data: Some(err),
                };
                let serialized = serde_json::to_string(&err).unwrap();
                let body = Body::from(serialized);
                Response::builder()
                    .status(StatusCode::BAD_REQUEST)
                    .body(body)
                    .unwrap()
            }
            Self::DbNameInvalidStartChar | Self::DbNameInvalidChar => {
                let err: ErrorMessage<()> = ErrorMessage {
                    error: self.to_string(),
                    data: None,
                };
                let serialized = serde_json::to_string(&err).unwrap();
                let body = Body::from(serialized);
                Response::builder()
                    .status(StatusCode::BAD_REQUEST)
                    .body(body)
                    .unwrap()
            }
            Self::PartialLpWrite(data) => {
                let err = ErrorMessage {
                    error: "partial write of line protocol ocurred".into(),
                    data: Some(data.invalid_lines),
                };
                let serialized = serde_json::to_string(&err).unwrap();
                let body = Body::from(serialized);
                Response::builder()
                    .status(StatusCode::BAD_REQUEST)
                    .body(body)
                    .unwrap()
            }
            _ => {
                let body = Body::from(self.to_string());
                Response::builder()
                    .status(StatusCode::INTERNAL_SERVER_ERROR)
                    .body(body)
                    .unwrap()
            }
        }
    }
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

const TRACE_SERVER_NAME: &str = "http_api";

#[derive(Debug)]
pub(crate) struct HttpApi<W, Q> {
    common_state: CommonServerState,
    write_buffer: Arc<W>,
    query_executor: Arc<Q>,
    max_request_bytes: usize,
}

impl<W, Q> HttpApi<W, Q> {
    pub(crate) fn new(
        common_state: CommonServerState,
        write_buffer: Arc<W>,
        query_executor: Arc<Q>,
        max_request_bytes: usize,
    ) -> Self {
        Self {
            common_state,
            write_buffer,
            query_executor,
            max_request_bytes,
        }
    }
}

impl<W, Q> HttpApi<W, Q>
where
    W: WriteBuffer,
    Q: QueryExecutor,
{
    async fn write_lp(&self, req: Request<Body>) -> Result<Response<Body>> {
        let query = req.uri().query().ok_or(Error::MissingWriteParams)?;
        let params: WriteParams = serde_urlencoded::from_str(query)?;
        validate_db_name(&params.db)?;
        info!("write_lp to {}", params.db);

        let body = self.read_body(req).await?;
        let body = std::str::from_utf8(&body).map_err(Error::NonUtf8Body)?;

        let database = NamespaceName::new(params.db)?;

        // TODO: use the time provider
        let default_time = SystemProvider::new().now().timestamp_nanos();

        let result = self
            .write_buffer
            .write_lp(
                database,
                body,
                default_time,
                params.accept_partial,
                params.precision,
            )
            .await?;

        if result.invalid_lines.is_empty() {
            Ok(Response::new(Body::empty()))
        } else {
            Err(Error::PartialLpWrite(result))
        }
    }

    async fn query_sql(&self, req: Request<Body>) -> Result<Response<Body>> {
        let query = req.uri().query().ok_or(Error::MissingQueryParams)?;
        let params: QuerySqlParams = serde_urlencoded::from_str(query)?;

        println!("query_sql {:?}", params);

        let result = self
            .query_executor
            .query(&params.db, &params.q, None, None)
            .await
            .unwrap();

        let batches: Vec<RecordBatch> = result
            .collect::<Vec<datafusion::common::Result<RecordBatch>>>()
            .await
            .into_iter()
            .map(|b| b.unwrap())
            .collect();

        fn to_json(batches: Vec<RecordBatch>) -> Result<Bytes> {
            let batches: Vec<&RecordBatch> = batches.iter().collect();
            Ok(Bytes::from(serde_json::to_string(
                &arrow_json::writer::record_batches_to_json_rows(batches.as_slice())?,
            )?))
        }

        fn to_csv(batches: Vec<RecordBatch>) -> Result<Bytes> {
            let mut writer = arrow_csv::writer::Writer::new(Vec::new());
            for batch in batches {
                writer.write(&batch)?;
            }

            Ok(Bytes::from(writer.into_inner()))
        }

        fn to_pretty(batches: Vec<RecordBatch>) -> Result<Bytes> {
            Ok(Bytes::from(format!(
                "{}",
                pretty::pretty_format_batches(&batches)?
            )))
        }

        fn to_parquet(batches: Vec<RecordBatch>) -> Result<Bytes> {
            let mut bytes = Vec::new();
            let mem_pool = Arc::new(UnboundedMemoryPool::default());
            let mut writer =
                TrackedMemoryArrowWriter::try_new(&mut bytes, batches[0].schema(), mem_pool)?;
            for batch in batches {
                writer.write(batch)?;
            }
            writer.close()?;
            Ok(Bytes::from(bytes))
        }

        enum Format {
            Parquet,
            Csv,
            Pretty,
            Json,
            Error,
        }

        let (body, format) = match params.format {
            None => match req
                .headers()
                .get(ACCEPT)
                .map(HeaderValue::to_str)
                .transpose()?
            {
                // Accept Headers use the MIME types maintained by IANA here:
                // https://www.iana.org/assignments/media-types/media-types.xhtml
                // Note parquet hasn't been accepted yet just Arrow, but there
                // is the possibility it will be:
                // https://issues.apache.org/jira/browse/PARQUET-1889
                Some("application/vnd.apache.parquet") => {
                    (to_parquet(batches)?, Format::Parquet)
                }
                Some("text/csv") => (to_csv(batches)?, Format::Csv),
                Some("text/plain") => (to_pretty(batches)?, Format::Pretty),
                Some("application/json") => (to_json(batches)?, Format::Json),
                Some("*/*") | None => (to_json(batches)?, Format::Json),
                Some(_) => (Bytes::from("{ \"error\": \"Available mime types are: application/vnd.apache.parquet, text/csv, text/plain, and application/json\" }"), Format::Error),
            },
            Some(format) => match format.as_str() {
                "parquet" => (to_parquet(batches)?, Format::Parquet),
                "csv" => (to_csv(batches)?, Format::Csv),
                "pretty" => (to_pretty(batches)?, Format::Pretty),
                "json" => (to_json(batches)?, Format::Json),
                _ => (Bytes::from("{ \"error\": \"Available formats are: parquet, csv, pretty, and json\" }"), Format::Error),
            },
        };

        match format {
            Format::Parquet => Ok(Response::builder()
                .status(StatusCode::OK)
                .header("Content-Type", "application/vnd.apache.parquet")
                .body(Body::from(body))?),
            Format::Csv => Ok(Response::builder()
                .status(StatusCode::OK)
                .header("Content-Type", "text/csv")
                .body(Body::from(body))?),
            Format::Pretty => Ok(Response::builder()
                .status(StatusCode::OK)
                .header("Content-Type", "text/plain; charset=utf-8")
                .body(Body::from(body))?),
            Format::Json => Ok(Response::builder()
                .status(StatusCode::OK)
                .header("Content-Type", "application/json")
                .body(Body::from(body))?),
            Format::Error => Ok(Response::builder()
                .status(StatusCode::BAD_REQUEST)
                .header("Content-Type", "application/json")
                .body(Body::from(body))?),
        }
    }

    fn health(&self) -> Result<Response<Body>> {
        let response_body = "OK";
        Ok(Response::new(Body::from(response_body.to_string())))
    }

    fn handle_metrics(&self) -> Result<Response<Body>> {
        let mut body: Vec<u8> = Default::default();
        let mut reporter = metric_exporters::PrometheusTextEncoder::new(&mut body);
        self.common_state.metrics.report(&mut reporter);

        Ok(Response::new(Body::from(body)))
    }

    /// Parse the request's body into raw bytes, applying the configured size
    /// limits and decoding any content encoding.
    async fn read_body(&self, req: hyper::Request<Body>) -> Result<Bytes> {
        let encoding = req
            .headers()
            .get(&CONTENT_ENCODING)
            .map(|v| v.to_str().map_err(Error::NonUtf8ContentHeader))
            .transpose()?;
        let ungzip = match encoding {
            None | Some("identity") => false,
            Some("gzip") => true,
            Some(v) => return Err(Error::InvalidContentEncoding(v.to_string())),
        };

        let mut payload = req.into_body();

        let mut body = BytesMut::new();
        while let Some(chunk) = payload.next().await {
            let chunk = chunk.map_err(Error::ClientHangup)?;
            // limit max size of in-memory payload
            if (body.len() + chunk.len()) > self.max_request_bytes {
                return Err(Error::RequestSizeExceeded(self.max_request_bytes));
            }
            body.extend_from_slice(&chunk);
        }
        let body = body.freeze();

        // If the body is not compressed, return early.
        if !ungzip {
            return Ok(body);
        }

        // Unzip the gzip-encoded content
        use std::io::Read;
        let decoder = flate2::read::GzDecoder::new(&body[..]);

        // Read at most max_request_bytes bytes to prevent a decompression bomb
        // based DoS.
        //
        // In order to detect if the entire stream ahs been read, or truncated,
        // read an extra byte beyond the limit and check the resulting data
        // length - see the max_request_size_truncation test.
        let mut decoder = decoder.take(self.max_request_bytes as u64 + 1);
        let mut decoded_data = Vec::new();
        decoder
            .read_to_end(&mut decoded_data)
            .map_err(Error::InvalidGzip)?;

        // If the length is max_size+1, the body is at least max_size+1 bytes in
        // length, and possibly longer, but truncated.
        if decoded_data.len() > self.max_request_bytes {
            return Err(Error::RequestSizeExceeded(self.max_request_bytes));
        }

        Ok(decoded_data.into())
    }

    fn authorize_request(&self, req: &mut Request<Body>) -> Result<(), AuthorizationError> {
        // We won't need the authorization header anymore and we don't want to accidentally log it.
        // Take it out so we can use it and not log it later by accident.
        let auth = req.headers_mut().remove(AUTHORIZATION);

        if let Some(bearer_token) = self.common_state.bearer_token() {
            let Some(header) = &auth else {
                return Err(AuthorizationError::Unauthorized);
            };

            // Split the header value into two parts
            let mut header = header.to_str()?.split(' ');

            // Check that the header is the 'Bearer' auth scheme
            let bearer = header.next().ok_or(AuthorizationError::MalformedRequest)?;
            if bearer != "Bearer" {
                return Err(AuthorizationError::MalformedRequest);
            }

            // Get the token that we want to hash to check the request is valid
            let token = header.next().ok_or(AuthorizationError::MalformedRequest)?;

            // There should only be two parts the 'Bearer' scheme and the actual
            // token, error otherwise
            if header.next().is_some() {
                return Err(AuthorizationError::MalformedRequest);
            }

            // Check that the hashed token is acceptable
            let authorized = &Sha256::digest(token)[..] == bearer_token;
            if !authorized {
                return Err(AuthorizationError::Unauthorized);
            }
        }

        req.extensions_mut()
            .insert(AuthorizationHeaderExtension::new(auth));
        Ok(())
    }
}

/// A valid name:
/// - Starts with a letter or a number
/// - Is ASCII not UTF-8
/// - Contains only letters, numbers, underscores or hyphens
fn validate_db_name(name: &str) -> Result<()> {
    let mut is_first_char = true;
    for grapheme in name.graphemes(true) {
        if grapheme.as_bytes().len() > 1 {
            // In the case of a unicode we need to handle multibyte chars
            return Err(Error::DbNameInvalidChar);
        }
        let char = grapheme.as_bytes()[0] as char;
        if !is_first_char {
            if !(char.is_ascii_alphanumeric() || char == '_' || char == '-') {
                return Err(Error::DbNameInvalidChar);
            }
        } else {
            if !char.is_ascii_alphanumeric() {
                return Err(Error::DbNameInvalidStartChar);
            }
            is_first_char = false;
        }
    }

    Ok(())
}

#[derive(Debug, Deserialize)]
pub(crate) struct QuerySqlParams {
    pub(crate) db: String,
    pub(crate) q: String,
    pub(crate) format: Option<String>,
}

// This is a hack around the fact that bool default is false not true
const fn true_fn() -> bool {
    true
}
#[derive(Debug, Deserialize)]
pub(crate) struct WriteParams {
    pub(crate) db: String,
    #[serde(default = "true_fn")]
    pub(crate) accept_partial: bool,
    #[serde(default)]
    pub(crate) precision: Precision,
}

pub(crate) async fn serve<W: WriteBuffer, Q: QueryExecutor>(
    http_server: Arc<HttpApi<W, Q>>,
    shutdown: CancellationToken,
) -> Result<()> {
    let listener = AddrIncoming::bind(&http_server.common_state.http_addr)?;
    println!("binding listener");
    info!(bind_addr=%listener.local_addr(), "bound HTTP listener");

    let req_metrics = RequestMetrics::new(
        Arc::clone(&http_server.common_state.metrics),
        MetricFamily::HttpServer,
    );
    let trace_layer = TraceLayer::new(
        http_server.common_state.trace_header_parser.clone(),
        Arc::new(req_metrics),
        http_server.common_state.trace_collector().clone(),
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

async fn route_request<W: WriteBuffer, Q: QueryExecutor>(
    http_server: Arc<HttpApi<W, Q>>,
    mut req: Request<Body>,
) -> Result<Response<Body>, Infallible> {
    if let Err(e) = http_server.authorize_request(&mut req) {
        match e {
            AuthorizationError::Unauthorized => {
                return Ok(Response::builder()
                    .status(StatusCode::UNAUTHORIZED)
                    .body(Body::empty())
                    .unwrap())
            }
            AuthorizationError::MalformedRequest => {
                return Ok(Response::builder()
                    .status(StatusCode::BAD_REQUEST)
                    .body(Body::from("{\"error\":\
                        \"Authorization header was malformed and should be in the form 'Authorization: Bearer <token>'\"\
                    }"))
                    .unwrap());
            }
            // We don't expect this to happen, but if the header is messed up
            // better to handle it then not at all
            AuthorizationError::ToStr(_) => {
                return Ok(Response::builder()
                    .status(StatusCode::INTERNAL_SERVER_ERROR)
                    .body(Body::empty())
                    .unwrap())
            }
        }
    }
    debug!(request = ?req,"Processing request");

    let method = req.method().clone();
    let uri = req.uri().clone();
    let content_length = req.headers().get("content-length").cloned();

    let response = match (method.clone(), uri.path()) {
        (Method::POST, "/api/v3/write_lp") => http_server.write_lp(req).await,
        (Method::GET | Method::POST, "/api/v3/query_sql") => http_server.query_sql(req).await,
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
        }
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

    let report = self::heappy::dump_heappy_rsprof(query.seconds, query.interval.get()).await?;

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
