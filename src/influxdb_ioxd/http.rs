//! This module contains the HTTP api for InfluxDB IOx, including a
//! partial implementation of the /v2 HTTP api routes from InfluxDB
//! for compatibility.
//!
//! Note that these routes are designed to be just helpers for now,
//! and "close enough" to the real /v2 api to be able to test InfluxDB IOx
//! without needing to create and manage a mapping layer from name -->
//! id (this is done by other services in the influx cloud)
//!
//! Long term, we expect to create IOx specific api in terms of
//! database names and may remove this quasi /v2 API.

// Influx crates
use super::planner::Planner;
use data_types::{
    names::{org_and_bucket_to_database, OrgBucketMappingError},
    DatabaseName,
};
use influxdb_iox_client::format::QueryOutputFormat;
use influxdb_line_protocol::parse_lines;
use query::{exec::ExecutorType, QueryDatabase};
use server::{ApplicationState, ConnectionManager, Server as AppServer};

// External crates
use bytes::{Bytes, BytesMut};
use chrono::Utc;
use futures::{self, StreamExt};
use http::header::{CONTENT_ENCODING, CONTENT_TYPE};
use hyper::{http::HeaderValue, Body, Method, Request, Response, StatusCode};
use observability_deps::{
    opentelemetry::KeyValue,
    tracing::{self, debug, error, info},
};
use routerify::{prelude::*, Middleware, RequestInfo, Router, RouterError, RouterService};
use serde::Deserialize;
use snafu::{OptionExt, ResultExt, Snafu};

use hyper::server::conn::AddrIncoming;
use pprof::protos::Message;
use std::num::NonZeroI32;
use std::{
    fmt::Debug,
    str::{self, FromStr},
    sync::Arc,
};
use tokio::time::Duration;
use tokio_util::sync::CancellationToken;

/// Constants used in API error codes.
///
/// Expressing this as a enum prevents reuse of discriminants, and as they're
/// effectively consts this uses UPPER_SNAKE_CASE.
#[allow(non_camel_case_types, clippy::upper_case_acronyms)]
#[derive(Debug, PartialEq)]
pub enum ApiErrorCode {
    /// An unknown/unhandled error
    UNKNOWN = 100,

    /// The database name in the request is invalid.
    DB_INVALID_NAME = 101,

    /// The database referenced already exists.
    DB_ALREADY_EXISTS = 102,

    /// The database referenced does not exist.
    DB_NOT_FOUND = 103,
}

impl From<ApiErrorCode> for u32 {
    fn from(v: ApiErrorCode) -> Self {
        v as Self
    }
}

#[allow(clippy::large_enum_variant)]
#[derive(Debug, Snafu)]
pub enum ApplicationError {
    // Internal (unexpected) errors
    #[snafu(display(
        "Internal error accessing org {}, bucket {}:  {}",
        org,
        bucket_name,
        source
    ))]
    BucketByName {
        org: String,
        bucket_name: String,
        source: Box<dyn std::error::Error + Send + Sync>,
    },
    #[snafu(display("Internal error mapping org & bucket: {}", source))]
    BucketMappingError { source: OrgBucketMappingError },

    #[snafu(display(
        "Internal error writing points into org {}, bucket {}:  {}",
        org,
        bucket_name,
        source
    ))]
    WritingPoints {
        org: String,
        bucket_name: String,
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    #[snafu(display("Internal error reading points from database {}:  {}", db_name, source))]
    Query {
        db_name: String,
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    // Application level errors
    #[snafu(display("Bucket {} not found in org {}", bucket, org))]
    BucketNotFound { org: String, bucket: String },

    #[snafu(display("Body exceeds limit of {} bytes", max_body_size))]
    RequestSizeExceeded { max_body_size: usize },

    #[snafu(display("Expected query string in request, but none was provided"))]
    ExpectedQueryString {},

    /// Error for when we could not parse the http query uri (e.g.
    /// `?foo=bar&bar=baz)`
    #[snafu(display("Invalid query string in HTTP URI '{}': {}", query_string, source))]
    InvalidQueryString {
        query_string: String,
        source: serde_urlencoded::de::Error,
    },

    #[snafu(display("Query error: {}", source))]
    QueryError {
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    #[snafu(display("Invalid request body: {}", source))]
    InvalidRequestBody { source: serde_json::error::Error },

    #[snafu(display("Invalid response body: {}", source))]
    InternalSerializationError { source: serde_json::error::Error },

    #[snafu(display("Invalid content encoding: {}", content_encoding))]
    InvalidContentEncoding { content_encoding: String },

    #[snafu(display("Error reading request header '{}' as Utf8: {}", header_name, source))]
    ReadingHeaderAsUtf8 {
        header_name: String,
        source: hyper::header::ToStrError,
    },

    #[snafu(display("Error reading request body: {}", source))]
    ReadingBody { source: hyper::Error },

    #[snafu(display("Error reading request body as utf8: {}", source))]
    ReadingBodyAsUtf8 { source: std::str::Utf8Error },

    #[snafu(display("Error parsing line protocol: {}", source))]
    ParsingLineProtocol {
        source: influxdb_line_protocol::Error,
    },

    #[snafu(display("Error decompressing body as gzip: {}", source))]
    ReadingBodyAsGzip { source: std::io::Error },

    #[snafu(display("Client hung up while sending body: {}", source))]
    ClientHangup { source: hyper::Error },

    #[snafu(display("No handler for {:?} {}", method, path))]
    RouteNotFound { method: Method, path: String },

    #[snafu(display("Internal error from database {}: {}", database, source))]
    DatabaseError {
        database: String,
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    #[snafu(display("Error generating json response: {}", source))]
    JsonGenerationError { source: serde_json::Error },

    #[snafu(display("Error creating database: {}", source))]
    ErrorCreatingDatabase { source: server::Error },

    #[snafu(display("Invalid database name: {}", source))]
    DatabaseNameError {
        source: data_types::DatabaseNameError,
    },

    #[snafu(display("Database {} not found", name))]
    DatabaseNotFound { name: String },

    #[snafu(display("Database {} does not have a Write Buffer", name))]
    WriteBufferNotFound { name: String },

    #[snafu(display("Internal error creating HTTP response:  {}", source))]
    CreatingResponse { source: http::Error },

    #[snafu(display("Invalid format '{}': : {}", format, source))]
    ParsingFormat {
        format: String,
        source: influxdb_iox_client::format::Error,
    },

    #[snafu(display(
        "Error formatting results of SQL query '{}' using '{:?}': {}",
        q,
        format,
        source
    ))]
    FormattingResult {
        q: String,
        format: QueryOutputFormat,
        source: influxdb_iox_client::format::Error,
    },

    #[snafu(display("Error while planning query: {}", source))]
    Planning { source: super::planner::Error },

    #[snafu(display(
        "Cannot create snapshot because there is no data: {} {}:{}",
        db_name,
        partition,
        table_name
    ))]
    NoSnapshot {
        db_name: String,
        partition: String,
        table_name: String,
    },

    #[snafu(display("PProf error: {}", source))]
    PProf { source: pprof::Error },

    #[snafu(display("Protobuf error: {}", source))]
    Prost { source: prost::EncodeError },

    #[snafu(display("Empty flamegraph"))]
    EmptyFlamegraph,
}

impl ApplicationError {
    pub fn response(&self) -> Response<Body> {
        match self {
            Self::BucketByName { .. } => self.internal_error(),
            Self::BucketMappingError { .. } => self.internal_error(),
            Self::WritingPoints { .. } => self.internal_error(),
            Self::Query { .. } => self.internal_error(),
            Self::QueryError { .. } => self.bad_request(),
            Self::BucketNotFound { .. } => self.not_found(),
            Self::RequestSizeExceeded { .. } => self.bad_request(),
            Self::ExpectedQueryString { .. } => self.bad_request(),
            Self::InvalidQueryString { .. } => self.bad_request(),
            Self::InvalidRequestBody { .. } => self.bad_request(),
            Self::InternalSerializationError { .. } => self.internal_error(),
            Self::InvalidContentEncoding { .. } => self.bad_request(),
            Self::ReadingHeaderAsUtf8 { .. } => self.bad_request(),
            Self::ReadingBody { .. } => self.bad_request(),
            Self::ReadingBodyAsUtf8 { .. } => self.bad_request(),
            Self::ParsingLineProtocol { .. } => self.bad_request(),
            Self::ReadingBodyAsGzip { .. } => self.bad_request(),
            Self::ClientHangup { .. } => self.bad_request(),
            Self::RouteNotFound { .. } => self.not_found(),
            Self::DatabaseError { .. } => self.internal_error(),
            Self::JsonGenerationError { .. } => self.internal_error(),
            Self::ErrorCreatingDatabase { .. } => self.bad_request(),
            Self::DatabaseNameError { .. } => self.bad_request(),
            Self::DatabaseNotFound { .. } => self.not_found(),
            Self::WriteBufferNotFound { .. } => self.not_found(),
            Self::CreatingResponse { .. } => self.internal_error(),
            Self::FormattingResult { .. } => self.internal_error(),
            Self::ParsingFormat { .. } => self.bad_request(),
            Self::Planning { .. } => self.bad_request(),
            Self::NoSnapshot { .. } => self.not_modified(),
            Self::PProf { .. } => self.internal_error(),
            Self::Prost { .. } => self.internal_error(),
            Self::EmptyFlamegraph => self.no_content(),
        }
    }

    fn bad_request(&self) -> Response<Body> {
        Response::builder()
            .status(StatusCode::BAD_REQUEST)
            .body(self.body())
            .unwrap()
    }

    fn internal_error(&self) -> Response<Body> {
        Response::builder()
            .status(StatusCode::INTERNAL_SERVER_ERROR)
            .body(self.body())
            .unwrap()
    }

    fn not_found(&self) -> Response<Body> {
        Response::builder()
            .status(StatusCode::NOT_FOUND)
            .body(Body::empty())
            .unwrap()
    }

    fn not_modified(&self) -> Response<Body> {
        Response::builder()
            .status(StatusCode::NOT_MODIFIED)
            .body(self.body())
            .unwrap()
    }

    fn no_content(&self) -> Response<Body> {
        Response::builder()
            .status(StatusCode::NO_CONTENT)
            .body(self.body())
            .unwrap()
    }

    fn body(&self) -> Body {
        let json =
            serde_json::json!({"error": self.to_string(), "error_code": self.api_error_code()})
                .to_string();
        Body::from(json)
    }

    /// Map the error type into an API error code.
    fn api_error_code(&self) -> u32 {
        match self {
            Self::DatabaseNameError { .. } => ApiErrorCode::DB_INVALID_NAME,
            Self::DatabaseNotFound { .. } => ApiErrorCode::DB_NOT_FOUND,

            // Some errors are wrapped
            Self::ErrorCreatingDatabase {
                source: server::Error::InvalidDatabaseName { .. },
            } => ApiErrorCode::DB_INVALID_NAME,

            Self::ErrorCreatingDatabase {
                source: server::Error::DatabaseNotFound { .. },
            } => ApiErrorCode::DB_NOT_FOUND,

            Self::ErrorCreatingDatabase {
                source: server::Error::DatabaseAlreadyExists { .. },
            } => ApiErrorCode::DB_ALREADY_EXISTS,

            // A "catch all" error code
            _ => ApiErrorCode::UNKNOWN,
        }
        .into()
    }
}

struct Server<M>
where
    M: ConnectionManager + Send + Sync + Debug + 'static,
{
    app_server: Arc<AppServer<M>>,
    max_request_size: usize,
}

fn router<M>(
    application: Arc<ApplicationState>,
    app_server: Arc<AppServer<M>>,
    max_request_size: usize,
) -> Router<Body, ApplicationError>
where
    M: ConnectionManager + Send + Sync + Debug + 'static,
{
    let server = Server {
        app_server,
        max_request_size,
    };

    // Create a router and specify the the handlers.
    Router::builder()
        .data(server)
        .data(application)
        .middleware(Middleware::pre(|mut req| async move {
            // we don't need the authorization header and we don't want to accidentally log it.
            req.headers_mut().remove("authorization");
            debug!(request = ?req,"Processing request");
            Ok(req)
        }))
        .middleware(Middleware::post(|res| async move {
            debug!(response = ?res, "Successfully processed request");
            Ok(res)
        })) // this endpoint is for API backward compatibility with InfluxDB 2.x
        .post("/api/v2/write", write::<M>)
        .get("/health", health::<M>)
        .get("/metrics", handle_metrics::<M>)
        .get("/iox/api/v1/databases/:name/query", query::<M>)
        .get("/api/v1/partitions", list_partitions::<M>)
        .get("/debug/pprof", pprof_home::<M>)
        .get("/debug/pprof/profile", pprof_profile::<M>)
        // Specify the error handler to handle any errors caused by
        // a route or any middleware.
        .err_handler_with_info(error_handler)
        .build()
        .unwrap()
}

// The API-global error handler, handles ApplicationErrors originating from
// individual routes and middlewares, along with errors from the router itself
async fn error_handler(err: RouterError<ApplicationError>, req: RequestInfo) -> Response<Body> {
    let method = req.method().clone();
    let uri = req.uri().clone();
    let span_id = req.headers().get("x-b3-spanid");
    let content_length = req.headers().get("content-length");
    error!(error = ?err, error_message = ?err.to_string(), method = ?method, uri = ?uri, ?span_id, ?content_length, "Error while handling request");

    match err {
        RouterError::HandleRequest(e, _)
        | RouterError::HandlePreMiddlewareRequest(e)
        | RouterError::HandlePostMiddlewareWithInfoRequest(e)
        | RouterError::HandlePostMiddlewareWithoutInfoRequest(e) => e.response(),
        _ => {
            let json = serde_json::json!({"error": err.to_string()}).to_string();
            Response::builder()
                .status(StatusCode::INTERNAL_SERVER_ERROR)
                .body(Body::from(json))
                .unwrap()
        }
    }
}

#[derive(Debug, Deserialize)]
/// Body of the request to the /write endpoint
struct WriteInfo {
    org: String,
    bucket: String,
}

/// Parse the request's body into raw bytes, applying size limits and
/// content encoding as needed.
async fn parse_body(req: hyper::Request<Body>, max_size: usize) -> Result<Bytes, ApplicationError> {
    // clippy says the const needs to be assigned to a local variable:
    // error: a `const` item with interior mutability should not be borrowed
    let header_name = CONTENT_ENCODING;
    let ungzip = match req.headers().get(&header_name) {
        None => false,
        Some(content_encoding) => {
            let content_encoding = content_encoding.to_str().context(ReadingHeaderAsUtf8 {
                header_name: header_name.as_str(),
            })?;
            match content_encoding {
                "gzip" => true,
                _ => InvalidContentEncoding { content_encoding }.fail()?,
            }
        }
    };

    let mut payload = req.into_body();

    let mut body = BytesMut::new();
    while let Some(chunk) = payload.next().await {
        let chunk = chunk.context(ClientHangup)?;
        // limit max size of in-memory payload
        if (body.len() + chunk.len()) > max_size {
            return Err(ApplicationError::RequestSizeExceeded {
                max_body_size: max_size,
            });
        }
        body.extend_from_slice(&chunk);
    }
    let body = body.freeze();

    // apply any content encoding needed
    if ungzip {
        use std::io::Read;
        let decoder = flate2::read::GzDecoder::new(&body[..]);

        // Read at most max_size bytes to prevent a decompression bomb based
        // DoS.
        let mut decoder = decoder.take(max_size as u64);
        let mut decoded_data = Vec::new();
        decoder
            .read_to_end(&mut decoded_data)
            .context(ReadingBodyAsGzip)?;
        Ok(decoded_data.into())
    } else {
        Ok(body)
    }
}

#[observability_deps::instrument(level = "debug")]
async fn write<M>(req: Request<Body>) -> Result<Response<Body>, ApplicationError>
where
    M: ConnectionManager + Send + Sync + Debug + 'static,
{
    let path = req.uri().path().to_string();
    let Server {
        app_server: server,
        max_request_size,
    } = req.data::<Server<M>>().expect("server state");
    let max_request_size = *max_request_size;
    let server = Arc::clone(server);

    // TODO(edd): figure out best way of catching all errors in this observation.
    let obs = server.metrics().http_requests.observation(); // instrument request

    // TODO - metrics. Implement a macro/something that will catch all the
    // early returns.

    let query = req.uri().query().context(ExpectedQueryString)?;

    let write_info: WriteInfo = serde_urlencoded::from_str(query).context(InvalidQueryString {
        query_string: String::from(query),
    })?;

    let db_name = org_and_bucket_to_database(&write_info.org, &write_info.bucket)
        .context(BucketMappingError)?;

    let body = parse_body(req, max_request_size).await?;

    let body = str::from_utf8(&body).context(ReadingBodyAsUtf8)?;

    // The time, in nanoseconds since the epoch, to assign to any points that don't
    // contain a timestamp
    let default_time = Utc::now().timestamp_nanos();

    let mut num_fields = 0;

    let lines = parse_lines(body)
        .inspect(|line| {
            if let Ok(line) = line {
                num_fields += line.field_set.len();
            }
        })
        .collect::<Result<Vec<_>, influxdb_line_protocol::Error>>()
        .context(ParsingLineProtocol)?;

    let num_lines = lines.len();
    debug!(num_lines, num_fields, body_size=body.len(), %db_name, org=%write_info.org, bucket=%write_info.bucket, "inserting lines into database");

    let metric_kv = vec![
        KeyValue::new("org", write_info.org.to_string()),
        KeyValue::new("bucket", write_info.bucket.to_string()),
        KeyValue::new("path", path),
    ];

    server
        .write_lines(&db_name, &lines, default_time)
        .await
        .map_err(|e| {
            let labels = &[
                metrics::KeyValue::new("status", "error"),
                metrics::KeyValue::new("db_name", db_name.to_string()),
            ];

            server
                .metrics()
                .ingest_lines_total
                .add_with_labels(num_lines as u64, labels);

            server
                .metrics()
                .ingest_fields_total
                .add_with_labels(num_fields as u64, labels);

            server.metrics().ingest_points_bytes_total.add_with_labels(
                body.len() as u64,
                &[
                    metrics::KeyValue::new("status", "error"),
                    metrics::KeyValue::new("db_name", db_name.to_string()),
                ],
            );
            debug!(?e, ?db_name, ?num_lines, "error writing lines");

            obs.client_error_with_labels(&metric_kv); // user error
            match e {
                server::Error::DatabaseNotFound { .. } => ApplicationError::DatabaseNotFound {
                    name: db_name.to_string(),
                },
                _ => ApplicationError::WritingPoints {
                    org: write_info.org.clone(),
                    bucket_name: write_info.bucket.clone(),
                    source: Box::new(e),
                },
            }
        })?;

    let labels = &[
        metrics::KeyValue::new("status", "ok"),
        metrics::KeyValue::new("db_name", db_name.to_string()),
    ];

    server
        .metrics()
        .ingest_lines_total
        .add_with_labels(num_lines as u64, labels);

    server
        .metrics()
        .ingest_fields_total
        .add_with_labels(num_fields as u64, labels);

    // line protocol bytes successfully written
    server
        .metrics()
        .ingest_points_bytes_total
        .add_with_labels(body.len() as u64, labels);

    obs.ok_with_labels(&metric_kv); // request completed successfully
    Ok(Response::builder()
        .status(StatusCode::NO_CONTENT)
        .body(Body::empty())
        .unwrap())
}

#[derive(Deserialize, Debug, PartialEq)]
/// Parsed URI Parameters of the request to the .../query endpoint
struct QueryParams {
    q: String,
    #[serde(default = "default_format")]
    format: String,
}

fn default_format() -> String {
    QueryOutputFormat::default().to_string()
}

#[tracing::instrument(level = "debug")]
async fn query<M: ConnectionManager + Send + Sync + Debug + 'static>(
    req: Request<Body>,
) -> Result<Response<Body>, ApplicationError> {
    let path = req.uri().path().to_string();
    let server = Arc::clone(&req.data::<Server<M>>().expect("server state").app_server);

    // TODO(edd): figure out best way of catching all errors in this observation.
    let obs = server.metrics().http_requests.observation(); // instrument request

    let uri_query = req.uri().query().context(ExpectedQueryString {})?;

    let QueryParams { q, format } =
        serde_urlencoded::from_str(uri_query).context(InvalidQueryString {
            query_string: uri_query,
        })?;

    let format = QueryOutputFormat::from_str(&format).context(ParsingFormat { format })?;

    let db_name_str = req
        .param("name")
        .expect("db name must have been set by routerify")
        .clone();

    let metric_kv = vec![
        KeyValue::new("db_name", db_name_str.clone()),
        KeyValue::new("path", path),
    ];

    let db_name = DatabaseName::new(&db_name_str).context(DatabaseNameError)?;
    debug!(uri = ?req.uri(), %q, ?format, %db_name, "running SQL query");

    let db = server
        .db(&db_name)
        .context(DatabaseNotFound { name: &db_name_str })?;

    let executor = db.executor();
    let physical_plan = Planner::new(Arc::clone(&executor))
        .sql(db, &q)
        .await
        .context(Planning)?;

    // TODO: stream read results out rather than rendering the
    // whole thing in mem
    let batches = executor
        .collect(physical_plan, ExecutorType::Query)
        .await
        .map_err(|e| Box::new(e) as _)
        .context(Query { db_name })?;

    let results = format
        .format(&batches)
        .context(FormattingResult { q, format })?;

    let body = Body::from(results.into_bytes());

    let response = Response::builder()
        .header(CONTENT_TYPE, format.content_type())
        .body(body)
        .context(CreatingResponse)?;

    // successful query
    obs.ok_with_labels(&metric_kv);

    Ok(response)
}

#[tracing::instrument(level = "debug")]
async fn health<M: ConnectionManager + Send + Sync + Debug + 'static>(
    req: Request<Body>,
) -> Result<Response<Body>, ApplicationError> {
    let server = Arc::clone(&req.data::<Server<M>>().expect("server state").app_server);
    let path = req.uri().path().to_string();
    server
        .metrics()
        .http_requests
        .observation()
        .ok_with_labels(&[metrics::KeyValue::new("path", path)]);

    let response_body = "OK";
    Ok(Response::new(Body::from(response_body.to_string())))
}

#[tracing::instrument(level = "debug")]
async fn handle_metrics<M: ConnectionManager + Send + Sync + Debug + 'static>(
    req: Request<Body>,
) -> Result<Response<Body>, ApplicationError> {
    let server = Arc::clone(&req.data::<Server<M>>().expect("server state").app_server);
    let path = req.uri().path().to_string();
    server
        .metrics()
        .http_requests
        .observation()
        .ok_with_labels(&[metrics::KeyValue::new("path", path)]);

    let body = req
        .data::<Arc<ApplicationState>>()
        .expect("application state")
        .metric_registry()
        .metrics_as_text();

    Ok(Response::new(Body::from(body)))
}

#[derive(Deserialize, Debug)]
/// Arguments in the query string of the request to /partitions
struct DatabaseInfo {
    org: String,
    bucket: String,
}

#[tracing::instrument(level = "debug")]
async fn list_partitions<M: ConnectionManager + Send + Sync + Debug + 'static>(
    req: Request<Body>,
) -> Result<Response<Body>, ApplicationError> {
    let path = req.uri().path().to_string();

    let server = Arc::clone(&req.data::<Server<M>>().expect("server state").app_server);

    // TODO - catch error conditions
    let obs = server.metrics().http_requests.observation();
    let query = req.uri().query().context(ExpectedQueryString {})?;

    let info: DatabaseInfo = serde_urlencoded::from_str(query).context(InvalidQueryString {
        query_string: query,
    })?;

    let db_name =
        org_and_bucket_to_database(&info.org, &info.bucket).context(BucketMappingError)?;

    let metric_kv = vec![
        KeyValue::new("db_name", db_name.to_string()),
        KeyValue::new("path", path),
    ];

    let db = server.db(&db_name).context(BucketNotFound {
        org: &info.org,
        bucket: &info.bucket,
    })?;

    let partition_keys =
        db.partition_keys()
            .map_err(|e| Box::new(e) as _)
            .context(BucketByName {
                org: &info.org,
                bucket_name: &info.bucket,
            })?;

    let result = serde_json::to_string(&partition_keys).context(JsonGenerationError)?;

    obs.ok_with_labels(&metric_kv);
    Ok(Response::new(Body::from(result)))
}

#[derive(Deserialize, Debug)]
/// Arguments in the query string of the request to /snapshot
struct SnapshotInfo {
    org: String,
    bucket: String,
    partition: String,
    table_name: String,
}

#[tracing::instrument(level = "debug")]
async fn pprof_home<M: ConnectionManager + Send + Sync + Debug + 'static>(
    req: Request<Body>,
) -> Result<Response<Body>, ApplicationError> {
    let default_host = HeaderValue::from_static("localhost");
    let host = req
        .headers()
        .get("host")
        .unwrap_or(&default_host)
        .to_str()
        .unwrap_or_default();
    let cmd = format!(
        "/debug/pprof/profile?seconds={}",
        PProfArgs::default_seconds()
    );
    Ok(Response::new(Body::from(format!(
        r#"<a href="{}">http://{}{}</a>"#,
        cmd, host, cmd
    ))))
}

async fn dump_rsprof(seconds: u64, frequency: i32) -> pprof::Result<pprof::Report> {
    let guard = pprof::ProfilerGuard::new(frequency)?;
    info!(
        "start profiling {} seconds with frequency {} /s",
        seconds, frequency
    );

    tokio::time::sleep(Duration::from_secs(seconds)).await;

    info!(
        "done profiling {} seconds with frequency {} /s",
        seconds, frequency
    );
    guard.report().build()
}

#[derive(Debug, Deserialize)]
struct PProfArgs {
    #[serde(default = "PProfArgs::default_seconds")]
    seconds: u64,
    #[serde(default = "PProfArgs::default_frequency")]
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

#[tracing::instrument(level = "debug")]
async fn pprof_profile<M: ConnectionManager + Send + Sync + Debug + 'static>(
    req: Request<Body>,
) -> Result<Response<Body>, ApplicationError> {
    let query_string = req.uri().query().unwrap_or_default();
    let query: PProfArgs =
        serde_urlencoded::from_str(query_string).context(InvalidQueryString { query_string })?;

    let report = dump_rsprof(query.seconds, query.frequency.get())
        .await
        .context(PProf)?;

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
        report.flamegraph(&mut body).context(PProf)?;
        if body.is_empty() {
            return EmptyFlamegraph.fail();
        }
    } else {
        let profile = report.pprof().context(PProf)?;
        profile.encode(&mut body).context(Prost)?;
    }

    Ok(Response::new(Body::from(body)))
}

pub async fn serve<M>(
    addr: AddrIncoming,
    application: Arc<ApplicationState>,
    server: Arc<AppServer<M>>,
    shutdown: CancellationToken,
    max_request_size: usize,
) -> Result<(), hyper::Error>
where
    M: ConnectionManager + Send + Sync + Debug + 'static,
{
    let router = router(application, server, max_request_size);
    let service = RouterService::new(router).unwrap();

    hyper::Server::builder(addr)
        .serve(service)
        .with_graceful_shutdown(shutdown.cancelled())
        .await
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{
        convert::TryFrom,
        net::{IpAddr, Ipv4Addr, SocketAddr},
    };

    use arrow::record_batch::RecordBatch;
    use arrow_util::assert_batches_eq;
    use reqwest::{Client, Response};

    use data_types::{database_rules::DatabaseRules, server_id::ServerId, DatabaseName};
    use metrics::TestMetricRegistry;
    use object_store::ObjectStore;
    use serde::de::DeserializeOwned;
    use server::{db::Db, ApplicationState, ConnectionManagerImpl};
    use tokio_stream::wrappers::ReceiverStream;

    fn make_application() -> Arc<ApplicationState> {
        Arc::new(ApplicationState::new(
            Arc::new(ObjectStore::new_in_memory()),
            None,
        ))
    }

    fn make_server(application: Arc<ApplicationState>) -> Arc<AppServer<ConnectionManagerImpl>> {
        Arc::new(AppServer::new(
            ConnectionManagerImpl::new(),
            application,
            Default::default(),
        ))
    }

    #[tokio::test]
    async fn test_health() {
        let application = make_application();
        let app_server = make_server(Arc::clone(&application));
        let server_url = test_server(application, Arc::clone(&app_server));

        let client = Client::new();
        let response = client.get(&format!("{}/health", server_url)).send().await;

        // Print the response so if the test fails, we have a log of what went wrong
        check_response("health", response, StatusCode::OK, Some("OK")).await;
    }

    #[tokio::test]
    async fn test_write() {
        let application = make_application();
        let app_server = make_server(Arc::clone(&application));
        app_server.set_id(ServerId::try_from(1).unwrap()).unwrap();
        app_server.maybe_initialize_server().await;
        app_server
            .create_database(DatabaseRules::new(
                DatabaseName::new("MyOrg_MyBucket").unwrap(),
            ))
            .await
            .unwrap();
        let server_url = test_server(application, Arc::clone(&app_server));

        let client = Client::new();

        let lp_data = "h2o_temperature,location=santa_monica,state=CA surface_degrees=65.2,bottom_degrees=50.4 1617286224000000000";

        // send write data
        let bucket_name = "MyBucket";
        let org_name = "MyOrg";
        let response = client
            .post(&format!(
                "{}/api/v2/write?bucket={}&org={}",
                server_url, bucket_name, org_name
            ))
            .body(lp_data)
            .send()
            .await;

        check_response("write", response, StatusCode::NO_CONTENT, Some("")).await;

        // Check that the data got into the right bucket
        let test_db = app_server
            .db(&DatabaseName::new("MyOrg_MyBucket").unwrap())
            .expect("Database exists");

        let batches = run_query(test_db, "select * from h2o_temperature").await;
        let expected = vec![
            "+----------------+--------------+-------+-----------------+----------------------+",
            "| bottom_degrees | location     | state | surface_degrees | time                 |",
            "+----------------+--------------+-------+-----------------+----------------------+",
            "| 50.4           | santa_monica | CA    | 65.2            | 2021-04-01T14:10:24Z |",
            "+----------------+--------------+-------+-----------------+----------------------+",
        ];
        assert_batches_eq!(expected, &batches);
    }

    #[tokio::test]
    async fn test_write_metrics() {
        let application = make_application();
        let app_server = make_server(Arc::clone(&application));
        let metric_registry = TestMetricRegistry::new(Arc::clone(application.metric_registry()));

        app_server.set_id(ServerId::try_from(1).unwrap()).unwrap();
        app_server.maybe_initialize_server().await;
        app_server
            .create_database(DatabaseRules::new(
                DatabaseName::new("MetricsOrg_MetricsBucket").unwrap(),
            ))
            .await
            .unwrap();

        let server_url = test_server(application, Arc::clone(&app_server));

        let client = Client::new();

        let lp_data = "h2o_temperature,location=santa_monica,state=CA surface_degrees=65.2,bottom_degrees=50.4 1568756160";

        // send good data
        let org_name = "MetricsOrg";
        let bucket_name = "MetricsBucket";
        client
            .post(&format!(
                "{}/api/v2/write?bucket={}&org={}",
                server_url, bucket_name, org_name
            ))
            .body(lp_data)
            .send()
            .await
            .expect("sent data");

        // The request completed successfully
        metric_registry
            .has_metric_family("http_request_duration_seconds")
            .with_labels(&[
                ("bucket", "MetricsBucket"),
                ("org", "MetricsOrg"),
                ("path", "/api/v2/write"),
                ("status", "ok"),
            ])
            .histogram()
            .sample_count_eq(1)
            .unwrap();

        // A single successful point landed
        metric_registry
            .has_metric_family("ingest_points_total")
            .with_labels(&[("db_name", "MetricsOrg_MetricsBucket"), ("status", "ok")])
            .counter()
            .eq(1.0)
            .unwrap();

        // Which consists of two fields
        metric_registry
            .has_metric_family("ingest_fields_total")
            .with_labels(&[("db_name", "MetricsOrg_MetricsBucket"), ("status", "ok")])
            .counter()
            .eq(2.0)
            .unwrap();

        // Bytes of data were written
        metric_registry
            .has_metric_family("ingest_points_bytes_total")
            .with_labels(&[("status", "ok"), ("db_name", "MetricsOrg_MetricsBucket")])
            .counter()
            .eq(98.0)
            .unwrap();

        // Generate an error
        client
            .post(&format!(
                "{}/api/v2/write?bucket=NotMyBucket&org=NotMyOrg",
                server_url,
            ))
            .body(lp_data)
            .send()
            .await
            .unwrap();

        // A single point was rejected
        metric_registry
            .has_metric_family("ingest_points_total")
            .with_labels(&[("db_name", "NotMyOrg_NotMyBucket"), ("status", "error")])
            .counter()
            .eq(1.0)
            .unwrap();
    }

    /// Sets up a test database with some data for testing the query endpoint
    /// returns a client for communicating with the server, and the server
    /// endpoint
    async fn setup_test_data() -> (Client, String) {
        let application = make_application();
        let app_server = make_server(Arc::clone(&application));
        app_server.set_id(ServerId::try_from(1).unwrap()).unwrap();
        app_server.maybe_initialize_server().await;
        app_server
            .create_database(DatabaseRules::new(
                DatabaseName::new("MyOrg_MyBucket").unwrap(),
            ))
            .await
            .unwrap();
        let server_url = test_server(application, Arc::clone(&app_server));

        let client = Client::new();

        let lp_data = "h2o_temperature,location=santa_monica,state=CA surface_degrees=65.2,bottom_degrees=50.4 1617286224000000000";

        // send write data
        let bucket_name = "MyBucket";
        let org_name = "MyOrg";
        let response = client
            .post(&format!(
                "{}/api/v2/write?bucket={}&org={}",
                server_url, bucket_name, org_name
            ))
            .body(lp_data)
            .send()
            .await;

        check_response("write", response, StatusCode::NO_CONTENT, Some("")).await;
        (client, server_url)
    }

    #[tokio::test]
    async fn test_query_pretty() {
        let (client, server_url) = setup_test_data().await;

        // send query data
        let response = client
            .get(&format!(
                "{}/iox/api/v1/databases/MyOrg_MyBucket/query?q={}",
                server_url, "select%20*%20from%20h2o_temperature"
            ))
            .send()
            .await;

        assert_eq!(get_content_type(&response), "text/plain");

        let expected = r#"+----------------+--------------+-------+-----------------+----------------------+
| bottom_degrees | location     | state | surface_degrees | time                 |
+----------------+--------------+-------+-----------------+----------------------+
| 50.4           | santa_monica | CA    | 65.2            | 2021-04-01T14:10:24Z |
+----------------+--------------+-------+-----------------+----------------------+
"#;

        check_response("query", response, StatusCode::OK, Some(expected)).await;

        // same response is expected if we explicitly request 'format=pretty'
        let response = client
            .get(&format!(
                "{}/iox/api/v1/databases/MyOrg_MyBucket/query?q={}&format=pretty",
                server_url, "select%20*%20from%20h2o_temperature"
            ))
            .send()
            .await;
        assert_eq!(get_content_type(&response), "text/plain");

        check_response("query", response, StatusCode::OK, Some(expected)).await;
    }

    #[tokio::test]
    async fn test_query_csv() {
        let (client, server_url) = setup_test_data().await;

        // send query data
        let response = client
            .get(&format!(
                "{}/iox/api/v1/databases/MyOrg_MyBucket/query?q={}&format=csv",
                server_url, "select%20*%20from%20h2o_temperature"
            ))
            .send()
            .await;

        assert_eq!(get_content_type(&response), "text/csv");

        let res = "bottom_degrees,location,state,surface_degrees,time\n\
                   50.4,santa_monica,CA,65.2,2021-04-01T14:10:24.000000000\n";
        check_response("query", response, StatusCode::OK, Some(res)).await;
    }

    #[tokio::test]
    async fn test_query_json() {
        let (client, server_url) = setup_test_data().await;

        // send a second line of data to demonstrate how that works
        let lp_data =
            "h2o_temperature,location=Boston,state=MA surface_degrees=50.2 1617286224000000000";

        // send write data
        let bucket_name = "MyBucket";
        let org_name = "MyOrg";
        let response = client
            .post(&format!(
                "{}/api/v2/write?bucket={}&org={}",
                server_url, bucket_name, org_name
            ))
            .body(lp_data)
            .send()
            .await;

        check_response("write", response, StatusCode::NO_CONTENT, Some("")).await;

        // send query data
        let response = client
            .get(&format!(
                "{}/iox/api/v1/databases/MyOrg_MyBucket/query?q={}&format=json",
                server_url, "select%20*%20from%20h2o_temperature"
            ))
            .send()
            .await;

        assert_eq!(get_content_type(&response), "application/json");

        // Note two json records: one record on each line
        let res = r#"[{"location":"Boston","state":"MA","surface_degrees":50.2,"time":"2021-04-01 14:10:24"},{"bottom_degrees":50.4,"location":"santa_monica","state":"CA","surface_degrees":65.2,"time":"2021-04-01 14:10:24"}]"#;
        check_response("query", response, StatusCode::OK, Some(res)).await;
    }

    fn gzip_str(s: &str) -> Vec<u8> {
        use flate2::{write::GzEncoder, Compression};
        use std::io::Write;
        let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
        write!(encoder, "{}", s).expect("writing into encoder");
        encoder.finish().expect("successfully encoding gzip data")
    }

    #[tokio::test]
    async fn test_gzip_write() {
        let (app_server, server_url) = setup_server().await;

        let client = Client::new();
        let lp_data = "h2o_temperature,location=santa_monica,state=CA surface_degrees=65.2,bottom_degrees=50.4 1617286224000000000";

        // send write data encoded with gzip
        let bucket_name = "MyBucket";
        let org_name = "MyOrg";
        let response = client
            .post(&format!(
                "{}/api/v2/write?bucket={}&org={}",
                server_url, bucket_name, org_name
            ))
            .header(CONTENT_ENCODING, "gzip")
            .body(gzip_str(lp_data))
            .send()
            .await;

        check_response("gzip_write", response, StatusCode::NO_CONTENT, Some("")).await;

        // Check that the data got into the right bucket
        let test_db = app_server
            .db(&DatabaseName::new("MyOrg_MyBucket").unwrap())
            .expect("Database exists");

        let batches = run_query(test_db, "select * from h2o_temperature").await;

        let expected = vec![
            "+----------------+--------------+-------+-----------------+----------------------+",
            "| bottom_degrees | location     | state | surface_degrees | time                 |",
            "+----------------+--------------+-------+-----------------+----------------------+",
            "| 50.4           | santa_monica | CA    | 65.2            | 2021-04-01T14:10:24Z |",
            "+----------------+--------------+-------+-----------------+----------------------+",
        ];
        assert_batches_eq!(expected, &batches);
    }

    #[tokio::test]
    async fn write_to_invalid_database() {
        let (_, server_url) = setup_server().await;

        let client = Client::new();

        let bucket_name = "NotMyBucket";
        let org_name = "MyOrg";
        let response = client
            .post(&format!(
                "{}/api/v2/write?bucket={}&org={}",
                server_url, bucket_name, org_name
            ))
            .send()
            .await;

        check_response(
            "write_to_invalid_databases",
            response,
            StatusCode::NOT_FOUND,
            Some(""),
        )
        .await;
    }

    const TEST_MAX_REQUEST_SIZE: usize = 1024 * 1024;

    #[tokio::test]
    async fn client_hangup_during_parse() {
        #[derive(Debug, Snafu)]
        enum TestError {
            #[snafu(display("Blarg Error"))]
            Blarg {},
        }

        let (tx, rx) = tokio::sync::mpsc::channel(2);
        let body = Body::wrap_stream(ReceiverStream::new(rx));

        tx.send(Ok("foo")).await.unwrap();
        tx.send(Err(TestError::Blarg {})).await.unwrap();

        let request = Request::builder()
            .uri("https://ye-olde-non-existent-server/")
            .body(body)
            .unwrap();

        let parse_result = parse_body(request, TEST_MAX_REQUEST_SIZE)
            .await
            .unwrap_err();
        assert_eq!(
            parse_result.to_string(),
            "Client hung up while sending body: error reading a body from connection: Blarg Error"
        );
    }

    fn get_content_type(response: &Result<Response, reqwest::Error>) -> String {
        if let Ok(response) = response {
            response
                .headers()
                .get(CONTENT_TYPE)
                .map(|v| v.to_str().unwrap())
                .unwrap_or("")
                .to_string()
        } else {
            "".to_string()
        }
    }

    /// checks a http response against expected results
    async fn check_response(
        description: &str,
        response: Result<Response, reqwest::Error>,
        expected_status: StatusCode,
        expected_body: Option<&str>,
    ) {
        // Print the response so if the test fails, we have a log of
        // what went wrong
        println!("{} response: {:?}", description, response);

        if let Ok(response) = response {
            let status = response.status();
            let body = response
                .text()
                .await
                .expect("Converting request body to string");

            assert_eq!(status, expected_status);
            if let Some(expected_body) = expected_body {
                assert_eq!(body, expected_body);
            }
        } else {
            panic!("Unexpected error response: {:?}", response);
        }
    }

    #[allow(dead_code)]
    async fn check_json_response<T: DeserializeOwned + Eq + Debug>(
        client: &Client,
        url: &str,
        expected_status: StatusCode,
    ) -> T {
        let response = client.get(url).send().await;

        // Print the response so if the test fails, we have a log of
        // what went wrong
        println!("{} response: {:?}", url, response);

        if let Ok(response) = response {
            let status = response.status();
            let body: T = response
                .json()
                .await
                .expect("Converting request body to string");

            assert_eq!(status, expected_status);
            body
        } else {
            panic!("Unexpected error response: {:?}", response);
        }
    }

    /// creates an instance of the http service backed by a in-memory
    /// testable database.  Returns the url of the server
    fn test_server(
        application: Arc<ApplicationState>,
        server: Arc<AppServer<ConnectionManagerImpl>>,
    ) -> String {
        // NB: specify port 0 to let the OS pick the port.
        let bind_addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 0);
        let addr = AddrIncoming::bind(&bind_addr).expect("failed to bind server");
        let server_url = format!("http://{}", addr.local_addr());

        tokio::task::spawn(serve(
            addr,
            application,
            server,
            CancellationToken::new(),
            TEST_MAX_REQUEST_SIZE,
        ));
        println!("Started server at {}", server_url);
        server_url
    }

    /// Run the specified SQL query and return formatted results as a string
    async fn run_query(db: Arc<Db>, query: &str) -> Vec<RecordBatch> {
        let executor = db.executor();
        let physical_plan = Planner::new(Arc::clone(&executor))
            .sql(db, query)
            .await
            .unwrap();

        executor
            .collect(physical_plan, ExecutorType::Query)
            .await
            .unwrap()
    }

    /// return a test server and the url to contact it for `MyOrg_MyBucket`
    async fn setup_server() -> (Arc<AppServer<ConnectionManagerImpl>>, String) {
        let application = make_application();
        let app_server = make_server(Arc::clone(&application));
        app_server.set_id(ServerId::try_from(1).unwrap()).unwrap();
        app_server.maybe_initialize_server().await;
        app_server
            .create_database(DatabaseRules::new(
                DatabaseName::new("MyOrg_MyBucket").unwrap(),
            ))
            .await
            .unwrap();
        let server_url = test_server(application, Arc::clone(&app_server));

        (app_server, server_url)
    }
}
