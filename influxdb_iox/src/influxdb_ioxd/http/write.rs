use std::sync::Arc;

use async_trait::async_trait;
use chrono::Utc;
use data_types::{
    names::{org_and_bucket_to_database, OrgBucketMappingError},
    DatabaseName,
};
use hyper::{Body, Method, Request, Response, StatusCode};
use mutable_batch::{DbWrite, WriteMeta};
use observability_deps::tracing::debug;
use serde::Deserialize;
use snafu::{OptionExt, ResultExt, Snafu};

use crate::influxdb_ioxd::{
    http::utils::parse_body,
    server_type::{ApiErrorCode, RouteError, ServerType},
};

use super::metrics::LineProtocolMetrics;

#[allow(clippy::large_enum_variant)]
#[derive(Debug, Snafu)]
pub enum HttpWriteError {
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

    #[snafu(display("Expected query string in request, but none was provided"))]
    ExpectedQueryString {},

    /// Error for when we could not parse the http query uri (e.g.
    /// `?foo=bar&bar=baz)`
    #[snafu(display("Invalid query string in HTTP URI '{}': {}", query_string, source))]
    InvalidQueryString {
        query_string: String,
        source: serde_urlencoded::de::Error,
    },

    #[snafu(display("Error reading request body as utf8: {}", source))]
    ReadingBodyAsUtf8 { source: std::str::Utf8Error },

    #[snafu(display("Error parsing line protocol: {}", source))]
    ParsingLineProtocol { source: mutable_batch_lp::Error },

    #[snafu(display("Database {} not found", db_name))]
    DatabaseNotFound { db_name: String },

    #[snafu(display("Cannot parse body: {}", source))]
    ParseBody {
        source: crate::influxdb_ioxd::http::utils::ParseBodyError,
    },
}

impl RouteError for HttpWriteError {
    fn response(&self) -> Response<Body> {
        match self {
            Self::BucketMappingError { .. } => self.internal_error(),
            Self::WritingPoints { .. } => self.internal_error(),
            Self::ExpectedQueryString { .. } => self.bad_request(),
            Self::InvalidQueryString { .. } => self.bad_request(),
            Self::ReadingBodyAsUtf8 { .. } => self.bad_request(),
            Self::ParsingLineProtocol { .. } => self.bad_request(),
            Self::DatabaseNotFound { .. } => self.not_found(),
            Self::ParseBody { source } => source.response(),
        }
    }

    /// Map the error type into an API error code.
    fn api_error_code(&self) -> u32 {
        match self {
            Self::DatabaseNotFound { .. } => ApiErrorCode::DB_NOT_FOUND.into(),
            Self::ParseBody { source } => source.api_error_code(),
            // A "catch all" error code
            _ => ApiErrorCode::UNKNOWN.into(),
        }
    }
}

/// Write error when calling the underlying server type.
#[derive(Debug, Snafu)]
pub enum InnerWriteError {
    #[snafu(display("Database {} not found", db_name))]
    NotFound { db_name: String },

    #[snafu(display("Error while writing: {}", source))]
    OtherError {
        source: Box<dyn std::error::Error + Send + Sync>,
    },
}

/// Contains a request or a response.
///
/// This is used to be able to consume a reqest and transform it into a response if routing was successfull.
pub enum RequestOrResponse {
    /// Request still there, wasn't routed.
    Request(Request<Body>),

    /// Request was consumed and transformed into a response object. Routing was successfull.
    Response(Response<Body>),
}

#[async_trait]
pub trait HttpDrivenWrite: ServerType {
    /// Routes HTTP write requests.
    ///
    /// Returns `RequestOrResponse::Response` if the request routed, otherwise the route does not match.
    async fn route_write_http_request(
        &self,
        req: Request<Body>,
    ) -> Result<RequestOrResponse, HttpWriteError> {
        if (req.method() != Method::POST) || (req.uri().path() != "/api/v2/write") {
            return Ok(RequestOrResponse::Request(req));
        }

        let span_ctx = req.extensions().get().cloned();

        let max_request_size = self.max_request_size();
        let lp_metrics = self.lp_metrics();

        let query = req.uri().query().context(ExpectedQueryString)?;

        let write_info: WriteInfo =
            serde_urlencoded::from_str(query).context(InvalidQueryString {
                query_string: String::from(query),
            })?;

        let db_name = org_and_bucket_to_database(&write_info.org, &write_info.bucket)
            .context(BucketMappingError)?;

        let body = parse_body(req, max_request_size).await.context(ParseBody)?;

        let body = std::str::from_utf8(&body).context(ReadingBodyAsUtf8)?;

        // The time, in nanoseconds since the epoch, to assign to any points that don't
        // contain a timestamp
        let default_time = Utc::now().timestamp_nanos();

        let (tables, stats) = match mutable_batch_lp::lines_to_batches_stats(body, default_time) {
            Ok(x) => x,
            Err(mutable_batch_lp::Error::EmptyPayload) => {
                debug!("nothing to write");
                return Ok(RequestOrResponse::Response(
                    Response::builder()
                        .status(StatusCode::NO_CONTENT)
                        .body(Body::empty())
                        .unwrap(),
                ));
            }
            Err(source) => return Err(HttpWriteError::ParsingLineProtocol { source }),
        };

        debug!(
            num_lines=stats.num_lines,
            num_fields=stats.num_fields,
            body_size=body.len(),
            %db_name,
            org=%write_info.org,
            bucket=%write_info.bucket,
            "inserting lines into database",
        );

        let write = DbWrite::new(tables, WriteMeta::unsequenced(span_ctx));

        match self.write(&db_name, write).await {
            Ok(_) => {
                lp_metrics.record_write(
                    &db_name,
                    stats.num_lines,
                    stats.num_fields,
                    body.len(),
                    true,
                );
                Ok(RequestOrResponse::Response(
                    Response::builder()
                        .status(StatusCode::NO_CONTENT)
                        .body(Body::empty())
                        .unwrap(),
                ))
            }
            Err(InnerWriteError::NotFound { .. }) => {
                debug!(%db_name, ?stats, "database not found");
                // Purposefully do not record ingest metrics
                Err(HttpWriteError::DatabaseNotFound {
                    db_name: db_name.to_string(),
                })
            }
            Err(InnerWriteError::OtherError { source }) => {
                debug!(e=%source, %db_name, ?stats, "error writing lines");
                lp_metrics.record_write(
                    &db_name,
                    stats.num_lines,
                    stats.num_fields,
                    body.len(),
                    false,
                );
                Err(HttpWriteError::WritingPoints {
                    org: write_info.org.clone(),
                    bucket_name: write_info.bucket.clone(),
                    source,
                })
            }
        }
    }

    /// Max request size.
    fn max_request_size(&self) -> usize;

    /// Line protocol metrics.
    fn lp_metrics(&self) -> Arc<LineProtocolMetrics>;

    /// Perform write.
    async fn write(
        &self,
        db_name: &DatabaseName<'_>,
        write: DbWrite,
    ) -> Result<(), InnerWriteError>;
}

#[derive(Debug, Deserialize)]
/// Body of the request to the /write endpoint
pub struct WriteInfo {
    pub org: String,
    pub bucket: String,
}
