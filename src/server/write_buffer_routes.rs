//! This module contains a parallel implementation of the /v2 HTTP api
//! routes for Delorean.
//!
//! The goal is that eventually the implementation in these routes
//! will replace the implementation in http_routes.rs

#![deny(rust_2018_idioms)]

use tracing::{debug, error, info};

use delorean::storage::write_buffer_database::{Error as DatabaseError, WriteBufferDatabases};
use delorean_line_parser::parse_lines;

use bytes::BytesMut;
use futures::{self, StreamExt};
use hyper::{Body, Method, StatusCode};
use serde::Deserialize;
use snafu::{OptionExt, ResultExt, Snafu};
use std::str;
use std::sync::Arc;

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
        source: DatabaseError,
    },

    #[snafu(display(
        "Internal error writing points into org {}, bucket {}:  {}",
        org,
        bucket_name,
        source
    ))]
    WritingPoints {
        org: String,
        bucket_name: String,
        source: DatabaseError,
    },

    #[snafu(display(
        "Internal error reading points from database {}:  {}",
        database,
        source
    ))]
    Query {
        database: String,
        source: DatabaseError,
    },

    // Application level errors
    #[snafu(display("Bucket {} not found in org {}", bucket, org))]
    BucketNotFound { org: String, bucket: String },

    #[snafu(display("Body exceeds limit of {} bytes", max_body_size))]
    RequestSizeExceeded { max_body_size: usize },

    #[snafu(display("Expected query string in request, but none was provided"))]
    ExpectedQueryString {},

    #[snafu(display("Invalid query string '{}': {}", query_string, source))]
    InvalidQueryString {
        query_string: String,
        source: serde_urlencoded::de::Error,
    },

    #[snafu(display("Query error: {}", source))]
    QueryError { source: DatabaseError },

    #[snafu(display("Invalid request body '{}': {}", request_body, source))]
    InvalidRequestBody {
        request_body: String,
        source: serde_json::error::Error,
    },

    #[snafu(display("Error reading request body: {}", source))]
    ReadingBody { source: hyper::error::Error },

    #[snafu(display("Error reading request body as utf8: {}", source))]
    ReadingBodyAsUtf8 { source: std::str::Utf8Error },

    #[snafu(display("Error parsing line protocol: {}", source))]
    ParsingLineProtocol { source: delorean_line_parser::Error },

    #[snafu(display("No handler for {:?} {}", method, path))]
    RouteNotFound { method: Method, path: String },
}

impl ApplicationError {
    pub fn status_code(&self) -> StatusCode {
        match self {
            Self::BucketByName { .. } => StatusCode::INTERNAL_SERVER_ERROR,
            Self::WritingPoints { .. } => StatusCode::INTERNAL_SERVER_ERROR,
            Self::Query { .. } => StatusCode::INTERNAL_SERVER_ERROR,
            Self::QueryError { .. } => StatusCode::BAD_REQUEST,
            Self::BucketNotFound { .. } => StatusCode::NOT_FOUND,
            Self::RequestSizeExceeded { .. } => StatusCode::BAD_REQUEST,
            Self::ExpectedQueryString { .. } => StatusCode::BAD_REQUEST,
            Self::InvalidQueryString { .. } => StatusCode::BAD_REQUEST,
            Self::InvalidRequestBody { .. } => StatusCode::BAD_REQUEST,
            Self::ReadingBody { .. } => StatusCode::BAD_REQUEST,
            Self::ReadingBodyAsUtf8 { .. } => StatusCode::BAD_REQUEST,
            Self::ParsingLineProtocol { .. } => StatusCode::BAD_REQUEST,
            Self::RouteNotFound { .. } => StatusCode::NOT_FOUND,
        }
    }
}

const MAX_SIZE: usize = 10_485_760; // max write request size of 10MB

#[derive(Debug, Deserialize)]
/// Body of the request to the /write endpoint
struct WriteInfo {
    org: String,
    bucket: String,
}

#[tracing::instrument(level = "debug")]
async fn write(
    req: hyper::Request<Body>,
    storage: Arc<WriteBufferDatabases>,
) -> Result<Option<Body>, ApplicationError> {
    let query = req.uri().query().context(ExpectedQueryString)?;

    let write_info: WriteInfo = serde_urlencoded::from_str(query).context(InvalidQueryString {
        query_string: String::from(query),
    })?;

    let db = storage
        .db_or_create(&write_info.org, &write_info.bucket)
        .await
        .context(BucketByName {
            org: write_info.org.clone(),
            bucket_name: write_info.bucket.clone(),
        })?;

    let mut payload = req.into_body();

    let mut body = BytesMut::new();
    while let Some(chunk) = payload.next().await {
        let chunk = chunk.expect("Should have been able to read the next chunk");
        // limit max size of in-memory payload
        if (body.len() + chunk.len()) > MAX_SIZE {
            return Err(ApplicationError::RequestSizeExceeded {
                max_body_size: MAX_SIZE,
            });
        }
        body.extend_from_slice(&chunk);
    }
    let body = body.freeze();
    let body = str::from_utf8(&body).unwrap();

    let lines: Vec<_> = parse_lines(body)
        .collect::<Result<Vec<_>, delorean_line_parser::Error>>()
        .context(ParsingLineProtocol)?;

    debug!("Parsed {} lines", lines.len());

    db.write_lines(&lines).await.context(WritingPoints {
        org: write_info.org.clone(),
        bucket_name: write_info.bucket.clone(),
    })?;

    Ok(None)
}

#[derive(Deserialize, Debug)]
/// Body of the request to the /read endpoint
struct ReadInfo {
    org: String,
    bucket: String,
    // TODL This is currently a "SQL" request -- should be updated to conform
    // to the V2 API for reading (using timestamps, etc).
    query: String,
}

// TODO: figure out how to stream read results out rather than rendering the whole thing in mem
#[tracing::instrument(level = "debug")]
async fn read(
    req: hyper::Request<Body>,
    storage: Arc<WriteBufferDatabases>,
) -> Result<Option<Body>, ApplicationError> {
    let query = req.uri().query().context(ExpectedQueryString {})?;

    let read_info: ReadInfo = serde_urlencoded::from_str(query).context(InvalidQueryString {
        query_string: query,
    })?;

    let db = storage
        .db(&read_info.org, &read_info.bucket)
        .await
        .context(BucketNotFound {
            org: read_info.org.clone(),
            bucket: read_info.bucket.clone(),
        })?;

    let results = db.query(&read_info.query).await.context(QueryError {})?;
    let results = arrow::util::pretty::pretty_format_batches(&results).unwrap();

    Ok(Some(results.into_bytes().into()))
}

// Route to test that the server is alive
#[tracing::instrument(level = "debug")]
async fn ping(req: hyper::Request<Body>) -> Result<Option<Body>, ApplicationError> {
    let response_body = "PONG";
    Ok(Some(response_body.into()))
}

fn no_op(name: &str) -> Result<Option<Body>, ApplicationError> {
    info!("NOOP: {}", name);
    Ok(None)
}

pub async fn service(
    req: hyper::Request<Body>,
    storage: Arc<WriteBufferDatabases>,
) -> http::Result<hyper::Response<Body>> {
    let method = req.method().clone();
    let uri = req.uri().clone();

    let response = match (req.method(), req.uri().path()) {
        (&Method::POST, "/api/v2/write") => write(req, storage).await,
        (&Method::POST, "/api/v2/buckets") => no_op("create bucket"),
        (&Method::GET, "/ping") => ping(req).await,
        (&Method::GET, "/api/v2/read") => read(req, storage).await,
        _ => Err(ApplicationError::RouteNotFound {
            method: method.clone(),
            path: uri.to_string(),
        }),
    };

    let result = match response {
        Ok(Some(body)) => hyper::Response::builder()
            .body(body)
            .expect("Should have been able to construct a response"),
        Ok(None) => hyper::Response::builder()
            .status(StatusCode::NO_CONTENT)
            .body(Body::empty())
            .expect("Should have been able to construct a response"),
        Err(e) => {
            error!(error = ?e, method = ?method, uri = ?uri, "Error while handing request");
            let json = serde_json::json!({"error": e.to_string()}).to_string();
            hyper::Response::builder()
                .status(e.status_code())
                .body(json.into())
                .expect("Should have been able to construct a response")
        }
    };
    info!(method = ?method, uri = ?uri, status = ?result.status(), "Handled request");
    Ok(result)
}
